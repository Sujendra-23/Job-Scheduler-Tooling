// Command devices runs the device-config registry against simulated device
// workers (single-host simulation: in-process devices behind a lossy channel).
//
//	devices -addr :8090 -devices 5 -drop-ack-rate 0.3     # serve HTTP status/config API
//	devices -demo -devices 6 -drop-ack-rate 0.4            # scripted run, prints final table, exits
//
// Status is also written as telemetry events (-out) so the tracer can show it:
//
//	tracer -log devices.jsonl -devices
//	tracer -log devices.jsonl -job device:cam-01
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"scheduler/internal/devices"
	"scheduler/internal/telemetry"
)

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address (serve mode)")
	n := flag.Int("devices", 5, "number of simulated devices (cam-01, cam-02, ...)")
	dropPush := flag.Float64("drop-push-rate", 0, "probability a push is lost on the wire")
	dropAck := flag.Float64("drop-ack-rate", 0, "probability an ack is lost on the way back")
	seed := flag.Int64("seed", 1, "fault-injection seed")
	ackTimeout := flag.Duration("ack-timeout", 300*time.Millisecond, "per-attempt wait for an ack")
	maxAttempts := flag.Int("max-attempts", 5, "pushes per config version before marking the device failed")
	backoff := flag.Duration("backoff", 50*time.Millisecond, "base retry backoff (doubles each attempt)")
	out := flag.String("out", "devices.jsonl", "telemetry JSONL path")
	demo := flag.Bool("demo", false, "run a scripted scenario, print the result table, and exit")
	flag.Parse()

	rec, err := telemetry.NewRecorder(*out, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Close()

	net := devices.NewSimNetwork()
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(*seed))
	roll := func(p float64) bool {
		if p <= 0 {
			return false
		}
		rngMu.Lock()
		defer rngMu.Unlock()
		return rng.Float64() < p
	}
	net.DropPush = func(string, int64, int) bool { return roll(*dropPush) }
	net.DropAck = func(string, int64, int) bool { return roll(*dropAck) }
	net.Latency = 5 * time.Millisecond

	reg := devices.NewRegistry(net, rec, devices.Options{AckTimeout: *ackTimeout, MaxAttempts: *maxAttempts, Backoff: *backoff})
	defer reg.Close()
	ids := make([]string, *n)
	for i := range ids {
		ids[i] = fmt.Sprintf("cam-%02d", i+1)
		net.AddDevice(ids[i])
		if err := reg.Register(ids[i]); err != nil {
			log.Fatal(err)
		}
	}

	if *demo {
		runDemo(reg, net, ids)
		fmt.Printf("event log: %s (try: tracer -log %s -devices)\n", *out, *out)
		return
	}
	log.Printf("devices: simulation of %d devices (single host), API on %s, drop-push=%.2f drop-ack=%.2f", *n, *addr, *dropPush, *dropAck)
	log.Fatal(http.ListenAndServe(*addr, devices.Handler(reg)))
}

func runDemo(reg *devices.Registry, net *devices.SimNetwork, ids []string) {
	for _, id := range ids {
		h := fnv.New32a()
		h.Write([]byte(id))
		reg.SetConfig(id, map[string]string{"fps": "30", "profile": fmt.Sprint(h.Sum32() % 3)})
	}
	time.Sleep(100 * time.Millisecond)
	for _, id := range ids[:len(ids)/2] { // push a newer config to half the fleet mid-flight
		reg.SetConfig(id, map[string]string{"fps": "60", "night_mode": "on"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := reg.WaitSettled(ctx); err != nil {
		log.Fatalf("demo did not settle: %v", err)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "DEVICE\tDESIRED\tACKED\tSTATE\tATTEMPTS\tDEVICE-RUNS\tAPPLIED-COUNT\tPUSHES-SEEN")
	for _, s := range reg.List() {
		ver, applies, recv := net.Device(s.ID).Snapshot()
		fmt.Fprintf(tw, "%s\tv%d\tv%d\t%s\tv%d:%d\tv%d\t%d\t%d\n", s.ID, s.DesiredVersion, s.AckedVersion, s.State, s.DesiredVersion, s.Attempts, ver, applies, recv)
	}
	tw.Flush()
	b, _ := json.Marshal(reg.List()[0])
	fmt.Printf("sample status JSON: %s\n", b)
}
