// Command technicians runs the technician-dispatch simulation: work orders are assigned to
// technicians by skill and travel time, then a scripted day throws a cancellation, a
// running-late report, a technician failure and duplicate completion reports at the plan.
//
//	technicians                                  # haversine travel times, no network
//	technicians -osrm-url http://localhost:5000  # OSRM travel times, haversine if it is down
//
// Events are written as telemetry JSONL (-out) so the tracer can show them:
//
//	tracer -log technicians.jsonl -job WO-104
//
// This is a single-process simulation over fixed Austin-area coordinates, not a live fleet.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"scheduler/internal/technician"
	"scheduler/internal/telemetry"
)

func main() {
	osrmURL := flag.String("osrm-url", os.Getenv("OSRM_URL"), "OSRM base URL for travel times (empty = haversine estimates only)")
	out := flag.String("out", "technicians.jsonl", "telemetry JSONL path")
	flag.Parse()

	rec, err := telemetry.NewRecorder(*out, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Close()

	var router technician.Router = technician.HaversineRouter{}
	if *osrmURL != "" {
		router = technician.FallbackRouter{
			Primary:  technician.OSRMRouter{BaseURL: *osrmURL, Client: nil},
			Fallback: technician.HaversineRouter{},
			OnFallback: func(err error) {
				log.Printf("routing: %v; using haversine estimate", err)
			},
		}
	}
	clock := time.Date(2026, 10, 12, 8, 0, 0, 0, time.Local)
	d := technician.New(technician.Options{Router: router, Sink: rec, Clock: func() time.Time { return clock }})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	runScenario(ctx, d, &clock)
	fmt.Printf("event log: %s (try: tracer -log %s -job WO-104)\n", *out, *out)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func runScenario(ctx context.Context, d *technician.Dispatcher, clock *time.Time) {
	for _, t := range []technician.Technician{
		{ID: "tech-ana", Name: "Ana", Skills: []string{"hvac", "electrical"}, Home: technician.Point{Lat: 30.2672, Lng: -97.7431}, MaxJobs: 4},
		{ID: "tech-ben", Name: "Ben", Skills: []string{"plumbing", "gas"}, Home: technician.Point{Lat: 30.3500, Lng: -97.7200}, MaxJobs: 4},
		{ID: "tech-cy", Name: "Cy", Skills: []string{"hvac", "plumbing"}, Home: technician.Point{Lat: 30.2100, Lng: -97.8000}, MaxJobs: 4},
	} {
		must(d.AddTechnician(ctx, t))
	}
	orders := []technician.WorkOrder{
		{ID: "WO-101", Skills: []string{"hvac"}, Location: technician.Point{Lat: 30.2849, Lng: -97.7341}, Priority: 2},
		{ID: "WO-102", Skills: []string{"plumbing"}, Location: technician.Point{Lat: 30.3300, Lng: -97.7000}, Priority: 2},
		{ID: "WO-103", Skills: []string{"gas"}, Location: technician.Point{Lat: 30.3000, Lng: -97.7600}, Priority: 5},
		{ID: "WO-104", Skills: []string{"hvac"}, Location: technician.Point{Lat: 30.2400, Lng: -97.7900}, Priority: 3},
		{ID: "WO-105", Skills: []string{"electrical"}, Location: technician.Point{Lat: 30.2500, Lng: -97.7500}, Priority: 1},
		{ID: "WO-106", Skills: []string{"roofing"}, Location: technician.Point{Lat: 30.2700, Lng: -97.7300}, Priority: 1}, // nobody qualifies
	}
	for _, o := range orders {
		must(d.Submit(ctx, o))
	}
	printRoutes("initial plan", d)

	_, err := d.Cancel(ctx, "WO-101")
	must(err)
	printRoutes("after WO-101 is cancelled (plan is rebalanced)", d)

	must(d.RunningLate(ctx, "tech-cy", 3*time.Hour))
	printRoutes("after tech-cy reports running 3h late", d)

	_, err = d.ReportFailure(ctx, "WO-103", "tech-ben", "customer not home")
	must(err)
	for i := 0; i < 2; i++ { // two more passes exhaust WO-106's attempts: it is dead-lettered
		must(d.Rebalance(ctx))
	}
	for i := 0; i < 2; i++ { // the same completion delivered twice is applied once
		if v, ok := d.Job("WO-102"); ok && v.Tech != "" {
			dup, err := d.Complete(ctx, "WO-102", v.Tech)
			must(err)
			fmt.Printf("complete WO-102 (delivery %d): duplicate=%v\n", i+1, dup)
		}
	}
	printRoutes("end of scenario", d)
	fmt.Println("\n== job states ==")
	for _, j := range d.Jobs() {
		fmt.Printf("%s\t%s\t%s\n", j.ID, j.State, j.Tech)
	}
	s := d.Stats()
	fmt.Printf("stats: submitted=%d assigned=%d reassigned=%d completed=%d cancelled=%d retried=%d dead_lettered=%d duplicates=%d\n",
		s.Submitted, s.Assigned, s.Reassigned, s.Completed, s.Cancelled, s.Retried, s.DeadLettered, s.Duplicates)
}

func printRoutes(title string, d *technician.Dispatcher) {
	fmt.Printf("\n== %s ==\n", title)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TECHNICIAN\tSTOP\tJOB\tETA\tTRAVEL\tSOURCE")
	for _, r := range d.Routes() {
		if len(r.Stops) == 0 {
			fmt.Fprintf(tw, "%s\t-\t(idle)\t\t\t\n", r.Technician)
		}
		for i, s := range r.Stops {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\n", r.Technician, i+1, s.ID, s.ETA.Format("15:04"), s.Travel.Round(time.Minute), s.Source)
		}
	}
	tw.Flush()
}
