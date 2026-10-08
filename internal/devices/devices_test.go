package devices

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var fast = Options{AckTimeout: 40 * time.Millisecond, MaxAttempts: 4, Backoff: 5 * time.Millisecond}

func setup(t *testing.T, opts Options, ids ...string) (*Registry, *SimNetwork) {
	t.Helper()
	net := NewSimNetwork()
	reg := NewRegistry(net, nil, opts)
	t.Cleanup(reg.Close)
	for _, id := range ids {
		net.AddDevice(id)
		if err := reg.Register(id); err != nil {
			t.Fatal(err)
		}
	}
	return reg, net
}

func settle(t *testing.T, r *Registry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.WaitSettled(ctx); err != nil {
		t.Fatalf("registry never settled: %v (%+v)", err, r.List())
	}
}

func TestPushIsAckedAndDeviceRunsVersion(t *testing.T) {
	reg, net := setup(t, fast, "cam-1")
	v, err := reg.SetConfig("cam-1", map[string]string{"fps": "30"})
	if err != nil || v != 1 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	settle(t, reg)
	s, _ := reg.Status("cam-1")
	if s.State != StateAcked || s.AckedVersion != 1 || s.Attempts != 1 {
		t.Fatalf("status = %+v", s)
	}
	ver, applies, _ := net.Device("cam-1").Snapshot()
	if ver != 1 || applies != 1 || net.Device("cam-1").Config()["fps"] != "30" {
		t.Fatalf("device ver=%d applies=%d", ver, applies)
	}
}

// The ack is lost on the way back. The device DID apply v1, so the registry
// times out, retries, the device ignores the duplicate (applied exactly once)
// but re-acks, and the registry converges.
func TestDroppedAckTriggersRetryAndDeviceAppliesOnce(t *testing.T) {
	reg, net := setup(t, fast, "cam-1")
	net.DropAck = func(id string, v int64, nth int) bool { return nth == 1 }
	reg.SetConfig("cam-1", map[string]string{"fps": "60"})
	settle(t, reg)
	s, _ := reg.Status("cam-1")
	if s.State != StateAcked || s.Attempts != 2 {
		t.Fatalf("want acked after 2 attempts, got %+v", s)
	}
	ver, applies, received := net.Device("cam-1").Snapshot()
	if ver != 1 || applies != 1 || received != 2 {
		t.Fatalf("device ver=%d applies=%d received=%d (want 1,1,2)", ver, applies, received)
	}
}

func TestDroppedPushIsRetried(t *testing.T) {
	reg, net := setup(t, fast, "cam-1")
	net.DropPush = func(id string, v int64, nth int) bool { return nth <= 2 }
	reg.SetConfig("cam-1", map[string]string{"a": "b"})
	settle(t, reg)
	s, _ := reg.Status("cam-1")
	if s.State != StateAcked || s.Attempts != 3 {
		t.Fatalf("want acked on attempt 3, got %+v", s)
	}
	if _, applies, received := net.Device("cam-1").Snapshot(); applies != 1 || received != 1 {
		t.Fatalf("applies=%d received=%d (device should only have seen the 3rd push)", applies, received)
	}
}

func TestRetryIsBoundedThenFailed(t *testing.T) {
	reg, net := setup(t, fast, "cam-1")
	net.DropAck = func(string, int64, int) bool { return true }
	reg.SetConfig("cam-1", map[string]string{"a": "b"})
	settle(t, reg)
	s, _ := reg.Status("cam-1")
	if s.State != StateFailed || s.Attempts != fast.MaxAttempts || s.AckedVersion != 0 {
		t.Fatalf("status = %+v", s)
	}
	_, _, received := net.Device("cam-1").Snapshot()
	if received != fast.MaxAttempts {
		t.Fatalf("device received %d pushes, want exactly %d (bounded)", received, fast.MaxAttempts)
	}
	// Operator retry after the network heals.
	net.DropAck = nil
	reg.Retry("cam-1")
	settle(t, reg)
	if s, _ := reg.Status("cam-1"); s.State != StateAcked || s.AckedVersion != 1 {
		t.Fatalf("after Retry: %+v", s)
	}
}

func TestDeviceIgnoresStaleAndDuplicateVersions(t *testing.T) {
	d := &SimDevice{ID: "d"}
	if a := d.Receive(Push{Version: 3, Config: map[string]string{"k": "v3"}}); !a.Applied || a.Version != 3 {
		t.Fatalf("v3: %+v", a)
	}
	if a := d.Receive(Push{Version: 2, Config: map[string]string{"k": "v2"}}); a.Applied || a.Version != 3 {
		t.Fatalf("stale v2 must be ignored and report v3: %+v", a)
	}
	for i := 0; i < 3; i++ { // idempotent re-delivery of the same version
		if a := d.Receive(Push{Version: 3, Config: map[string]string{"k": "other"}}); a.Applied {
			t.Fatal("duplicate v3 re-applied")
		}
	}
	ver, applies, received := d.Snapshot()
	if ver != 3 || applies != 1 || received != 5 || d.Config()["k"] != "v3" {
		t.Fatalf("ver=%d applies=%d received=%d cfg=%v", ver, applies, received, d.Config())
	}
}

// v1's acks are all lost, then v2 is set. The registry must abandon v1,
// deliver v2, ignore any late v1 ack, and never run v1 again.
func TestNewerConfigSupersedesInFlightPush(t *testing.T) {
	reg, net := setup(t, Options{AckTimeout: 40 * time.Millisecond, MaxAttempts: 10, Backoff: 5 * time.Millisecond}, "cam-1")
	net.DropAck = func(id string, v int64, nth int) bool { return v == 1 }
	reg.SetConfig("cam-1", map[string]string{"gen": "1"})
	time.Sleep(60 * time.Millisecond) // let v1 retry at least once
	v2, _ := reg.SetConfig("cam-1", map[string]string{"gen": "2"})
	settle(t, reg)
	s, _ := reg.Status("cam-1")
	if v2 != 2 || s.State != StateAcked || s.AckedVersion != 2 || s.Attempts != 1 {
		t.Fatalf("status = %+v", s)
	}
	ver, applies, _ := net.Device("cam-1").Snapshot()
	if ver != 2 || applies != 2 || net.Device("cam-1").Config()["gen"] != "2" {
		t.Fatalf("device ver=%d applies=%d cfg=%v", ver, applies, net.Device("cam-1").Config())
	}
}

func TestIndependentVersionsPerDevice(t *testing.T) {
	reg, _ := setup(t, fast, "a", "b")
	reg.SetConfig("a", nil)
	reg.SetConfig("a", nil)
	reg.SetConfig("b", nil)
	settle(t, reg)
	a, _ := reg.Status("a")
	b, _ := reg.Status("b")
	if a.AckedVersion != 2 || b.AckedVersion != 1 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	if _, err := reg.SetConfig("nope", nil); err != ErrUnknownDevice {
		t.Fatalf("err = %v", err)
	}
}

func TestHTTPStatusAndConfigPush(t *testing.T) {
	reg, net := setup(t, fast, "cam-1")
	net.DropAck = func(id string, v int64, nth int) bool { return nth == 1 }
	srv := httptest.NewServer(Handler(reg))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/devices/cam-1/config", strings.NewReader(`{"fps":"24"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("PUT: %v %v", resp, err)
	}
	settle(t, reg)
	resp, _ = http.Get(srv.URL + "/devices/cam-1")
	var s Status
	json.NewDecoder(resp.Body).Decode(&s)
	if s.State != StateAcked || s.AckedVersion != 1 || s.Attempts != 2 || s.Config["fps"] != "24" {
		t.Fatalf("GET status = %+v", s)
	}
	resp, _ = http.Get(srv.URL + "/devices")
	var all []Status
	json.NewDecoder(resp.Body).Decode(&all)
	if len(all) != 1 {
		t.Fatalf("list = %+v", all)
	}
	if resp, _ := http.Get(srv.URL + "/devices/ghost"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown device status %d", resp.StatusCode)
	}
}

type staleAckTransport struct{ ackVersion int64 }

func (s staleAckTransport) Send(ctx context.Context, p Push) (Ack, error) {
	return Ack{DeviceID: p.DeviceID, Version: s.ackVersion, Applied: true}, nil
}

// An ack carrying the wrong (older) version must never mark the desired
// version delivered.
func TestStaleAckIsIgnored(t *testing.T) {
	reg := NewRegistry(staleAckTransport{ackVersion: 1}, nil, fast)
	defer reg.Close()
	reg.Register("d")
	reg.SetConfig("d", nil) // v1: acked fine
	settle(t, reg)
	reg.SetConfig("d", nil) // v2: transport keeps acking v1
	settle(t, reg)
	s, _ := reg.Status("d")
	if s.State != StateFailed || s.AckedVersion != 1 || s.DesiredVersion != 2 {
		t.Fatalf("stale acks must not complete v2: %+v", s)
	}
}
