package technician

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"scheduler/internal/telemetry"
)

var t0 = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

// lineRouter puts the world on a line: Lat is "minutes from the depot", Lng is ignored, and
// travel time is the absolute difference. ETAs are then exact and easy to reason about.
type lineRouter struct {
	mu    sync.Mutex
	calls int
	fail  error
}

func (l *lineRouter) TravelTimes(_ context.Context, origins, dests []Point) (Matrix, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.fail != nil {
		return Matrix{}, l.fail
	}
	m := Matrix{Source: "line", Durations: make([][]time.Duration, len(origins))}
	for i, o := range origins {
		m.Durations[i] = make([]time.Duration, len(dests))
		for j, d := range dests {
			m.Durations[i][j] = time.Duration(math.Abs(o.Lat-d.Lat)) * time.Minute
		}
	}
	return m, nil
}

func (l *lineRouter) Calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type fixture struct {
	d      *Dispatcher
	router *lineRouter
	clock  *time.Time
	events *collector
}

type collector struct {
	mu     sync.Mutex
	events []telemetry.Event
}

func (c *collector) Record(e telemetry.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *collector) types(jobID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.events {
		if e.JobID == jobID {
			out = append(out, e.EventType)
		}
	}
	return out
}

func newFixture(t *testing.T, techs ...Technician) *fixture {
	t.Helper()
	now := t0
	f := &fixture{router: &lineRouter{}, clock: &now, events: &collector{}}
	f.d = New(Options{Router: f.router, Sink: f.events, Clock: func() time.Time { return now }})
	for _, tech := range techs {
		if err := f.d.AddTechnician(context.Background(), tech); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func tech(id string, at float64, skills ...string) Technician {
	return Technician{ID: id, Skills: skills, Home: Point{Lat: at}}
}

func order(id string, at float64, priority int, skills ...string) WorkOrder {
	return WorkOrder{ID: id, Skills: skills, Location: Point{Lat: at}, Priority: priority, Duration: time.Hour}
}

func (f *fixture) submit(t *testing.T, wo WorkOrder) {
	t.Helper()
	if err := f.d.Submit(context.Background(), wo); err != nil {
		t.Fatalf("submit %s: %v", wo.ID, err)
	}
}

func (f *fixture) job(t *testing.T, id string) JobView {
	t.Helper()
	v, ok := f.d.Job(id)
	if !ok {
		t.Fatalf("job %s unknown", id)
	}
	return v
}

func (f *fixture) wantAssigned(t *testing.T, id, techID string, eta time.Duration) {
	t.Helper()
	v := f.job(t, id)
	if v.State != StateAssigned || v.Tech != techID || !v.ETA.Equal(t0.Add(eta)) {
		t.Fatalf("job %s = {state:%s tech:%s eta:+%s}, want assigned to %s at +%s",
			id, v.State, v.Tech, v.ETA.Sub(t0), techID, eta)
	}
}

func TestAssignsNearestQualifiedTechnicianByETA(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 60, "hvac"))
	f.submit(t, order("J1", 10, 1, "hvac"))
	f.wantAssigned(t, "J1", "A", 10*time.Minute)
	f.submit(t, order("J2", 55, 1, "hvac"))
	f.wantAssigned(t, "J2", "B", 5*time.Minute)
}

func TestSkillsAreRequiredNotJustPreferred(t *testing.T) {
	f := newFixture(t, tech("near", 1, "plumbing"), tech("far", 50, "plumbing", "gas"))
	f.submit(t, order("gas-leak", 2, 5, "gas"))
	f.wantAssigned(t, "gas-leak", "far", 48*time.Minute)

	f.submit(t, order("multi", 2, 5, "plumbing", "gas")) // needs both
	if v := f.job(t, "multi"); v.Tech != "far" {
		t.Fatalf("multi-skill job went to %s", v.Tech)
	}
}

func TestEtaIncludesTheTechniciansQueueNotJustDistance(t *testing.T) {
	// A is closest to the new job but is booked solid with gas work only A can do.
	f := newFixture(t, tech("A", 0, "hvac", "gas"), tech("B", 40, "hvac"))
	f.submit(t, order("g1", 5, 1, "gas"))
	f.submit(t, order("g2", 6, 1, "gas"))
	f.wantAssigned(t, "g2", "A", 66*time.Minute) // 09:05 + 1h job, then 1 min drive
	f.submit(t, order("hv", 20, 1, "hvac"))
	// A: free at 11:06 then 14 min = 11:20. B: 20 min away = 09:20.
	f.wantAssigned(t, "hv", "B", 20*time.Minute)
}

func TestHigherPriorityIsServedBeforePlannedLowerPriorityWork(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("low1", 10, 1, "hvac"))
	f.submit(t, order("low2", 12, 1, "hvac"))
	f.submit(t, order("urgent", 20, 9, "hvac"))
	routes := f.d.Routes()[0].Stops
	if got := []string{routes[0].ID, routes[1].ID, routes[2].ID}; fmt.Sprint(got) != "[urgent low1 low2]" {
		t.Fatalf("route order = %v", got)
	}
	f.wantAssigned(t, "urgent", "A", 20*time.Minute)
	// The displaced jobs now wait behind it: urgent ends 10:20, +10 drive.
	f.wantAssigned(t, "low1", "A", 90*time.Minute)
}

func TestFIFOWithinAPriorityTier(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("first", 30, 3, "hvac"))
	f.submit(t, order("second", 1, 3, "hvac"))
	if r := f.d.Routes()[0].Stops; r[0].ID != "first" || r[1].ID != "second" {
		t.Fatalf("equal priority must not reorder: %v", r)
	}
}

func TestCapacityLimitsPlannedJobs(t *testing.T) {
	a := tech("A", 0, "hvac")
	a.MaxJobs = 1
	f := newFixture(t, a, tech("B", 80, "hvac"))
	f.submit(t, order("J1", 1, 1, "hvac"))
	f.submit(t, order("J2", 2, 1, "hvac"))
	if v := f.job(t, "J2"); v.Tech != "B" {
		t.Fatalf("A is full; J2 should go to B, got %q", v.Tech)
	}
	onlyA := newFixture(t, a)
	onlyA.submit(t, order("J1", 1, 1, "hvac"))
	onlyA.submit(t, order("J2", 2, 1, "hvac"))
	if v := onlyA.job(t, "J2"); v.State != StatePending {
		t.Fatalf("with A full and nobody else, J2 should wait, got %s", v.State)
	}
	// Capacity-blocked is not a dead-letter condition however many passes happen.
	for i := 0; i < 5; i++ {
		_ = onlyA.d.Rebalance(context.Background())
	}
	if v := onlyA.job(t, "J2"); v.State != StatePending {
		t.Fatalf("capacity wait must not dead-letter, got %s", v.State)
	}
}

func TestNoQualifiedTechnicianDeadLettersAfterMaxAttempts(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("roof", 5, 1, "roofing"))
	ctx := context.Background()
	if v := f.job(t, "roof"); v.State != StatePending {
		t.Fatalf("pass 1: %s", v.State)
	}
	_ = f.d.Rebalance(ctx)
	if v := f.job(t, "roof"); v.State != StatePending {
		t.Fatalf("pass 2: %s", v.State)
	}
	_ = f.d.Rebalance(ctx)
	if v := f.job(t, "roof"); v.State != StateDeadLettered {
		t.Fatalf("pass 3 should dead-letter, got %s", v.State)
	}
	if s := f.d.Stats(); s.DeadLettered != 1 {
		t.Fatalf("stats = %+v", s)
	}
	if got := strings.Join(f.events.types("roof"), ","); got != "submitted,unassigned,unassigned,unassigned,dead_lettered" {
		t.Fatalf("events = %s", got)
	}
	_ = f.d.Rebalance(ctx) // terminal jobs are left alone
	if f.d.Stats().DeadLettered != 1 {
		t.Fatal("dead-lettered twice")
	}
}

func TestAddingASkilledTechnicianRescuesAPendingOrder(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("roof", 5, 1, "roofing"))
	if err := f.d.AddTechnician(context.Background(), tech("R", 3, "roofing")); err != nil {
		t.Fatal(err)
	}
	f.wantAssigned(t, "roof", "R", 2*time.Minute)
}

func TestCancellationFreesCapacityAndPullsWorkForward(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 60, "hvac"))
	f.submit(t, order("J1", 5, 1, "hvac"))
	f.submit(t, order("J2", 8, 1, "hvac"))
	f.wantAssigned(t, "J1", "A", 5*time.Minute)
	f.wantAssigned(t, "J2", "B", 52*time.Minute) // A would not reach it until 10:08

	dup, err := f.d.Cancel(context.Background(), "J1")
	if err != nil || dup {
		t.Fatalf("cancel: dup=%v err=%v", dup, err)
	}
	f.wantAssigned(t, "J2", "A", 8*time.Minute) // A is free now: 44 minutes sooner
	if f.job(t, "J1").State != StateCancelled {
		t.Fatal("J1 should be cancelled")
	}
	if got := strings.Join(f.events.types("J2"), ","); got != "submitted,assigned,reassigned" {
		t.Fatalf("J2 events = %s", got)
	}
	if s := f.d.Stats(); s.Reassigned != 1 || s.Cancelled != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestCancelIsIdempotentAndRejectsUnknownJobs(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("J1", 5, 1, "hvac"))
	ctx := context.Background()
	if dup, _ := f.d.Cancel(ctx, "J1"); dup {
		t.Fatal("first cancel is not a duplicate")
	}
	if dup, err := f.d.Cancel(ctx, "J1"); !dup || err != nil {
		t.Fatalf("second cancel: dup=%v err=%v", dup, err)
	}
	if s := f.d.Stats(); s.Cancelled != 1 || s.Duplicates != 1 {
		t.Fatalf("stats = %+v", s)
	}
	if _, err := f.d.Cancel(ctx, "nope"); !errors.Is(err, ErrUnknownJob) {
		t.Fatalf("err = %v", err)
	}
}

func TestRunningLateMovesPlannedJobToATechnicianWhoCanBeSoonerOrKeepsItWithinThreshold(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 30, "hvac"))
	f.submit(t, order("J1", 10, 1, "hvac"))
	f.wantAssigned(t, "J1", "A", 10*time.Minute) // B would arrive at +20

	ctx := context.Background()
	// 12 minutes late: A arrives +22 vs B +20, only 2 minutes better: not worth moving.
	if err := f.d.RunningLate(ctx, "A", 12*time.Minute); err != nil {
		t.Fatal(err)
	}
	f.wantAssigned(t, "J1", "A", 22*time.Minute)
	if f.d.Stats().Reassigned != 0 {
		t.Fatal("small ETA difference must not trigger a reassignment")
	}
	// Two more hours of delay: B is now far sooner.
	if err := f.d.RunningLate(ctx, "A", 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	f.wantAssigned(t, "J1", "B", 20*time.Minute)
	if got := strings.Join(f.events.types("J1"), ","); got != "submitted,assigned,reassigned" {
		t.Fatalf("events = %s", got)
	}
	if got := strings.Join(f.events.types("tech:A"), ","); got != "running_late,running_late" {
		t.Fatalf("lateness events = %s", got)
	}
}

func TestRunningLateWhileOnAJobShiftsTheWholeRoute(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	ctx := context.Background()
	f.submit(t, order("now", 10, 1, "hvac"))
	f.submit(t, order("next", 12, 1, "hvac"))
	f.wantAssigned(t, "next", "A", 10*time.Minute+time.Hour+2*time.Minute) // 09:10 + 1h job + 2 min drive
	if err := f.d.Start(ctx, "now", "A"); err != nil {
		t.Fatal(err)
	}
	// "now" runs from 09:00 (Start time) for 1h; next is 2 minutes from it.
	f.wantAssigned(t, "next", "A", time.Hour+2*time.Minute)
	if err := f.d.RunningLate(ctx, "A", 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	f.wantAssigned(t, "next", "A", time.Hour+32*time.Minute)
	if _, err := f.d.Complete(ctx, "now", "A"); err != nil {
		t.Fatal(err)
	}
	// Completing frees the technician at 09:00 (the fake clock has not moved): re-planned.
	f.wantAssigned(t, "next", "A", 2*time.Minute)
}

func TestCompletionIsIdempotent(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 5, "hvac"))
	f.submit(t, order("J1", 1, 1, "hvac"))
	ctx := context.Background()

	if _, err := f.d.Complete(ctx, "J1", "B"); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("wrong technician must be rejected, got %v", err)
	}
	if _, err := f.d.Complete(ctx, "ghost", "A"); !errors.Is(err, ErrUnknownJob) {
		t.Fatalf("err = %v", err)
	}
	if dup, err := f.d.Complete(ctx, "J1", "A"); dup || err != nil {
		t.Fatalf("first completion: dup=%v err=%v", dup, err)
	}
	for i := 0; i < 3; i++ {
		if dup, err := f.d.Complete(ctx, "J1", "A"); !dup || err != nil {
			t.Fatalf("redelivery %d: dup=%v err=%v", i, dup, err)
		}
	}
	if s := f.d.Stats(); s.Completed != 1 || s.Duplicates != 3 {
		t.Fatalf("stats = %+v", s)
	}
	if got := strings.Join(f.events.types("J1"), ","); got != "submitted,assigned,completed,duplicate_completion,duplicate_completion,duplicate_completion" {
		t.Fatalf("events = %s", got)
	}
}

func TestLateCompletionOfACancelledOrDeadLetteredJobIsADuplicateNotAnError(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	ctx := context.Background()
	f.submit(t, order("J1", 1, 1, "hvac"))
	_, _ = f.d.Cancel(ctx, "J1")
	if dup, err := f.d.Complete(ctx, "J1", "A"); !dup || err != nil {
		t.Fatalf("completion after cancel: dup=%v err=%v", dup, err)
	}
	if f.job(t, "J1").State != StateCancelled || f.d.Stats().Completed != 0 {
		t.Fatal("a terminal job must not change state")
	}
}

func TestFailuresRetryOnOtherTechniciansThenDeadLetter(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 10, "hvac"), tech("C", 20, "hvac"))
	ctx := context.Background()
	f.submit(t, order("J1", 1, 1, "hvac"))
	f.wantAssigned(t, "J1", "A", time.Minute)

	if _, err := f.d.ReportFailure(ctx, "J1", "B", "x"); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("err = %v", err)
	}
	for _, step := range []struct{ failed, next string }{{"A", "B"}, {"B", "C"}} {
		if _, err := f.d.ReportFailure(ctx, "J1", step.failed, "no-show"); err != nil {
			t.Fatal(err)
		}
		if v := f.job(t, "J1"); v.State != StateAssigned || v.Tech != step.next {
			t.Fatalf("after %s failed: %+v", step.failed, v)
		}
	}
	if _, err := f.d.ReportFailure(ctx, "J1", "C", "no-show"); err != nil {
		t.Fatal(err)
	}
	if v := f.job(t, "J1"); v.State != StateDeadLettered || v.Failures != 3 {
		t.Fatalf("expected dead-lettered after 3 failures: %+v", v)
	}
	want := "submitted,assigned,failed,retried,assigned,failed,retried,assigned,failed,dead_lettered"
	if got := strings.Join(f.events.types("J1"), ","); got != want {
		t.Fatalf("events = %s\nwant     %s", got, want)
	}
	if dup, err := f.d.ReportFailure(ctx, "J1", "C", "again"); !dup || err != nil {
		t.Fatalf("failure after terminal: dup=%v err=%v", dup, err)
	}
	if _, err := f.d.Complete(ctx, "J1", "A"); err != nil {
		t.Fatalf("completing a dead-lettered job is a duplicate, not an error: %v", err)
	}
}

func TestFailedJobWaitsWhenNoOtherTechnicianQualifies(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("J1", 1, 1, "hvac"))
	if _, err := f.d.ReportFailure(context.Background(), "J1", "A", "wrong part"); err != nil {
		t.Fatal(err)
	}
	if v := f.job(t, "J1"); v.State != StatePending {
		t.Fatalf("state = %s; the only technician is excluded so the job should wait", v.State)
	}
	_ = f.d.AddTechnician(context.Background(), tech("B", 4, "hvac"))
	f.wantAssigned(t, "J1", "B", 3*time.Minute)
}

func TestSubmitIsIdempotentAndValidates(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.submit(t, order("J1", 1, 1, "hvac"))
	f.submit(t, order("J1", 88, 9, "hvac")) // redelivered submission: ignored
	f.wantAssigned(t, "J1", "A", time.Minute)
	if s := f.d.Stats(); s.Submitted != 1 || s.Duplicates != 1 {
		t.Fatalf("stats = %+v", s)
	}
	ctx := context.Background()
	for _, bad := range []WorkOrder{{}, {ID: "x", Location: Point{Lat: 200}}} {
		if err := f.d.Submit(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("Submit(%+v) = %v, want ErrInvalid", bad, err)
		}
	}
	if err := f.d.AddTechnician(ctx, Technician{ID: ""}); !errors.Is(err, ErrInvalid) {
		t.Error("technician without id must be rejected")
	}
	if err := f.d.AddTechnician(ctx, tech("A", 0)); !errors.Is(err, ErrInvalid) {
		t.Error("duplicate technician must be rejected")
	}
	if err := f.d.RunningLate(ctx, "A", 0); !errors.Is(err, ErrInvalid) {
		t.Error("non-positive delay must be rejected")
	}
	if err := f.d.RunningLate(ctx, "ghost", time.Minute); !errors.Is(err, ErrUnknownTechnician) {
		t.Error("unknown technician must be rejected")
	}
}

func TestRoutingFailureLeavesJobPendingAndRecoversOnRebalance(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"))
	f.router.fail = errors.New("router down")
	err := f.d.Submit(context.Background(), order("J1", 5, 1, "hvac"))
	if err == nil || !strings.Contains(err.Error(), "routing") {
		t.Fatalf("expected a routing error, got %v", err)
	}
	if v := f.job(t, "J1"); v.State != StatePending {
		t.Fatalf("state = %s", v.State)
	}
	f.router.fail = nil
	if err := f.d.Rebalance(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.wantAssigned(t, "J1", "A", 5*time.Minute)
}

func TestRoutingOutageFallsBackToHaversineAndSaysSo(t *testing.T) {
	sink := &collector{}
	now := t0
	router := FallbackRouter{Primary: failingRouter{errors.New("osrm down")}, Fallback: HaversineRouter{}}
	d := New(Options{Router: router, Sink: sink, Clock: func() time.Time { return now }})
	ctx := context.Background()
	_ = d.AddTechnician(ctx, Technician{ID: "A", Skills: []string{"hvac"}, Home: Point{30.2672, -97.7431}})
	if err := d.Submit(ctx, WorkOrder{ID: "J1", Skills: []string{"hvac"}, Location: Point{30.3, -97.7}}); err != nil {
		t.Fatal(err)
	}
	v, _ := d.Job("J1")
	if v.State != StateAssigned || v.Source != SourceFallback || v.Travel <= 0 {
		t.Fatalf("job = %+v", v)
	}
	var found bool
	for _, e := range sink.events {
		if e.EventType == "assigned" && strings.Contains(e.Detail, "route_source="+SourceFallback) {
			found = true
		}
	}
	if !found {
		t.Fatal("assigned event should record that the ETA came from the haversine fallback")
	}
}

func TestLegCacheAvoidsRepeatedRouterCalls(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 30, "hvac"))
	f.submit(t, order("J1", 10, 1, "hvac"))
	before := f.router.Calls()
	for i := 0; i < 5; i++ {
		if err := f.d.Rebalance(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if after := f.router.Calls(); after != before {
		t.Fatalf("router called %d more times for already-known legs", after-before)
	}
}

func TestConcurrentUseIsSafeAndEveryOrderIsAssignedOnce(t *testing.T) {
	f := newFixture(t, tech("A", 0, "hvac"), tech("B", 50, "hvac"), tech("C", 80, "hvac"))
	const workers, perWorker = 8, 15
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := fmt.Sprintf("J%d-%d", w, i)
				if err := f.d.Submit(context.Background(), order(id, float64((w*7+i*3)%90), i%4, "hvac")); err != nil {
					t.Errorf("submit %s: %v", id, err)
					return
				}
				if i%5 == 0 {
					_, _ = f.d.Cancel(context.Background(), id)
				}
			}
		}(w)
	}
	wg.Wait()
	planned := 0
	seen := map[string]bool{}
	for _, r := range f.d.Routes() {
		for _, s := range r.Stops {
			if seen[s.ID] {
				t.Fatalf("job %s planned twice", s.ID)
			}
			seen[s.ID] = true
			planned++
		}
	}
	s := f.d.Stats()
	if s.Submitted != workers*perWorker || planned != s.Submitted-s.Cancelled {
		t.Fatalf("stats=%+v planned=%d", s, planned)
	}
}
