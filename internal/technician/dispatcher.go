package technician

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"scheduler/internal/telemetry"
)

// Job lifecycle states. completed, cancelled and dead_lettered are terminal: once a job is
// there, every later report about it is a harmless duplicate (the same idempotent-completion
// rule the SQS coordinator applies to ML batches).
const (
	StatePending      = "pending"
	StateAssigned     = "assigned"
	StateInProgress   = "in_progress"
	StateCompleted    = "completed"
	StateCancelled    = "cancelled"
	StateDeadLettered = "dead_lettered"
)

var (
	ErrUnknownJob        = errors.New("technician: unknown job")
	ErrUnknownTechnician = errors.New("technician: unknown technician")
	ErrNotAssigned       = errors.New("technician: job is not assigned to that technician")
	ErrInvalid           = errors.New("technician: invalid input")
)

type Technician struct {
	ID     string   `json:"id"`
	Name   string   `json:"name,omitempty"`
	Skills []string `json:"skills"`
	Home   Point    `json:"home"`
	// MaxJobs caps unfinished jobs (in progress + planned). 0 means unlimited.
	MaxJobs int `json:"max_jobs,omitempty"`
}

type WorkOrder struct {
	ID       string   `json:"id"`
	Skills   []string `json:"skills"` // technician must have all of these
	Location Point    `json:"location"`
	Priority int      `json:"priority"` // higher = more urgent
	Duration time.Duration
	// MaxAttempts bounds failed executions and "nobody qualified" passes before the order is
	// dead-lettered. 0 means 3.
	MaxAttempts int       `json:"max_attempts,omitempty"`
	SubmitTime  time.Time `json:"submit_time,omitempty"`
}

// EventSink receives telemetry; *telemetry.Recorder satisfies it. Nil disables recording.
type EventSink interface{ Record(telemetry.Event) }

type Options struct {
	Router Router
	Sink   EventSink
	Clock  func() time.Time
	// MinImprovement is how much sooner a technician must arrive before a planned (not yet
	// started) job is moved to them. It stops two technicians trading the same job back and
	// forth over small ETA differences. 0 means 5 minutes.
	MinImprovement time.Duration
}

type Stats struct {
	Submitted    int
	Assigned     int
	Reassigned   int
	Completed    int
	Cancelled    int
	DeadLettered int
	Retried      int
	Duplicates   int // repeat submissions/reports for jobs already known or terminal
}

type job struct {
	WorkOrder
	state       string
	tech        string
	failures    int
	noQualified int
	excluded    map[string]bool
	eta         time.Time
	source      string
	travel      time.Duration
}

type techState struct {
	Technician
	pos        Point // last known position (home, or where the last job ended)
	current    *job
	currentEnd time.Time
	busyUntil  time.Time // running-late shift while idle
	route      []*job    // planned, not started, in service order
}

type leg struct {
	dur    time.Duration
	source string
}

type legKey struct{ from, to Point }

// Dispatcher assigns work orders to technicians. It is safe for concurrent use; the router is
// called with the lock held (bounded by ctx), so give it a short timeout.
type Dispatcher struct {
	mu     sync.Mutex
	opts   Options
	techs  map[string]*techState
	order  []string // technician IDs in insertion order (deterministic tie-breaks)
	jobs   map[string]*job
	legs   map[legKey]leg
	stats  Stats
	events []telemetry.Event
}

func New(opts Options) *Dispatcher {
	if opts.Router == nil {
		opts.Router = HaversineRouter{}
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.MinImprovement <= 0 {
		opts.MinImprovement = 5 * time.Minute
	}
	return &Dispatcher{
		opts: opts, techs: map[string]*techState{}, jobs: map[string]*job{}, legs: map[legKey]leg{},
	}
}

func (d *Dispatcher) AddTechnician(ctx context.Context, t Technician) error {
	if t.ID == "" || !t.Home.Valid() {
		return fmt.Errorf("%w: technician needs an id and a valid home location", ErrInvalid)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.techs[t.ID]; exists {
		return fmt.Errorf("%w: technician %s already exists", ErrInvalid, t.ID)
	}
	d.techs[t.ID] = &techState{Technician: t, pos: t.Home}
	d.order = append(d.order, t.ID)
	d.emit("", "technician_added", t.ID, 0, 0, fmt.Sprintf("skills=%v", t.Skills))
	// New capacity may unblock pending orders or improve planned ones.
	return d.rebalanceLocked(ctx, "technician "+t.ID+" added")
}

// Submit registers a work order and assigns it. Submitting an ID twice is a no-op. If routing
// fails outright the order stays pending and the error is returned; Rebalance retries it.
func (d *Dispatcher) Submit(ctx context.Context, wo WorkOrder) error {
	if wo.ID == "" || !wo.Location.Valid() {
		return fmt.Errorf("%w: work order needs an id and a valid location", ErrInvalid)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.jobs[wo.ID]; exists {
		d.stats.Duplicates++
		d.emit(wo.ID, "duplicate_submission", "", wo.Priority, 0, "order already known; ignored")
		return nil
	}
	if wo.MaxAttempts < 1 {
		wo.MaxAttempts = 3
	}
	if wo.Duration <= 0 {
		wo.Duration = time.Hour
	}
	if wo.SubmitTime.IsZero() {
		wo.SubmitTime = d.opts.Clock()
	}
	j := &job{WorkOrder: wo, state: StatePending, excluded: map[string]bool{}}
	d.jobs[wo.ID] = j
	d.stats.Submitted++
	d.emit(wo.ID, "submitted", "", wo.Priority, 0, fmt.Sprintf("skills=%v duration=%s", wo.Skills, wo.Duration))
	return d.assignLocked(ctx, j, "")
}

// Start marks a planned job as in progress for its technician.
func (d *Dispatcher) Start(ctx context.Context, jobID, techID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, t, err := d.lookup(jobID, techID)
	if err != nil {
		return err
	}
	if j.state != StateAssigned || j.tech != techID {
		return ErrNotAssigned
	}
	if t.current != nil {
		return fmt.Errorf("%w: %s is already working on %s", ErrInvalid, techID, t.current.ID)
	}
	d.removeFromRoute(t, j)
	j.state = StateInProgress
	t.current = j
	t.currentEnd = d.opts.Clock().Add(j.Duration)
	d.emit(j.ID, "started", techID, j.Priority, j.failures+1, "")
	return d.timelineLocked(ctx, t)
}

// Complete finishes a job. It is idempotent: completing a job that is already terminal
// (completed, cancelled, or dead-lettered) changes nothing and reports duplicate=true.
func (d *Dispatcher) Complete(ctx context.Context, jobID, techID string) (duplicate bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, ok := d.jobs[jobID]
	if !ok {
		return false, ErrUnknownJob
	}
	if isTerminal(j.state) {
		d.stats.Duplicates++
		d.emit(j.ID, "duplicate_completion", techID, j.Priority, 0, "job already "+j.state+"; ignoring")
		return true, nil
	}
	t, ok := d.techs[techID]
	if !ok {
		return false, ErrUnknownTechnician
	}
	if j.tech != techID || (j.state != StateInProgress && j.state != StateAssigned) {
		return false, ErrNotAssigned
	}
	if j.state == StateAssigned { // completed without an explicit Start
		d.removeFromRoute(t, j)
	}
	t.current, t.currentEnd, t.pos = nil, time.Time{}, j.Location
	j.state = StateCompleted
	d.stats.Completed++
	d.emit(j.ID, "completed", techID, j.Priority, j.failures+1, "")
	return false, d.timelineLocked(ctx, t)
}

// Cancel cancels a job (idempotent) and rebalances the remaining plan.
func (d *Dispatcher) Cancel(ctx context.Context, jobID string) (duplicate bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, ok := d.jobs[jobID]
	if !ok {
		return false, ErrUnknownJob
	}
	if isTerminal(j.state) {
		d.stats.Duplicates++
		d.emit(j.ID, "duplicate_cancellation", j.tech, j.Priority, 0, "job already "+j.state)
		return true, nil
	}
	tech := j.tech
	d.detach(j)
	j.state = StateCancelled
	d.stats.Cancelled++
	d.emit(j.ID, "cancelled", tech, j.Priority, 0, "")
	if t, ok := d.techs[tech]; ok {
		if err := d.timelineLocked(ctx, t); err != nil {
			return false, err
		}
	}
	return false, d.rebalanceLocked(ctx, "cancellation of "+jobID)
}

// RunningLate tells the dispatcher a technician will be delayed. Their planned arrivals shift
// and any planned job that another technician can now reach meaningfully sooner is moved.
func (d *Dispatcher) RunningLate(ctx context.Context, techID string, delay time.Duration) error {
	if delay <= 0 {
		return fmt.Errorf("%w: delay must be positive", ErrInvalid)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.techs[techID]
	if !ok {
		return ErrUnknownTechnician
	}
	now := d.opts.Clock()
	if t.current != nil {
		t.currentEnd = t.currentEnd.Add(delay)
	} else {
		if t.busyUntil.Before(now) {
			t.busyUntil = now
		}
		t.busyUntil = t.busyUntil.Add(delay)
	}
	d.emit("tech:"+techID, "running_late", techID, 0, 0, "delay="+delay.String())
	if err := d.timelineLocked(ctx, t); err != nil {
		return err
	}
	return d.rebalanceLocked(ctx, fmt.Sprintf("%s running %s late", techID, delay))
}

// ReportFailure records that a technician could not do a job (no-show, wrong parts, ...). The
// job is retried with someone else; after MaxAttempts failures it is dead-lettered.
func (d *Dispatcher) ReportFailure(ctx context.Context, jobID, techID, reason string) (duplicate bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, ok := d.jobs[jobID]
	if !ok {
		return false, ErrUnknownJob
	}
	if isTerminal(j.state) {
		d.stats.Duplicates++
		d.emit(j.ID, "duplicate_failure", techID, j.Priority, 0, "job already "+j.state)
		return true, nil
	}
	if j.tech != techID {
		return false, ErrNotAssigned
	}
	t := d.techs[techID]
	d.detach(j)
	j.failures++
	j.excluded[techID] = true
	d.emit(j.ID, "failed", techID, j.Priority, j.failures, reason)
	if j.failures >= j.MaxAttempts {
		j.state = StateDeadLettered
		d.stats.DeadLettered++
		d.emit(j.ID, "dead_lettered", techID, j.Priority, j.failures,
			fmt.Sprintf("failed %d times (max %d)", j.failures, j.MaxAttempts))
		return false, d.timelineLocked(ctx, t)
	}
	d.stats.Retried++
	d.emit(j.ID, "retried", techID, j.Priority, j.failures, "reassigning to another technician")
	if err := d.timelineLocked(ctx, t); err != nil {
		return false, err
	}
	return false, d.assignLocked(ctx, j, techID)
}

// Rebalance assigns pending orders and moves planned orders to technicians who can reach
// them meaningfully sooner. Call it on a timer or after external changes.
func (d *Dispatcher) Rebalance(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rebalanceLocked(ctx, "manual")
}

// --------------------------------------------------------------------------------------- //

func isTerminal(state string) bool {
	return state == StateCompleted || state == StateCancelled || state == StateDeadLettered
}

func (d *Dispatcher) lookup(jobID, techID string) (*job, *techState, error) {
	j, ok := d.jobs[jobID]
	if !ok {
		return nil, nil, ErrUnknownJob
	}
	t, ok := d.techs[techID]
	if !ok {
		return nil, nil, ErrUnknownTechnician
	}
	return j, t, nil
}

func (d *Dispatcher) removeFromRoute(t *techState, j *job) {
	for i, r := range t.route {
		if r == j {
			t.route = append(t.route[:i], t.route[i+1:]...)
			return
		}
	}
}

// detach takes a job off whatever technician holds it, without changing its state.
func (d *Dispatcher) detach(j *job) {
	if t, ok := d.techs[j.tech]; ok {
		d.removeFromRoute(t, j)
		if t.current == j {
			t.current, t.currentEnd = nil, time.Time{}
		}
	}
	j.tech, j.eta, j.source, j.travel = "", time.Time{}, "", 0
	j.state = StatePending
}

func qualifies(t *techState, j *job) bool {
	have := make(map[string]bool, len(t.Skills))
	for _, s := range t.Skills {
		have[s] = true
	}
	for _, s := range j.Skills {
		if !have[s] {
			return false
		}
	}
	return true
}

func (t *techState) unfinished() int {
	n := len(t.route)
	if t.current != nil {
		n++
	}
	return n
}

// start returns where and when a technician becomes free to travel to their first planned job.
func (d *Dispatcher) start(t *techState) (time.Time, Point) {
	cursor := d.opts.Clock()
	pos := t.pos
	if t.current != nil {
		pos = t.current.Location
		if t.currentEnd.After(cursor) {
			cursor = t.currentEnd
		}
	}
	if t.busyUntil.After(cursor) {
		cursor = t.busyUntil
	}
	return cursor, pos
}

// insertIndex keeps the route ordered by priority (high first), FIFO within a tier.
func insertIndex(t *techState, j *job) int {
	k := 0
	for k < len(t.route) && t.route[k].Priority >= j.Priority {
		k++
	}
	return k
}

// travel returns cached leg times, fetching the missing origins in one router call.
func (d *Dispatcher) travel(ctx context.Context, origins []Point, dest Point) ([]leg, error) {
	out := make([]leg, len(origins))
	var missing []Point
	var missingAt []int
	for i, o := range origins {
		if o == dest {
			out[i] = leg{0, SourceHaversine}
			continue
		}
		if l, ok := d.legs[legKey{o, dest}]; ok {
			out[i] = l
			continue
		}
		missing, missingAt = append(missing, o), append(missingAt, i)
	}
	if len(missing) > 0 {
		m, err := d.opts.Router.TravelTimes(ctx, missing, []Point{dest})
		if err != nil {
			return nil, fmt.Errorf("routing: %w", err)
		}
		if len(m.Durations) != len(missing) {
			return nil, fmt.Errorf("routing: router returned %d rows for %d origins", len(m.Durations), len(missing))
		}
		for i, row := range m.Durations {
			if len(row) != 1 {
				return nil, errors.New("routing: router returned a malformed matrix")
			}
			l := leg{row[0], m.Source}
			d.legs[legKey{missing[i], dest}] = l
			out[missingAt[i]] = l
		}
	}
	return out, nil
}

// timelineLocked recomputes planned arrival times along a technician's route.
func (d *Dispatcher) timelineLocked(ctx context.Context, t *techState) error {
	cursor, pos := d.start(t)
	for _, j := range t.route {
		l, err := d.travel(ctx, []Point{pos}, j.Location)
		if err != nil {
			return err
		}
		j.eta = cursor.Add(l[0].dur)
		j.travel, j.source = l[0].dur, l[0].source
		cursor = j.eta.Add(j.Duration)
		pos = j.Location
	}
	return nil
}

type candidate struct {
	tech   *techState
	index  int
	eta    time.Time
	travel time.Duration
	source string
}

// candidates scores every qualified, non-excluded technician with spare capacity. It also
// reports whether anyone in the fleet has the skills at all.
func (d *Dispatcher) candidates(ctx context.Context, j *job, skip string) (cands []candidate, anyQualified bool, err error) {
	var pool []*techState
	var origins []Point
	var prevEnd []time.Time
	for _, id := range d.order {
		t := d.techs[id]
		if !qualifies(t, j) {
			continue
		}
		anyQualified = true
		if id == skip || j.excluded[id] || (t.MaxJobs > 0 && t.unfinished() >= t.MaxJobs) {
			continue
		}
		k := insertIndex(t, j)
		cursor, pos := d.start(t)
		if k > 0 {
			cursor, pos = t.route[k-1].eta.Add(t.route[k-1].Duration), t.route[k-1].Location
		}
		pool, origins, prevEnd = append(pool, t), append(origins, pos), append(prevEnd, cursor)
	}
	if len(pool) == 0 {
		return nil, anyQualified, nil
	}
	legs, err := d.travel(ctx, origins, j.Location)
	if err != nil {
		return nil, anyQualified, err
	}
	for i, t := range pool {
		cands = append(cands, candidate{
			tech: t, index: insertIndex(t, j), eta: prevEnd[i].Add(legs[i].dur),
			travel: legs[i].dur, source: legs[i].source,
		})
	}
	sort.SliceStable(cands, func(a, b int) bool { return cands[a].eta.Before(cands[b].eta) })
	return cands, anyQualified, nil
}

func (d *Dispatcher) place(ctx context.Context, j *job, c candidate) error {
	t := c.tech
	t.route = append(t.route, nil)
	copy(t.route[c.index+1:], t.route[c.index:])
	t.route[c.index] = j
	j.state, j.tech = StateAssigned, t.ID
	return d.timelineLocked(ctx, t)
}

// assignLocked gives a pending job to the technician who can reach it soonest. fromTech, if
// set, is the technician it was just taken from (for the event trail).
func (d *Dispatcher) assignLocked(ctx context.Context, j *job, fromTech string) error {
	cands, anyQualified, err := d.candidates(ctx, j, "")
	if err != nil {
		return err
	}
	if len(cands) == 0 {
		if !anyQualified {
			j.noQualified++
			d.emit(j.ID, "unassigned", "", j.Priority, j.noQualified,
				fmt.Sprintf("no technician has skills %v (pass %d of %d)", j.Skills, j.noQualified, j.MaxAttempts))
			if j.noQualified >= j.MaxAttempts {
				j.state = StateDeadLettered
				d.stats.DeadLettered++
				d.emit(j.ID, "dead_lettered", "", j.Priority, j.noQualified,
					fmt.Sprintf("no qualified technician after %d passes", j.noQualified))
			}
			return nil
		}
		d.emit(j.ID, "unassigned", "", j.Priority, j.noQualified,
			"qualified technicians are at capacity or excluded; waiting")
		return nil
	}
	best := cands[0]
	if err := d.place(ctx, j, best); err != nil {
		d.detach(j)
		return err
	}
	d.stats.Assigned++
	detail := fmt.Sprintf("eta=%s travel=%s route_source=%s", j.eta.Format(time.RFC3339), best.travel.Round(time.Second), best.source)
	if fromTech != "" {
		detail += " retried_from=" + fromTech
	}
	d.emit(j.ID, "assigned", best.tech.ID, j.Priority, j.failures+1, detail)
	return nil
}

// rebalanceLocked first assigns pending orders, then reconsiders every planned order.
func (d *Dispatcher) rebalanceLocked(ctx context.Context, reason string) error {
	for _, j := range d.sortedJobs(func(j *job) bool { return j.state == StatePending }) {
		if err := d.assignLocked(ctx, j, ""); err != nil {
			return err
		}
	}
	for _, j := range d.sortedJobs(func(j *job) bool { return j.state == StateAssigned }) {
		if j.state != StateAssigned { // moved or dead-lettered earlier in this pass
			continue
		}
		if err := d.reconsider(ctx, j, reason); err != nil {
			return err
		}
	}
	return nil
}

// reconsider lifts a planned job out of its route, re-scores everyone, and puts it back with
// the best technician - the original one unless another is at least MinImprovement sooner.
func (d *Dispatcher) reconsider(ctx context.Context, j *job, reason string) error {
	from := d.techs[j.tech]
	oldETA := j.eta
	d.removeFromRoute(from, j)
	j.state, j.tech = StatePending, ""
	if err := d.timelineLocked(ctx, from); err != nil {
		return d.restore(ctx, j, from, oldETA, err)
	}
	cands, _, err := d.candidates(ctx, j, "")
	if err != nil || len(cands) == 0 {
		return d.restore(ctx, j, from, oldETA, err)
	}
	best := cands[0]
	for _, c := range cands {
		if c.tech == from {
			// Stick with the current technician unless someone beats them by the threshold.
			if !best.eta.Add(d.opts.MinImprovement).Before(c.eta) {
				best = c
			}
			break
		}
	}
	if err := d.place(ctx, j, best); err != nil {
		return d.restore(ctx, j, from, oldETA, err)
	}
	if best.tech != from {
		d.stats.Reassigned++
		d.emit(j.ID, "reassigned", best.tech.ID, j.Priority, j.failures+1, fmt.Sprintf(
			"from=%s reason=%q eta=%s (was %s, %s sooner) route_source=%s",
			from.ID, reason, j.eta.Format(time.RFC3339), oldETA.Format(time.RFC3339),
			oldETA.Sub(j.eta).Round(time.Second), best.source))
	}
	return nil
}

// restore puts a job back on its original technician after a failed re-evaluation.
func (d *Dispatcher) restore(ctx context.Context, j *job, from *techState, oldETA time.Time, cause error) error {
	from.route = append(from.route, nil)
	k := insertIndex(from, j)
	copy(from.route[k+1:], from.route[k:])
	from.route[k] = j
	j.state, j.tech = StateAssigned, from.ID
	if terr := d.timelineLocked(ctx, from); terr != nil {
		j.eta = oldETA
	}
	return cause
}

func (d *Dispatcher) sortedJobs(keep func(*job) bool) []*job {
	var out []*job
	for _, j := range d.jobs {
		if keep(j) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Priority != out[b].Priority {
			return out[a].Priority > out[b].Priority
		}
		if !out[a].SubmitTime.Equal(out[b].SubmitTime) {
			return out[a].SubmitTime.Before(out[b].SubmitTime)
		}
		return out[a].ID < out[b].ID
	})
	return out
}

func (d *Dispatcher) emit(jobID, eventType, techID string, priority, attempt int, detail string) {
	e := telemetry.Event{
		Timestamp: d.opts.Clock(), JobID: jobID, EventType: eventType, NodeName: techID,
		Priority: priority, Attempt: attempt, Detail: detail,
	}
	d.events = append(d.events, e)
	if d.opts.Sink != nil {
		d.opts.Sink.Record(e)
	}
}

// ------------------------------- read-only views -------------------------------------- //

type JobView struct {
	ID       string
	State    string
	Tech     string
	ETA      time.Time
	Travel   time.Duration
	Source   string
	Failures int
}

type RouteView struct {
	Technician string
	Current    string
	Stops      []JobView
}

func (d *Dispatcher) Job(id string) (JobView, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, ok := d.jobs[id]
	if !ok {
		return JobView{}, false
	}
	return viewOf(j), true
}

func viewOf(j *job) JobView {
	return JobView{ID: j.ID, State: j.state, Tech: j.tech, ETA: j.eta, Travel: j.travel, Source: j.source, Failures: j.failures}
}

func (d *Dispatcher) Routes() []RouteView {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]RouteView, 0, len(d.order))
	for _, id := range d.order {
		t := d.techs[id]
		r := RouteView{Technician: id}
		if t.current != nil {
			r.Current = t.current.ID
		}
		for _, j := range t.route {
			r.Stops = append(r.Stops, viewOf(j))
		}
		out = append(out, r)
	}
	return out
}

// Jobs lists every known order (any state), sorted by ID.
func (d *Dispatcher) Jobs() []JobView {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]JobView, 0, len(d.jobs))
	for _, j := range d.jobs {
		out = append(out, viewOf(j))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

func (d *Dispatcher) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// Events returns a copy of every telemetry event emitted so far (useful when no sink is set).
func (d *Dispatcher) Events() []telemetry.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]telemetry.Event(nil), d.events...)
}
