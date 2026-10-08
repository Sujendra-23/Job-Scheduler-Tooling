// Package devices is a single-host SIMULATION of a device registry that pushes
// versioned configuration to remote device workers with acknowledgement and
// bounded retry. The "network" is an in-process lossy channel (SimNetwork) and
// the "devices" are goroutine-safe structs; nothing here speaks a real device
// protocol. What it does model faithfully is the delivery logic:
//
//   - every device has its own monotonically increasing config version;
//   - a push is retried (bounded) until the device acknowledges that exact
//     version or the attempt budget is spent (state "failed");
//   - an ack that does not match the desired version (stale) is ignored;
//   - devices ignore pushes whose version is <= what they already applied, so
//     redelivery of the same version is idempotent, and still re-ack so a lost
//     ack can be recovered by retrying;
//   - a newer SetConfig supersedes an older push that is still being retried.
package devices

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"scheduler/internal/telemetry"
)

// Push is a versioned config message sent to one device.
type Push struct {
	DeviceID string
	Version  int64
	Config   map[string]string
	Attempt  int
}

// Ack is a device's reply. Version is the version the device now runs.
// Applied is false when the device ignored the push as stale/duplicate.
type Ack struct {
	DeviceID string
	Version  int64
	Applied  bool
}

// Transport delivers a push and waits for the ack until ctx expires. A lost
// push or lost ack surfaces as ctx.Err() (no reply), like a real timeout.
type Transport interface {
	Send(ctx context.Context, p Push) (Ack, error)
}

type State string

const (
	StateIdle    State = "idle"    // registered, no config ever set
	StatePending State = "pending" // desired > acked, push loop running
	StateAcked   State = "acked"   // device acknowledged the desired version
	StateFailed  State = "failed"  // attempt budget exhausted for desired version
)

// Status is the externally visible view of one device.
type Status struct {
	ID             string            `json:"id"`
	DesiredVersion int64             `json:"desired_version"`
	AckedVersion   int64             `json:"acked_version"`
	State          State             `json:"state"`
	Attempts       int               `json:"attempts"` // pushes made for the desired version
	LastError      string            `json:"last_error,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
}

type Options struct {
	AckTimeout  time.Duration // per-attempt wait for an ack
	MaxAttempts int           // pushes per version before giving up
	Backoff     time.Duration // base delay between attempts (doubles each retry)
}

func (o *Options) defaults() {
	if o.AckTimeout <= 0 {
		o.AckTimeout = 500 * time.Millisecond
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 4
	}
	if o.Backoff <= 0 {
		o.Backoff = 100 * time.Millisecond
	}
}

type entry struct {
	Status
	loopRunning bool
	pushedFor   int64 // version the attempt counter refers to
}

type Registry struct {
	mu        sync.Mutex
	opts      Options
	transport Transport
	rec       *telemetry.Recorder
	devs      map[string]*entry
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewRegistry(t Transport, rec *telemetry.Recorder, opts Options) *Registry {
	opts.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{opts: opts, transport: t, rec: rec, devs: map[string]*entry{}, ctx: ctx, cancel: cancel}
}

// Close stops all push loops and waits for them.
func (r *Registry) Close() { r.cancel(); r.wg.Wait() }

func (r *Registry) Register(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.devs[id]; ok {
		return fmt.Errorf("device %q already registered", id)
	}
	r.devs[id] = &entry{Status: Status{ID: id, State: StateIdle}}
	return nil
}

var ErrUnknownDevice = errors.New("unknown device")

// SetConfig records a new desired config for the device at the next version and
// starts (or lets the running loop pick up) the push. It returns that version.
func (r *Registry) SetConfig(id string, cfg map[string]string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.devs[id]
	if !ok {
		return 0, ErrUnknownDevice
	}
	e.DesiredVersion++
	e.Config = copyMap(cfg)
	e.State = StatePending
	e.LastError = ""
	r.event(id, "config_set", e.DesiredVersion, 0, fmt.Sprintf("desired config v%d (%d keys)", e.DesiredVersion, len(cfg)))
	if !e.loopRunning {
		e.loopRunning = true
		r.wg.Add(1)
		go r.pushLoop(e)
	}
	return e.DesiredVersion, nil
}

// Retry restarts delivery for a device in the failed state.
func (r *Registry) Retry(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.devs[id]
	if !ok {
		return ErrUnknownDevice
	}
	if e.State != StateFailed {
		return nil
	}
	e.State, e.pushedFor, e.LastError = StatePending, 0, ""
	if !e.loopRunning {
		e.loopRunning = true
		r.wg.Add(1)
		go r.pushLoop(e)
	}
	return nil
}

func (r *Registry) pushLoop(e *entry) {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		if r.ctx.Err() != nil || e.AckedVersion >= e.DesiredVersion {
			e.loopRunning = false
			r.mu.Unlock()
			return
		}
		version, cfg := e.DesiredVersion, copyMap(e.Config)
		if e.pushedFor != version { // new desired version: fresh attempt budget
			e.pushedFor, e.Attempts = version, 0
		}
		if e.Attempts >= r.opts.MaxAttempts {
			e.State = StateFailed
			e.loopRunning = false
			r.event(e.ID, "push_failed", version, e.Attempts, fmt.Sprintf("no ack for v%d after %d attempts; giving up", version, e.Attempts))
			r.mu.Unlock()
			return
		}
		e.Attempts++
		attempt := e.Attempts
		r.event(e.ID, "push", version, attempt, fmt.Sprintf("push v%d attempt %d/%d", version, attempt, r.opts.MaxAttempts))
		r.mu.Unlock()

		ctx, cancel := context.WithTimeout(r.ctx, r.opts.AckTimeout)
		ack, err := r.transport.Send(ctx, Push{DeviceID: e.ID, Version: version, Config: cfg, Attempt: attempt})
		cancel()

		r.mu.Lock()
		switch {
		case err != nil:
			e.LastError = err.Error()
			r.event(e.ID, "ack_timeout", version, attempt, fmt.Sprintf("no ack for v%d within %s", version, r.opts.AckTimeout))
			r.mu.Unlock()
			if !r.sleep(r.opts.Backoff << (attempt - 1)) {
				r.finish(e)
				return
			}
			continue
		case ack.Version != e.DesiredVersion:
			// Ack for a version we no longer (or do not yet) want: ignore it.
			r.event(e.ID, "stale_ack_ignored", ack.Version, attempt, fmt.Sprintf("ack v%d != desired v%d", ack.Version, e.DesiredVersion))
		default:
			if !ack.Applied {
				r.event(e.ID, "duplicate_ignored_by_device", version, attempt, fmt.Sprintf("device already ran v%d; idempotent re-ack", version))
			}
			if ack.Version > e.AckedVersion {
				e.AckedVersion = ack.Version
				e.State = StateAcked
				e.LastError = ""
				r.event(e.ID, "acked", ack.Version, attempt, fmt.Sprintf("device acked v%d after %d attempt(s)", ack.Version, attempt))
			}
		}
		r.mu.Unlock()
	}
}

func (r *Registry) finish(e *entry) {
	r.mu.Lock()
	e.loopRunning = false
	r.mu.Unlock()
}

func (r *Registry) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *Registry) Status(id string) (Status, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.devs[id]
	if !ok {
		return Status{}, false
	}
	s := e.Status
	s.Config = copyMap(s.Config)
	return s, true
}

func (r *Registry) List() []Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Status, 0, len(r.devs))
	for _, e := range r.devs {
		s := e.Status
		s.Config = copyMap(s.Config)
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// WaitSettled blocks until no device is pending (all acked or failed).
func (r *Registry) WaitSettled(ctx context.Context) error {
	for {
		pending := false
		for _, s := range r.List() {
			if s.State == StatePending {
				pending = true
			}
		}
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// event must be called with r.mu held (Recorder has its own lock).
func (r *Registry) event(id, typ string, version int64, attempt int, detail string) {
	if r.rec == nil {
		return
	}
	r.rec.Record(telemetry.Event{
		Timestamp: time.Now(), JobID: "device:" + id, EventType: typ,
		Version: version, Attempt: attempt, Detail: detail,
	})
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
