package devices

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SimDevice is a simulated device worker. It applies a pushed config only if
// the version is strictly newer than the one it already runs; otherwise it
// ignores the push but still acks the version it runs (so a registry whose ack
// was lost can recover by resending).
type SimDevice struct {
	mu       sync.Mutex
	ID       string
	version  int64
	config   map[string]string
	applies  int // times a config was actually applied
	received int // pushes received, including duplicates/stale
}

func (d *SimDevice) Receive(p Push) Ack {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.received++
	if p.Version <= d.version {
		return Ack{DeviceID: d.ID, Version: d.version, Applied: false}
	}
	d.version, d.config = p.Version, copyMap(p.Config)
	d.applies++
	return Ack{DeviceID: d.ID, Version: d.version, Applied: true}
}

// Snapshot returns the device's running version, applied count and received count.
func (d *SimDevice) Snapshot() (version int64, applies, received int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.version, d.applies, d.received
}

func (d *SimDevice) Config() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return copyMap(d.config)
}

// SimNetwork is a Transport to in-process SimDevices with fault injection.
// DropPush / DropAck receive the 1-based number of times this exact
// (device, version) has been sent, so tests can say "drop the first ack".
type SimNetwork struct {
	mu       sync.Mutex
	devices  map[string]*SimDevice
	sends    map[string]int
	DropPush func(deviceID string, version int64, nth int) bool
	DropAck  func(deviceID string, version int64, nth int) bool
	Latency  time.Duration
}

func NewSimNetwork() *SimNetwork {
	return &SimNetwork{devices: map[string]*SimDevice{}, sends: map[string]int{}}
}

func (n *SimNetwork) AddDevice(id string) *SimDevice {
	d := &SimDevice{ID: id}
	n.mu.Lock()
	n.devices[id] = d
	n.mu.Unlock()
	return d
}

func (n *SimNetwork) Device(id string) *SimDevice {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.devices[id]
}

func (n *SimNetwork) Send(ctx context.Context, p Push) (Ack, error) {
	n.mu.Lock()
	dev := n.devices[p.DeviceID]
	key := fmt.Sprintf("%s@%d", p.DeviceID, p.Version)
	n.sends[key]++
	nth := n.sends[key]
	dropPush, dropAck := n.DropPush, n.DropAck
	latency := n.Latency
	n.mu.Unlock()
	if dev == nil {
		return Ack{}, fmt.Errorf("device %q unreachable", p.DeviceID)
	}
	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return Ack{}, ctx.Err()
		}
	}
	if dropPush != nil && dropPush(p.DeviceID, p.Version, nth) {
		<-ctx.Done() // push lost on the wire: the device never sees it
		return Ack{}, ctx.Err()
	}
	ack := dev.Receive(p)
	if dropAck != nil && dropAck(p.DeviceID, p.Version, nth) {
		<-ctx.Done() // device applied it, but the ack is lost on the way back
		return Ack{}, ctx.Err()
	}
	return ack, nil
}
