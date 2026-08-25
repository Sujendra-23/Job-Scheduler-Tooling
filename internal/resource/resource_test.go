package resource

import "testing"

func TestFitsRespectsAllThreeDimensions(t *testing.T) {
	n := NewNode("n", 8, 16384, 1)
	if !n.Fits(Requirements{CPUCores: 8, MemoryMB: 16384, GPUs: 1}) {
		t.Fatal("expected exact-capacity request to fit")
	}
	if n.Fits(Requirements{CPUCores: 9, MemoryMB: 1, GPUs: 0}) {
		t.Fatal("expected over-capacity CPU request to not fit")
	}
	if n.Fits(Requirements{CPUCores: 1, MemoryMB: 99999, GPUs: 0}) {
		t.Fatal("expected over-capacity memory request to not fit")
	}
	if n.Fits(Requirements{CPUCores: 1, MemoryMB: 1, GPUs: 2}) {
		t.Fatal("expected over-capacity GPU request to not fit")
	}
}

func TestReserveAndRelease(t *testing.T) {
	n := NewNode("n", 8, 16384, 2)
	req := Requirements{CPUCores: 4, MemoryMB: 8192, GPUs: 1}
	n.Reserve(req)

	if n.Fits(Requirements{CPUCores: 5, MemoryMB: 1, GPUs: 0}) {
		t.Fatal("expected reserved CPU to reduce remaining capacity")
	}
	n.Release(req)
	if !n.Fits(Requirements{CPUCores: 8, MemoryMB: 16384, GPUs: 2}) {
		t.Fatal("expected full capacity back after release")
	}
}

func TestFindFitPicksTightestSlack(t *testing.T) {
	pool := &Pool{Nodes: []*Node{
		NewNode("roomy", 32, 65536, 4),
		NewNode("tight", 4, 8192, 1),
		NewNode("medium", 8, 16384, 2),
	}}
	req := Requirements{CPUCores: 2, MemoryMB: 1024, GPUs: 0}

	got := pool.FindFit(req)
	if got == nil || got.Name != "tight" {
		name := "nil"
		if got != nil {
			name = got.Name
		}
		t.Fatalf("expected best-fit to pick the tightest-slack node 'tight', got %s", name)
	}
}

func TestFindFitReturnsNilWhenNothingFits(t *testing.T) {
	pool := &Pool{Nodes: []*Node{NewNode("small", 2, 4096, 0)}}
	if got := pool.FindFit(Requirements{CPUCores: 4, MemoryMB: 1, GPUs: 0}); got != nil {
		t.Fatalf("expected nil when no node fits, got %s", got.Name)
	}
}
