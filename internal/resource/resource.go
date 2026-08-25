// Package resource models cluster nodes as bundles of CPU, memory, and GPU
// capacity, and implements the resource-aware placement decision a
// scheduler needs: "does this job fit on this node right now."
package resource

import "fmt"

// Requirements is what a job asks for.
type Requirements struct {
	CPUCores int
	MemoryMB int
	GPUs     int
}

// Node tracks a physical (or simulated) machine's total and currently
// allocated capacity.
type Node struct {
	Name       string
	TotalCPU   int
	TotalMemMB int
	TotalGPUs  int

	usedCPU   int
	usedMemMB int
	usedGPUs  int
}

func NewNode(name string, cpu, memMB, gpus int) *Node {
	return &Node{Name: name, TotalCPU: cpu, TotalMemMB: memMB, TotalGPUs: gpus}
}

// Fits reports whether the node has enough free capacity for req.
func (n *Node) Fits(req Requirements) bool {
	return n.TotalCPU-n.usedCPU >= req.CPUCores &&
		n.TotalMemMB-n.usedMemMB >= req.MemoryMB &&
		n.TotalGPUs-n.usedGPUs >= req.GPUs
}

// Reserve allocates req's resources on the node. Caller must have checked
// Fits first; Reserve does not itself validate, so callers under lock can
// avoid a redundant check.
func (n *Node) Reserve(req Requirements) {
	n.usedCPU += req.CPUCores
	n.usedMemMB += req.MemoryMB
	n.usedGPUs += req.GPUs
}

// Release frees req's resources back to the node, e.g. when a job finishes
// or is preempted.
func (n *Node) Release(req Requirements) {
	n.usedCPU -= req.CPUCores
	n.usedMemMB -= req.MemoryMB
	n.usedGPUs -= req.GPUs
}

func (n *Node) Utilization() (cpuPct, memPct, gpuPct float64) {
	if n.TotalCPU > 0 {
		cpuPct = 100 * float64(n.usedCPU) / float64(n.TotalCPU)
	}
	if n.TotalMemMB > 0 {
		memPct = 100 * float64(n.usedMemMB) / float64(n.TotalMemMB)
	}
	if n.TotalGPUs > 0 {
		gpuPct = 100 * float64(n.usedGPUs) / float64(n.TotalGPUs)
	}
	return
}

func (n *Node) String() string {
	cpuPct, memPct, gpuPct := n.Utilization()
	return fmt.Sprintf("%s [cpu %d/%d (%.0f%%) mem %d/%dMB (%.0f%%) gpu %d/%d (%.0f%%)]",
		n.Name, n.usedCPU, n.TotalCPU, cpuPct, n.usedMemMB, n.TotalMemMB, memPct, n.usedGPUs, n.TotalGPUs, gpuPct)
}

// Pool is the set of nodes the scheduler can place jobs on.
type Pool struct {
	Nodes []*Node
}

// FindFit returns the first node with enough free capacity for req, using
// best-fit-by-CPU-slack among candidates so jobs pack tightly rather than
// spreading thin across every node (which would fragment GPU capacity
// especially badly, since GPUs can't be split across jobs here).
func (p *Pool) FindFit(req Requirements) *Node {
	var best *Node
	bestSlack := -1
	for _, n := range p.Nodes {
		if !n.Fits(req) {
			continue
		}
		slack := (n.TotalCPU - n.usedCPU) - req.CPUCores
		if best == nil || slack < bestSlack {
			best = n
			bestSlack = slack
		}
	}
	return best
}
