package main

import "testing"

func TestParseNodesValidSpec(t *testing.T) {
	nodes, err := parseNodes("node-a:16:65536:2,node-b:32:131072:4")
	if err != nil {
		t.Fatalf("parseNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0].Name != "node-a" || nodes[0].TotalCPU != 16 || nodes[0].TotalMemMB != 65536 || nodes[0].TotalGPUs != 2 {
		t.Errorf("node-a parsed wrong: %+v", nodes[0])
	}
	if nodes[1].Name != "node-b" || nodes[1].TotalCPU != 32 || nodes[1].TotalMemMB != 131072 || nodes[1].TotalGPUs != 4 {
		t.Errorf("node-b parsed wrong: %+v", nodes[1])
	}
}

func TestParseNodesDefaultSpec(t *testing.T) {
	nodes, err := parseNodes(defaultNodeSpec)
	if err != nil {
		t.Fatalf("parseNodes(defaultNodeSpec): %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(nodes))
	}
}

func TestParseNodesRejectsBadSpec(t *testing.T) {
	cases := []string{
		"node-a:16:65536",          // too few fields
		"node-a:notanumber:65536:2", // non-numeric cpu
		"",                          // empty
	}
	for _, spec := range cases {
		if _, err := parseNodes(spec); err == nil {
			t.Errorf("parseNodes(%q): expected error, got nil", spec)
		}
	}
}
