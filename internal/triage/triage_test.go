package triage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"scheduler/internal/inference"
)

func failure(message string) Failure {
	b, _ := inference.NewResNet50Batch(32, "fp32", 1)
	return Failure{JobID: "job-1", Attempt: 1, MaxAttempts: 3, Error: message, Batch: b}
}

func TestPolicyAgentDecisions(t *testing.T) {
	agent := PolicyAgent{}
	tests := []struct{ message, action string }{
		{"CUDA out of memory", AutoAdjust},
		{"temporary worker error", Retry},
		{"corrupt input image", Escalate},
	}
	for _, tt := range tests {
		d, err := agent.Decide(context.Background(), failure(tt.message))
		if err != nil || d.Action != tt.action {
			t.Errorf("%q: got (%+v, %v), want action %s", tt.message, d, err, tt.action)
		}
	}
}

func TestLLMAgentParsesDecision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q", got)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{
				"role": "assistant", "content": `{"action":"retry","reason":"transient GPU reset"}`,
			}}},
		})
	}))
	defer server.Close()

	agent := LLMAgent{URL: server.URL, APIKey: "secret", Model: "test-model"}
	d, err := agent.Decide(context.Background(), failure("GPU reset"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Action != Retry || d.Source != "llm" {
		t.Fatalf("unexpected decision: %+v", d)
	}
}

func TestPolicyEscalatesWhenBudgetExhausted(t *testing.T) {
	f := failure("temporary error")
	f.Attempt = f.MaxAttempts
	d, err := (PolicyAgent{}).Decide(context.Background(), f)
	if err != nil || d.Action != Escalate {
		t.Fatalf("got (%+v, %v), want escalate", d, err)
	}
}
