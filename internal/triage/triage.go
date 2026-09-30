// Package triage turns inference failures into an explicit operational
// decision: retry, adjust the batch, or escalate for a human.
package triage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"scheduler/internal/inference"
)

const (
	Retry      = "retry"
	AutoAdjust = "auto_adjust"
	Escalate   = "escalate"
)

type Failure struct {
	JobID       string          `json:"job_id"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	Error       string          `json:"error"`
	Batch       inference.Batch `json:"batch"`
}

type Decision struct {
	Action    string `json:"action"`
	Reason    string `json:"reason"`
	BatchSize int    `json:"batch_size,omitempty"`
	Precision string `json:"precision,omitempty"`
	Source    string `json:"source,omitempty"`
}

func (d Decision) Validate() error {
	switch d.Action {
	case Retry, Escalate:
		return nil
	case AutoAdjust:
		if d.BatchSize < 1 {
			return errors.New("auto_adjust requires a positive batch_size")
		}
		if d.Precision != "fp32" && d.Precision != "fp16" {
			return errors.New("auto_adjust precision must be fp32 or fp16")
		}
		return nil
	default:
		return fmt.Errorf("unknown triage action %q", d.Action)
	}
}

type Agent interface {
	Decide(context.Context, Failure) (Decision, error)
}

// PolicyAgent is the deterministic offline fallback. It applies the same
// tools as the LLM agent, making CI reproducible when no model endpoint is
// configured.
type PolicyAgent struct{}

func (PolicyAgent) Decide(_ context.Context, f Failure) (Decision, error) {
	errText := strings.ToLower(f.Error)
	if f.Attempt >= f.MaxAttempts {
		return Decision{Action: Escalate, Reason: "retry budget exhausted", Source: "policy"}, nil
	}
	if strings.Contains(errText, "out of memory") || strings.Contains(errText, "oom") {
		size := f.Batch.BatchSize / 2
		if size < 1 {
			size = 1
		}
		return Decision{Action: AutoAdjust, Reason: "reduce memory pressure", BatchSize: size, Precision: "fp16", Source: "policy"}, nil
	}
	if strings.Contains(errText, "corrupt") || strings.Contains(errText, "invalid input") {
		return Decision{Action: Escalate, Reason: "input needs human investigation", Source: "policy"}, nil
	}
	return Decision{Action: Retry, Reason: "failure appears transient", Source: "policy"}, nil
}

// LLMAgent calls an OpenAI-compatible chat-completions endpoint and requires
// the model to return one JSON decision. The HTTP shape keeps the scheduler
// provider-neutral and makes it easy to test with a local model server.
type LLMAgent struct {
	URL        string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

type chatRequest struct {
	Model          string        `json:"model"`
	Messages       []chatMessage `json:"messages"`
	ResponseFormat struct {
		Type string `json:"type"`
	} `json:"response_format"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func (a *LLMAgent) Decide(ctx context.Context, f Failure) (Decision, error) {
	if a.URL == "" || a.Model == "" {
		return Decision{}, errors.New("LLM triage requires URL and model")
	}
	failureJSON, err := json.Marshal(f)
	if err != nil {
		return Decision{}, err
	}
	reqBody := chatRequest{
		Model: a.Model,
		Messages: []chatMessage{
			{Role: "system", Content: "You triage failed ML inference batches. Return only JSON with action retry, auto_adjust, or escalate; reason; and for auto_adjust a positive batch_size and precision fp32 or fp16. Never exceed the supplied retry budget."},
			{Role: "user", Content: string(failureJSON)},
		},
	}
	reqBody.ResponseFormat.Type = "json_object"
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return Decision{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(payload))
	if err != nil {
		return Decision{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.APIKey)
	}
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return Decision{}, fmt.Errorf("calling triage model: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Decision{}, fmt.Errorf("triage model returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var chat chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chat); err != nil {
		return Decision{}, fmt.Errorf("decoding triage response: %w", err)
	}
	if len(chat.Choices) == 0 {
		return Decision{}, errors.New("triage model returned no choices")
	}
	var decision Decision
	if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &decision); err != nil {
		return Decision{}, fmt.Errorf("decoding triage decision: %w", err)
	}
	decision.Source = "llm"
	if err := decision.Validate(); err != nil {
		return Decision{}, err
	}
	if f.Attempt >= f.MaxAttempts && decision.Action != Escalate {
		return Decision{}, errors.New("triage model exceeded retry budget")
	}
	return decision, nil
}

// FallbackAgent keeps model-endpoint outages from silently dropping failed
// jobs. The fallback source is retained in telemetry.
type FallbackAgent struct {
	Primary  Agent
	Fallback Agent
}

func (a FallbackAgent) Decide(ctx context.Context, f Failure) (Decision, error) {
	d, err := a.Primary.Decide(ctx, f)
	if err == nil {
		return d, nil
	}
	d, fallbackErr := a.Fallback.Decide(ctx, f)
	if fallbackErr != nil {
		return Decision{}, errors.Join(err, fallbackErr)
	}
	d.Source = "policy_fallback"
	d.Reason = fmt.Sprintf("LLM unavailable (%v); %s", err, d.Reason)
	return d, nil
}
