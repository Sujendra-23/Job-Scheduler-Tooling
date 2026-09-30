// Package inference defines the ML payload executed by distributed workers.
// The payload deliberately mirrors the sibling GPU Benchmarking Suite's
// torchvision ResNet-50 benchmark: NCHW image batches at 224x224, with fp32
// or fp16 precision.
package inference

import "fmt"

const ResNet50 = "resnet50"

type Batch struct {
	Model     string `json:"model"`
	BatchSize int    `json:"batch_size"`
	Precision string `json:"precision"`
	Channels  int    `json:"channels"`
	Height    int    `json:"height"`
	Width     int    `json:"width"`
	Seed      int64  `json:"seed"`
}

func NewResNet50Batch(batchSize int, precision string, seed int64) (Batch, error) {
	b := Batch{
		Model: ResNet50, BatchSize: batchSize, Precision: precision,
		Channels: 3, Height: 224, Width: 224, Seed: seed,
	}
	return b, b.Validate()
}

func (b Batch) Validate() error {
	if b.Model != ResNet50 {
		return fmt.Errorf("unsupported model %q", b.Model)
	}
	if b.BatchSize < 1 || b.BatchSize > 256 {
		return fmt.Errorf("batch size %d is outside [1, 256]", b.BatchSize)
	}
	if b.Precision != "fp32" && b.Precision != "fp16" {
		return fmt.Errorf("unsupported precision %q", b.Precision)
	}
	if b.Channels != 3 || b.Height != 224 || b.Width != 224 {
		return fmt.Errorf("resnet50 input must be NCHW with 3x224x224 images")
	}
	return nil
}

type Result struct {
	Backend      string  `json:"backend"`
	Device       string  `json:"device"`
	BatchSize    int     `json:"batch_size"`
	Precision    string  `json:"precision"`
	LatencyMS    float64 `json:"latency_ms"`
	Top1ClassIDs []int   `json:"top1_class_ids"`
}
