package inference

import "testing"

func TestNewResNet50Batch(t *testing.T) {
	b, err := NewResNet50Batch(8, "fp16", 42)
	if err != nil {
		t.Fatal(err)
	}
	if b.Model != ResNet50 || b.BatchSize != 8 || b.Channels != 3 || b.Height != 224 || b.Width != 224 {
		t.Fatalf("unexpected batch: %+v", b)
	}
}

func TestBatchValidation(t *testing.T) {
	tests := []Batch{
		{Model: "bert", BatchSize: 8, Precision: "fp32", Channels: 3, Height: 224, Width: 224},
		{Model: ResNet50, BatchSize: 0, Precision: "fp32", Channels: 3, Height: 224, Width: 224},
		{Model: ResNet50, BatchSize: 8, Precision: "int8", Channels: 3, Height: 224, Width: 224},
	}
	for _, b := range tests {
		if err := b.Validate(); err == nil {
			t.Errorf("Validate(%+v) succeeded, want error", b)
		}
	}
}
