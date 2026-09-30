#!/usr/bin/env python3
"""Persistent ResNet-50 inference worker.

The torch path is intentionally the same model/input contract as the sibling
gpu-benchmarking-suite/harness/benchmark.py: torchvision.models.resnet50,
NCHW 224x224 image batches, fp32/fp16, CUDA when available. One process loads
the model once and then accepts JSON batches on stdin.

`cpu-reference` is a dependency-free, real two-layer neural-network forward
pass for CI and laptops without torch. It is a functional smoke-test backend,
not a performance substitute for ResNet-50 and is labeled as such in results.
"""

import argparse
import json
import math
import random
import sys
import time


class CPUReference:
    def infer(self, request):
        rng = random.Random(request["seed"])
        batch_size = request["batch_size"]
        started = time.perf_counter()
        predictions = []
        # Small deterministic MLP: 32 input features -> 16 ReLU -> 10 logits.
        # Inputs stand in for extracted image features so CI does useful model
        # computation without allocating a full ResNet tensor.
        w1 = [[math.sin((i + 1) * (j + 1)) * 0.1 for i in range(32)] for j in range(16)]
        w2 = [[math.cos((i + 1) * (j + 1)) * 0.1 for i in range(16)] for j in range(10)]
        for _ in range(batch_size):
            x = [rng.uniform(-1.0, 1.0) for _ in range(32)]
            hidden = [max(0.0, sum(a * b for a, b in zip(row, x))) for row in w1]
            logits = [sum(a * b for a, b in zip(row, hidden)) for row in w2]
            predictions.append(max(range(len(logits)), key=logits.__getitem__))
        return {
            "backend": "cpu-reference-mlp",
            "device": "cpu",
            "batch_size": batch_size,
            "precision": "fp32",
            "latency_ms": round((time.perf_counter() - started) * 1000.0, 4),
            "top1_class_ids": predictions,
        }


class TorchResNet50:
    def __init__(self):
        import torch
        import torchvision

        self.torch = torch
        self.torchvision = torchvision
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.models = {}

    def _model(self, precision):
        effective = precision if self.device == "cuda" else "fp32"
        if effective not in self.models:
            model = self.torchvision.models.resnet50(weights=None).to(self.device).eval()
            if effective == "fp16":
                model = model.half()
            self.models[effective] = model
        return self.models[effective], effective

    def infer(self, request):
        torch = self.torch
        batch_size = request["batch_size"]
        model, effective_precision = self._model(request["precision"])
        dtype = torch.float16 if effective_precision == "fp16" else torch.float32
        generator = torch.Generator(device=self.device).manual_seed(request["seed"])
        batch = torch.randn(
            batch_size, request["channels"], request["height"], request["width"],
            device=self.device, dtype=dtype, generator=generator,
        )
        if self.device == "cuda":
            torch.cuda.synchronize()
        started = time.perf_counter()
        with torch.no_grad():
            output = model(batch)
        if self.device == "cuda":
            torch.cuda.synchronize()
        predictions = output.argmax(dim=1).cpu().tolist()
        return {
            "backend": "torchvision-resnet50",
            "device": self.device,
            "batch_size": batch_size,
            "precision": effective_precision,
            "latency_ms": round((time.perf_counter() - started) * 1000.0, 4),
            "top1_class_ids": predictions,
        }


def build_backend(name):
    if name == "cpu-reference":
        return CPUReference()
    try:
        return TorchResNet50()
    except Exception as exc:
        if name == "torch":
            raise
        print(f"torch backend unavailable ({exc}); using cpu-reference", file=sys.stderr)
        return CPUReference()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("auto", "torch", "cpu-reference"), default="auto")
    args = parser.parse_args()
    backend = build_backend(args.backend)

    for line in sys.stdin:
        try:
            request = json.loads(line)
            result = backend.infer(request)
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(response, separators=(",", ":")), flush=True)


if __name__ == "__main__":
    main()
