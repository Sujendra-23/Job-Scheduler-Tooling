.PHONY: build run-distributed run-sim run-sim-pg trace allocator test clean localstack-up localstack-down serverless-deploy serverless-test serverless-integration
.DEFAULT_GOAL := build

# Use a virtual environment with serverless/requirements.txt installed.
PYTHON ?= python3

localstack-up:
	docker compose -f compose.serverless.yaml up -d --wait

localstack-down:
	docker compose -f compose.serverless.yaml down

serverless-deploy:
	$(PYTHON) serverless/deploy.py --create-queues

serverless-test:
	$(PYTHON) -m unittest discover -s serverless/tests -v

serverless-integration:
	RUN_LOCALSTACK_TESTS=1 $(PYTHON) -m unittest discover -s serverless/tests -p test_localstack.py -v

build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/dispatcher ./cmd/dispatcher
	go build -o bin/tracer ./cmd/tracer
	go build -o bin/devices ./cmd/devices

# Runs actual inference batch payloads through three separate local worker
# processes. auto uses torchvision ResNet-50 when installed and otherwise the
# dependency-free CPU reference model.
run-distributed: build
	./bin/dispatcher -jobs 12 -workers 3 -backend auto \
		-failure-rate 0.25 -out scheduling_events.jsonl

# Runs the simulation writing only to the local JSONL log (no Postgres
# required).
run-sim: build
	./bin/scheduler -jobs 40 -out scheduling_events.jsonl

# Runs the simulation against a local Postgres instance too. Requires the
# database and schema to already exist:
#   createdb scheduler
#   psql -d scheduler -f sql/schema.sql
run-sim-pg: build
	./bin/scheduler -jobs 40 -out scheduling_events.jsonl \
		-pg-dsn "postgres://scheduler:scheduler@localhost/scheduler?sslmode=disable"

trace: build
	./bin/tracer -log scheduling_events.jsonl -summary

allocator:
	gcc -O2 -Wall -o mem/allocator mem/allocator.c
	./mem/allocator

test:
	go test ./... -v

clean:
	rm -rf bin/ mem/allocator scheduling_events.jsonl
