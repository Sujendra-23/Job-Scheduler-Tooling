.PHONY: build run-technicians run-distributed run-sim run-sim-pg trace allocator test clean

build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/dispatcher ./cmd/dispatcher
	go build -o bin/tracer ./cmd/tracer
	go build -o bin/devices ./cmd/devices
	go build -o bin/technicians ./cmd/technicians

# Runs actual inference batch payloads through three separate local worker
# processes. auto uses torchvision ResNet-50 when installed and otherwise the
# dependency-free CPU reference model.
run-distributed: build
	./bin/dispatcher -jobs 12 -workers 3 -backend auto \
		-failure-rate 0.25 -out scheduling_events.jsonl

# Technician dispatch demo (skills + travel time, reassignment, dead-letter). Set OSRM_URL to use
# OSRM travel times instead of haversine estimates.
run-technicians: build
	./bin/technicians -out technicians.jsonl

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
