.PHONY: build run-sim run-sim-pg trace allocator test clean

build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/tracer ./cmd/tracer

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
