-- Scheduling telemetry schema. One row per scheduling decision (submitted,
-- started, preempted, completed, wait_reason), written by
-- internal/telemetry.Recorder alongside the JSONL log.

CREATE TABLE IF NOT EXISTS scheduling_events (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    job_id      TEXT NOT NULL,
    event_type  TEXT NOT NULL CHECK (event_type IN ('submitted', 'started', 'preempted', 'completed', 'wait_reason')),
    node_name   TEXT,
    priority    INTEGER NOT NULL,
    detail      TEXT
);

CREATE INDEX IF NOT EXISTS idx_scheduling_events_job_id ON scheduling_events (job_id);
CREATE INDEX IF NOT EXISTS idx_scheduling_events_type ON scheduling_events (event_type);
CREATE INDEX IF NOT EXISTS idx_scheduling_events_timestamp ON scheduling_events (timestamp);

-- ============================================================
-- Analytics queries
-- ============================================================

-- 1. Wait time per job (submitted -> started), the SQL equivalent of what
--    cmd/tracer computes from the JSONL log, useful when you want to slice
--    by priority or node instead of tracing one job at a time.
--
-- SELECT
--     s.job_id,
--     s.priority,
--     st.node_name,
--     st.timestamp - s.timestamp AS wait_time
-- FROM scheduling_events s
-- JOIN scheduling_events st
--     ON s.job_id = st.job_id AND st.event_type = 'started'
-- WHERE s.event_type = 'submitted'
-- ORDER BY wait_time DESC;

-- 2. Average wait time by priority tier - shows whether the priority
--    scheduling is actually working (higher priority should mean lower
--    average wait).
--
-- SELECT
--     s.priority,
--     COUNT(*) AS num_jobs,
--     AVG(st.timestamp - s.timestamp) AS avg_wait,
--     MAX(st.timestamp - s.timestamp) AS max_wait
-- FROM scheduling_events s
-- JOIN scheduling_events st
--     ON s.job_id = st.job_id AND st.event_type = 'started'
-- WHERE s.event_type = 'submitted'
-- GROUP BY s.priority
-- ORDER BY s.priority DESC;

-- 3. Preemption counts by node - which nodes are "hot" (high contention),
--    a candidate signal for whether the cluster needs more capacity of a
--    particular resource shape.
--
-- SELECT node_name, COUNT(*) AS preemption_count
-- FROM scheduling_events
-- WHERE event_type = 'preempted'
-- GROUP BY node_name
-- ORDER BY preemption_count DESC;

-- 4. Jobs that never started within the simulation window - the query
--    you'd run to catch starvation.
--
-- SELECT s.job_id, s.priority, s.timestamp AS submitted_at
-- FROM scheduling_events s
-- WHERE s.event_type = 'submitted'
--   AND NOT EXISTS (
--       SELECT 1 FROM scheduling_events st
--       WHERE st.job_id = s.job_id AND st.event_type = 'started'
--   )
-- ORDER BY s.timestamp;
