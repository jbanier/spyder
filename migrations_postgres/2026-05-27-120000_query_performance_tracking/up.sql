-- Query Performance Tracking Table
-- Stores historical query performance data for analysis and optimization

CREATE TABLE IF NOT EXISTS query_log (
    id SERIAL PRIMARY KEY,
    query_name TEXT NOT NULL,
    duration_ms BIGINT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying by time range (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_query_log_executed_at
    ON query_log(executed_at DESC);

-- Index for finding slow queries by name
CREATE INDEX IF NOT EXISTS idx_query_log_name_duration
    ON query_log(query_name, duration_ms DESC);

-- Partial index for very slow queries (>1 second)
CREATE INDEX IF NOT EXISTS idx_query_log_very_slow
    ON query_log(duration_ms DESC, query_name)
    WHERE duration_ms > 1000;

-- Add helpful comment
COMMENT ON TABLE query_log IS 'Historical query performance tracking for optimization analysis';
