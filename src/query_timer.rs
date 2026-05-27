use std::time::Instant;
use tracing::warn;

/// Threshold for logging slow queries (in milliseconds)
pub const SLOW_QUERY_THRESHOLD_MS: u64 = 100;

/// Helper to time and log slow database queries
pub struct QueryTimer {
    operation: &'static str,
    started_at: Instant,
}

impl QueryTimer {
    /// Start timing a database operation
    pub fn start(operation: &'static str) -> Self {
        Self {
            operation,
            started_at: Instant::now(),
        }
    }

    /// Finish timing and log if the query was slow
    pub fn finish(self) -> u64 {
        let elapsed_ms = self.started_at.elapsed().as_millis() as u64;

        if elapsed_ms > SLOW_QUERY_THRESHOLD_MS {
            warn!(
                operation = self.operation,
                duration_ms = elapsed_ms,
                "Slow database query"
            );
        }

        elapsed_ms
    }

    /// Finish timing, log if slow, and record to metrics
    pub fn finish_with_metrics(self, metrics: &crate::metrics::Metrics) -> u64 {
        let elapsed_ms = self.finish();
        metrics.record_db_query(elapsed_ms);
        elapsed_ms
    }
}

/// Macro to wrap a query execution with timing
#[macro_export]
macro_rules! timed_query {
    ($operation:expr, $query:expr) => {{
        let timer = $crate::query_timer::QueryTimer::start($operation);
        let result = $query;
        timer.finish();
        result
    }};
}

/// Macro to wrap a query execution with timing and metrics
#[macro_export]
macro_rules! timed_query_with_metrics {
    ($operation:expr, $metrics:expr, $query:expr) => {{
        let timer = $crate::query_timer::QueryTimer::start($operation);
        let result = $query;
        timer.finish_with_metrics($metrics);
        result
    }};
}
