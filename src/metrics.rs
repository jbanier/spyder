use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Application-wide metrics tracked with atomic counters
#[derive(Clone)]
pub struct Metrics {
    pub pages_crawled: Arc<AtomicU64>,
    pub pages_failed: Arc<AtomicU64>,
    pub links_discovered: Arc<AtomicU64>,
    pub emails_found: Arc<AtomicU64>,
    pub crypto_refs_found: Arc<AtomicU64>,
    pub db_queries_executed: Arc<AtomicU64>,
    pub db_query_time_ms: Arc<AtomicU64>,
    pub work_units_processed: Arc<AtomicU64>,
    pub work_units_skipped: Arc<AtomicU64>,
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,
    start_time: Arc<Instant>,
}

/// Snapshot of current metrics for serialization
#[derive(Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MetricsSnapshot {
    pub pages_crawled: u64,
    pub pages_failed: u64,
    pub links_discovered: u64,
    pub emails_found: u64,
    pub crypto_refs_found: u64,
    pub db_queries_executed: u64,
    pub avg_db_query_ms: u64,
    pub total_db_query_time_ms: u64,
    pub work_units_processed: u64,
    pub work_units_skipped: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub uptime_seconds: u64,
}

impl Metrics {
    /// Create a new metrics instance
    pub fn new() -> Self {
        Self {
            pages_crawled: Arc::new(AtomicU64::new(0)),
            pages_failed: Arc::new(AtomicU64::new(0)),
            links_discovered: Arc::new(AtomicU64::new(0)),
            emails_found: Arc::new(AtomicU64::new(0)),
            crypto_refs_found: Arc::new(AtomicU64::new(0)),
            db_queries_executed: Arc::new(AtomicU64::new(0)),
            db_query_time_ms: Arc::new(AtomicU64::new(0)),
            work_units_processed: Arc::new(AtomicU64::new(0)),
            work_units_skipped: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            start_time: Arc::new(Instant::now()),
        }
    }

    /// Increment pages crawled counter
    pub fn increment_pages_crawled(&self) {
        self.pages_crawled.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment pages failed counter
    pub fn increment_pages_failed(&self) {
        self.pages_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to links discovered counter
    pub fn add_links_discovered(&self, count: u64) {
        self.links_discovered.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to emails found counter
    pub fn add_emails_found(&self, count: u64) {
        self.emails_found.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to crypto refs found counter
    pub fn add_crypto_refs_found(&self, count: u64) {
        self.crypto_refs_found.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a database query execution with duration
    pub fn record_db_query(&self, duration_ms: u64) {
        self.db_queries_executed.fetch_add(1, Ordering::Relaxed);
        self.db_query_time_ms.fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Increment work units processed counter
    pub fn increment_work_units_processed(&self) {
        self.work_units_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment work units skipped counter
    pub fn increment_work_units_skipped(&self) {
        self.work_units_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache hits counter
    pub fn increment_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache misses counter
    pub fn increment_cache_misses(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let db_queries = self.db_queries_executed.load(Ordering::Relaxed);
        let total_query_time = self.db_query_time_ms.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache_requests = cache_hits + cache_misses;
        let cache_hit_rate = if total_cache_requests > 0 {
            (cache_hits as f64 / total_cache_requests as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            pages_crawled: self.pages_crawled.load(Ordering::Relaxed),
            pages_failed: self.pages_failed.load(Ordering::Relaxed),
            links_discovered: self.links_discovered.load(Ordering::Relaxed),
            emails_found: self.emails_found.load(Ordering::Relaxed),
            crypto_refs_found: self.crypto_refs_found.load(Ordering::Relaxed),
            db_queries_executed: db_queries,
            avg_db_query_ms: if db_queries > 0 {
                total_query_time / db_queries
            } else {
                0
            },
            total_db_query_time_ms: total_query_time,
            work_units_processed: self.work_units_processed.load(Ordering::Relaxed),
            work_units_skipped: self.work_units_skipped.load(Ordering::Relaxed),
            cache_hits,
            cache_misses,
            cache_hit_rate,
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }

    /// Print a summary of current metrics to logs
    pub fn log_summary(&self) {
        let snapshot = self.snapshot();
        tracing::info!(
            pages_crawled = snapshot.pages_crawled,
            pages_failed = snapshot.pages_failed,
            links_discovered = snapshot.links_discovered,
            db_queries = snapshot.db_queries_executed,
            avg_query_ms = snapshot.avg_db_query_ms,
            uptime_secs = snapshot.uptime_seconds,
            "Metrics summary"
        );
    }
}
