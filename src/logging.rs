use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize tracing subscriber for structured logging
///
/// Configure log level via RUST_LOG environment variable:
/// - RUST_LOG=spyder=debug (debug level for spyder only)
/// - RUST_LOG=debug (debug level for all crates)
/// - RUST_LOG=spyder=info (default if not set)
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("spyder=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_thread_ids(false)
                .with_line_number(true),
        )
        .init();
}
