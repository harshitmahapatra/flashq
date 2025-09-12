use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with an env-based filter and bridge `log` records.
///
/// - Reads `RUST_LOG` for level directives (e.g., "info", "debug,flashq=trace").
/// - Forwards `log` crate records to `tracing` via `LogTracer`.
/// - Sets up a compact formatter to stdout.
///
/// Safe to call multiple times; subsequent calls are no-ops.
pub fn init() {
    let _ = LogTracer::init();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer().with_target(true).compact();

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

/// Initialize a no-op tracing subscriber for benchmarks.
///
/// This sets up a subscriber that discards all events, ensuring zero overhead
/// from tracing instrumentation during benchmark runs.
///
/// Safe to call multiple times; subsequent calls are no-ops.
pub fn init_for_benchmarks() {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::new("off"))
        .try_init();
}
