use std::env;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    prelude::*,
    EnvFilter,
};

/// Initialize the logging system with tracing
pub fn init_logging() {
    // Cargo can enable both rustls providers, so choose aws-lc-rs before any rediss:// handshake.
    // An already-installed default is fine and keeps repeated startup calls harmless.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let log_level = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    let timer = fmt::time::UtcTime::rfc_3339();

    let subscriber = tracing_subscriber::registry().with(filter).with(
        fmt::layer()
            .json()
            .with_timer(timer)
            .with_line_number(true)
            .with_span_events(FmtSpan::CLOSE),
    );

    if let Err(error) = subscriber.try_init() {
        eprintln!("Failed to initialize tracing subscriber: {error}");
    }
}
