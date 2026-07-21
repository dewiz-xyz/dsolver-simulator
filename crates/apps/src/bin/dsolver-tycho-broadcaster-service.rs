#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service = runtime::broadcaster::app::build_broadcaster_service().await?;
    let addr = std::net::SocketAddr::from((service.config.host, service.config.port));
    let app_state = service.app_state.clone();
    let app = rpc::create_broadcaster_router(app_state.clone());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let result = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(app_state.clone()))
        .await
        .map_err(|error| anyhow::anyhow!("Failed to start server: {error}"));
    if let Err(error) = app_state.finish_shutdown().await {
        eprintln!("Broadcaster shutdown drain failed: {error}");
    }
    result
}

async fn shutdown_signal(app_state: runtime::broadcaster::app::BroadcasterAppState) {
    if let Err(error) = wait_for_shutdown_signal().await {
        eprintln!("Failed to listen for shutdown signal: {error:#}");
        return;
    }
    // Stop producers now; state history drains after Axum finishes active requests.
    app_state.begin_shutdown();
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}
