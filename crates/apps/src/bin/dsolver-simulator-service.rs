#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service = runtime::simulator_service::build_simulator_service().await?;
    let addr = std::net::SocketAddr::from((service.config.host, service.config.port));
    let app = rpc::create_router(service.runtime);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service())
        .await
        .map_err(|error| anyhow::anyhow!("Failed to start server: {error}"))
}
