use std::sync::Arc;

use anyhow::Context;
use historical_quote_service::app::{
    router, AppState, EngineCoverageResolver, EngineJobExecutor, ReaderStatusProbe,
};
use historical_quote_service::config::ServiceConfig;
use historical_quote_service::database::{
    IamConnectionPool, PostgresConnectionFactory, RdsIamTokenProvider,
};
use historical_quote_service::jobs::{JobRegistry, JobRunner};
use historical_quote_service::shutdown;
use state_history::{CheckpointObjectStore, StateHistoryReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|_| anyhow::anyhow!("failed to initialize structured logging"))?;
    let config = Arc::new(ServiceConfig::from_env()?);
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.object_store.region.clone()))
        .load()
        .await;
    let token_provider = RdsIamTokenProvider::new(&config.database, sdk_config)?;
    let connection_factory = PostgresConnectionFactory::new(&config.database);
    let connections = IamConnectionPool::new(
        token_provider,
        connection_factory,
        config.database.max_connections,
        config.database.max_connection_lifetime,
    )?;
    let objects = CheckpointObjectStore::from_env_config(
        config.object_store.bucket.clone(),
        config.object_store.prefix.clone(),
        config.object_store.region.clone(),
        config.object_store.endpoint_url.clone(),
        config.object_store.force_path_style,
    )
    .await;
    let reader = Arc::new(StateHistoryReader::new(connections, objects));
    let executor = Arc::new(
        EngineJobExecutor::new(Arc::clone(&reader), &config)
            .map_err(|_| anyhow::anyhow!("historical engine configuration is invalid"))?,
    );
    let registry = JobRegistry::new(config.scheduler.clone());
    let runner = JobRunner::new(registry.clone(), executor);
    let mut runner_task = tokio::spawn(runner.run());
    let app = router(AppState {
        config: Arc::clone(&config),
        registry: registry.clone(),
        coverage: Arc::new(EngineCoverageResolver::new(Arc::clone(&reader))),
        status: Arc::new(ReaderStatusProbe::new(
            reader,
            config.supported_chain_id,
            &config.enabled_backends,
        )),
    });
    let listener = tokio::net::TcpListener::bind(config.bind_address)
        .await
        .context("failed to bind historical quote service")?;
    let (stop_server, wait_for_stop) = tokio::sync::oneshot::channel();
    let mut server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = wait_for_stop.await;
            })
            .await
    });
    let early_server_result = tokio::select! {
        result = &mut server_task => Some(result),
        () = shutdown::signal() => None,
    };
    registry.begin_shutdown().await;
    let deadline = tokio::time::Instant::now()
        .checked_add(config.shutdown_timeout)
        .ok_or_else(|| anyhow::anyhow!("shutdown timeout is too large"))?;
    let server_result = match early_server_result {
        Some(result) => Some(result),
        None => {
            let _ = stop_server.send(());
            match tokio::time::timeout_at(deadline, &mut server_task).await {
                Ok(result) => Some(result),
                Err(_) => {
                    tracing::warn!("HTTP shutdown exceeded the configured deadline");
                    server_task.abort();
                    None
                }
            }
        }
    };
    if tokio::time::timeout_at(deadline, &mut runner_task)
        .await
        .is_err()
    {
        tracing::warn!("job shutdown exceeded the configured deadline");
        runner_task.abort();
    }
    if let Some(result) = server_result {
        result.context("historical quote server task stopped")??;
    }
    Ok(())
}
