use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use historical_quote_service::app::{
    router, AppState, EngineCoverageResolver, EngineJobExecutor, ReaderStatusProbe,
};
use historical_quote_service::auth::{BearerKeySetReloader, SecretsManagerKeySetSource};
use historical_quote_service::config::ServiceConfig;
use historical_quote_service::database::{
    IamConnectionPool, PostgresConnectionFactory, RdsIamTokenProvider,
};
use historical_quote_service::jobs::{JobExecutor, JobRegistry, JobRunner};
use historical_quote_service::shutdown::{self, StopReason};
use state_history::{CheckpointObjectStore, StateHistoryReader};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;

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
    let key_source = Arc::new(SecretsManagerKeySetSource::new(
        aws_sdk_secretsmanager::Client::new(&sdk_config),
        config.bearer_key_set_secret_arn.clone(),
    ));
    let (bearer_keys, reloader) = BearerKeySetReloader::load(key_source).await?;
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
    let app = router(AppState {
        config: Arc::clone(&config),
        bearer_keys,
        registry: registry.clone(),
        coverage: Arc::new(EngineCoverageResolver::new(Arc::clone(&reader))),
        status: Arc::new(ReaderStatusProbe::new(
            reader,
            config.supported_chain_id,
            &config.enabled_backends,
        )),
    });
    let listener = TcpListener::bind(config.bind_address)
        .await
        .context("failed to bind historical quote service")?;
    serve(
        listener,
        app,
        registry,
        runner,
        reloader,
        config.shutdown_timeout,
    )
    .await
}

#[expect(
    clippy::cognitive_complexity,
    reason = "Keep the shared shutdown deadline and ordered abort/join steps together."
)]
async fn serve<E: JobExecutor>(
    listener: TcpListener,
    app: Router,
    registry: JobRegistry,
    runner: JobRunner<E>,
    reloader: BearerKeySetReloader,
    shutdown_timeout: Duration,
) -> anyhow::Result<()> {
    let mut runner_task = tokio::spawn(runner.run());
    let cancel_refresh = CancellationToken::new();
    let mut refresh_task = tokio::spawn(reloader.run(cancel_refresh.clone()));
    let (stop_server, wait_for_stop) = oneshot::channel();
    let mut server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = wait_for_stop.await;
            })
            .await
    });
    let stop_reason =
        shutdown::wait_for_stop(&mut server_task, &mut refresh_task, shutdown::signal()).await;
    registry.begin_shutdown().await;
    cancel_refresh.cancel();
    let deadline = Instant::now()
        .checked_add(shutdown_timeout)
        .ok_or_else(|| anyhow::anyhow!("shutdown timeout is too large"))?;
    let refresh_stopped = matches!(stop_reason, StopReason::RefreshStopped);
    let server_result = if let StopReason::ServerStopped(result) = stop_reason {
        Some(result)
    } else {
        let _ = stop_server.send(());
        timeout_at(deadline, &mut server_task).await.ok()
    };
    if server_result.is_none() {
        tracing::warn!("HTTP shutdown exceeded the configured deadline");
        server_task.abort();
        let _ = server_task.await;
    }
    if !refresh_stopped && timeout_at(deadline, &mut refresh_task).await.is_err() {
        tracing::warn!("key refresh shutdown exceeded the configured deadline");
        refresh_task.abort();
        let _ = refresh_task.await;
    }
    if timeout_at(deadline, &mut runner_task).await.is_err() {
        tracing::warn!("job shutdown exceeded the configured deadline");
        runner_task.abort();
        let _ = runner_task.await;
    }
    if let Some(result) = server_result {
        result.context("historical quote server task stopped")??;
    }
    if refresh_stopped {
        anyhow::bail!("historical quote key refresh task stopped unexpectedly");
    }
    Ok(())
}
