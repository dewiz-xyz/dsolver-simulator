use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const BROADCASTER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<()> {
    let service = runtime::broadcaster::app::build_broadcaster_service().await?;
    let addr = std::net::SocketAddr::from((service.config.host, service.config.port));
    let app_state = service.app_state;
    let mut tasks = service.tasks;
    let app = rpc::create_broadcaster_router(app_state.clone());

    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            app_state.begin_shutdown();
            let shutdown_result = run_with_shutdown_deadline(
                async {
                    let task_result = tasks.stop_and_wait().await;
                    app_state.stop_recovery_tasks().await;
                    app_state.shutdown_state_history().await;
                    task_result
                },
                BROADCASTER_SHUTDOWN_TIMEOUT,
            )
            .await;
            return combine_service_and_shutdown_results(Err(error.into()), shutdown_result);
        }
    };

    let (server_stop, server_stopped) = tokio::sync::oneshot::channel::<()>();
    let server = std::future::IntoFuture::into_future(
        axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async move {
            let _ = server_stopped.await;
        }),
    );
    tokio::pin!(server);

    let (service_result, server_finished) = {
        let feed = tasks.wait_for_feed();
        tokio::pin!(feed);
        tokio::select! {
            biased;
            result = &mut server => {
                (
                    result.map_err(|error| anyhow::anyhow!("Broadcaster server failed: {error}")),
                    true,
                )
            }
            result = &mut feed => {
                (
                    result.and_then(|()| {
                        Err(anyhow::anyhow!("Broadcaster feed task stopped unexpectedly"))
                    }),
                    false,
                )
            }
            () = shutdown_signal() => (Ok(()), false),
        }
    };

    app_state.begin_shutdown();
    let _ = server_stop.send(());
    // Process exit is the reliable socket cleanup boundary with the current Tycho client.
    let shutdown_result = run_with_shutdown_deadline(
        async {
            let server_shutdown = async {
                if server_finished {
                    Ok(())
                } else {
                    server
                        .await
                        .map_err(|error| anyhow::anyhow!("Broadcaster server failed: {error}"))
                }
            };
            let task_shutdown = async {
                let task_result = tasks.stop_and_wait().await;
                app_state.stop_recovery_tasks().await;
                app_state.shutdown_state_history().await;
                task_result
            };
            let (server_result, task_result) = tokio::join!(server_shutdown, task_shutdown);
            server_result.and(task_result)
        },
        BROADCASTER_SHUTDOWN_TIMEOUT,
    )
    .await;

    combine_service_and_shutdown_results(service_result, shutdown_result)
}

async fn run_with_shutdown_deadline<F>(shutdown: F, deadline: Duration) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    tokio::time::timeout(deadline, shutdown)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Broadcaster shutdown exceeded {} seconds",
                deadline.as_secs()
            )
        })?
}

fn combine_service_and_shutdown_results(
    service_result: Result<()>,
    shutdown_result: Result<()>,
) -> Result<()> {
    match (service_result, shutdown_result) {
        (Err(service_error), Err(shutdown_error)) => {
            tracing::error!(
                event = "broadcaster_shutdown_failed",
                error = %shutdown_error,
                "Broadcaster shutdown also failed"
            );
            Err(service_error)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn shutdown_deadline_bounds_pending_cleanup() -> Result<()> {
        let result = run_with_shutdown_deadline(
            std::future::pending::<Result<()>>(),
            BROADCASTER_SHUTDOWN_TIMEOUT,
        )
        .await;
        let Err(error) = result else {
            return Err(anyhow::anyhow!(
                "pending cleanup should hit the shutdown deadline"
            ));
        };

        assert!(format!("{error:#}").contains("exceeded 20 seconds"));
        Ok(())
    }

    #[test]
    fn service_error_remains_primary_when_shutdown_also_fails() -> Result<()> {
        let result = combine_service_and_shutdown_results(
            Err(anyhow::anyhow!("planned feed failure")),
            Err(anyhow::anyhow!("planned shutdown failure")),
        );
        let Err(error) = result else {
            return Err(anyhow::anyhow!("service error should be returned"));
        };

        assert_eq!(error.to_string(), "planned feed failure");
        Ok(())
    }
}
