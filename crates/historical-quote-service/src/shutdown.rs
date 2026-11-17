use std::future::Future;
use std::io;

use tokio::task::{JoinError, JoinHandle};

pub enum StopReason {
    Signal,
    ServerStopped(Result<io::Result<()>, JoinError>),
    RefreshStopped,
}

/// Waits for a shutdown signal or a critical task to stop.
pub async fn wait_for_stop(
    server: &mut JoinHandle<io::Result<()>>,
    refresher: &mut JoinHandle<()>,
    signal: impl Future<Output = ()>,
) -> StopReason {
    tokio::select! {
        result = server => StopReason::ServerStopped(result),
        _ = refresher => StopReason::RefreshStopped,
        () = signal => StopReason::Signal,
    }
}

pub async fn signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    let _ = result;
                }
                signal = terminate.recv() => {
                    let _ = signal;
                }
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn unexpected_refresh_exit_requests_shutdown() {
        let mut server = tokio::spawn(pending());
        let mut refresher = tokio::spawn(async {});
        assert!(matches!(
            wait_for_stop(&mut server, &mut refresher, pending()).await,
            StopReason::RefreshStopped
        ));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn signal_leaves_tasks_for_graceful_shutdown() {
        let mut server = tokio::spawn(pending());
        let mut refresher = tokio::spawn(pending());
        assert!(matches!(
            wait_for_stop(&mut server, &mut refresher, async {}).await,
            StopReason::Signal
        ));
        assert!(!server.is_finished());
        assert!(!refresher.is_finished());
        server.abort();
        refresher.abort();
        let _ = server.await;
        let _ = refresher.await;
    }
}
