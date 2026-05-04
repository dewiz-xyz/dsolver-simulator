use std::time::{Duration, Instant};

use tokio::sync::OwnedSemaphorePermit;
use tokio::task::{spawn_blocking, JoinError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct BlockingSimulationExecutor;

#[derive(Debug)]
pub(crate) enum BlockingSimulationError {
    Cancelled,
    Join(JoinError),
    TimedOut,
}

impl BlockingSimulationExecutor {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) async fn run_until<R, F>(
        self,
        deadline: Instant,
        cancel_token: CancellationToken,
        permit: OwnedSemaphorePermit,
        work: F,
    ) -> Result<R, BlockingSimulationError>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let sleep_duration = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::from_millis(0));
        let handle = spawn_blocking(move || {
            let _permit = permit;
            work()
        });
        tokio::pin!(handle);

        tokio::select! {
            res = handle.as_mut() => {
                res.map_err(BlockingSimulationError::Join)
            }
            _ = cancel_token.cancelled() => {
                cancel_token.cancel();
                handle.as_mut().abort();
                Err(BlockingSimulationError::Cancelled)
            }
            _ = sleep(sleep_duration) => {
                cancel_token.cancel();
                handle.as_mut().abort();
                Err(BlockingSimulationError::TimedOut)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::Semaphore;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::{BlockingSimulationError, BlockingSimulationExecutor};

    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "executor tests use explicit panics for fixture setup failures"
    )]
    async fn run_until_returns_blocking_result() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(err) => panic!("test semaphore permit should be available: {err}"),
        };
        let result = BlockingSimulationExecutor::new()
            .run_until(
                Instant::now() + Duration::from_secs(1),
                CancellationToken::new(),
                permit,
                || 7,
            )
            .await;

        assert!(matches!(result, Ok(7)));
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "executor tests use explicit panics for fixture setup failures"
    )]
    async fn run_until_timeout_cancels_work_and_releases_permit() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(err) => panic!("test semaphore permit should be available: {err}"),
        };
        let cancel_token = CancellationToken::new();
        let worker_cancel = cancel_token.clone();
        let (observed_cancel_tx, observed_cancel_rx) = tokio::sync::oneshot::channel();

        let result = BlockingSimulationExecutor::new()
            .run_until(
                Instant::now() + Duration::from_millis(10),
                cancel_token.clone(),
                permit,
                move || {
                    while !worker_cancel.is_cancelled() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    let _ = observed_cancel_tx.send(());
                },
            )
            .await;

        assert!(matches!(result, Err(BlockingSimulationError::TimedOut)));
        assert!(cancel_token.is_cancelled());
        match timeout(Duration::from_millis(100), observed_cancel_rx).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("worker dropped cancellation observer: {err}"),
            Err(err) => panic!("worker did not observe cancellation: {err}"),
        }
        let semaphore_for_reacquire = Arc::clone(&semaphore);
        match timeout(
            Duration::from_millis(100),
            semaphore_for_reacquire.acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => drop(permit),
            Ok(Err(err)) => panic!("test semaphore closed unexpectedly: {err}"),
            Err(err) => panic!("blocking permit was not released: {err}"),
        }
    }
}
