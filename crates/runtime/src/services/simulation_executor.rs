use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tokio::sync::oneshot;

#[derive(Debug, Default, Clone)]
pub(crate) struct SimulationExecutor;

#[derive(Debug)]
pub(crate) enum SimulationExecutionError {
    WorkerPanicked(Box<dyn Any + Send + 'static>),
    ResultDropped,
}

impl SimulationExecutor {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) async fn run<R, F>(self, work: F) -> Result<R, SimulationExecutionError>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();

        rayon::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(work))
                .map_err(SimulationExecutionError::WorkerPanicked);
            let _ = result_tx.send(result);
        });

        result_rx
            .await
            .map_err(|_| SimulationExecutionError::ResultDropped)?
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::{SimulationExecutionError, SimulationExecutor};

    #[tokio::test]
    async fn run_returns_rayon_result() {
        let result = SimulationExecutor::new().run(|| 7).await;

        assert!(matches!(result, Ok(7)));
    }

    #[tokio::test]
    async fn run_executes_on_rayon_worker() {
        let caller_thread = thread::current().id();
        let result = SimulationExecutor::new()
            .run(move || thread::current().id() != caller_thread)
            .await;

        assert!(matches!(result, Ok(true)));
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "test intentionally verifies worker panic propagation"
    )]
    async fn run_reports_worker_panic() {
        let result = SimulationExecutor::new()
            .run(|| panic!("simulation worker panic"))
            .await;

        assert!(matches!(
            result,
            Err(SimulationExecutionError::WorkerPanicked(_))
        ));
    }
}
