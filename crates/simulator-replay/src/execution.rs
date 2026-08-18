use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tokio::sync::oneshot;

#[derive(Debug, Default, Clone)]
pub struct SimulationExecutor;

#[derive(Debug)]
pub enum SimulationExecutionError {
    WorkerPanicked(Box<dyn Any + Send + 'static>),
    ResultDropped,
}

impl SimulationExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn run<R, F>(self, work: F) -> Result<R, SimulationExecutionError>
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
