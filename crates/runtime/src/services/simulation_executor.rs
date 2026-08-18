pub(crate) use simulator_replay::{SimulationExecutionError, SimulationExecutor};

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
