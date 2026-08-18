use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, LazyLock};

use tokio::sync::{oneshot, Mutex, OwnedMutexGuard};

static VM_WORLD_LEASE: LazyLock<Arc<Mutex<()>>> = LazyLock::new(|| Arc::new(Mutex::new(())));

#[derive(Debug, Default, Clone)]
pub struct SimulationExecutor;

#[derive(Debug)]
pub enum SimulationExecutionError {
    WorkerPanicked(Box<dyn Any + Send + 'static>),
    ResultDropped,
}

/// Exclusive ownership of Tycho's process-wide VM state.
pub struct VmWorldLease {
    _guard: OwnedMutexGuard<()>,
}

/// Acquires the process-wide VM lease for one complete historical world.
pub async fn acquire_vm_world_lease() -> VmWorldLease {
    VmWorldLease {
        _guard: Arc::clone(&VM_WORLD_LEASE).lock_owned().await,
    }
}

/// Runs one complete VM-world operation while holding the process-wide lease.
pub async fn with_vm_world<T, E>(operation: impl Future<Output = Result<T, E>>) -> Result<T, E> {
    let _lease = acquire_vm_world_lease().await;
    operation.await
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
