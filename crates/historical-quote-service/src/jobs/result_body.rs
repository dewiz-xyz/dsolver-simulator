use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use bytes::Bytes;
use futures::Stream;

use super::{JobRegistry, TerminalDelivery};

pub fn terminal_response_body(delivery: TerminalDelivery) -> Body {
    let (registry, job_id, bytes) = delivery.into_parts();
    Body::from_stream(DeliveryStream {
        state: DeliveryState::Data(Some(bytes)),
        guard: DeliveryGuard::new(registry, job_id),
    })
}

struct DeliveryStream {
    state: DeliveryState,
    guard: DeliveryGuard,
}

enum DeliveryState {
    Data(Option<Bytes>),
    Commit(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
}

impl Stream for DeliveryStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                DeliveryState::Data(bytes) => {
                    if let Some(bytes) = bytes.take() {
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    let registry = self.guard.registry.clone();
                    let job_id = self.guard.job_id;
                    self.state = DeliveryState::Commit(Box::pin(async move {
                        registry.consume(job_id).await;
                    }));
                }
                DeliveryState::Commit(future) => match future.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.guard.committed = true;
                        self.state = DeliveryState::Done;
                        return Poll::Ready(None);
                    }
                },
                DeliveryState::Done => return Poll::Ready(None),
            }
        }
    }
}

struct DeliveryGuard {
    registry: JobRegistry,
    job_id: uuid::Uuid,
    committed: bool,
}

impl DeliveryGuard {
    const fn new(registry: JobRegistry, job_id: uuid::Uuid) -> Self {
        Self {
            registry,
            job_id,
            committed: false,
        }
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let registry = self.registry.clone();
        let job_id = self.job_id;
        tokio::spawn(async move {
            registry.release_delivery(job_id).await;
        });
    }
}
