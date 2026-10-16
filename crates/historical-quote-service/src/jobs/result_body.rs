use std::convert::Infallible;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use historical_quote::api::{JobEnvelope, JobResult};
use serde::ser::Error as _;

use super::{JobRegistry, TerminalDelivery};

pub fn terminal_response_body(delivery: TerminalDelivery) -> Body {
    let (registry, job_id, parts) = delivery.into_parts();
    Body::from_stream(DeliveryStream {
        state: DeliveryState::Data(parts.into_iter()),
        guard: DeliveryGuard::new(registry, job_id),
    })
}

/// A terminal response body, the envelope with a result written apart placed
/// in it, so no result is written while the registry is locked.
#[derive(Clone)]
pub(crate) struct TerminalBody {
    parts: Vec<Bytes>,
}

impl TerminalBody {
    /// The body of `envelope`, which carries no result, with `result` as its
    /// last field.
    pub(crate) fn new(envelope: &JobEnvelope, result: Option<Bytes>) -> serde_json::Result<Self> {
        let head = serde_json::to_vec(envelope)?;
        let Some(result) = result else {
            return Ok(Self {
                parts: vec![Bytes::from(head)],
            });
        };
        let open = head
            .strip_suffix(b"}")
            .ok_or_else(|| serde_json::Error::custom("an envelope is a JSON object"))?;
        Ok(Self {
            parts: vec![
                Bytes::copy_from_slice(open),
                Bytes::from_static(b",\"result\":"),
                result,
                Bytes::from_static(b"}"),
            ],
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.parts
            .iter()
            .map(|part| u64::try_from(part.len()).unwrap_or(u64::MAX))
            .fold(0, u64::saturating_add)
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        for part in &self.parts {
            bytes.extend_from_slice(part);
        }
        bytes.freeze()
    }

    pub(crate) fn into_parts(self) -> Vec<Bytes> {
        self.parts
    }
}

/// Why a result has no body.
pub(crate) enum ResultBodyError {
    /// The result is larger than the bytes the service keeps.
    TooLarge,
    Unwritable,
}

/// `result` written as JSON, refused once it passes `max_bytes`.
pub(crate) fn result_body(result: &JobResult, max_bytes: u64) -> Result<Bytes, ResultBodyError> {
    let mut body = BoundedBuffer {
        bytes: Vec::new(),
        max_bytes,
        exceeded: false,
    };
    match serde_json::to_writer(&mut body, result) {
        Ok(()) => Ok(Bytes::from(body.bytes)),
        Err(_) if body.exceeded => Err(ResultBodyError::TooLarge),
        Err(_) => Err(ResultBodyError::Unwritable),
    }
}

/// A buffer that refuses to grow past `max_bytes`.
struct BoundedBuffer {
    bytes: Vec<u8>,
    max_bytes: u64,
    exceeded: bool,
}

impl Write for BoundedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.bytes.len().saturating_add(buf.len());
        if u64::try_from(len).map_or(true, |len| len > self.max_bytes) {
            self.exceeded = true;
            return Err(io::Error::other("the result passes the kept bytes"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DeliveryStream {
    state: DeliveryState,
    guard: DeliveryGuard,
}

enum DeliveryState {
    Data(std::vec::IntoIter<Bytes>),
    Commit(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
}

impl Stream for DeliveryStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                DeliveryState::Data(parts) => {
                    if let Some(part) = parts.next() {
                        return Poll::Ready(Some(Ok(part)));
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

#[cfg(test)]
mod tests {
    use historical_quote::api::{
        JobEnvelope, JobResult, RawJson, StoredMessage, StoredMessagesResult, StreamPosition,
    };
    use serde_json::value::RawValue;

    use super::{result_body, ResultBodyError, TerminalBody};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// A body written in parts is the whole envelope written at once, the
    /// stored bytes of its result kept.
    #[test]
    fn a_body_in_parts_is_the_envelope_written_whole() -> TestResult {
        let result = stored_messages()?;
        let mut envelope: JobEnvelope = serde_json::from_value(serde_json::json!({
            "jobId": "997b4517-2377-4c83-91a8-577e919ce72a",
            "requestId": "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa",
            "jobType": "storedMessages",
            "state": "completed",
            "submittedAt": "2026-08-13T15:00:00Z",
            "deadlineAt": "2026-08-13T15:02:00Z",
            "finishedAt": "2026-08-13T15:00:02Z",
            "cancellationRequested": false,
            "progress": {"percentComplete": 100}
        }))?;
        let Ok(written) = result_body(&result, u64::MAX) else {
            return Err("an unbounded result is written".into());
        };
        let body = TerminalBody::new(&envelope, Some(written))?;

        envelope.result = Some(result);
        let whole = serde_json::to_vec(&envelope)?;
        assert_eq!(body.to_bytes(), whole);
        assert_eq!(body.len(), u64::try_from(whole.len())?);
        Ok(())
    }

    /// A result past its limit is refused, and one at its limit is written.
    #[test]
    fn a_result_past_its_limit_is_refused() -> TestResult {
        let result = stored_messages()?;
        let whole = u64::try_from(serde_json::to_vec(&result)?.len())?;

        assert!(matches!(
            result_body(&result, whole - 1),
            Err(ResultBodyError::TooLarge)
        ));
        assert!(result_body(&result, whole).is_ok());
        Ok(())
    }

    fn stored_messages() -> Result<JobResult, serde_json::Error> {
        let position = |message_seq| StreamPosition {
            generation: 9,
            message_seq,
        };
        Ok(JobResult::StoredMessages(StoredMessagesResult {
            after: position(41),
            through: position(44),
            messages: vec![StoredMessage {
                position: position(43),
                envelope: RawJson::from(RawValue::from_string(
                    r#"{"stream_id":"s",  "message_seq":43}"#.to_owned(),
                )?),
            }],
        }))
    }
}
