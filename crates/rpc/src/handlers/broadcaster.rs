use std::collections::VecDeque;

use axum::{
    extract::{
        rejection::JsonRejection,
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use runtime::{
    broadcaster_service::BroadcasterAppState,
    models::broadcaster::BroadcasterReadiness,
    services::broadcaster_sessions::{BroadcasterSessionRegistration, SessionCloseReason},
};
use serde_json::json;
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterTokenLookupRequest,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::warn;

use crate::models::broadcaster_rpc::BroadcasterStatusPayload;

pub async fn status(
    State(state): State<BroadcasterAppState>,
) -> (StatusCode, Json<BroadcasterStatusPayload>) {
    let snapshot = state.status_snapshot().await;
    let status_code = readiness_status_code(snapshot.readiness);

    (status_code, Json(BroadcasterStatusPayload::from(snapshot)))
}

pub async fn ws(ws: WebSocketUpgrade, State(state): State<BroadcasterAppState>) -> Response {
    let snapshot = state.status_snapshot().await;
    if snapshot.readiness != BroadcasterReadiness::Ready {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(BroadcasterStatusPayload::from(snapshot)),
        )
            .into_response();
    }

    let registration = match state.subscribe().await {
        Ok(Some(registration)) => registration,
        Ok(None) => {
            let snapshot = state.status_snapshot().await;
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(BroadcasterStatusPayload::from(snapshot)),
            )
                .into_response();
        }
        Err(error) => {
            warn!(error = %error, "Failed to register broadcaster websocket subscriber");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_session(socket, state, registration))
        .into_response()
}

pub async fn token_lookup(
    State(state): State<BroadcasterAppState>,
    payload: Result<Json<BroadcasterTokenLookupRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid token lookup request: {error}") })),
            )
                .into_response()
        }
    };

    if request.chain_id != state.chain_id() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!(
                    "token lookup chain_id {} does not match broadcaster chain_id {}",
                    request.chain_id,
                    state.chain_id()
                )
            })),
        )
            .into_response();
    }

    if let Some(address) = request.addresses.iter().find(|address| address.len() != 20) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("token lookup address {address} is not a 20-byte EVM address")
            })),
        )
            .into_response();
    }

    match state.lookup_tokens(request.addresses).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => {
            warn!(error = %error, "Broadcaster token lookup failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn token_snapshot(State(state): State<BroadcasterAppState>) -> Response {
    (StatusCode::OK, Json(state.token_snapshot().await)).into_response()
}

async fn handle_session(
    socket: WebSocket,
    state: BroadcasterAppState,
    registration: BroadcasterSessionRegistration,
) {
    let BroadcasterSessionRegistration {
        session_id,
        stream_id,
        snapshot_payloads,
        receiver,
        close_receiver,
    } = registration;
    let session_stream = BroadcasterSessionStream::new(stream_id, snapshot_payloads, receiver);
    let (sender, receiver) = socket.split();

    drive_session(sender, receiver, close_receiver, session_stream).await;
    state.remove_subscriber(session_id).await;
}

async fn drive_session(
    sender: SplitSink<WebSocket, Message>,
    receiver: SplitStream<WebSocket>,
    close_receiver: oneshot::Receiver<SessionCloseReason>,
    session_stream: BroadcasterSessionStream,
) {
    let mut send_task = tokio::spawn(pump_session(sender, close_receiver, session_stream));
    let mut receive_task = tokio::spawn(watch_client_disconnect(receiver));

    tokio::select! {
        _ = &mut send_task => receive_task.abort(),
        _ = &mut receive_task => send_task.abort(),
    }
    await_join(send_task).await;
    await_join(receive_task).await;
}

async fn pump_session(
    mut sender: SplitSink<WebSocket, Message>,
    mut close_receiver: oneshot::Receiver<SessionCloseReason>,
    mut session_stream: BroadcasterSessionStream,
) {
    loop {
        tokio::select! {
            _ = &mut close_receiver => break,
            maybe_envelope = session_stream.next_envelope() => {
                let Some(envelope) = maybe_envelope else {
                    break;
                };
                let text = match serde_json::to_string(&envelope) {
                    Ok(text) => text,
                    Err(error) => {
                        warn!(error = %error, "Failed to serialize broadcaster envelope");
                        break;
                    }
                };

                if sender.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn watch_client_disconnect(mut receiver: SplitStream<WebSocket>) {
    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {}
        }
    }
}

fn readiness_status_code(readiness: BroadcasterReadiness) -> StatusCode {
    if readiness == BroadcasterReadiness::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

struct BroadcasterSessionStream {
    stream_id: String,
    next_message_seq: u64,
    snapshot_payloads: VecDeque<BroadcasterPayload>,
    receiver: mpsc::Receiver<BroadcasterPayload>,
}

impl BroadcasterSessionStream {
    fn new(
        stream_id: String,
        snapshot_payloads: Vec<BroadcasterPayload>,
        receiver: mpsc::Receiver<BroadcasterPayload>,
    ) -> Self {
        Self {
            stream_id,
            next_message_seq: 1,
            snapshot_payloads: snapshot_payloads.into_iter().collect(),
            receiver,
        }
    }

    async fn next_envelope(&mut self) -> Option<BroadcasterEnvelope> {
        // New sessions must see the exported snapshot before any live traffic.
        if let Some(payload) = self.snapshot_payloads.pop_front() {
            return Some(self.wrap(payload));
        }

        self.receiver.recv().await.map(|payload| self.wrap(payload))
    }

    fn wrap(&mut self, payload: BroadcasterPayload) -> BroadcasterEnvelope {
        let envelope =
            BroadcasterEnvelope::new(self.stream_id.clone(), self.next_message_seq, payload);
        self.next_message_seq = self.next_message_seq.saturating_add(1);
        envelope
    }
}

async fn await_join(task: JoinHandle<()>) {
    let _ = task.await;
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use tokio::sync::mpsc;

    use super::BroadcasterSessionStream;
    use simulator_core::broadcaster::{
        BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterPayload, BroadcasterSnapshotEnd,
        BroadcasterSnapshotStart,
    };

    #[tokio::test]
    async fn session_stream_sends_snapshot_before_live_payloads() -> Result<()> {
        let (sender, receiver) = mpsc::channel(4);
        let mut session_stream = BroadcasterSessionStream::new(
            "stream-7".to_string(),
            vec![
                BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                    "snapshot-7",
                    1,
                    vec![],
                    0,
                )?),
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-7")),
            ],
            receiver,
        );

        sender
            .send(BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                1,
                "snapshot-7",
                vec![],
            )?))
            .await?;

        assert_envelope(
            session_stream.next_envelope().await,
            BroadcasterEnvelope::new(
                "stream-7",
                1,
                BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                    "snapshot-7",
                    1,
                    vec![],
                    0,
                )?),
            ),
        )?;
        assert_envelope(
            session_stream.next_envelope().await,
            BroadcasterEnvelope::new(
                "stream-7",
                2,
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-7")),
            ),
        )?;
        assert_envelope(
            session_stream.next_envelope().await,
            BroadcasterEnvelope::new(
                "stream-7",
                3,
                BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(1, "snapshot-7", vec![])?),
            ),
        )?;
        Ok(())
    }

    fn assert_envelope(
        found: Option<BroadcasterEnvelope>,
        expected: BroadcasterEnvelope,
    ) -> Result<()> {
        let found = found.ok_or_else(|| anyhow!("expected broadcaster envelope"))?;
        let found = serde_json::to_value(found)?;
        let expected = serde_json::to_value(expected)?;
        assert_eq!(found, expected);
        Ok(())
    }
}
