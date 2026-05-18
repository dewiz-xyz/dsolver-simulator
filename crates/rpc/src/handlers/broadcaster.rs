use std::collections::HashMap;

use axum::{
    extract::{
        rejection::JsonRejection,
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
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
    broadcaster::state::BroadcasterReadiness,
    broadcaster_service::BroadcasterAppState,
    services::broadcaster_sessions::{
        BroadcasterAttachedSession, SessionCloseReason, SnapshotSessionError,
    },
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

pub async fn create_snapshot_session(State(state): State<BroadcasterAppState>) -> Response {
    match state.create_snapshot_session().await {
        Ok(Some(session)) => (StatusCode::CREATED, Json(session)).into_response(),
        Ok(None) => {
            let snapshot = state.status_snapshot().await;
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(BroadcasterStatusPayload::from(snapshot)),
            )
                .into_response()
        }
        Err(error) => {
            warn!(error = %error, "Failed to create broadcaster snapshot session");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn snapshot_session_payload(
    State(state): State<BroadcasterAppState>,
    Path((session_id, index)): Path<(u64, u32)>,
) -> Response {
    match state.snapshot_session_payload(session_id, index).await {
        Ok(envelope) => (StatusCode::OK, Json(envelope)).into_response(),
        Err(error) => snapshot_session_error_response(error, StatusCode::GONE),
    }
}

pub async fn ws(
    ws: WebSocketUpgrade,
    State(state): State<BroadcasterAppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(session_id) = query.get("sessionId") else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing sessionId" })),
        )
            .into_response();
    };
    let Ok(session_id) = session_id.parse::<u64>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid sessionId" })),
        )
            .into_response();
    };

    let registration = match state.attach_snapshot_session(session_id).await {
        Ok(registration) => registration,
        Err(error) => return snapshot_session_error_response(error, StatusCode::CONFLICT),
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
    registration: BroadcasterAttachedSession,
) {
    let BroadcasterAttachedSession {
        session_id,
        stream_id,
        next_message_seq,
        receiver,
        close_receiver,
    } = registration;
    let session_stream = BroadcasterSessionStream::new(stream_id, next_message_seq, receiver);
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
    receiver: mpsc::Receiver<BroadcasterPayload>,
}

impl BroadcasterSessionStream {
    fn new(
        stream_id: String,
        next_message_seq: u64,
        receiver: mpsc::Receiver<BroadcasterPayload>,
    ) -> Self {
        Self {
            stream_id,
            next_message_seq,
            receiver,
        }
    }

    async fn next_envelope(&mut self) -> Option<BroadcasterEnvelope> {
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

fn snapshot_session_error_response(
    error: SnapshotSessionError,
    already_attached_status: StatusCode,
) -> Response {
    let (status, message) = match error {
        SnapshotSessionError::NotFound => (StatusCode::NOT_FOUND, "snapshot session not found"),
        SnapshotSessionError::Expired => (StatusCode::GONE, "snapshot session expired"),
        SnapshotSessionError::AlreadyAttached => {
            (already_attached_status, "snapshot session already attached")
        }
        SnapshotSessionError::PayloadOutOfRange => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            "snapshot payload index out of range",
        ),
    };
    (status, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use tokio::sync::mpsc;

    use super::BroadcasterSessionStream;
    use simulator_core::broadcaster::{
        BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterPayload,
    };

    #[tokio::test]
    async fn session_stream_sends_live_payloads_from_next_sequence() -> Result<()> {
        let (sender, receiver) = mpsc::channel(4);
        let mut session_stream = BroadcasterSessionStream::new("stream-7".to_string(), 3, receiver);

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
