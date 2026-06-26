use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;
use simulator_core::broadcaster::{BroadcasterEnvelope, BroadcasterSnapshotSessionResponse};

use crate::error::{BroadcasterReplayClientError, Result};
use crate::url::derive_broadcaster_http_url;

pub(crate) const BROADCASTER_SNAPSHOT_SESSIONS_PATH: &str = "snapshot-sessions";

pub(crate) async fn create_broadcaster_snapshot_session(
    client: &Client,
    broadcaster_url: &str,
    request_timeout: Duration,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let snapshot_sessions_url =
        derive_broadcaster_http_url(broadcaster_url, BROADCASTER_SNAPSHOT_SESSIONS_PATH)?;
    let operation = "create broadcaster snapshot session";
    let response = client
        .post(&snapshot_sessions_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            BroadcasterReplayClientError::http_request(
                operation,
                &snapshot_sessions_url,
                error.to_string(),
            )
        })?;
    decode_success_json(response, &snapshot_sessions_url, operation).await
}

pub(crate) async fn fetch_broadcaster_snapshot_payload(
    client: &Client,
    broadcaster_url: &str,
    session: &BroadcasterSnapshotSessionResponse,
    index: u32,
    request_timeout: Duration,
) -> Result<BroadcasterEnvelope> {
    let payload_url = derive_broadcaster_http_url(
        broadcaster_url,
        &broadcaster_snapshot_payload_path(session.session_id, index),
    )?;
    let operation = "fetch broadcaster snapshot payload";
    let response = client
        .get(&payload_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            BroadcasterReplayClientError::http_request(operation, &payload_url, error.to_string())
        })?;
    decode_success_json(response, &payload_url, operation).await
}

fn broadcaster_snapshot_payload_path(session_id: u64, index: u32) -> String {
    format!("{BROADCASTER_SNAPSHOT_SESSIONS_PATH}/{session_id}/payloads/{index}")
}

async fn decode_success_json<T>(
    response: reqwest::Response,
    url: &str,
    operation: &'static str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(BroadcasterReplayClientError::http_status(
            operation,
            url,
            status.as_u16(),
        ));
    }
    let body = response.bytes().await.map_err(|error| {
        BroadcasterReplayClientError::http_body(operation, url, error.to_string())
    })?;
    serde_json::from_slice(&body).map_err(|error| {
        BroadcasterReplayClientError::json_decode(operation, url, error.to_string())
    })
}
