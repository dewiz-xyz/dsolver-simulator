use std::future::Future;
use std::time::Duration;

use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use simulator_core::broadcaster::{BroadcasterEnvelope, BroadcasterSnapshotSessionResponse};

use crate::error::{BroadcasterReplayClientError, Result};
use crate::url::derive_broadcaster_http_url;

pub(crate) const BROADCASTER_SNAPSHOT_SESSIONS_PATH: &str = "snapshot-sessions";
const SNAPSHOT_DOWNLOAD_CONCURRENCY: usize = 4;

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

pub(crate) fn fetch_broadcaster_snapshot_payloads<'a>(
    client: &'a Client,
    broadcaster_url: &'a str,
    session: &'a BroadcasterSnapshotSessionResponse,
    request_timeout: Duration,
) -> impl Stream<Item = Result<BroadcasterEnvelope>> + 'a {
    ordered_payload_fetches(session.payload_count, move |index| async move {
        fetch_broadcaster_snapshot_payload(client, broadcaster_url, session, index, request_timeout)
            .await
    })
}

fn ordered_payload_fetches<'a, Fetch, Fut, Payload, Error>(
    payload_count: u32,
    fetch: Fetch,
) -> impl Stream<Item = std::result::Result<Payload, Error>> + 'a
where
    Fetch: FnMut(u32) -> Fut + 'a,
    Fut: Future<Output = std::result::Result<Payload, Error>> + 'a,
{
    futures::stream::iter(0..payload_count)
        .map(fetch)
        .buffered(SNAPSHOT_DOWNLOAD_CONCURRENCY)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{Error, Result};
    use futures::TryStreamExt;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    #[tokio::test]
    async fn ordered_payload_fetches_overlap_and_yield_in_order() -> Result<()> {
        let second_payload_ready = &Notify::new();
        // Payload 0 waits for payload 1, so fetching sequentially cannot finish.
        let payloads = super::ordered_payload_fetches(2, |index| async move {
            if index == 0 {
                second_payload_ready.notified().await;
            } else {
                second_payload_ready.notify_one();
            }
            Ok::<_, Error>(index)
        });
        let indices = timeout(Duration::from_secs(2), payloads.try_collect::<Vec<_>>()).await??;

        assert_eq!(indices, vec![0, 1]);
        Ok(())
    }
}
