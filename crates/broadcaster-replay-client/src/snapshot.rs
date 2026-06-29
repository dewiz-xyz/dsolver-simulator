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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use anyhow::{anyhow, Result};
    use futures::StreamExt;
    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn ordered_payload_fetches_overlap_and_yield_in_order() -> Result<()> {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let first_pair_started = Arc::new(AtomicUsize::new(0));
        let first_pair_ready = Arc::new(Notify::new());

        let mut payloads = super::ordered_payload_fetches(2, {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            let first_pair_started = Arc::clone(&first_pair_started);
            let first_pair_ready = Arc::clone(&first_pair_ready);
            move |index| {
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                let first_pair_started = Arc::clone(&first_pair_started);
                let first_pair_ready = Arc::clone(&first_pair_ready);
                async move {
                    let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(active, Ordering::SeqCst);
                    if first_pair_started.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
                        first_pair_ready.notify_waiters();
                    } else {
                        first_pair_ready.notified().await;
                    }
                    if index == 0 {
                        sleep(Duration::from_millis(50)).await;
                    }
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(index)
                }
            }
        });

        let first = next_payload(&mut payloads).await?;
        let second = next_payload(&mut payloads).await?;

        assert_eq!([first, second], [0, 1]);
        assert!(
            max_in_flight.load(Ordering::SeqCst) >= 2,
            "payload fetches should overlap"
        );
        Ok(())
    }

    async fn next_payload<T>(
        payloads: &mut (impl futures::Stream<Item = std::result::Result<T, anyhow::Error>> + Unpin),
    ) -> Result<T> {
        timeout(Duration::from_secs(2), payloads.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for concurrent snapshot payload fetch"))?
            .ok_or_else(|| anyhow!("snapshot payload stream ended early"))?
    }
}
