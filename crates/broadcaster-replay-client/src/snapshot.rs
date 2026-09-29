use std::future::Future;
use std::time::Duration;

use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterSnapshotSessionResponse, BroadcasterTokenSnapshotResponse,
};

use crate::error::{BroadcasterReplayClientError, Result};
use crate::url::derive_broadcaster_http_url;

pub(crate) const BROADCASTER_SNAPSHOT_SESSIONS_PATH: &str = "snapshot-sessions";
const BROADCASTER_TOKEN_SNAPSHOT_PATH: &str = "tokens/snapshot";
const SNAPSHOT_DOWNLOAD_CONCURRENCY: usize = 4;

pub(crate) async fn create_broadcaster_snapshot_session(
    client: &Client,
    broadcaster_url: &str,
    request_timeout: Duration,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let snapshot_sessions_url =
        derive_broadcaster_http_url(broadcaster_url, BROADCASTER_SNAPSHOT_SESSIONS_PATH)?;
    request_json(
        client.post(&snapshot_sessions_url).timeout(request_timeout),
        &snapshot_sessions_url,
        "create broadcaster snapshot session",
    )
    .await
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
    request_json(
        client.get(&payload_url).timeout(request_timeout),
        &payload_url,
        "fetch broadcaster snapshot payload",
    )
    .await
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

pub(crate) async fn fetch_broadcaster_token_snapshot(
    client: &Client,
    broadcaster_url: &str,
    request_timeout: Duration,
) -> Result<BroadcasterTokenSnapshotResponse> {
    let url = derive_broadcaster_http_url(broadcaster_url, BROADCASTER_TOKEN_SNAPSHOT_PATH)?;
    request_json(
        client.get(&url).timeout(request_timeout),
        &url,
        "fetch broadcaster token snapshot",
    )
    .await
}

fn broadcaster_snapshot_payload_path(session_id: u64, index: u32) -> String {
    format!("{BROADCASTER_SNAPSHOT_SESSIONS_PATH}/{session_id}/payloads/{index}")
}

/// Sends `request` and decodes a successful response body as `T`, naming the operation and
/// the URL in every failure.
async fn request_json<T>(
    request: reqwest::RequestBuilder,
    url: &str,
    operation: &'static str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let response = request.send().await.map_err(|error| {
        BroadcasterReplayClientError::http_request(operation, url, error.to_string())
    })?;
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

    use crate::error::BroadcasterReplayClientError;
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

    #[tokio::test]
    async fn the_token_catalog_is_fetched_from_the_broadcaster() -> Result<()> {
        let body = serde_json::json!({
            "chainId": 8453,
            "tokens": [{
                "address": "0x4200000000000000000000000000000000000006",
                "symbol": "WETH",
                "decimals": 18,
                "tax": 0,
                "gas": [null],
                "chainId": 8453,
                "quality": 100,
            }],
        });
        let app = axum::Router::new().route(
            "/tokens/snapshot",
            axum::routing::get(move || async move { axum::Json(body) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move { axum::serve(listener, app).await });

        let catalog = super::fetch_broadcaster_token_snapshot(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            Duration::from_secs(5),
        )
        .await?;

        assert_eq!(catalog.chain_id, 8453);
        assert_eq!(catalog.tokens.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_status_or_an_undecodable_body_is_a_typed_error() -> Result<()> {
        let app = axum::Router::new()
            .route(
                "/failing/tokens/snapshot",
                axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
            )
            .route(
                "/garbled/tokens/snapshot",
                axum::routing::get(|| async { "not json" }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move { axum::serve(listener, app).await });
        let fetch = |base: &'static str| {
            let url = format!("http://{address}/{base}");
            async move {
                super::fetch_broadcaster_token_snapshot(
                    &reqwest::Client::new(),
                    &url,
                    Duration::from_secs(5),
                )
                .await
            }
        };

        assert!(matches!(
            fetch("failing").await,
            Err(BroadcasterReplayClientError::HttpStatus { status: 500, .. })
        ));
        assert!(matches!(
            fetch("garbled").await,
            Err(BroadcasterReplayClientError::JsonDecode { .. })
        ));
        Ok(())
    }
}
