use std::cmp::Ordering;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use alloy_primitives::hex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use simulator_core::broadcaster::BlockIdentity;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tycho_common::Bytes;

const METRICS_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
pub struct ChainHeadConfig {
    pub poll_interval: Duration,
    pub rpc_request_timeout: Duration,
    pub observation_max_age: Duration,
}

#[derive(Clone, Debug)]
pub struct ChainHeadObserver {
    config: ChainHeadConfig,
    state: Arc<RwLock<ObserverState>>,
    #[cfg(any(test, feature = "test-util"))]
    unmonitored_for_test: bool,
}

#[derive(Default, Debug)]
struct ObserverState {
    observation: Option<HeadObservation>,
    outage_started_at: Option<Instant>,
    revision: u64,
    last_error: Option<&'static str>,
}

#[derive(Clone, Debug)]
struct HeadObservation {
    head: BlockIdentity,
    request_started_at: Instant,
}

#[derive(Clone, Debug)]
pub struct ChainHeadSnapshot {
    pub observed_head: Option<BlockIdentity>,
    pub observation_age: Option<Duration>,
    pub rpc_outage_age: Option<Duration>,
    /// Changes on head replacement or when an expired observation becomes available again.
    pub revision: u64,
    pub last_error: Option<&'static str>,
    request_started_at: Option<Instant>,
    observation_max_age: Duration,
    #[cfg(any(test, feature = "test-util"))]
    unmonitored_for_test: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainHeadAgreement {
    Matches,
    AppliedStateIncomplete,
    ChainAhead,
    HashMismatch,
    ObserverBehind,
    ObservationUnavailable,
}

impl ChainHeadAgreement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Matches => "matches",
            Self::AppliedStateIncomplete => "applied_state_incomplete",
            Self::ChainAhead => "chain_ahead",
            Self::HashMismatch => "hash_mismatch",
            Self::ObserverBehind => "observer_behind",
            Self::ObservationUnavailable => "observation_unavailable",
        }
    }
}

impl ChainHeadSnapshot {
    pub fn is_observation_available(&self) -> bool {
        self.request_started_at
            .is_some_and(|started| started.elapsed() < self.observation_max_age)
    }

    pub fn agreement(&self, applied_head: Option<&BlockIdentity>) -> ChainHeadAgreement {
        #[cfg(any(test, feature = "test-util"))]
        if self.unmonitored_for_test {
            return ChainHeadAgreement::Matches;
        }
        if !self.is_observation_available() {
            return ChainHeadAgreement::ObservationUnavailable;
        }
        let Some(observed_head) = self.observed_head.as_ref() else {
            return ChainHeadAgreement::ObservationUnavailable;
        };
        let Some(applied_head) = applied_head else {
            return ChainHeadAgreement::AppliedStateIncomplete;
        };
        match observed_head.number.cmp(&applied_head.number) {
            Ordering::Greater => ChainHeadAgreement::ChainAhead,
            Ordering::Less => ChainHeadAgreement::ObserverBehind,
            Ordering::Equal if observed_head.hash != applied_head.hash => {
                ChainHeadAgreement::HashMismatch
            }
            Ordering::Equal => ChainHeadAgreement::Matches,
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_unmonitored_for_test(&self) -> bool {
        self.unmonitored_for_test
    }
}

impl ChainHeadObserver {
    pub fn new(config: ChainHeadConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(ObserverState::default())),
            #[cfg(any(test, feature = "test-util"))]
            unmonitored_for_test: false,
        }
    }

    pub fn snapshot(&self) -> ChainHeadSnapshot {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        ChainHeadSnapshot {
            observed_head: state.observation.as_ref().map(|value| value.head.clone()),
            observation_age: state
                .observation
                .as_ref()
                .map(|value| now.saturating_duration_since(value.request_started_at)),
            rpc_outage_age: state
                .outage_started_at
                .map(|started| now.saturating_duration_since(started)),
            revision: state.revision,
            last_error: state.last_error,
            request_started_at: state
                .observation
                .as_ref()
                .map(|value| value.request_started_at),
            observation_max_age: self.config.observation_max_age,
            #[cfg(any(test, feature = "test-util"))]
            unmonitored_for_test: self.unmonitored_for_test,
        }
    }

    pub async fn run(&self, rpc_url: &str, chain_id: u64, stop: CancellationToken) {
        let client = Client::new();
        let mut polling = interval(self.config.poll_interval);
        polling.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut chain_verified = false;
        let mut last_metric_at = None;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                _ = polling.tick() => {}
            }
            let request_started_at = Instant::now();
            let observation = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                result = self.poll(&client, rpc_url, chain_id, &mut chain_verified) => result,
            };
            let outage_transition =
                self.record_observation_outcome(observation, request_started_at, chain_id);
            let now = Instant::now();
            if outage_transition
                || last_metric_at.is_none_or(|last| now.duration_since(last) >= METRICS_INTERVAL)
            {
                crate::metrics::emit_chain_head_observation(
                    chain_id,
                    self.snapshot().rpc_outage_age.unwrap_or_default().as_secs(),
                );
                last_metric_at = Some(now);
            }
        }
    }

    fn record_observation_outcome(
        &self,
        observation: Result<BlockIdentity, RpcFailure>,
        request_started_at: Instant,
        chain_id: u64,
    ) -> bool {
        match observation {
            Ok(head) => {
                let recovered = self.record_success(head, request_started_at);
                if recovered {
                    info!(
                        chain_id,
                        event = "chain_head_rpc_recovered",
                        "Chain head RPC recovered"
                    );
                }
                recovered
            }
            Err(error) => {
                let outage_started = self.record_failure(error.label(), request_started_at);
                if outage_started {
                    warn!(
                        chain_id,
                        event = "chain_head_rpc_unavailable",
                        reason = error.label(),
                        "Chain head RPC observation failed"
                    );
                }
                outage_started
            }
        }
    }

    async fn poll(
        &self,
        client: &Client,
        rpc_url: &str,
        chain_id: u64,
        chain_verified: &mut bool,
    ) -> Result<BlockIdentity, RpcFailure> {
        if !*chain_verified {
            let result = self
                .request(client, rpc_url, "eth_chainId", json!([]))
                .await?;
            if parse_quantity(&result)? != chain_id {
                return Err(RpcFailure::WrongChain);
            }
            *chain_verified = true;
        }
        let result = self
            .request(
                client,
                rpc_url,
                "eth_getBlockByNumber",
                json!(["latest", false]),
            )
            .await?;
        parse_head(&result)
    }

    async fn request(
        &self,
        client: &Client,
        rpc_url: &str,
        method: &'static str,
        params: Value,
    ) -> Result<Value, RpcFailure> {
        let response = client
            .post(rpc_url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .timeout(self.config.rpc_request_timeout)
            .send()
            .await
            .map_err(RpcFailure::from_request_error)?
            .error_for_status()
            .map_err(|_| RpcFailure::HttpStatus)?;
        let response = response
            .json::<RpcResponse>()
            .await
            .map_err(RpcFailure::from_request_error)?;
        response.result()
    }

    fn record_success(&self, head: BlockIdentity, request_started_at: Instant) -> bool {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let recovered = state.outage_started_at.take().is_some();
        let changes_revision = state.observation.as_ref().is_none_or(|previous| {
            previous.head != head
                || previous.request_started_at.elapsed() >= self.config.observation_max_age
        });
        if changes_revision {
            state.revision = state.revision.saturating_add(1);
        }
        // Request latency consumes the observation lifetime; a delayed reply is not a new tip.
        state.observation = Some(HeadObservation {
            head,
            request_started_at,
        });
        state.last_error = None;
        recovered
    }

    fn record_failure(&self, reason: &'static str, request_started_at: Instant) -> bool {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let outage_started = state.outage_started_at.is_none();
        state.outage_started_at.get_or_insert(request_started_at);
        state.last_error = Some(reason);
        outage_started
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn observe_for_test(&self, head: BlockIdentity) {
        self.record_success(head, Instant::now());
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn fail_for_test(&self) {
        self.record_failure("transport", Instant::now());
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn ready_for_test(head: BlockIdentity) -> Self {
        let observer = Self::new(ChainHeadConfig {
            poll_interval: Duration::from_secs(1),
            rpc_request_timeout: Duration::from_secs(2),
            observation_max_age: Duration::from_secs(120),
        });
        observer.observe_for_test(head);
        observer
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn unmonitored_for_test() -> Self {
        let mut observer = Self::new(ChainHeadConfig {
            poll_interval: Duration::from_secs(1),
            rpc_request_timeout: Duration::from_secs(2),
            observation_max_age: Duration::from_secs(120),
        });
        observer.unmonitored_for_test = true;
        observer
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    result: Option<Value>,
    error: Option<Value>,
}

impl RpcResponse {
    fn result(self) -> Result<Value, RpcFailure> {
        if self.jsonrpc != "2.0" || self.id != 1 {
            return Err(RpcFailure::InvalidResponse);
        }
        if self.error.is_some() {
            return Err(RpcFailure::JsonRpcError);
        }
        self.result.ok_or(RpcFailure::InvalidResponse)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RpcFailure {
    Transport,
    Timeout,
    HttpStatus,
    InvalidResponse,
    JsonRpcError,
    WrongChain,
}

impl RpcFailure {
    fn from_request_error(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else if error.is_decode() {
            Self::InvalidResponse
        } else {
            Self::Transport
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::Timeout => "timeout",
            Self::HttpStatus => "http_status",
            Self::InvalidResponse => "invalid_response",
            Self::JsonRpcError => "json_rpc_error",
            Self::WrongChain => "wrong_chain",
        }
    }
}

fn parse_quantity(value: &Value) -> Result<u64, RpcFailure> {
    let digits = value
        .as_str()
        .and_then(|value| value.strip_prefix("0x"))
        .filter(|digits| !digits.is_empty() && (digits.len() == 1 || !digits.starts_with('0')))
        .filter(|digits| digits.bytes().all(|digit| digit.is_ascii_hexdigit()))
        .ok_or(RpcFailure::InvalidResponse)?;
    u64::from_str_radix(digits, 16).map_err(|_| RpcFailure::InvalidResponse)
}

fn parse_head(value: &Value) -> Result<BlockIdentity, RpcFailure> {
    let number = parse_quantity(&value["number"])?;
    let hash = value["hash"]
        .as_str()
        .and_then(|value| value.strip_prefix("0x"))
        .filter(|value| value.len() == 64)
        .ok_or(RpcFailure::InvalidResponse)?;
    let hash = hex::decode(hash).map_err(|_| RpcFailure::InvalidResponse)?;
    Ok(BlockIdentity {
        number,
        hash: Bytes::from(hash),
    })
}

#[cfg(test)]
mod tests {
    use anyhow::{bail, Context, Result};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::advance;

    use super::*;

    fn observer() -> ChainHeadObserver {
        ChainHeadObserver::new(ChainHeadConfig {
            poll_interval: Duration::from_secs(1),
            rpc_request_timeout: Duration::from_secs(2),
            observation_max_age: Duration::from_secs(5),
        })
    }

    fn head(number: u64, hash: u8) -> BlockIdentity {
        BlockIdentity {
            number,
            hash: Bytes::from(vec![hash; 32]),
        }
    }

    async fn read_http_request(socket: &mut TcpStream) -> Result<Value> {
        let mut request = Vec::new();
        let body_start = loop {
            if socket.read_buf(&mut request).await? == 0 {
                bail!("RPC connection ended before the request headers");
            }
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..body_start])?;
        let content_length: usize = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .context("RPC request must declare a content length")?
            .1
            .trim()
            .parse()?;
        while request.len() < body_start + content_length {
            if socket.read_buf(&mut request).await? == 0 {
                bail!("RPC connection ended before the request body");
            }
        }
        Ok(serde_json::from_slice(&request[body_start..])?)
    }

    async fn serve_rpc_responses(
        exchanges: Vec<(&'static str, Value)>,
    ) -> Result<(String, JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            for (method, result) in exchanges {
                let (mut socket, _) = listener.accept().await?;
                let request = read_http_request(&mut socket).await?;
                assert_eq!(request["method"], method);
                let response = json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                            response.len()
                        )
                        .as_bytes(),
                    )
                    .await?;
                socket.shutdown().await?;
            }
            Ok(())
        });
        Ok((format!("http://{address}"), server))
    }

    #[tokio::test]
    async fn polling_requires_the_configured_chain_before_reading_its_head() -> Result<()> {
        let rpc_head = json!({"number": "0xa", "hash": format!("0x{}", "ab".repeat(32))});
        let (url, server) = serve_rpc_responses(vec![
            ("eth_chainId", json!("0x1")),
            ("eth_chainId", json!("0x2105")),
            ("eth_getBlockByNumber", rpc_head.clone()),
            ("eth_getBlockByNumber", rpc_head),
        ])
        .await?;
        let observer = observer();
        let client = Client::new();
        let mut verified = false;
        assert_eq!(
            observer.poll(&client, &url, 8453, &mut verified).await,
            Err(RpcFailure::WrongChain)
        );
        assert!(!verified);
        assert_eq!(
            observer.poll(&client, &url, 8453, &mut verified).await,
            Ok(head(10, 0xab))
        );
        assert!(verified);
        assert_eq!(
            observer.poll(&client, &url, 8453, &mut verified).await,
            Ok(head(10, 0xab))
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_observer_does_not_begin_an_rpc_attempt() {
        let observer = observer();
        let stop = CancellationToken::new();
        stop.cancel();
        observer.run("not an RPC URL", 8453, stop).await;
        assert_eq!(observer.snapshot().last_error, None);
        assert_eq!(observer.snapshot().observed_head, None);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_polls_retain_the_head_only_until_its_observation_expires() {
        let observer = observer();
        let applied = head(10, 1);
        observer.observe_for_test(applied.clone());
        let original = observer.snapshot();
        advance(Duration::from_secs(1)).await;
        observer.fail_for_test();
        advance(Duration::from_secs(3)).await;
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::Matches
        );
        assert_eq!(
            observer.snapshot().rpc_outage_age,
            Some(Duration::from_secs(3))
        );
        advance(Duration::from_secs(1)).await;
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::ObservationUnavailable
        );
        assert_eq!(
            original.agreement(Some(&applied)),
            ChainHeadAgreement::ObservationUnavailable
        );
        assert_eq!(observer.snapshot().observed_head, Some(applied));
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_current_heads_renew_freshness_without_changing_revision() {
        let observer = observer();
        let applied = head(10, 1);
        observer.observe_for_test(applied.clone());
        let revision = observer.snapshot().revision;
        advance(Duration::from_secs(4)).await;
        observer.observe_for_test(applied.clone());
        advance(Duration::from_secs(4)).await;
        assert_eq!(observer.snapshot().revision, revision);
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::Matches
        );
        observer.fail_for_test();
        advance(Duration::from_secs(1)).await;
        observer.observe_for_test(applied.clone());
        assert_eq!(observer.snapshot().revision, revision + 1);
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::Matches
        );
        assert_eq!(observer.snapshot().rpc_outage_age, None);
    }

    #[tokio::test(start_paused = true)]
    async fn head_replacement_and_reversion_are_visible_even_when_the_number_does_not_advance() {
        let observer = observer();
        let applied = head(10, 1);
        observer.observe_for_test(applied.clone());
        let revision = observer.snapshot().revision;
        observer.observe_for_test(head(10, 2));
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::HashMismatch
        );
        observer.observe_for_test(applied.clone());
        assert_eq!(observer.snapshot().revision, revision + 2);
        observer.observe_for_test(head(9, 3));
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::ObserverBehind
        );
        observer.observe_for_test(head(11, 4));
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::ChainAhead
        );
        assert_eq!(
            observer.snapshot().agreement(None),
            ChainHeadAgreement::AppliedStateIncomplete
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_responses_do_not_receive_a_new_observation_lifetime() {
        let observer = observer();
        let started = Instant::now();
        let applied = head(10, 1);
        advance(Duration::from_secs(5)).await;
        observer.record_success(applied.clone(), started);
        assert_eq!(
            observer.snapshot().observation_age,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            observer.snapshot().agreement(Some(&applied)),
            ChainHeadAgreement::ObservationUnavailable
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rpc_outage_age_starts_without_a_head_and_does_not_reset_on_repeated_failures() {
        let observer = observer();
        observer.fail_for_test();
        advance(Duration::from_secs(301)).await;
        observer.fail_for_test();
        let snapshot = observer.snapshot();
        assert_eq!(snapshot.rpc_outage_age, Some(Duration::from_secs(301)));
        assert_eq!(
            snapshot.agreement(None),
            ChainHeadAgreement::ObservationUnavailable
        );
    }

    #[test]
    fn rpc_envelope_rejects_errors_null_results_and_unrelated_replies() -> Result<()> {
        for response in [
            json!({"jsonrpc": "2.0", "id": 1, "result": null}),
            json!({"jsonrpc": "2.0", "id": 1, "error": {"message": "secret RPC URL"}}),
            json!({"jsonrpc": "2.0", "id": 1, "result": "0x1", "error": {"code": -1}}),
            json!({"jsonrpc": "2.0", "id": 2, "result": "0x1"}),
            json!({"jsonrpc": "1.0", "id": 1, "result": "0x1"}),
        ] {
            assert!(serde_json::from_value::<RpcResponse>(response)?
                .result()
                .is_err());
        }
        let response: RpcResponse =
            serde_json::from_value(json!({"jsonrpc": "2.0", "id": 1, "result": "0x1"}))?;
        assert_eq!(response.result(), Ok(json!("0x1")));
        Ok(())
    }

    #[test]
    fn rpc_block_parser_requires_complete_block_identity_and_canonical_quantities() {
        let valid_hash = format!("0x{}", "ab".repeat(32));
        assert_eq!(
            parse_head(&json!({"number": "0xa", "hash": valid_hash})),
            Ok(head(10, 0xab))
        );
        for number in [
            json!(10),
            json!("10"),
            json!("0x"),
            json!("0x01"),
            json!("0x+1"),
            json!("0x10000000000000000"),
        ] {
            assert!(parse_head(&json!({"number": number, "hash": valid_hash})).is_err());
        }
        for hash in [
            json!(null),
            json!("0xab"),
            json!("zz".repeat(32)),
            json!(format!("0x{}", "gg".repeat(32))),
        ] {
            assert!(parse_head(&json!({"number": "0xa", "hash": hash})).is_err());
        }
    }
}
