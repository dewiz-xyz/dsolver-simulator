use std::cmp::Ordering;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, RootProvider};
use alloy_transport::{TransportError, TransportErrorKind};
use simulator_core::broadcaster::BlockIdentity;
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing::subscriber::NoSubscriber;
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainHeadAgreement {
    Matches,
    AppliedStateIncomplete,
    ObserverAhead,
    HashMismatch,
    ObserverBehind,
    ObservationUnavailable,
}

impl ChainHeadAgreement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Matches => "matches",
            Self::AppliedStateIncomplete => "applied_state_incomplete",
            Self::ObserverAhead => "observer_ahead",
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
            Ordering::Greater => ChainHeadAgreement::ObserverAhead,
            Ordering::Less => ChainHeadAgreement::ObserverBehind,
            Ordering::Equal if observed_head.hash != applied_head.hash => {
                ChainHeadAgreement::HashMismatch
            }
            Ordering::Equal => ChainHeadAgreement::Matches,
        }
    }
}

impl ChainHeadObserver {
    pub fn new(config: ChainHeadConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(ObserverState::default())),
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
        }
    }

    pub async fn run(&self, rpc_url: &str, chain_id: u64, stop: CancellationToken) {
        let provider = rpc_url
            .parse()
            .map(RootProvider::new_http)
            .map_err(|_| RpcFailure::Transport);
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
                // Alloy's HTTP spans include the RPC URL, which can contain credentials.
                result = async {
                    match &provider {
                        Ok(provider) => self.poll(provider, chain_id, &mut chain_verified).await,
                        Err(error) => Err(*error),
                    }
                }.with_subscriber(NoSubscriber::default()) => result,
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
        provider: &RootProvider,
        chain_id: u64,
        chain_verified: &mut bool,
    ) -> Result<BlockIdentity, RpcFailure> {
        if !*chain_verified {
            let observed_chain_id =
                timeout(self.config.rpc_request_timeout, provider.get_chain_id())
                    .await
                    .map_err(|_| RpcFailure::Timeout)?
                    .map_err(RpcFailure::from_transport_error)?;
            if observed_chain_id != chain_id {
                return Err(RpcFailure::WrongChain);
            }
            *chain_verified = true;
        }
        let block = timeout(
            self.config.rpc_request_timeout,
            provider.get_block_by_number(BlockNumberOrTag::Latest),
        )
        .await
        .map_err(|_| RpcFailure::Timeout)?
        .map_err(RpcFailure::from_transport_error)?
        .ok_or(RpcFailure::InvalidResponse)?;
        Ok(BlockIdentity {
            number: block.header.number,
            hash: Bytes::from(block.header.hash.as_slice()),
        })
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
    fn from_transport_error(error: TransportError) -> Self {
        match error {
            TransportError::ErrorResp(_) => Self::JsonRpcError,
            TransportError::NullResp | TransportError::DeserError { .. } => Self::InvalidResponse,
            TransportError::Transport(TransportErrorKind::HttpError(_)) => Self::HttpStatus,
            _ => Self::Transport,
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

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use alloy_provider::network::{Ethereum, Network};
    use anyhow::{bail, Context, Error, Result};
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::{advance, pause, resume};

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
        let serve = async move {
            for (method, result) in exchanges {
                let (mut socket, _) = listener.accept().await?;
                let request = read_http_request(&mut socket).await?;
                assert_eq!(request["method"], method);
                if method == "eth_getBlockByNumber" {
                    assert_eq!(request["params"], json!(["latest", false]));
                }
                let response =
                    json!({"jsonrpc": "2.0", "id": request["id"], "result": result}).to_string();
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
        };
        let server = tokio::spawn(async move { timeout(Duration::from_secs(5), serve).await? });
        Ok((format!("http://{address}"), server))
    }

    #[tokio::test]
    async fn polling_requires_the_configured_chain_before_reading_its_head() -> Result<()> {
        let mut rpc_block = <Ethereum as Network>::BlockResponse::default();
        rpc_block.header.number = 10;
        rpc_block.header.hash = B256::repeat_byte(0xab);
        let rpc_head = serde_json::to_value(rpc_block)?;
        let (url, server) = serve_rpc_responses(vec![
            ("eth_chainId", json!("0x1")),
            ("eth_chainId", json!("0x2105")),
            ("eth_getBlockByNumber", rpc_head.clone()),
            ("eth_getBlockByNumber", rpc_head),
        ])
        .await?;
        let observer = observer();
        let provider = RootProvider::new_http(url.parse()?);
        let mut verified = false;
        assert_eq!(
            observer.poll(&provider, 8453, &mut verified).await,
            Err(RpcFailure::WrongChain)
        );
        assert!(!verified);
        assert_eq!(
            observer.poll(&provider, 8453, &mut verified).await,
            Ok(head(10, 0xab))
        );
        assert!(verified);
        assert_eq!(
            observer.poll(&provider, 8453, &mut verified).await,
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
            ChainHeadAgreement::ObserverAhead
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

    #[tokio::test]
    async fn polling_bounds_each_rpc_request() -> Result<()> {
        for (mut chain_verified, method) in [(false, "eth_chainId"), (true, "eth_getBlockByNumber")]
        {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let provider =
                RootProvider::new_http(format!("http://{}", listener.local_addr()?).parse()?);
            let observer = observer();
            let request_timeout = observer.config.rpc_request_timeout;
            let polling =
                tokio::spawn(
                    async move { observer.poll(&provider, 8453, &mut chain_verified).await },
                );
            let (socket, request) = timeout(Duration::from_secs(5), async {
                let (mut socket, _) = listener.accept().await?;
                let request = read_http_request(&mut socket).await?;
                Ok::<_, Error>((socket, request))
            })
            .await??;
            assert_eq!(request["method"], method);
            // Finish socket setup before advancing the clock manually.
            pause();
            advance(request_timeout).await;
            assert_eq!(
                timeout(Duration::from_secs(1), polling).await??,
                Err(RpcFailure::Timeout)
            );
            drop(socket);
            resume();
        }
        Ok(())
    }

    #[tokio::test]
    async fn polling_rejects_a_missing_latest_block() -> Result<()> {
        let (url, server) = serve_rpc_responses(vec![
            ("eth_chainId", json!("0x2105")),
            ("eth_getBlockByNumber", Value::Null),
        ])
        .await?;
        let provider = RootProvider::new_http(url.parse()?);
        let mut verified = false;
        assert_eq!(
            observer().poll(&provider, 8453, &mut verified).await,
            Err(RpcFailure::InvalidResponse)
        );
        assert!(verified);
        server.await??;
        Ok(())
    }
}
