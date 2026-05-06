use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use reqwest::{header, Client, ClientBuilder, Response};
use simulator_core::broadcaster::{BroadcasterTokenLookupRequest, BroadcasterTokenLookupResponse};
use tokio::sync::{watch, Mutex, RwLock, Semaphore};
use tracing::{debug, info, warn};
use tycho_simulation::tycho_common::{
    dto::{TokensRequestBody, TokensRequestResponse},
    models::{token::Token, Chain},
    Bytes,
};

/// In-memory token metadata cache. The fetch source is explicit so simulator request
/// paths can be broadcaster-backed while the broadcaster remains the Tycho authority.
type InflightRx = watch::Receiver<Option<Result<Option<Token>, TokenStoreError>>>;
type InflightMap = HashMap<Bytes, InflightRx>;
const NATIVE_TOKEN_ADDRESS_BYTES: [u8; 20] = [0u8; 20];

fn native_token_address() -> Bytes {
    Bytes::from(NATIVE_TOKEN_ADDRESS_BYTES)
}

#[derive(Debug)]
pub struct TokenStore {
    tokens: RwLock<HashMap<Bytes, Token>>,
    inflight: Mutex<InflightMap>,
    fetch_semaphore: Semaphore,
    fetch_source: TokenFetchSource,
    chain: Chain,
    fetch_timeout: Duration,
    client: Client,
}

#[derive(Debug, Clone)]
enum TokenFetchSource {
    Tycho { tycho_url: String, api_key: String },
    Broadcaster { lookup_url: String },
    LocalOnly,
}

impl TokenStore {
    pub fn new(
        initial: HashMap<Bytes, Token>,
        tycho_url: String,
        api_key: String,
        chain: Chain,
        fetch_timeout: Duration,
    ) -> Self {
        Self::with_source(
            initial,
            TokenFetchSource::Tycho { tycho_url, api_key },
            chain,
            fetch_timeout,
        )
    }

    pub fn broadcaster_backed(
        initial: HashMap<Bytes, Token>,
        lookup_url: String,
        chain: Chain,
        fetch_timeout: Duration,
    ) -> Self {
        Self::with_source(
            initial,
            TokenFetchSource::Broadcaster { lookup_url },
            chain,
            fetch_timeout,
        )
    }

    pub fn local_only(
        initial: HashMap<Bytes, Token>,
        chain: Chain,
        fetch_timeout: Duration,
    ) -> Self {
        Self::with_source(initial, TokenFetchSource::LocalOnly, chain, fetch_timeout)
    }

    fn with_source(
        initial: HashMap<Bytes, Token>,
        fetch_source: TokenFetchSource,
        chain: Chain,
        fetch_timeout: Duration,
    ) -> Self {
        Self {
            tokens: RwLock::new(initial),
            inflight: Mutex::new(HashMap::new()),
            fetch_semaphore: Semaphore::new(16),
            fetch_source,
            chain,
            fetch_timeout,
            client: build_http_client(),
        }
    }

    pub async fn snapshot(&self) -> HashMap<Bytes, Token> {
        self.tokens.read().await.clone()
    }

    pub async fn get(&self, address: &Bytes) -> Option<Token> {
        self.tokens.read().await.get(address).cloned()
    }

    pub fn wrapped_native_token(&self) -> Option<Bytes> {
        let address = self.chain.wrapped_native_token().address;
        (address != native_token_address()).then_some(address)
    }

    /// Ensure the token metadata exists. Misses resolve through this store's configured source.
    pub async fn ensure(&self, address: &Bytes) -> Result<Option<Token>, TokenStoreError> {
        if let Some(token) = self.get(address).await {
            return Ok(Some(token));
        }

        self.fetch_token(address).await
    }

    /// Merge a batch of tokens into the in-memory cache. Existing entries are
    /// left untouched to preserve any richer metadata already present.
    pub async fn insert_batch(&self, tokens: impl IntoIterator<Item = Token>) {
        let mut guard = self.tokens.write().await;
        for token in tokens {
            guard.entry(token.address.clone()).or_insert(token);
        }
    }

    async fn fetch_token(&self, address: &Bytes) -> Result<Option<Token>, TokenStoreError> {
        // Coalesce concurrent fetches for the same address using a watch channel.
        {
            let mut inflight = self.inflight.lock().await;
            if let Some(rx) = inflight.get(address) {
                let mut rx = rx.clone();
                drop(inflight);
                while rx.changed().await.is_ok() {
                    if let Some(res) = rx.borrow().clone() {
                        return res;
                    }
                }
            } else {
                let (tx, rx) = watch::channel(None);
                inflight.insert(address.clone(), rx);
                drop(inflight);
                let res = self.fetch_token_inner(address, tx).await;
                let mut inflight = self.inflight.lock().await;
                inflight.remove(address);
                return res;
            }
        }

        // If the watch channel closed unexpectedly, this caller becomes the new fetch owner.
        let (tx, rx) = watch::channel(None);
        {
            let mut inflight = self.inflight.lock().await;
            inflight.insert(address.clone(), rx);
        }
        let res = self.fetch_token_inner(address, tx).await;
        let mut inflight = self.inflight.lock().await;
        inflight.remove(address);
        res
    }

    async fn fetch_token_inner(
        &self,
        address: &Bytes,
        tx: watch::Sender<Option<Result<Option<Token>, TokenStoreError>>>,
    ) -> Result<Option<Token>, TokenStoreError> {
        let _permit = self.fetch_semaphore.acquire().await.map_err(|_| {
            TokenStoreError::RequestFailed("token fetch semaphore unexpectedly closed".to_string())
        })?;

        // Another task may have populated the cache while we were waiting.
        if let Some(token) = self.tokens.read().await.get(address).cloned() {
            let _ = tx.send(Some(Ok(Some(token.clone()))));
            return Ok(Some(token));
        }

        info!(scope = "token_single_fetch", token_address = %address, "Token not in cache");

        let result = self.fetch_token_result(address).await;

        let _ = tx.send(Some(result.clone()));
        result
    }

    async fn fetch_token_result(&self, address: &Bytes) -> Result<Option<Token>, TokenStoreError> {
        if matches!(self.fetch_source, TokenFetchSource::LocalOnly) {
            return Ok(None);
        }

        let start = Instant::now();
        match &self.fetch_source {
            TokenFetchSource::Tycho { .. } => {
                let body = build_tokens_request_body(address, self.chain);
                let response = self.send_tycho_fetch_request(address, &body, start).await?;
                self.parse_tycho_token_response(address, response, start)
                    .await
            }
            TokenFetchSource::Broadcaster { .. } => {
                let body = BroadcasterTokenLookupRequest {
                    chain_id: self.chain.id(),
                    addresses: vec![address.clone()],
                };
                let response = self
                    .send_broadcaster_fetch_request(address, &body, start)
                    .await?;
                self.parse_broadcaster_token_response(address, response, start)
                    .await
            }
            TokenFetchSource::LocalOnly => Ok(None),
        }
    }

    async fn send_tycho_fetch_request(
        &self,
        address: &Bytes,
        body: &TokensRequestBody,
        start: Instant,
    ) -> Result<Response, TokenStoreError> {
        let TokenFetchSource::Tycho { api_key, .. } = &self.fetch_source else {
            return Err(TokenStoreError::RequestFailed(
                "token store is not Tycho-backed".to_string(),
            ));
        };
        let url = format!("{}/v1/tokens", self.tycho_rpc_base_url()?);
        self.client
            .post(url)
            .header(header::AUTHORIZATION, api_key.clone())
            .json(body)
            .timeout(self.fetch_timeout)
            .send()
            .await
            .map_err(|err| map_fetch_error(address, start, self.fetch_timeout, err))
    }

    async fn send_broadcaster_fetch_request(
        &self,
        address: &Bytes,
        body: &BroadcasterTokenLookupRequest,
        start: Instant,
    ) -> Result<Response, TokenStoreError> {
        let TokenFetchSource::Broadcaster { lookup_url } = &self.fetch_source else {
            return Err(TokenStoreError::RequestFailed(
                "token store is not broadcaster-backed".to_string(),
            ));
        };
        self.client
            .post(lookup_url)
            .json(body)
            .timeout(self.fetch_timeout)
            .send()
            .await
            .map_err(|err| map_fetch_error(address, start, self.fetch_timeout, err))
    }

    async fn parse_tycho_token_response(
        &self,
        address: &Bytes,
        response: Response,
        start: Instant,
    ) -> Result<Option<Token>, TokenStoreError> {
        if !response.status().is_success() {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            info!(
                scope = "token_single_fetch",
                token_address = %address,
                status = %response.status(),
                elapsed_ms,
                "Token fetch returned non-success status"
            );
            return Err(TokenStoreError::RequestFailed(format!(
                "Tycho RPC returned status {} when fetching token {}",
                response.status(),
                address
            )));
        }

        let TokensRequestResponse { tokens, .. } = response
            .json::<TokensRequestResponse>()
            .await
            .map_err(|err| {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            warn!(
                scope = "token_single_fetch",
                token_address = %address,
                elapsed_ms,
                error = %err,
                "Failed to decode token response"
            );
            TokenStoreError::RequestFailed(format!("Failed to parse token response: {err}"))
        })?;

        let maybe_token = tokens
            .into_iter()
            .find(|token| token.address == *address)
            .and_then(|token| Token::try_from(token).ok());

        if let Some(token) = maybe_token {
            self.tokens
                .write()
                .await
                .entry(token.address.clone())
                .or_insert_with(|| token.clone());
            let elapsed_ms = start.elapsed().as_millis() as u64;
            debug!(
                scope = "token_single_fetch",
                token_address = %address,
                elapsed_ms,
                "Token fetch succeeded"
            );
            Ok(Some(token))
        } else {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            warn!(
                scope = "token_single_fetch",
                token_address = %address,
                elapsed_ms,
                "Token response contained no matching token"
            );
            Ok(None)
        }
    }

    async fn parse_broadcaster_token_response(
        &self,
        address: &Bytes,
        response: Response,
        start: Instant,
    ) -> Result<Option<Token>, TokenStoreError> {
        if !response.status().is_success() {
            return Err(TokenStoreError::RequestFailed(format!(
                "broadcaster token lookup returned status {} when fetching token {}",
                response.status(),
                address
            )));
        }

        let body = response
            .json::<BroadcasterTokenLookupResponse>()
            .await
            .map_err(|err| {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                warn!(
                    scope = "token_single_fetch",
                    token_address = %address,
                    elapsed_ms,
                    error = %err,
                    "Failed to decode broadcaster token response"
                );
                TokenStoreError::RequestFailed(format!(
                    "Failed to parse broadcaster token response: {err}"
                ))
            })?;

        let maybe_token = body
            .tokens
            .into_iter()
            .find(|token| token.address == *address)
            .map(|token| token.into_token(self.chain))
            .transpose()
            .map_err(|err| TokenStoreError::RequestFailed(err.to_string()))?;

        if let Some(token) = maybe_token {
            self.tokens
                .write()
                .await
                .entry(token.address.clone())
                .or_insert_with(|| token.clone());
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }

    fn tycho_rpc_base_url(&self) -> Result<String, TokenStoreError> {
        let TokenFetchSource::Tycho { tycho_url, .. } = &self.fetch_source else {
            return Err(TokenStoreError::RequestFailed(
                "token store is not Tycho-backed".to_string(),
            ));
        };
        if tycho_url.starts_with("http://") || tycho_url.starts_with("https://") {
            Ok(tycho_url.trim_end_matches('/').to_string())
        } else {
            Ok(format!("https://{}", tycho_url.trim_end_matches('/')))
        }
    }
}

#[derive(Debug, Clone)]
pub enum TokenStoreError {
    FetchTimeout(Duration),
    RequestFailed(String),
}

impl fmt::Display for TokenStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenStoreError::FetchTimeout(duration) => {
                write!(f, "token fetch timed out after {} ms", duration.as_millis())
            }
            TokenStoreError::RequestFailed(message) => write!(f, "token fetch failed: {}", message),
        }
    }
}

impl std::error::Error for TokenStoreError {}

fn build_tokens_request_body(address: &Bytes, chain: Chain) -> TokensRequestBody {
    TokensRequestBody {
        token_addresses: Some(vec![address.clone()]),
        chain: chain.into(),
        ..Default::default()
    }
}

fn build_http_client() -> Client {
    let builder = ClientBuilder::new()
        .connect_timeout(Duration::from_millis(750))
        .pool_idle_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(1)
        .tcp_nodelay(true);
    match builder.build() {
        Ok(client) => client,
        Err(err) => {
            eprintln!("Failed to build token-store HTTP client: {err}");
            std::process::abort();
        }
    }
}

fn map_fetch_error(
    address: &Bytes,
    start: Instant,
    fetch_timeout: Duration,
    err: reqwest::Error,
) -> TokenStoreError {
    let elapsed_ms = start.elapsed().as_millis() as u64;
    warn!(
        scope = "token_single_fetch",
        token_address = %address,
        elapsed_ms,
        is_timeout = err.is_timeout(),
        is_connect = err.is_connect(),
        error = %err,
        "Token fetch request failed before response"
    );
    if err.is_timeout() {
        TokenStoreError::FetchTimeout(fetch_timeout)
    } else {
        TokenStoreError::RequestFailed(err.to_string())
    }
}

pub fn derive_broadcaster_token_lookup_url(ws_url: &str) -> Result<String, TokenStoreError> {
    derive_broadcaster_token_url(ws_url, "lookup")
}

pub fn derive_broadcaster_token_snapshot_url(ws_url: &str) -> Result<String, TokenStoreError> {
    derive_broadcaster_token_url(ws_url, "snapshot")
}

fn derive_broadcaster_token_url(ws_url: &str, endpoint: &str) -> Result<String, TokenStoreError> {
    let mut url = reqwest::Url::parse(ws_url).map_err(|err| {
        TokenStoreError::RequestFailed(format!("invalid TYCHO_BROADCASTER_WS_URL: {err}"))
    })?;
    let scheme = match url.scheme() {
        "ws" => "http",
        "wss" => "https",
        other => {
            return Err(TokenStoreError::RequestFailed(format!(
                "TYCHO_BROADCASTER_WS_URL must use ws or wss, got {other}"
            )))
        }
    };
    let Some(prefix) = url.path().strip_suffix("/ws") else {
        return Err(TokenStoreError::RequestFailed(
            "TYCHO_BROADCASTER_WS_URL must end with /ws".to_string(),
        ));
    };
    let lookup_path = format!("{prefix}/tokens/{endpoint}");
    url.set_scheme(scheme).map_err(|_| {
        TokenStoreError::RequestFailed("invalid broadcaster URL scheme".to_string())
    })?;
    url.set_path(&lookup_path);
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Barrier, Notify},
        task::JoinHandle,
        time::sleep,
    };

    fn test_address() -> Bytes {
        Bytes::from([0x11_u8; 20])
    }

    fn test_token(address: &Bytes) -> Token {
        Token::new(address, "LOOKUP", 18, 0, &[], Chain::Ethereum, 100)
    }

    async fn spawn_hanging_token_server(
        hold_duration: Duration,
        request_count: Arc<AtomicUsize>,
    ) -> Result<(String, Arc<Notify>, JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://{}", listener.local_addr()?);
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_signal.notified() => break,
                    accept_result = listener.accept() => {
                        let (socket, _) = accept_result?;
                        request_count.fetch_add(1, Ordering::SeqCst);
                        tokio::spawn(async move {
                            // Keep the socket open long enough for reqwest to time out
                            // before any response bytes arrive.
                            let _socket = socket;
                            sleep(hold_duration).await;
                        });
                    }
                }
            }
            Ok(())
        });
        Ok((address, shutdown, task))
    }

    async fn spawn_lookup_token_server(
        token: Token,
        request_count: Arc<AtomicUsize>,
    ) -> Result<(String, Arc<Notify>, JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://{}", listener.local_addr()?);
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            let response_body = serde_json::to_string(&BroadcasterTokenLookupResponse {
                tokens: vec![token.into()],
                missing: Vec::new(),
            })?;

            loop {
                tokio::select! {
                    _ = shutdown_signal.notified() => break,
                    accept_result = listener.accept() => {
                        let (mut socket, _) = accept_result?;
                        request_count.fetch_add(1, Ordering::SeqCst);
                        let mut buffer = [0_u8; 4096];
                        let read = socket.read(&mut buffer).await?;
                        let request = String::from_utf8_lossy(&buffer[..read]);
                        assert!(
                            request.starts_with("POST /tokens/lookup HTTP/1.1"),
                            "expected broadcaster lookup request, got {request}"
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        socket.write_all(response.as_bytes()).await?;
                    }
                }
            }
            Ok(())
        });
        Ok((address, shutdown, task))
    }

    #[tokio::test]
    async fn concurrent_waiters_share_timeout_error_without_duplicate_fetches() -> Result<()> {
        const CONCURRENT_CALLS: usize = 6;

        let request_count = Arc::new(AtomicUsize::new(0));
        let fetch_timeout = Duration::from_millis(50);
        let hold_duration = Duration::from_millis(150);
        let (tycho_url, shutdown, server_task) =
            spawn_hanging_token_server(hold_duration, Arc::clone(&request_count)).await?;
        let store = Arc::new(TokenStore::new(
            HashMap::new(),
            tycho_url,
            "test".to_string(),
            Chain::Ethereum,
            fetch_timeout,
        ));
        let address = test_address();
        let barrier = Arc::new(Barrier::new(CONCURRENT_CALLS + 1));

        let handles: Vec<_> = (0..CONCURRENT_CALLS)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let address = address.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    store.ensure(&address).await
                })
            })
            .collect();

        barrier.wait().await;

        for handle in handles {
            let result = handle.await?;
            assert!(
                matches!(result, Err(TokenStoreError::FetchTimeout(duration)) if duration == fetch_timeout),
                "expected shared timeout error, got {result:?}"
            );
        }

        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "concurrent cache misses should share one upstream timeout"
        );

        shutdown.notify_waiters();
        server_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn broadcaster_cache_hit_does_not_call_lookup() -> Result<()> {
        let address = test_address();
        let token = test_token(&address);
        let request_count = Arc::new(AtomicUsize::new(0));
        let (lookup_url, shutdown, server_task) =
            spawn_lookup_token_server(token.clone(), Arc::clone(&request_count)).await?;
        let store = TokenStore::broadcaster_backed(
            HashMap::from([(address.clone(), token.clone())]),
            format!("{lookup_url}/tokens/lookup"),
            Chain::Ethereum,
            Duration::from_millis(200),
        );

        let resolved = store.ensure(&address).await?;

        assert_eq!(resolved, Some(token));
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        shutdown.notify_waiters();
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn broadcaster_miss_uses_lookup_and_caches_result() -> Result<()> {
        let address = test_address();
        let token = test_token(&address);
        let request_count = Arc::new(AtomicUsize::new(0));
        let (lookup_url, shutdown, server_task) =
            spawn_lookup_token_server(token.clone(), Arc::clone(&request_count)).await?;
        let store = TokenStore::broadcaster_backed(
            HashMap::new(),
            format!("{lookup_url}/tokens/lookup"),
            Chain::Ethereum,
            Duration::from_millis(200),
        );

        let resolved = store.ensure(&address).await?;
        let cached = store.ensure(&address).await?;

        assert_eq!(resolved, Some(token.clone()));
        assert_eq!(cached, Some(token));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        shutdown.notify_waiters();
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn local_only_misses_do_not_fetch() -> Result<()> {
        let store =
            TokenStore::local_only(HashMap::new(), Chain::Ethereum, Duration::from_millis(1));

        assert_eq!(store.ensure(&test_address()).await?, None);
        Ok(())
    }

    #[test]
    fn derives_token_lookup_url_from_broadcaster_websocket_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_token_lookup_url("ws://127.0.0.1:3001/ws")?,
            "http://127.0.0.1:3001/tokens/lookup"
        );
        assert_eq!(
            derive_broadcaster_token_lookup_url("wss://broadcaster.example/ws")?,
            "https://broadcaster.example/tokens/lookup"
        );
        assert_eq!(
            derive_broadcaster_token_lookup_url("wss://broadcaster.example/prod/base/ws")?,
            "https://broadcaster.example/prod/base/tokens/lookup"
        );
        Ok(())
    }

    #[test]
    fn derives_token_snapshot_url_from_broadcaster_websocket_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_token_snapshot_url("ws://127.0.0.1:3001/ws")?,
            "http://127.0.0.1:3001/tokens/snapshot"
        );
        assert_eq!(
            derive_broadcaster_token_snapshot_url("wss://broadcaster.example/prod/base/ws")?,
            "https://broadcaster.example/prod/base/tokens/snapshot"
        );
        Ok(())
    }

    #[test]
    fn derives_token_lookup_url_rejects_non_websocket_url() {
        let Err(err) = derive_broadcaster_token_lookup_url("http://127.0.0.1:3001/ws") else {
            unreachable!("non-websocket URL should fail");
        };

        assert!(err.to_string().contains("must use ws or wss"));
    }
}
