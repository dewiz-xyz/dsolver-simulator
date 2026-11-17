use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use state_history::BaseFeeSource;
use tracing::warn;

const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const CACHE_CAPACITY: usize = 4_096;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    base_fee_per_gas: Option<String>,
    hash: Option<String>,
    number: Option<String>,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<RpcBlock>,
}

enum RpcTransportError {
    Http(String),
    Timeout(String),
    Decode(String),
}

trait BlockTransport: Send + Sync {
    fn get_block<'a>(
        &'a self,
        rpc_url: &'a str,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RpcBlock>, RpcTransportError>> + Send + 'a>>;
}

struct ReqwestBlockTransport {
    client: Client,
}

impl BlockTransport for ReqwestBlockTransport {
    fn get_block<'a>(
        &'a self,
        rpc_url: &'a str,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RpcBlock>, RpcTransportError>> + Send + 'a>>
    {
        Box::pin(async move {
            let response = self
                .client
                .post(rpc_url)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getBlockByNumber",
                    "params": [format!("{block_number:#x}"), false],
                }))
                .timeout(RPC_TIMEOUT)
                .send()
                .await
                .map_err(classify_request_error)?
                .error_for_status()
                .map_err(|error| RpcTransportError::Http(error_message_without_url(error)))?;

            response
                .json::<RpcResponse>()
                .await
                .map(|response| response.result)
                .map_err(classify_decode_error)
        })
    }
}

fn classify_request_error(error: reqwest::Error) -> RpcTransportError {
    let is_timeout = error.is_timeout();
    let message = error_message_without_url(error);
    if is_timeout {
        RpcTransportError::Timeout(message)
    } else {
        RpcTransportError::Http(message)
    }
}

fn classify_decode_error(error: reqwest::Error) -> RpcTransportError {
    let is_timeout = error.is_timeout();
    let message = error_message_without_url(error);
    if is_timeout {
        RpcTransportError::Timeout(message)
    } else {
        RpcTransportError::Decode(message)
    }
}

fn error_message_without_url(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

/// Resolves historical EIP-1559 base fees through an Ethereum JSON-RPC endpoint.
pub struct RpcBaseFeeSource {
    rpc_url: String,
    transport: Arc<dyn BlockTransport>,
    cache: Mutex<BTreeMap<(u64, u64, String), String>>,
}

impl RpcBaseFeeSource {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            transport: Arc::new(ReqwestBlockTransport {
                client: Client::new(),
            }),
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    #[cfg(test)]
    fn with_transport(rpc_url: String, transport: Arc<dyn BlockTransport>) -> Self {
        Self {
            rpc_url,
            transport,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    async fn resolve_base_fee(
        &self,
        chain_id: u64,
        block_number: u64,
        block_hash: Option<&str>,
    ) -> Option<String> {
        let cache_key =
            block_hash.map(|block_hash| (chain_id, block_number, block_hash.to_owned()));
        if let Some(base_fee) = cache_key
            .as_ref()
            .and_then(|cache_key| self.cached_base_fee(cache_key))
        {
            return Some(base_fee);
        }

        let block = self.fetch_block(chain_id, block_number).await?;

        if !rpc_block_matches(&block, chain_id, block_number, block_hash) {
            return None;
        }

        let Some(base_fee_hex) = block.base_fee_per_gas.as_deref() else {
            warn!(
                scope = "state_history_base_fee",
                chain_id, block_number, "Base fee RPC block omitted baseFeePerGas"
            );
            return None;
        };
        let Ok(base_fee) = parse_hex_u128(base_fee_hex) else {
            warn!(
                scope = "state_history_base_fee",
                chain_id,
                block_number,
                base_fee_hex,
                "Base fee RPC baseFeePerGas was not valid hex"
            );
            return None;
        };

        let base_fee = base_fee.to_string();
        if let Some(cache_key) = cache_key {
            self.cache_base_fee(cache_key, base_fee.clone());
        }
        Some(base_fee)
    }

    async fn fetch_block(&self, chain_id: u64, block_number: u64) -> Option<RpcBlock> {
        match self.transport.get_block(&self.rpc_url, block_number).await {
            Ok(Some(block)) => Some(block),
            Ok(None) => {
                warn!(
                    scope = "state_history_base_fee",
                    chain_id, block_number, "Base fee RPC returned no block"
                );
                None
            }
            Err(error) => {
                let (message, error) = match error {
                    RpcTransportError::Http(error) => ("Base fee RPC HTTP request failed", error),
                    RpcTransportError::Timeout(error) => ("Base fee RPC request timed out", error),
                    RpcTransportError::Decode(error) => {
                        ("Base fee RPC response could not be decoded", error)
                    }
                };
                warn!(scope = "state_history_base_fee", chain_id, block_number, %error, "{message}");
                None
            }
        }
    }

    fn cached_base_fee(&self, key: &(u64, u64, String)) -> Option<String> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    fn cache_base_fee(&self, key: (u64, u64, String), base_fee: String) {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(key, base_fee);
        if cache.len() > CACHE_CAPACITY {
            cache.pop_first();
        }
    }
}

fn rpc_block_matches(
    block: &RpcBlock,
    chain_id: u64,
    block_number: u64,
    block_hash: Option<&str>,
) -> bool {
    let echoed_number = block
        .number
        .as_deref()
        .and_then(|value| parse_hex_u64(value).ok());
    if echoed_number != Some(block_number) {
        warn!(
            scope = "state_history_base_fee",
            chain_id,
            block_number,
            actual_number = ?block.number,
            "Base fee RPC returned a different block number"
        );
        return false;
    }
    if block_hash.is_some_and(|expected_hash| block.hash.as_deref() != Some(expected_hash)) {
        warn!(
            scope = "state_history_base_fee",
            chain_id,
            block_number,
            expected_hash = ?block_hash,
            actual_hash = ?block.hash,
            "Base fee RPC returned a different block"
        );
        return false;
    }
    true
}

impl BaseFeeSource for RpcBaseFeeSource {
    fn base_fee_wei<'a>(
        &'a self,
        chain_id: u64,
        block_number: u64,
        block_hash: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(self.resolve_base_fee(chain_id, block_number, block_hash))
    }
}

fn parse_hex_u64(value: &str) -> Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(strip_hex_prefix(value), 16)
}

fn parse_hex_u128(value: &str) -> Result<u128, std::num::ParseIntError> {
    u128::from_str_radix(strip_hex_prefix(value), 16)
}

fn strip_hex_prefix(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use state_history::BaseFeeSource;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{
        BlockTransport, ReqwestBlockTransport, RpcBaseFeeSource, RpcBlock, RpcTransportError,
    };

    struct CountingTransport {
        calls: AtomicUsize,
        response: Option<RpcBlock>,
    }

    impl CountingTransport {
        fn new(response: Option<RpcBlock>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                response,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl BlockTransport for CountingTransport {
        fn get_block<'a>(
            &'a self,
            _rpc_url: &'a str,
            _block_number: u64,
        ) -> Pin<Box<dyn Future<Output = Result<Option<RpcBlock>, RpcTransportError>> + Send + 'a>>
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let response = self.response.clone();
            Box::pin(async move { Ok(response) })
        }
    }

    fn source_with_response(response: RpcBlock) -> (RpcBaseFeeSource, Arc<CountingTransport>) {
        let transport = Arc::new(CountingTransport::new(Some(response)));
        let source =
            RpcBaseFeeSource::with_transport("http://rpc.example".to_string(), transport.clone());
        (source, transport)
    }

    #[tokio::test]
    async fn rejects_mismatched_block_number_echo() {
        let (source, _) = source_with_response(RpcBlock {
            base_fee_per_gas: Some("0x3b9aca00".to_string()),
            hash: Some("block-100".to_string()),
            number: Some("0x65".to_string()),
        });

        assert_eq!(source.base_fee_wei(1, 100, Some("block-100")).await, None);
    }

    #[tokio::test]
    async fn missing_base_fee_yields_none() {
        let (source, _) = source_with_response(RpcBlock {
            base_fee_per_gas: None,
            hash: Some("block-100".to_string()),
            number: Some("0x64".to_string()),
        });

        assert_eq!(source.base_fee_wei(1, 100, Some("block-100")).await, None);
    }

    #[tokio::test]
    async fn cache_hits_skip_lookup() {
        let (source, transport) = source_with_response(RpcBlock {
            base_fee_per_gas: Some("0x3b9aca00".to_string()),
            hash: Some("block-100".to_string()),
            number: Some("0x64".to_string()),
        });

        assert_eq!(
            source.base_fee_wei(1, 100, Some("block-100")).await,
            Some("1000000000".to_string())
        );
        assert_eq!(
            source.base_fee_wei(1, 100, Some("block-100")).await,
            Some("1000000000".to_string())
        );
        assert_eq!(transport.calls(), 1);
    }

    #[tokio::test]
    async fn changed_block_hash_misses_cache_and_rejects_old_block() {
        let (source, transport) = source_with_response(RpcBlock {
            base_fee_per_gas: Some("0x3b9aca00".to_string()),
            hash: Some("old-block".to_string()),
            number: Some("0x64".to_string()),
        });

        assert_eq!(
            source.base_fee_wei(1, 100, Some("old-block")).await,
            Some("1000000000".to_string())
        );
        assert_eq!(source.base_fee_wei(1, 100, Some("new-block")).await, None);
        assert_eq!(transport.calls(), 2);
    }

    #[tokio::test]
    async fn rpc_error_hides_url() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 4_096];
            let _ = socket.read(&mut request).await?;
            socket
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await?;
            Ok::<(), std::io::Error>(())
        });
        let url = format!("http://{address}/super-secret-token?key=super-secret-token");
        let transport = ReqwestBlockTransport {
            client: reqwest::Client::new(),
        };

        let message = match transport.get_block(&url, 100).await {
            Err(RpcTransportError::Http(message)) => message,
            Err(_) => anyhow::bail!("HTTP 500 returned the wrong error type"),
            Ok(_) => anyhow::bail!("HTTP 500 returned a block"),
        };

        assert!(!message.contains("super-secret-token"));
        assert!(!message.contains(&url));
        server.await??;
        Ok(())
    }
}
