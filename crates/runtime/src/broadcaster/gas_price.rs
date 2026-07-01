use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use num_bigint::BigUint;
use reqwest::Client;
use serde_json::{json, Value};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterRedisStreamEntry};
use state_history::{
    indexed_backends_for_entry, CheckpointPayload, GasPriceMetadata, GasPriceSource,
};
use tokio::sync::Mutex;
use tracing::warn;

const GAS_PRICE_RPC_TIMEOUT: Duration = Duration::from_secs(2);
const GAS_PRICE_CACHE_CAPACITY: usize = 4_096;

#[derive(Debug, Clone)]
pub(crate) struct StateHistoryGasPriceProvider {
    chain_id: u64,
    rpc_url: Option<String>,
    client: Client,
    cache: Arc<Mutex<BTreeMap<GasPriceCacheKey, RpcBlockGasPrice>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct GasPriceCacheKey {
    chain_id: u64,
    block_number: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RpcBlockGasPrice {
    price_wei: String,
    block_hash: Option<String>,
    block_timestamp_secs: Option<u64>,
}

impl StateHistoryGasPriceProvider {
    pub(crate) fn new(chain_id: u64, rpc_url: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(GAS_PRICE_RPC_TIMEOUT)
            .build()
            .context("failed to build state history gas price RPC client")?;
        Ok(Self {
            chain_id,
            rpc_url,
            client,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub(crate) async fn gas_prices_for_entry(
        &self,
        entry: &BroadcasterRedisStreamEntry,
    ) -> Vec<GasPriceMetadata> {
        let backend_index = match indexed_backends_for_entry(entry) {
            Ok(backend_index) => backend_index,
            Err(error) => {
                warn!(
                    event = "state_history_gas_price_index_failed",
                    message_seq = entry.message_seq,
                    error = %error,
                    "State history gas lookup skipped for malformed backend index"
                );
                return Vec::new();
            }
        };
        self.gas_prices_for_backend_index(&backend_index).await
    }

    pub(crate) async fn gas_prices_for_backend_index(
        &self,
        backend_index: &[CheckpointPayload],
    ) -> Vec<GasPriceMetadata> {
        let mut gas_prices = Vec::new();
        for cursor in backend_index {
            let Some(block_number) = cursor.block_number else {
                continue;
            };
            let Some(gas_price) = self
                .gas_price_for_backend(cursor.backend, block_number)
                .await
            else {
                continue;
            };
            gas_prices.push(gas_price);
        }
        gas_prices
    }

    pub(crate) async fn gas_price_for_backend(
        &self,
        backend: BroadcasterBackend,
        block_number: u64,
    ) -> Option<GasPriceMetadata> {
        if !matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm) {
            return None;
        }
        self.gas_price_for_block(block_number)
            .await
            .map(|block_gas| block_gas.into_metadata(backend, block_number))
    }

    async fn gas_price_for_block(&self, block_number: u64) -> Option<RpcBlockGasPrice> {
        let key = GasPriceCacheKey {
            chain_id: self.chain_id,
            block_number,
        };
        {
            let cache = self.cache.lock().await;
            if let Some(cached) = cache.get(&key) {
                return Some(cached.clone());
            }
        }

        let gas_price = self.lookup_rpc_block_gas_price(block_number).await;
        if let Some(gas_price) = &gas_price {
            let mut cache = self.cache.lock().await;
            if cache.len() >= GAS_PRICE_CACHE_CAPACITY && !cache.contains_key(&key) {
                if let Some(oldest_key) = cache.keys().next().copied() {
                    cache.remove(&oldest_key);
                }
            }
            cache.insert(key, gas_price.clone());
        }
        gas_price
    }

    async fn lookup_rpc_block_gas_price(&self, block_number: u64) -> Option<RpcBlockGasPrice> {
        let Some(rpc_url) = &self.rpc_url else {
            warn!(
                event = "state_history_gas_price_missing_rpc_url",
                chain_id = self.chain_id,
                block_number,
                "State history gas metadata is unset because RPC_URL is not configured"
            );
            return None;
        };
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{block_number:x}"), false],
        });
        let response = match self.client.post(rpc_url).json(&request).send().await {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    event = "state_history_gas_price_rpc_failed",
                    chain_id = self.chain_id,
                    block_number,
                    error = %error,
                    "State history gas metadata is unset because RPC block lookup failed"
                );
                return None;
            }
        };
        if !response.status().is_success() {
            warn!(
                event = "state_history_gas_price_rpc_status_failed",
                chain_id = self.chain_id,
                block_number,
                status = response.status().as_u16(),
                "State history gas metadata is unset because RPC block lookup returned an error status"
            );
            return None;
        }
        let body = match response.json::<Value>().await {
            Ok(body) => body,
            Err(error) => {
                warn!(
                    event = "state_history_gas_price_rpc_decode_failed",
                    chain_id = self.chain_id,
                    block_number,
                    error = %error,
                    "State history gas metadata is unset because RPC block response was invalid"
                );
                return None;
            }
        };
        if let Some(error) = body.get("error") {
            warn!(
                event = "state_history_gas_price_rpc_error",
                chain_id = self.chain_id,
                block_number,
                error = %error,
                "State history gas metadata is unset because RPC returned an error"
            );
            return None;
        }
        let Some(result) = rpc_block_result(&body) else {
            warn!(
                event = "state_history_gas_price_rpc_empty",
                chain_id = self.chain_id,
                block_number,
                "State history gas metadata is unset because RPC returned no block"
            );
            return None;
        };
        parse_rpc_block_gas_price(result, block_number).or_else(|| {
            warn!(
                event = "state_history_gas_price_missing_base_fee",
                chain_id = self.chain_id,
                block_number,
                "State history gas metadata is unset because the RPC block has no baseFeePerGas"
            );
            None
        })
    }
}

impl RpcBlockGasPrice {
    fn into_metadata(self, backend: BroadcasterBackend, block_number: u64) -> GasPriceMetadata {
        GasPriceMetadata {
            backend,
            block_number,
            price_wei: self.price_wei,
            block_hash: self.block_hash,
            block_timestamp_secs: self.block_timestamp_secs,
            source: GasPriceSource::RpcBlock,
        }
    }
}

#[cfg(test)]
fn parse_rpc_block_gas_price_metadata(
    backend: BroadcasterBackend,
    requested_block_number: u64,
    block: &Value,
) -> Option<GasPriceMetadata> {
    parse_rpc_block_gas_price(block, requested_block_number)
        .map(|gas_price| gas_price.into_metadata(backend, requested_block_number))
}

#[cfg(test)]
fn parse_rpc_block_gas_price_response(
    body: &Value,
    requested_block_number: u64,
) -> Option<RpcBlockGasPrice> {
    rpc_block_result(body)
        .and_then(|result| parse_rpc_block_gas_price(result, requested_block_number))
}

fn rpc_block_result(body: &Value) -> Option<&Value> {
    body.get("result").filter(|result| !result.is_null())
}

fn parse_rpc_block_gas_price(
    block: &Value,
    requested_block_number: u64,
) -> Option<RpcBlockGasPrice> {
    let block_number = block
        .get("number")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)?;
    if block_number != requested_block_number {
        return None;
    }
    let price_wei = block
        .get("baseFeePerGas")
        .and_then(Value::as_str)
        .and_then(parse_hex_decimal_string)?;
    let block_hash = block
        .get("hash")
        .and_then(Value::as_str)
        .filter(|hash| !hash.is_empty())
        .map(str::to_string);
    let block_timestamp_secs = block
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64);
    Some(RpcBlockGasPrice {
        price_wei,
        block_hash,
        block_timestamp_secs,
    })
}

fn parse_hex_decimal_string(value: &str) -> Option<String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    BigUint::parse_bytes(value.as_bytes(), 16).map(|number| number.to_str_radix(10))
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16).ok()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;
    use simulator_core::broadcaster::BroadcasterBackend;
    use state_history::{CheckpointPayload, GasPriceSource};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        parse_rpc_block_gas_price_metadata, parse_rpc_block_gas_price_response,
        StateHistoryGasPriceProvider,
    };

    #[test]
    fn rpc_block_parser_extracts_base_fee_hash_timestamp_and_number() -> Result<()> {
        let metadata = parse_rpc_block_gas_price_metadata(
            BroadcasterBackend::Native,
            123,
            &json!({
                "number": "0x7b",
                "baseFeePerGas": "0x59682f00",
                "hash": "0xabc",
                "timestamp": "0x6682a2c0"
            }),
        )
        .ok_or_else(|| anyhow::anyhow!("expected gas metadata"))?;

        assert_eq!(metadata.backend, BroadcasterBackend::Native);
        assert_eq!(metadata.block_number, 123);
        assert_eq!(metadata.price_wei, "1500000000");
        assert_eq!(metadata.block_hash.as_deref(), Some("0xabc"));
        assert_eq!(metadata.block_timestamp_secs, Some(1_719_837_376));
        assert_eq!(metadata.source, GasPriceSource::RpcBlock);
        Ok(())
    }

    #[test]
    fn rpc_block_parser_returns_none_without_base_fee() {
        assert!(parse_rpc_block_gas_price_metadata(
            BroadcasterBackend::Native,
            123,
            &json!({
                "number": "0x7b",
                "hash": "0xabc",
                "timestamp": "0x6682a2c0"
            }),
        )
        .is_none());
    }

    #[test]
    fn rpc_block_response_parser_returns_none_without_block_result() {
        assert!(parse_rpc_block_gas_price_response(
            &json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
            123,
        )
        .is_none());
    }

    #[tokio::test]
    async fn provider_returns_none_without_rpc_url() -> Result<()> {
        let provider = StateHistoryGasPriceProvider::new(8453, None)?;

        assert!(provider
            .gas_price_for_backend(BroadcasterBackend::Native, 123)
            .await
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn provider_returns_none_on_rpc_failure() -> Result<()> {
        let provider =
            StateHistoryGasPriceProvider::new(8453, Some("not a valid rpc url".to_string()))?;

        assert!(provider
            .gas_price_for_backend(BroadcasterBackend::Native, 123)
            .await
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn provider_retries_after_missing_rpc_block() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            serve_rpc_response(&listener, r#"{"jsonrpc":"2.0","id":1,"result":null}"#).await?;
            serve_rpc_response(
                &listener,
                r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x7b","baseFeePerGas":"0x59682f00","hash":"0xabc","timestamp":"0x6682a2c0"}}"#,
            )
            .await?;
            anyhow::Ok(())
        });
        let provider = StateHistoryGasPriceProvider::new(8453, Some(rpc_url))?;

        assert!(provider
            .gas_price_for_backend(BroadcasterBackend::Native, 123)
            .await
            .is_none());
        let gas_price = provider
            .gas_price_for_backend(BroadcasterBackend::Native, 123)
            .await
            .ok_or_else(|| anyhow::anyhow!("expected retry to fetch base fee"))?;

        assert_eq!(gas_price.price_wei, "1500000000");
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn provider_skips_rfq_only_cursors() -> Result<()> {
        let provider = StateHistoryGasPriceProvider::new(8453, None)?;
        let gas_prices = provider
            .gas_prices_for_backend_index(&[CheckpointPayload {
                backend: BroadcasterBackend::Rfq,
                block_number: None,
                observed_timestamp_ms: Some(1_700_000_000_000),
                gas_price: None,
            }])
            .await;

        assert!(gas_prices.is_empty());
        Ok(())
    }

    async fn serve_rpc_response(listener: &tokio::net::TcpListener, body: &str) -> Result<()> {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0u8; 1024];
        let _ = socket.read(&mut request).await?;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await?;
        Ok(())
    }
}
