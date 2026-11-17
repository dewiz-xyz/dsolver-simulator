//! Adapt complete provider books to the broadcaster's existing RFQ state/delta contract.

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use futures::{future::ready, Stream, StreamExt};
use simulator_core::models::protocol::ProtocolKind;
use tracing::warn;
use tycho_simulation::{
    book::{
        quote_tokens::usd_stablecoins_for_chain, BookFeedConfig, BookFeedEvent, BookFeedStreams,
        BookSnapshot,
    },
    pamm::protocols::metric::feed::MetricFeedBuilder,
    protocol::models::{ProtocolComponent, Update},
    rfq::protocols::{
        bebop::feed::BebopFeedBuilder, hashflow::feed::HashflowFeedBuilder,
        liquorice::feed::LiquoriceFeedBuilder, native::feed::NativeFeedBuilder,
    },
    tycho_common::models::Chain,
};

use super::stream_builder::{
    RFQConfig, RFQTokenStores, LIQUORICE_QUOTE_EXPIRY_SECS, RFQ_QUOTE_TIMEOUT,
};
use crate::broadcaster::redis_publisher::current_time_ms;

type StreamError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn build_rfq_stream(
    token_stores: RFQTokenStores,
    chain: Chain,
    protocols: &[ProtocolKind],
    config: RFQConfig,
) -> Result<impl Stream<Item = Result<Update, StreamError>> + Unpin + Send> {
    let mut feeds = BookFeedStreams::new();
    let quote_tokens = Arc::new(
        usd_stablecoins_for_chain(chain)
            .ok_or_else(|| anyhow!("No USD quote tokens for {chain}"))?,
    );
    for kind in protocols {
        let protocol = kind.as_str();
        let tokens = match protocol {
            "rfq:bebop" => token_stores.bebop.snapshot().await,
            "rfq:hashflow" => token_stores.hashflow.snapshot().await,
            "rfq:liquorice" => token_stores.liquorice.snapshot().await,
            "rfq:native" | "rfq:metric" => token_stores.tokens.snapshot().await,
            other => bail!("Unknown RFQ protocol in chain profile: {other}"),
        };
        let book_config = BookFeedConfig {
            chain,
            tokens: Arc::new(tokens),
            min_tvl_usd: config.min_tvl_usd,
        };
        let duplicate = || anyhow!("Duplicate RFQ provider: {protocol}");
        match protocol {
            "rfq:bebop" => feeds
                .add(
                    protocol,
                    BebopFeedBuilder::new(
                        book_config,
                        Arc::clone(&quote_tokens),
                        config.bebop_key.clone(),
                    )
                    .quote_timeout(RFQ_QUOTE_TIMEOUT)
                    .build()?,
                )
                .map_err(|_| duplicate())?,
            "rfq:hashflow" => feeds
                .add(
                    protocol,
                    HashflowFeedBuilder::new(
                        book_config,
                        Arc::clone(&quote_tokens),
                        config.hashflow_user.clone(),
                        config.hashflow_key.clone(),
                    )
                    .quote_timeout(RFQ_QUOTE_TIMEOUT)
                    .build()?,
                )
                .map_err(|_| duplicate())?,
            "rfq:liquorice" => feeds
                .add(
                    protocol,
                    LiquoriceFeedBuilder::new(
                        book_config,
                        Arc::clone(&quote_tokens),
                        config.liquorice_user.clone(),
                        config.liquorice_key.clone(),
                    )
                    .quote_timeout(RFQ_QUOTE_TIMEOUT)
                    .quote_expiry_secs(LIQUORICE_QUOTE_EXPIRY_SECS)
                    .build()?,
                )
                .map_err(|_| duplicate())?,
            "rfq:native" => feeds
                .add(
                    protocol,
                    NativeFeedBuilder::new(
                        book_config,
                        Arc::clone(&quote_tokens),
                        config.native_key.clone(),
                    )
                    .quote_timeout(RFQ_QUOTE_TIMEOUT)
                    .build()?,
                )
                .map_err(|_| duplicate())?,
            "rfq:metric" => feeds
                .add(
                    protocol,
                    MetricFeedBuilder::new(book_config, config.metric_key.clone()).build()?,
                )
                .map_err(|_| duplicate())?,
            other => bail!("Unknown RFQ provider: {other}"),
        }
    }
    let mut books = ProviderBooks::default();
    Ok(feeds.filter_map(move |(provider, event)| {
        let snapshot = match event {
            BookFeedEvent::Published(snapshot) => Some(snapshot),
            BookFeedEvent::Withdrawn => None,
            BookFeedEvent::Ended(outcome) => {
                warn!(%provider, ?outcome, "RFQ provider ended; withdrawing its liquidity");
                None
            }
        };
        ready(books.replace(&provider, snapshot).map(Ok))
    }))
}

#[derive(Default)]
struct ProviderBooks {
    pairs: HashMap<String, HashMap<String, ProtocolComponent>>,
}

impl ProviderBooks {
    fn replace(
        &mut self,
        provider: &str,
        snapshot: Option<BookSnapshot<tycho_simulation::book::ReceivedAt>>,
    ) -> Option<Update> {
        let previous = self.pairs.entry(provider.to_string()).or_default();
        let mut pairs = HashMap::new();
        let mut states = HashMap::new();
        if let Some(snapshot) = snapshot {
            for (id, book) in snapshot.books.iter() {
                pairs.insert(id.clone(), book.component.clone());
                states.insert(id.clone(), book.state.clone_box());
            }
        }
        let removed = previous
            .iter()
            .filter(|(id, _)| !pairs.contains_key(*id))
            .map(|(id, component)| (id.clone(), component.clone()))
            .collect::<HashMap<_, _>>();
        let added = pairs
            .iter()
            .filter(|(id, component)| previous.get(*id) != Some(*component))
            .map(|(id, component)| (id.clone(), component.clone()))
            .collect();
        *previous = pairs;
        if states.is_empty() && removed.is_empty() {
            return None;
        }
        Some(Update::new(current_time_ms(), states, added).set_removed_pairs(removed))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
mod tests {
    use super::*;
    use crate::broadcaster::state::BroadcasterSnapshotCache;
    use chrono::NaiveDateTime;
    use num_bigint::BigUint;
    use serde_json::json;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterPayload, BroadcasterUpdateMessage,
    };
    use tycho_simulation::{
        book::{Book, Books},
        pamm::protocols::metric::state::MetricState,
        rfq::protocols::{bebop::state::BebopState, native::state::NativeState},
        tycho_common::{models::token::Token, simulation::protocol_sim::ProtocolSim, Bytes},
    };

    fn tokens() -> (Token, Token) {
        (
            Token::new(
                &Bytes::from([1u8; 20]),
                "WETH",
                18,
                0,
                &[],
                Chain::Base,
                100,
            ),
            Token::new(&Bytes::from([2u8; 20]), "USDC", 6, 0, &[], Chain::Base, 100),
        )
    }

    fn book(provider: &str, id: u8) -> Book {
        let (base, quote) = tokens();
        let levels = json!([{"price": 3000.0, "quantity": 10.0}]);
        let state: Arc<dyn ProtocolSim> = match provider {
            "rfq:native" => Arc::new(
                serde_json::from_value::<NativeState>(json!({
                    "base_token": base, "quote_token": quote,
                    "book": {
                        "base_address": base.address, "quote_address": quote.address,
                        "bids": {"levels": levels, "minimum_in": 0.0, "minimum_out": 0.0},
                        "asks": {"levels": levels, "minimum_in": 0.0, "minimum_out": 0.0}
                    }
                }))
                .unwrap(),
            ),
            "rfq:metric" => Arc::new(
                serde_json::from_value::<MetricState>(json!({
                    "base_token": base, "quote_token": quote,
                    "metadata": {
                        "poolAddress": Bytes::from([id; 20]), "token0": base.address,
                        "token1": quote.address, "tvlFiat": 30000.0, "pauseLevel": 0
                    },
                    "bid_ask": {
                        "bidAdj": "55340232221128654848000", "askAdj": "55524699661865750400000",
                        "totalToken0Available": "10000000000000000000",
                        "totalToken1Available": "30000000000", "serverTs": 100,
                        "priceProviderStatus": "healthy"
                    }
                }))
                .unwrap(),
            ),
            _ => Arc::new(
                serde_json::from_value::<BebopState>(json!({
                    "base_token": base, "quote_token": quote,
                    "book": {"base": base.address, "quote": quote.address,
                             "last_update_ts": 1, "bids": levels, "asks": levels},
                    "usd_per_quote_token": 1.0
                }))
                .unwrap(),
            ),
        };
        let component = ProtocolComponent::new(
            Bytes::from([id; 20]),
            provider.to_string(),
            match provider {
                "rfq:native" => "native_relay_pool",
                "rfq:metric" => "metric_pool",
                _ => "bebop_pool",
            }
            .to_string(),
            Chain::Base,
            vec![base, quote],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );
        Book {
            component,
            state,
            updated_at: None,
        }
    }

    fn snapshot(books: Vec<Book>) -> BookSnapshot<tycho_simulation::book::ReceivedAt> {
        let mut result = Books::new();
        result.extend(books);
        BookSnapshot::received_now(result)
    }

    #[tokio::test]
    async fn providers_round_trip_through_broadcast_and_keep_exact_quotes() {
        let cache = BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Rfq]);
        let mut providers = ProviderBooks::default();
        let (base, quote) = tokens();
        let amount = BigUint::from(1_000_000_000_000_000_000u64);
        for (provider, id) in [("rfq:bebop", 3), ("rfq:native", 4), ("rfq:metric", 5)] {
            let original = book(provider, id);
            let expected = original
                .state
                .get_amount_out(amount.clone(), &base, &quote)
                .unwrap();
            let update = providers
                .replace(provider, Some(snapshot(vec![original])))
                .unwrap();
            let message = cache.apply_update(&update).await.unwrap();
            let wire = serde_json::to_string(&message).unwrap();
            assert!(!wire.contains("client"));
            let replay: BroadcasterUpdateMessage = serde_json::from_str(&wire).unwrap();
            assert_eq!(replay.partitions.len(), 1);
            let partition = &replay.partitions[0];
            assert_eq!(partition.backend, BroadcasterBackend::Rfq);
            let entry = &partition.new_pairs[0];
            assert_eq!(entry.component.protocol_system, provider);
            let actual = entry
                .state
                .get_amount_out(amount.clone(), &base, &quote)
                .unwrap();
            assert_eq!(actual.amount, expected.amount);
            assert_eq!(actual.gas, expected.gas);
        }
        let update = providers.replace("rfq:native", None).unwrap();
        let removal = cache.apply_update(&update).await.unwrap();
        let removal: BroadcasterUpdateMessage =
            serde_json::from_slice(&serde_json::to_vec(&removal).unwrap()).unwrap();
        assert_eq!(removal.partitions[0].removed_pairs.len(), 1);
        assert_eq!(
            removal.partitions[0].removed_pairs[0]
                .component
                .protocol_system,
            "rfq:native"
        );
        let exported = cache.export_snapshot(1_000_000).await.unwrap();
        let protocols: Vec<_> = exported
            .payloads
            .iter()
            .filter_map(|payload| match payload {
                BroadcasterPayload::SnapshotChunk(chunk) => Some(chunk),
                _ => None,
            })
            .flat_map(|chunk| &chunk.partitions)
            .flat_map(|partition| &partition.states)
            .map(|entry| entry.component.protocol_system.as_str())
            .collect();
        assert_eq!(protocols.len(), 2);
        assert!(protocols.contains(&"rfq:bebop"));
        assert!(protocols.contains(&"rfq:metric"));
    }

    #[test]
    fn complete_books_replace_pairs_and_withdrawal_is_idempotent() {
        let mut providers = ProviderBooks::default();
        let initial = providers
            .replace(
                "rfq:native",
                Some(snapshot(vec![book("rfq:native", 3), book("rfq:native", 4)])),
            )
            .unwrap();
        assert_eq!(initial.new_pairs.len(), 2);
        let next = providers
            .replace(
                "rfq:native",
                Some(snapshot(vec![book("rfq:native", 4), book("rfq:native", 5)])),
            )
            .unwrap();
        assert_eq!(next.states.len(), 2);
        assert_eq!(next.new_pairs.len(), 1);
        assert_eq!(next.removed_pairs.len(), 1);
        let empty = providers
            .replace("rfq:native", Some(snapshot(vec![])))
            .unwrap();
        assert_eq!(empty.removed_pairs.len(), 2);
        assert!(providers.replace("rfq:native", None).is_none());
        let resumed = providers
            .replace("rfq:native", Some(snapshot(vec![book("rfq:native", 4)])))
            .unwrap();
        assert_eq!(resumed.new_pairs.len(), 1);
    }
    #[tokio::test]
    #[ignore = "requires NATIVE_API_KEY and METRIC_API_KEY; reads live Base books"]
    async fn live_base_native_and_metric_broadcast() -> Result<()> {
        use crate::models::tokens::TokenStore;
        use std::{collections::HashSet, str::FromStr, time::Duration};
        let tokens: HashMap<_, _> = [
            ("0x4200000000000000000000000000000000000006", "WETH", 18),
            ("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "USDC", 6),
            ("0xcBb7C0000aB88B473b1f5aFd9ef808440eed33Bf", "cbBTC", 8),
        ]
        .into_iter()
        .map(|(address, symbol, decimals)| {
            let token = Token::new(
                &Bytes::from_str(address).unwrap(),
                symbol,
                decimals,
                0,
                &[],
                Chain::Base,
                100,
            );
            (token.address.clone(), token)
        })
        .collect();
        let store = Arc::new(TokenStore::new(
            tokens,
            String::new(),
            String::new(),
            Chain::Base,
            Duration::from_secs(1),
        ));
        let mut stream = build_rfq_stream(
            RFQTokenStores {
                tokens: store.clone(),
                bebop: store.clone(),
                hashflow: store.clone(),
                liquorice: store,
            },
            Chain::Base,
            &[ProtocolKind::NativeRfq, ProtocolKind::Metric],
            RFQConfig {
                native_key: std::env::var("NATIVE_API_KEY")?,
                metric_key: std::env::var("METRIC_API_KEY")?,
                min_tvl_usd: 100.0,

                bebop_key: String::new(),
                hashflow_user: String::new(),
                hashflow_key: String::new(),
                liquorice_user: String::new(),
                liquorice_key: String::new(),
            },
        )
        .await?;
        let cache = BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Rfq]);
        let mut seen = HashSet::new();
        tokio::time::timeout(Duration::from_secs(45), async {
            while seen.len() < 2 {
                let update = stream
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("RFQ feed ended"))?
                    .map_err(|error| anyhow!("RFQ feed failed: {error}"))?;
                let published = cache.apply_update(&update).await?;
                let replay: BroadcasterUpdateMessage =
                    serde_json::from_slice(&serde_json::to_vec(&published)?)?;
                seen.extend(verify_live_update(replay, &update)?);
            }
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }
    fn verify_live_update(
        replay: BroadcasterUpdateMessage,
        update: &Update,
    ) -> Result<Vec<String>> {
        let mut providers = Vec::new();
        for partition in replay.partitions {
            assert_eq!(partition.backend, BroadcasterBackend::Rfq);
            for entry in partition.new_pairs {
                let component = &entry.component;
                let base = &component.tokens[0];
                let quote = &component.tokens[1];
                let before = update.states[&entry.component_id].spot_price(base, quote)?;
                assert_eq!(entry.state.spot_price(base, quote)?, before);
                assert!(before.is_finite() && before > 0.0);
                providers.push(component.protocol_system.clone());
                eprintln!(
                    "Live Base broadcast round-trip: {} {} / {}",
                    component.protocol_system, base.symbol, quote.symbol
                );
            }
        }
        Ok(providers)
    }
}
