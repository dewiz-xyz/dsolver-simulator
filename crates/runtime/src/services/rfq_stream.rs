//! Adapt complete provider books to the broadcaster's existing RFQ state/delta contract.

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use futures::{
    future::ready,
    stream::{self, BoxStream},
    Stream, StreamExt,
};
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
) -> Result<BoxStream<'static, Result<Update, StreamError>>> {
    let books = protocols
        .iter()
        .copied()
        .filter(|protocol| protocol.titan_venue().is_none())
        .collect::<Vec<_>>();
    let titan = if protocols
        .iter()
        .any(|protocol| protocol.titan_venue().is_some())
    {
        super::titan_stream::build_titan_stream(
            chain,
            protocols,
            token_stores.tokens.snapshot().await,
        )?
        .map(RfqEvent::Update)
        .boxed()
    } else {
        stream::empty().boxed()
    };
    let books = build_book_stream(token_stores, chain, &books, config).await?;
    Ok(merge_streams(books, titan))
}

async fn build_book_stream(
    token_stores: RFQTokenStores,
    chain: Chain,
    protocols: &[ProtocolKind],
    config: RFQConfig,
) -> Result<BoxStream<'static, RfqEvent>> {
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
    Ok(book_updates(feeds))
}

#[expect(
    clippy::large_enum_variant,
    reason = "updates dominate; avoid allocating each update just for a terminal sentinel"
)]
enum RfqEvent {
    Update(Update),
    Ended,
}

fn merge_streams(
    books: BoxStream<'static, RfqEvent>,
    titan: BoxStream<'static, RfqEvent>,
) -> BoxStream<'static, Result<Update, StreamError>> {
    stream::select(books, titan)
        .scan((), |(), event| {
            ready(match event {
                RfqEvent::Update(update) => Some(Ok(update)),
                // Closing the whole stream lets the existing supervisor rebuild every feed with
                // backoff. A surviving provider (including Titan) must not hide a terminal failure.
                RfqEvent::Ended => None,
            })
        })
        .boxed()
}

fn book_updates(
    feeds: impl Stream<Item = (String, BookFeedEvent)> + Send + 'static,
) -> BoxStream<'static, RfqEvent> {
    let mut books = ProviderBooks::default();
    feeds.flat_map(move |(provider, event)| {
        let ended = matches!(event, BookFeedEvent::Ended(_));
        let snapshot = match event {
            BookFeedEvent::Published(snapshot) => Some(snapshot),
            BookFeedEvent::Withdrawn => None,
            BookFeedEvent::Ended(outcome) => {
                warn!(%provider, ?outcome, "RFQ provider ended; withdrawing liquidity and restarting feeds");
                None
            }
        };
        let mut events = Vec::with_capacity(2);
        if let Some(update) = books.replace(&provider, snapshot) {
            events.push(RfqEvent::Update(update));
        }
        if ended {
            events.push(RfqEvent::Ended);
        }
        stream::iter(events)
    }).boxed()
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
        rfq::protocols::{
            bebop::state::BebopState, hashflow::state::HashflowState, native::state::NativeState,
        },
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
                Chain::Ethereum,
                100,
            ),
            Token::new(
                &Bytes::from([2u8; 20]),
                "USDC",
                6,
                0,
                &[],
                Chain::Ethereum,
                100,
            ),
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
            "rfq:hashflow" => Arc::new(
                serde_json::from_value::<HashflowState>(json!({
                    "base_token": base, "quote_token": quote,
                    "market_maker": format!("maker-{id}"),
                    "levels": {"pair": {"baseToken": base.address, "quoteToken": quote.address},
                               "levels": [{"p": "3000", "q": "1"}, {"p": "2990", "q": "10"}]}
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
                "rfq:hashflow" => "hashflow_pool",
                "rfq:metric" => "metric_pool",
                _ => "bebop_pool",
            }
            .to_string(),
            Chain::Ethereum,
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
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]);
        let mut providers = ProviderBooks::default();
        let (base, quote) = tokens();
        let amount = BigUint::from(1_000_000_000_000_000_000u64);
        for (provider, id) in [
            ("rfq:bebop", 3),
            ("rfq:native", 4),
            ("rfq:metric", 5),
            ("rfq:hashflow", 6),
        ] {
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
        assert_eq!(protocols.len(), 3);
        assert!(protocols.contains(&"rfq:bebop"));
        assert!(protocols.contains(&"rfq:metric"));
        assert!(protocols.contains(&"rfq:hashflow"));
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
    async fn ended_provider_withdraws_then_ends_combined_stream_even_with_active_titan() {
        use tycho_simulation::book::BookFeedOutcome;
        for published_first in [false, true] {
            let mut events = vec![(
                "rfq:bebop".to_string(),
                BookFeedEvent::Published(snapshot(vec![book("rfq:bebop", 3)])),
            )];
            if published_first {
                events.push((
                    "rfq:native".into(),
                    BookFeedEvent::Published(snapshot(vec![book("rfq:native", 4)])),
                ));
            }
            events.push((
                "rfq:native".into(),
                BookFeedEvent::Ended(BookFeedOutcome::RanOut),
            ));
            let books = book_updates(stream::iter(events));
            let titan = stream::repeat_with(|| {
                RfqEvent::Update(Update::new(
                    current_time_ms(),
                    HashMap::new(),
                    HashMap::new(),
                ))
            })
            .boxed();
            let updates = merge_streams(books, titan)
                .take(12)
                .collect::<Vec<_>>()
                .await;
            assert!(
                updates.len() < 12,
                "Titan must not hide a terminal book failure"
            );
            let removed = updates
                .into_iter()
                .flat_map(|update| update.unwrap().removed_pairs.into_values())
                .collect::<Vec<_>>();
            assert_eq!(removed.len(), usize::from(published_first));
            assert!(removed
                .iter()
                .all(|component| component.protocol_system == "rfq:native"));
        }
    }

    #[tokio::test]
    async fn temporary_withdrawal_keeps_feed_alive_for_republication() {
        let events = stream::iter([
            (
                "rfq:native".into(),
                BookFeedEvent::Published(snapshot(vec![book("rfq:native", 4)])),
            ),
            ("rfq:native".into(), BookFeedEvent::Withdrawn),
            (
                "rfq:native".into(),
                BookFeedEvent::Published(snapshot(vec![book("rfq:native", 4)])),
            ),
        ]);
        let updates = merge_streams(book_updates(events), stream::empty().boxed())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[1].as_ref().unwrap().removed_pairs.len(), 1);
        assert_eq!(updates[2].as_ref().unwrap().new_pairs.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires NATIVE_API_KEY and METRIC_API_KEY; reads live Base books"]
    async fn live_base_native_and_metric_broadcast() -> Result<()> {
        live_book_broadcast(
            Chain::Base,
            &[ProtocolKind::NativeRfq, ProtocolKind::Metric],
        )
        .await
    }

    #[tokio::test]
    #[ignore = "requires NATIVE_API_KEY and METRIC_API_KEY; reads live Ethereum books"]
    async fn live_ethereum_native_and_metric_broadcast() -> Result<()> {
        live_book_broadcast(
            Chain::Ethereum,
            &[ProtocolKind::NativeRfq, ProtocolKind::Metric],
        )
        .await
    }

    #[tokio::test]
    #[ignore = "requires HASHFLOW_USER and HASHFLOW_KEY; reads live Ethereum books"]
    async fn live_ethereum_hashflow_broadcast() -> Result<()> {
        live_book_broadcast(Chain::Ethereum, &[ProtocolKind::Hashflow]).await
    }

    async fn live_book_broadcast(chain: Chain, protocols: &[ProtocolKind]) -> Result<()> {
        use crate::models::tokens::TokenStore;
        use std::{collections::HashSet, str::FromStr, time::Duration};
        let addresses = match chain {
            Chain::Base => [
                ("0x4200000000000000000000000000000000000006", "WETH", 18),
                ("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "USDC", 6),
                ("0xcBb7C0000aB88B473b1f5aFd9ef808440eed33Bf", "cbBTC", 8),
            ],
            Chain::Ethereum => [
                ("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "WETH", 18),
                ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC", 6),
                ("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "WBTC", 8),
            ],
            _ => bail!("unsupported test chain"),
        };
        let tokens: HashMap<_, _> = addresses
            .into_iter()
            .map(|(address, symbol, decimals)| {
                let token = Token::new(
                    &Bytes::from_str(address).unwrap(),
                    symbol,
                    decimals,
                    0,
                    &[],
                    chain,
                    100,
                );
                (token.address.clone(), token)
            })
            .collect();
        let store = Arc::new(TokenStore::new(
            tokens,
            String::new(),
            String::new(),
            chain,
            Duration::from_secs(1),
        ));
        let mut stream = build_rfq_stream(
            RFQTokenStores {
                tokens: store.clone(),
                bebop: store.clone(),
                hashflow: store.clone(),
                liquorice: store,
            },
            chain,
            protocols,
            RFQConfig {
                native_key: std::env::var("NATIVE_API_KEY").unwrap_or_default(),
                metric_key: std::env::var("METRIC_API_KEY").unwrap_or_default(),
                min_tvl_usd: 100.0,

                bebop_key: String::new(),
                hashflow_user: std::env::var("HASHFLOW_USER").unwrap_or_default(),
                hashflow_key: std::env::var("HASHFLOW_KEY").unwrap_or_default(),
                liquorice_user: String::new(),
                liquorice_key: String::new(),
            },
        )
        .await?;
        let cache = BroadcasterSnapshotCache::new(
            if chain == Chain::Base { 8453 } else { 1 },
            vec![BroadcasterBackend::Rfq],
        );
        let mut seen = HashSet::new();
        tokio::time::timeout(Duration::from_secs(45), async {
            while seen.len() < protocols.len() {
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
                    "Live broadcast round-trip: {} {} / {}",
                    component.protocol_system, base.symbol, quote.symbol
                );
            }
        }
        Ok(providers)
    }
}
