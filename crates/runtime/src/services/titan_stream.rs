//! Titan's Ethereum books share the RFQ broadcast partition, but retain their own identities.

use std::{collections::HashMap, str::FromStr};

use anyhow::{anyhow, bail, Result};
use futures::{stream::BoxStream, StreamExt};
use simulator_core::models::protocol::ProtocolKind;
use tycho_simulation::{
    price_level_stream::{
        config::{default_served_pamms, PriceLevelStreamConfig},
        stream::PriceLevelStreamBuilder,
    },
    protocol::models::Update,
    tycho_common::{
        models::{token::Token, Chain},
        Bytes,
    },
};

use crate::broadcaster::redis_publisher::current_time_ms;

// Titan's live Fermi router replaced the address in Tycho 0.525.0's built-in registry.
// https://docs.titanbuilder.xyz/propamms
const FERMI_ROUTER: &str = "0xf6d5b2fefec7b420d9990b95790adfd3d4c4f977";

pub(super) fn build_titan_stream(
    chain: Chain,
    protocols: &[ProtocolKind],
    tokens: HashMap<Bytes, Token>,
) -> Result<BoxStream<'static, Update>> {
    let mut builder = PriceLevelStreamBuilder::new()
        .with_tokens(tokens)
        // This service publishes prices; downstream consumers choose their settlement path.
        .without_fallback_router();
    for config in venue_configs(chain, protocols)? {
        builder = builder.add_pamm(config);
    }
    Ok(builder
        .build()
        .map(|mut update| {
            // RFQ partition heads are observation timestamps, not mined chain heads. Titan's
            // target block and sequence remain in the serialized simulation overrides.
            update.block_number = current_time_ms();
            update
        })
        .boxed())
}

fn venue_configs(chain: Chain, protocols: &[ProtocolKind]) -> Result<Vec<PriceLevelStreamConfig>> {
    let mut selected = Vec::new();
    let mut registry = default_served_pamms();
    for protocol in protocols {
        let Some(venue) = protocol.titan_venue() else {
            continue;
        };
        if chain != Chain::Ethereum {
            bail!("Titan price-level feeds support Ethereum only");
        }
        let config = registry
            .iter_mut()
            .find(|config| config.protocol == venue)
            .ok_or_else(|| anyhow!("Tycho has no Titan registration for {venue}"))?;
        if venue == "fermiswap" {
            config.address = Bytes::from_str(FERMI_ROUTER)?;
        }
        selected.push(config.clone());
    }
    Ok(selected)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
mod tests {
    use super::*;
    use crate::broadcaster::state::BroadcasterSnapshotCache;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterPayload, BroadcasterUpdateMessage,
    };
    use std::collections::HashSet;
    use std::time::Duration;

    fn protocols() -> Vec<ProtocolKind> {
        ProtocolKind::ALL
            .iter()
            .copied()
            .filter(|protocol| protocol.titan_venue().is_some())
            .collect()
    }

    #[test]
    fn named_venues_exclude_direct_metric_and_use_current_fermi_router() {
        let configs = venue_configs(Chain::Ethereum, &protocols()).unwrap();
        let names: HashSet<_> = configs
            .iter()
            .map(|config| config.protocol.as_str())
            .collect();
        assert_eq!(
            names,
            HashSet::from([
                "fermiswap",
                "kipseli",
                "bebop",
                "taurusfi",
                "tempest",
                "goliath",
                "stelaxis",
                "elzorro"
            ])
        );
        assert_eq!(
            configs
                .iter()
                .find(|config| config.protocol == "fermiswap")
                .unwrap()
                .address,
            Bytes::from_str(FERMI_ROUTER).unwrap()
        );
        assert!(venue_configs(Chain::Base, &protocols()).is_err());
    }

    #[tokio::test]
    #[ignore = "reads Titan's public Ethereum WebSocket; requires network access and active books"]
    async fn live_ethereum_titan_broadcast() -> Result<()> {
        let tokens: HashMap<_, _> = [
            ("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "WETH", 18),
            ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC", 6),
            ("0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT", 6),
            ("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "WBTC", 8),
        ]
        .into_iter()
        .map(|(address, symbol, decimals)| {
            let token = Token::new(
                &Bytes::from_str(address).unwrap(),
                symbol,
                decimals,
                0,
                &[],
                Chain::Ethereum,
                100,
            );
            (token.address.clone(), token)
        })
        .collect();
        let protocols = protocols();
        let mut stream = build_titan_stream(Chain::Ethereum, &protocols, tokens)?;
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]);
        let mut seen = HashSet::new();
        let mut components = HashMap::new();
        let result = tokio::time::timeout(Duration::from_secs(45), async {
            while seen.len() < protocols.len() {
                let update = stream
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("Titan stream ended"))?;
                assert!(
                    update.block_number > 1_000_000_000_000,
                    "RFQ cursor must use milliseconds"
                );
                let message = cache.apply_update(&update).await?;
                let replay: BroadcasterUpdateMessage =
                    serde_json::from_slice(&serde_json::to_vec(&message)?)?;
                verify_live_update(replay, &update, &mut seen, &mut components)?;
            }
            // Snapshots use a separate state entry serializer; validate that wire path too.
            for payload in cache.export_snapshot(1_000_000).await?.payloads {
                let _: BroadcasterPayload = serde_json::from_slice(&serde_json::to_vec(&payload)?)?;
            }
            anyhow::Ok(())
        })
        .await;
        result.map_err(|error| anyhow!("{error}; received venues: {seen:?}"))??;
        Ok(())
    }
    fn verify_live_update(
        replay: BroadcasterUpdateMessage,
        update: &Update,
        seen: &mut HashSet<String>,
        components: &mut HashMap<String, tycho_simulation::protocol::models::ProtocolComponent>,
    ) -> Result<()> {
        components.extend(update.new_pairs.clone());
        for partition in replay.partitions {
            assert_eq!(partition.backend, BroadcasterBackend::Rfq);
            let states = partition
                .new_pairs
                .into_iter()
                .map(|entry| (entry.component_id, entry.state))
                .chain(
                    partition
                        .updated_states
                        .into_iter()
                        .map(|entry| (entry.component_id, entry.state)),
                );
            for (id, state) in states {
                let component = &components[&id];
                if verify_live_quotes(component, update.states[&id].as_ref(), state.as_ref())?
                    && seen.insert(component.protocol_system.clone())
                {
                    eprintln!(
                        "Live Ethereum Titan broadcast: {}",
                        component.protocol_system
                    );
                }
            }
        }
        for id in update.removed_pairs.keys() {
            components.remove(id);
        }
        Ok(())
    }

    fn verify_live_quotes(
        component: &tycho_simulation::protocol::models::ProtocolComponent,
        original: &dyn tycho_simulation::tycho_common::simulation::protocol_sim::ProtocolSim,
        replayed: &dyn tycho_simulation::tycho_common::simulation::protocol_sim::ProtocolSim,
    ) -> Result<bool> {
        let mut quotable = false;
        let (base, quote) = (&component.tokens[0], &component.tokens[1]);
        for (base, quote) in [(base, quote), (quote, base)] {
            match (
                original.spot_price(base, quote),
                replayed.spot_price(base, quote),
            ) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(actual, expected);
                    assert!(expected.is_finite() && expected > 0.0);
                    let (amount, _) =
                        original.get_limits(base.address.clone(), quote.address.clone())?;
                    let before = original.get_amount_out(amount.clone(), base, quote)?;
                    let after = replayed.get_amount_out(amount, base, quote)?;
                    assert_eq!(after.amount, before.amount);
                    assert_eq!(after.gas, before.gas);
                    quotable = true;
                }
                (Err(expected), Err(actual)) => {
                    assert_eq!(actual.to_string(), expected.to_string())
                }
                (expected, actual) => bail!(
                    "Titan replay quote mismatch for {}: {expected:?} vs {actual:?}",
                    component.protocol_system
                ),
            }
        }
        Ok(quotable)
    }
}
