//! Preserve Titan's process-local quote deadline across snapshots and Redis deltas.
//! The absolute deadline survives transport delays; receiving a book never renews it.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tycho_simulation::{
    price_level_stream::state::{PriceLevelStreamState, QUOTE_TTL},
    tycho_common::simulation::protocol_sim::ProtocolSim,
};

const DEADLINE: &str = "quote_expires_at_unix_ms";

#[expect(
    clippy::borrowed_box,
    reason = "serde with requires the field's Box type"
)]
pub(super) fn serialize<S>(state: &Box<dyn ProtocolSim>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(titan) = state.as_any().downcast_ref::<PriceLevelStreamState>() else {
        return state.serialize(serializer);
    };
    let until = titan.quotable_until().ok_or_else(|| {
        serde::ser::Error::custom("Titan broadcast state must carry a quote deadline")
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(serde::ser::Error::custom)?;
    let remaining = until.saturating_duration_since(Instant::now());
    let deadline = u64::try_from(now.saturating_add(remaining).as_millis())
        .map_err(serde::ser::Error::custom)?;
    let mut value = serde_json::to_value(state).map_err(serde::ser::Error::custom)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| serde::ser::Error::custom("expected Titan state object"))?;
    object.insert(DEADLINE.into(), deadline.into());
    value.serialize(serializer)
}

pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Box<dyn ProtocolSim>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let deadline = value.get(DEADLINE).and_then(serde_json::Value::as_u64);
    let mut state: Box<dyn ProtocolSim> =
        serde_json::from_value(value).map_err(serde::de::Error::custom)?;
    if let Some(titan) = state.as_any_mut().downcast_mut::<PriceLevelStreamState>() {
        let deadline = deadline.ok_or_else(|| {
            serde::de::Error::custom("Titan broadcast state is missing its quote deadline")
        })?;
        let received_at = Instant::now();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(serde::de::Error::custom)?;
        let remaining = Duration::from_millis(deadline)
            .saturating_sub(now)
            .min(QUOTE_TTL);
        *titan = titan.clone().with_quotable_until(received_at + remaining);
    }
    Ok(state)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
mod tests {
    use super::*;
    use crate::broadcaster::{BroadcasterBackend, BroadcasterStateDelta, BroadcasterStateEntry};
    use tycho_simulation::{
        price_level_stream::state::PriceLevelStreamQuote,
        tycho_common::{
            models::{token::Token, Chain},
            Bytes,
        },
    };

    fn fixture() -> (BroadcasterStateDelta, Token, Token) {
        let token = |id| {
            Token::new(
                &Bytes::from([id; 20]),
                "TOKEN",
                0,
                0,
                &[],
                Chain::Ethereum,
                100,
            )
        };
        let (base, quote) = (token(1), token(2));
        let state = PriceLevelStreamState::new(
            base.address.clone(),
            quote.address.clone(),
            vec![PriceLevelStreamQuote::new(10u64.into(), 20u64.into())],
            vec![PriceLevelStreamQuote::new(20u64.into(), 10u64.into())],
            100u64.into(),
        )
        .with_quotable_until(Instant::now() + Duration::from_secs(5));
        (
            BroadcasterStateDelta::new("pool", BroadcasterBackend::Rfq, Box::new(state)),
            base,
            quote,
        )
    }

    #[test]
    fn round_trip_preserves_quotes_and_does_not_renew_deadline() {
        let (delta, base, quote) = fixture();
        let original_until = delta
            .state
            .as_any()
            .downcast_ref::<PriceLevelStreamState>()
            .unwrap()
            .quotable_until()
            .unwrap();
        let encoded = serde_json::to_vec(&delta).unwrap();
        let decoded: BroadcasterStateDelta = serde_json::from_slice(&encoded).unwrap();
        let actual = decoded
            .state
            .get_amount_out(10u64.into(), &base, &quote)
            .unwrap();
        assert_eq!(actual.amount, 20u64.into());
        let until = decoded
            .state
            .as_any()
            .downcast_ref::<PriceLevelStreamState>()
            .unwrap()
            .quotable_until()
            .unwrap();
        assert!(until <= original_until);
        // A second publication (e.g. a cached snapshot) must keep the same deadline.
        let before: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        let after = serde_json::to_value(decoded).unwrap();
        assert!(
            after["state"][DEADLINE].as_u64().unwrap()
                <= before["state"][DEADLINE].as_u64().unwrap()
        );
    }

    #[test]
    fn delayed_replay_rejects_expired_quotes() {
        let (delta, base, quote) = fixture();
        let mut component = crate::broadcaster::test_support::protocol_component(
            "pool",
            "pricelevelstream:fermiswap",
        );
        component.tokens = vec![base.clone(), quote.clone()];
        let snapshot = BroadcasterStateEntry::new("pool", component, delta.state.clone_box());
        let mut snapshot_wire = serde_json::to_value(snapshot).unwrap();
        let fresh: BroadcasterStateEntry = serde_json::from_value(snapshot_wire.clone()).unwrap();
        assert_eq!(fresh.state.spot_price(&base, &quote).unwrap(), 2.0);
        snapshot_wire["state"][DEADLINE] = 0u64.into();
        let expired: BroadcasterStateEntry = serde_json::from_value(snapshot_wire).unwrap();
        assert!(expired.state.spot_price(&base, &quote).is_err());

        let mut encoded = serde_json::to_value(delta).unwrap();
        encoded["state"][DEADLINE] = 0u64.into();
        let decoded: BroadcasterStateDelta = serde_json::from_value(encoded).unwrap();
        assert!(decoded
            .state
            .get_amount_out(10u64.into(), &base, &quote)
            .is_err());
        assert!(decoded.state.spot_price(&base, &quote).is_err());
        assert!(decoded
            .state
            .get_limits(base.address.clone(), quote.address.clone())
            .is_err());
    }

    #[test]
    fn unguarded_or_missing_deadline_is_rejected() {
        let (mut delta, _, _) = fixture();
        let mut encoded = serde_json::to_value(&delta).unwrap();
        encoded["state"].as_object_mut().unwrap().remove(DEADLINE);
        assert!(serde_json::from_value::<BroadcasterStateDelta>(encoded).is_err());
        let state = delta
            .state
            .as_any()
            .downcast_ref::<PriceLevelStreamState>()
            .unwrap();
        // Upstream serde drops the monotonic deadline.
        delta.state = Box::new(
            serde_json::from_value::<PriceLevelStreamState>(serde_json::to_value(state).unwrap())
                .unwrap(),
        );
        assert!(serde_json::to_value(delta).is_err());
    }
}
