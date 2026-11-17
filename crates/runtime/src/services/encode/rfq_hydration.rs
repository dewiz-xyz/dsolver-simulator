//! Restore local firm-quote clients without changing the broadcast pricing state.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use tycho_simulation::{
    book::BookFeedConfig,
    rfq::protocols::{
        bebop::{state::BebopState, BebopBook, BebopBookAssemblerBuilder, BebopPrices},
        hashflow::{
            state::HashflowState, HashflowBookAssemblerBuilder, HashflowMarketMakerLevels,
            HashflowPrices,
        },
    },
    tycho_common::{
        dto::ProtocolStateDelta,
        models::{token::Token, Chain},
        simulation::{
            errors::{SimulationError, TransitionError},
            indicatively_priced::IndicativelyPriced,
            protocol_sim::{Balances, BlockContext, GetAmountOutResult, ProtocolSim},
        },
        Bytes,
    },
};

use crate::models::state::RfqClientConfig;

#[derive(Deserialize)]
struct BebopSnapshot {
    base_token: Token,
    quote_token: Token,
    book: BebopBook,
}

#[derive(Deserialize)]
struct HashflowSnapshot {
    base_token: Token,
    quote_token: Token,
    levels: HashflowMarketMakerLevels,
    market_maker: String,
}

pub(super) fn hydrate(
    state: &dyn ProtocolSim,
    chain: Chain,
    config: &RfqClientConfig,
    quote_timeout: Duration,
    bebop: bool,
) -> Result<Arc<dyn ProtocolSim>> {
    let books = if bebop {
        let state = state
            .as_any()
            .downcast_ref::<BebopState>()
            .ok_or_else(|| anyhow!("Expected Bebop snapshot"))?;
        let snapshot: BebopSnapshot = serde_json::from_value(serde_json::to_value(state)?)?;
        let (book_config, quote_tokens) =
            signing_config(chain, snapshot.base_token, snapshot.quote_token);
        BebopBookAssemblerBuilder::new(book_config, quote_tokens, config.bebop_key.clone())
            .quote_timeout(quote_timeout)
            .build()?
            .books(BebopPrices {
                pairs: vec![snapshot.book],
            })
    } else {
        let state = state
            .as_any()
            .downcast_ref::<HashflowState>()
            .ok_or_else(|| anyhow!("Expected Hashflow snapshot"))?;
        let snapshot: HashflowSnapshot = serde_json::from_value(serde_json::to_value(state)?)?;
        let (book_config, quote_tokens) =
            signing_config(chain, snapshot.base_token, snapshot.quote_token);
        HashflowBookAssemblerBuilder::new(
            book_config,
            quote_tokens,
            config.hashflow_user.clone(),
            config.hashflow_key.clone(),
        )
        .quote_timeout(quote_timeout)
        .build()?
        .books(HashflowPrices {
            levels_by_maker: HashMap::from([(snapshot.market_maker, vec![snapshot.levels])]),
        })
    };
    let signing = books
        .into_values()
        .next()
        .ok_or_else(|| anyhow!("RFQ snapshot has no quoteable book"))?
        .state;
    Ok(Arc::new(RehydratedRfqState {
        pricing: state.clone_box(),
        signing: Some(signing),
    }))
}

fn signing_config(chain: Chain, base: Token, quote: Token) -> (BookFeedConfig, HashSet<Bytes>) {
    // This assembler only supplies the signing client. Its USD/TVL classification never
    // prices a trade: every simulation operation delegates to the original snapshot below.
    let quote_tokens = HashSet::from([quote.address.clone()]);
    (
        BookFeedConfig {
            chain,
            tokens: Arc::new(HashMap::from([
                (base.address.clone(), base),
                (quote.address.clone(), quote),
            ])),
            min_tvl_usd: 0.0,
        },
        quote_tokens,
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RehydratedRfqState {
    pricing: Box<dyn ProtocolSim>,
    #[serde(skip)]
    signing: Option<Arc<dyn ProtocolSim>>,
}

#[typetag::serde]
impl ProtocolSim for RehydratedRfqState {
    fn fee(&self) -> f64 {
        self.pricing.fee()
    }
    fn spot_price(&self, base: &Token, quote: &Token) -> Result<f64, SimulationError> {
        self.pricing.spot_price(base, quote)
    }
    fn get_amount_out(
        &self,
        amount: BigUint,
        token_in: &Token,
        token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        let result = self.pricing.get_amount_out(amount, token_in, token_out)?;
        Ok(GetAmountOutResult::new(
            result.amount,
            result.gas,
            Box::new(Self {
                pricing: result.new_state,
                signing: self.signing.clone(),
            }),
        ))
    }
    fn get_limits(&self, sell: Bytes, buy: Bytes) -> Result<(BigUint, BigUint), SimulationError> {
        self.pricing.get_limits(sell, buy)
    }
    fn delta_transition(
        &mut self,
        delta: ProtocolStateDelta,
        tokens: &HashMap<Bytes, Token>,
        balances: &Balances,
    ) -> Result<(), TransitionError> {
        self.pricing.delta_transition(delta, tokens, balances)
    }
    fn apply_block(&mut self, block: &BlockContext) -> bool {
        self.pricing.apply_block(block)
    }
    fn clone_box(&self) -> Box<dyn ProtocolSim> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self.pricing.eq(other.pricing.as_ref()))
    }
    fn as_indicatively_priced(&self) -> Result<&dyn IndicativelyPriced, SimulationError> {
        self.signing
            .as_ref()
            .ok_or_else(|| {
                SimulationError::FatalError("RFQ signing client was not restored".to_string())
            })?
            .as_indicatively_priced()
    }
}
