use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use futures::StreamExt;
use simulator_core::broadcaster::BroadcasterBackend;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::models::tokens::TokenStore;
use simulator_core::broadcaster::wire::FeedMessage;
use tycho_simulation::{
    evm::{
        decoder::TychoStreamDecoder,
        engine_db::tycho_db::PreCachedDB,
        protocol::{
            aero_basic::state::AeroBasicState,
            aerodrome_slipstreams::state::AerodromeSlipstreamsState,
            aerodrome_v1::state::AerodromeV1State,
            biconomy::state::BiconomyState,
            ekubo::state::EkuboState,
            ekubo_v3::{filter_fn as ekubo_v3_filter, state::EkuboV3State},
            erc4626::state::ERC4626State,
            filters::{balancer_v2_pool_filter, erc4626_filter, fluid_v1_paused_pools_filter},
            fluid::FluidV1,
            lunarbase::state::LunarBaseState,
            pancakeswap_infinity_bin::state::PancakeswapInfinityBinState,
            pancakeswap_v2::state::PancakeswapV2State,
            rocketpool::state::RocketpoolState,
            uniswap_v2::state::UniswapV2State,
            uniswap_v3::state::UniswapV3State,
            uniswap_v4::hooks::hook_handler_creator::initialize_hook_handlers,
            uniswap_v4::state::UniswapV4State,
            vm::state::EVMPoolState,
        },
    },
    tycho_client::feed::component_tracker::ComponentFilter,
    tycho_client::{
        feed::BlockHeader,
        stream::{RetryConfiguration, TychoStreamBuilder},
    },
    tycho_common::models::Chain,
};

pub(crate) const RFQ_QUOTE_TIMEOUT: Duration = Duration::from_secs(5);
// Upper cap for the firm-quote timeout on hydrated encode clients. Firm quotes block the encode
// task through block_in_place/block_on, so the request guard cannot interrupt them; the encode path
// additionally clamps this below the configured guard. Streaming clients keep RFQ_QUOTE_TIMEOUT
// because they do not request firm quotes.
pub(crate) const ENCODE_RFQ_QUOTE_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const LIQUORICE_QUOTE_EXPIRY_SECS: u64 = 300;

pub async fn build_broadcaster_raw_stream(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    chain: Chain,
    protocols: &BroadcasterProtocols,
) -> Result<
    impl futures::Stream<
            Item = Result<
                FeedMessage<BlockHeader>,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
> {
    let (mut builder, tvl_filter) = raw_base_builder(
        tycho_url,
        api_key,
        tvl_add_threshold,
        tvl_keep_threshold,
        chain,
    );

    for protocol in protocols.native.iter().chain(protocols.vm.iter()) {
        builder = builder.exchange(protocol, tvl_filter.clone());
    }

    let (_handle, rx) = builder.build().await?;
    Ok(ReceiverStream::new(rx).map(|item| {
        item.map(Into::into)
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) })
    }))
}

pub async fn build_broadcaster_subscription_decoder(
    tokens: Arc<TokenStore>,
    chain: Chain,
    backend: BroadcasterBackend,
    protocols: &[String],
) -> Result<Arc<TychoStreamDecoder<BlockHeader>>> {
    let mut decoder = TychoStreamDecoder::new(chain);
    decoder.skip_state_decode_failures(true);

    match backend {
        BroadcasterBackend::Native => {
            for protocol in protocols {
                register_native_decoder(&mut decoder, protocol)?;
            }
        }
        BroadcasterBackend::Vm => {
            for protocol in protocols {
                register_vm_decoder(&mut decoder, protocol)?;
            }
        }
        BroadcasterBackend::Rfq => {}
    }

    initialize_hook_handlers().map_err(|error| {
        anyhow::anyhow!("failed to initialize Uniswap v4 hook handlers: {error:?}")
    })?;
    decoder.set_tokens(tokens.snapshot().await).await;
    Ok(Arc::new(decoder))
}

#[derive(Clone)]
pub struct RFQConfig {
    pub bebop_user: String,
    pub bebop_key: String,
    pub hashflow_user: String,
    pub hashflow_key: String,
    pub liquorice_user: String,
    pub liquorice_key: String,
    pub native_key: String,
    pub metric_key: String,
    pub min_tvl_usd: f64,
}

#[derive(Clone)]
pub struct RFQTokenStores {
    pub tokens: Arc<TokenStore>,
    pub bebop: Arc<TokenStore>,
    pub hashflow: Arc<TokenStore>,
    pub liquorice: Arc<TokenStore>,
}

#[derive(Clone, Debug, Default)]
pub struct BroadcasterProtocols {
    pub native: Vec<String>,
    pub vm: Vec<String>,
}

pub use super::rfq_stream::build_rfq_stream;

fn register_native_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: &str,
) -> Result<()> {
    match protocol {
        "uniswap_v2" | "sushiswap_v2" => decoder.register_decoder::<UniswapV2State>(protocol),
        "pancakeswap_v2" => decoder.register_decoder::<PancakeswapV2State>(protocol),
        "uniswap_v3" | "pancakeswap_v3" => decoder.register_decoder::<UniswapV3State>(protocol),
        "uniswap_v4" | "pancakeswap_infinity_cl" => {
            decoder.register_decoder::<UniswapV4State>(protocol)
        }
        "ekubo_v2" => decoder.register_decoder::<EkuboState>(protocol),
        "fluid_v1" => {
            decoder.register_decoder::<FluidV1>(protocol);
            decoder.register_filter(protocol, fluid_v1_paused_pools_filter);
        }
        "rocketpool" => decoder.register_decoder::<RocketpoolState>(protocol),
        "ekubo_v3" => {
            decoder.register_decoder::<EkuboV3State>(protocol);
            decoder.register_filter(protocol, ekubo_v3_filter);
        }
        "aerodrome_slipstreams" | "aero_cl" => {
            decoder.register_decoder::<AerodromeSlipstreamsState>(protocol)
        }
        "aerodrome_v1" => decoder.register_decoder::<AerodromeV1State>(protocol),
        "aero_basic" => decoder.register_decoder::<AeroBasicState>(protocol),
        "lunarbase" => decoder.register_decoder::<LunarBaseState>(protocol),
        "biconomy" => decoder.register_decoder::<BiconomyState>(protocol),
        "pancakeswap_infinity_bin" => {
            decoder.register_decoder::<PancakeswapInfinityBinState>(protocol)
        }
        "erc4626" => {
            decoder.register_decoder::<ERC4626State>(protocol);
            decoder.register_filter(protocol, erc4626_filter);
        }
        other => bail!("Unknown native protocol in chain profile: {}", other),
    }
    Ok(())
}

fn register_vm_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: &str,
) -> Result<()> {
    match protocol {
        "vm:balancer_v2" => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol);
            decoder.register_filter(protocol, balancer_v2_pool_filter);
        }
        "vm:curve" | "vm:maverick_v2" => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol);
        }
        other => bail!("Unknown VM protocol in chain profile: {}", other),
    }
    Ok(())
}

fn raw_base_builder(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    chain: Chain,
) -> (TychoStreamBuilder, ComponentFilter) {
    let add_tvl = tvl_add_threshold;
    let keep_tvl = tvl_keep_threshold.min(add_tvl);
    info!(
        "Using TVL thresholds: remove/keep={} add={}",
        keep_tvl, add_tvl
    );
    let tvl_filter = ComponentFilter::with_tvl_range(keep_tvl, add_tvl);

    let builder = TychoStreamBuilder::new(tycho_url, chain)
        .timeout(15)
        .websockets_retry_config(&RetryConfiguration::constant(
            u64::MAX,
            Duration::from_secs(1),
        ))
        .state_synchronizer_retry_config(&RetryConfiguration::constant(
            u64::MAX,
            Duration::from_secs(2),
        ))
        .auth_key(Some(api_key.to_string()));

    (builder, tvl_filter)
}

#[cfg(test)]
#[expect(
    clippy::panic,
    reason = "stream builder tests use explicit panics for invalid fixture setup"
)]
mod tests {
    use super::{register_native_decoder, register_vm_decoder};
    use crate::config::{load_manifest_registries, resolve_chain_config, MANIFEST_PATH};
    use std::{collections::BTreeSet, path::PathBuf};
    use tycho_simulation::{
        evm::decoder::TychoStreamDecoder, tycho_client::feed::BlockHeader,
        tycho_common::models::Chain,
    };

    fn manifest_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../")
            .join(MANIFEST_PATH)
    }

    fn chain_protocols(chain_id: u64) -> (Vec<String>, Vec<String>) {
        let registries = load_manifest_registries(&manifest_path())
            .unwrap_or_else(|err| panic!("test manifest must load: {err}"));
        let resolved = resolve_chain_config(&registries, chain_id, None)
            .unwrap_or_else(|err| panic!("test manifest must resolve chain {chain_id}: {err}"));

        (
            resolved.chain_profile.native_protocols,
            resolved.chain_profile.vm_protocols,
        )
    }

    #[test]
    fn manifest_native_protocols_all_register_successfully() {
        let (ethereum_native_protocols, _) = chain_protocols(1);
        let (base_native_protocols, _) = chain_protocols(8453);

        for protocol in ethereum_native_protocols
            .iter()
            .chain(base_native_protocols.iter())
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
        {
            let mut decoder = TychoStreamDecoder::<BlockHeader>::new(Chain::Ethereum);
            let result = register_native_decoder(&mut decoder, protocol);
            assert!(
                result.is_ok(),
                "expected native protocol {protocol} to register"
            );
        }
    }

    #[test]
    fn manifest_vm_protocols_all_register_successfully() {
        let (_, ethereum_vm_protocols) = chain_protocols(1);
        let (_, base_vm_protocols) = chain_protocols(8453);

        for protocol in ethereum_vm_protocols
            .iter()
            .chain(base_vm_protocols.iter())
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
        {
            let mut decoder = TychoStreamDecoder::<BlockHeader>::new(Chain::Ethereum);
            let result = register_vm_decoder(&mut decoder, protocol);
            assert!(
                result.is_ok(),
                "expected VM protocol {protocol} to register"
            );
        }
    }
}
