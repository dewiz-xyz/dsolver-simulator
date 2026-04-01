use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use tycho_simulation::{
    evm::{
        engine_db::tycho_db::PreCachedDB,
        protocol::{
            aerodrome_slipstreams::state::AerodromeSlipstreamsState,
            ekubo::state::EkuboState,
            ekubo_v3::state::EkuboV3State,
            erc4626::state::ERC4626State,
            filters::{balancer_v2_pool_filter, erc4626_filter, fluid_v1_paused_pools_filter},
            fluid::FluidV1,
            pancakeswap_v2::state::PancakeswapV2State,
            rocketpool::state::RocketpoolState,
            uniswap_v2::state::UniswapV2State,
            uniswap_v3::state::UniswapV3State,
            uniswap_v4::state::UniswapV4State,
            vm::state::EVMPoolState,
        },
        stream::ProtocolStreamBuilder,
    },
    protocol::models::Update,
    rfq::{
        protocols::{
            bebop::{client_builder::BebopClientBuilder, state::BebopState},
            hashflow::{client_builder::HashflowClientBuilder, state::HashflowState},
        },
        stream::RFQStreamBuilder,
    },
    tycho_client::feed::component_tracker::ComponentFilter,
    tycho_common::models::Chain,
};

use crate::models::tokens::TokenStore;

#[derive(Clone, Copy)]
enum StreamDecodePolicy {
    Native,
    Vm,
}

pub async fn build_native_stream(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    tokens: Arc<TokenStore>,
    chain: Chain,
    protocols: &[String],
) -> Result<
    impl futures::Stream<
            Item = Result<
                tycho_simulation::protocol::models::Update,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
> {
    let (mut builder, tvl_filter) = base_builder(
        tycho_url,
        api_key,
        tvl_add_threshold,
        tvl_keep_threshold,
        decode_skip_state_failures(StreamDecodePolicy::Native),
        chain,
    );

    for protocol in protocols {
        builder = register_native_protocol(builder, protocol, &tvl_filter)?;
    }

    let snapshot = tokens.snapshot().await;
    let stream = builder.set_tokens(snapshot).await.build().await?;

    Ok(stream.map(|item| {
        item.map_err(|err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) })
    }))
}

fn register_native_protocol(
    builder: ProtocolStreamBuilder,
    protocol: &str,
    tvl_filter: &ComponentFilter,
) -> Result<ProtocolStreamBuilder> {
    match protocol {
        "uniswap_v2" | "sushiswap_v2" => {
            Ok(builder.exchange::<UniswapV2State>(protocol, tvl_filter.clone(), None))
        }
        "pancakeswap_v2" => {
            Ok(builder.exchange::<PancakeswapV2State>(protocol, tvl_filter.clone(), None))
        }
        "uniswap_v3" | "pancakeswap_v3" => {
            Ok(builder.exchange::<UniswapV3State>(protocol, tvl_filter.clone(), None))
        }
        "uniswap_v4" => Ok(builder.exchange::<UniswapV4State>(protocol, tvl_filter.clone(), None)),
        "ekubo_v2" => Ok(builder.exchange::<EkuboState>(protocol, tvl_filter.clone(), None)),
        "fluid_v1" => Ok(builder.exchange::<FluidV1>(
            protocol,
            tvl_filter.clone(),
            Some(fluid_v1_paused_pools_filter),
        )),
        "rocketpool" => Ok(builder.exchange::<RocketpoolState>(protocol, tvl_filter.clone(), None)),
        "ekubo_v3" => Ok(builder.exchange::<EkuboV3State>(protocol, tvl_filter.clone(), None)),
        "aerodrome_slipstreams" => {
            Ok(builder.exchange::<AerodromeSlipstreamsState>(protocol, tvl_filter.clone(), None))
        }
        "erc4626" => Ok(builder.exchange::<ERC4626State>(
            protocol,
            tvl_filter.clone(),
            Some(erc4626_filter),
        )),
        other => bail!("Unknown native protocol in chain profile: {}", other),
    }
}

pub async fn build_vm_stream(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    tokens: Arc<TokenStore>,
    chain: Chain,
    protocols: &[String],
) -> Result<
    impl futures::Stream<
            Item = Result<
                tycho_simulation::protocol::models::Update,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
> {
    let (mut builder, tvl_filter) = base_builder(
        tycho_url,
        api_key,
        tvl_add_threshold,
        tvl_keep_threshold,
        decode_skip_state_failures(StreamDecodePolicy::Vm),
        chain,
    );

    for protocol in protocols {
        builder = register_vm_protocol(builder, protocol, &tvl_filter)?;
    }

    let snapshot = tokens.snapshot().await;
    let stream = builder.set_tokens(snapshot).await.build().await?;

    Ok(stream.map(|item| {
        item.map_err(|err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) })
    }))
}

#[derive(Clone)]
pub struct RFQConfig {
    pub bebop_user: String,
    pub bebop_key: String,
    pub hashflow_user: String,
    pub hashflow_key: String,
}

pub async fn build_rfq_stream(
    tvl_add_threshold: f64,
    tokens: Arc<TokenStore>,
    _chain: Chain,
    _protocols: &[String],
    bebop_tokens: Arc<TokenStore>,
    hashflow_tokens: Arc<TokenStore>,
    rfq_config: RFQConfig,
) -> Result<
    impl futures::Stream<
            Item = Result<
                tycho_simulation::protocol::models::Update,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
> {
    let mut rfq_builder = RFQStreamBuilder::new();

    info!("Setting up Bebop RFQ client...\n");
    let (user, key) = (rfq_config.bebop_user, rfq_config.bebop_key);
    let mut rfq_tokens_bebop = HashSet::new();
    for bebop_token_addr in bebop_tokens.snapshot().await.keys().clone() {
        rfq_tokens_bebop.insert(bebop_token_addr.clone());
    }
    let bebop_client = BebopClientBuilder::new(Chain::Ethereum, user, key)
        .tokens(rfq_tokens_bebop)
        .tvl_threshold(tvl_add_threshold)
        .build()
        .map_err(|err| anyhow::anyhow!("failed to create Bebop RFQ client: {err}"))?;
    rfq_builder = rfq_builder.add_client::<BebopState>("bebop", Box::new(bebop_client));

    info!("Setting up Hashflow RFQ client...\n");
    let (user, key) = (rfq_config.hashflow_user, rfq_config.hashflow_key);
    let mut rfq_tokens_hashflow = HashSet::new();
    for hashflow_token_addr in hashflow_tokens.snapshot().await.keys() {
        rfq_tokens_hashflow.insert(hashflow_token_addr.clone());
    }
    let hashflow_client = HashflowClientBuilder::new(Chain::Ethereum, user, key)
        .tokens(rfq_tokens_hashflow)
        .tvl_threshold(tvl_add_threshold)
        .poll_time(Duration::from_secs(5))
        .build()
        .map_err(|err| anyhow::anyhow!("failed to create Hashflow RFQ client: {err}"))?;
    rfq_builder = rfq_builder.add_client::<HashflowState>("hashflow", Box::new(hashflow_client));

    info!("Building RFQ Stream...\n");
    let (tx, /* mut */ rx) = mpsc::channel::<Update>(100);

    let snapshot = tokens.snapshot().await;

    rfq_builder = rfq_builder.set_tokens(snapshot.clone()).await;
    tokio::spawn(rfq_builder.build(tx));
    info!("Connected to RFQs! Streaming live price levels...\n");

    // todo consider implementing register_rfq_protocol...

    Ok(ReceiverStream::new(rx).map(Ok).boxed())
}

fn register_vm_protocol(
    builder: ProtocolStreamBuilder,
    protocol: &str,
    tvl_filter: &ComponentFilter,
) -> Result<ProtocolStreamBuilder> {
    match protocol {
        "vm:balancer_v2" => Ok(builder.exchange::<EVMPoolState<PreCachedDB>>(
            protocol,
            tvl_filter.clone(),
            Some(balancer_v2_pool_filter),
        )),
        "vm:curve" | "vm:maverick_v2" => {
            Ok(builder.exchange::<EVMPoolState<PreCachedDB>>(protocol, tvl_filter.clone(), None))
        }
        other => bail!("Unknown VM protocol in chain profile: {}", other),
    }
}

fn base_builder(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    skip_state_decode_failures: bool,
    chain: Chain,
) -> (ProtocolStreamBuilder, ComponentFilter) {
    let add_tvl = tvl_add_threshold;
    let keep_tvl = tvl_keep_threshold.min(add_tvl);
    info!(
        "Using TVL thresholds: remove/keep={} add={}",
        keep_tvl, add_tvl
    );
    let tvl_filter = ComponentFilter::with_tvl_range(keep_tvl, add_tvl);

    let builder = ProtocolStreamBuilder::new(tycho_url, chain)
        .latency_buffer(15)
        .auth_key(Some(api_key.to_string()))
        .skip_state_decode_failures(skip_state_decode_failures);

    (builder, tvl_filter)
}

fn decode_skip_state_failures(policy: StreamDecodePolicy) -> bool {
    matches!(policy, StreamDecodePolicy::Native | StreamDecodePolicy::Vm)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        decode_skip_state_failures, register_native_protocol, register_vm_protocol,
        StreamDecodePolicy,
    };
    use crate::config::{
        BASE_NATIVE_PROTOCOLS, BASE_VM_PROTOCOLS, ETHEREUM_NATIVE_PROTOCOLS, ETHEREUM_VM_PROTOCOLS,
    };
    use tycho_simulation::{
        evm::stream::ProtocolStreamBuilder, tycho_client::feed::component_tracker::ComponentFilter,
    };

    fn test_builder() -> ProtocolStreamBuilder {
        ProtocolStreamBuilder::new(
            "localhost",
            tycho_simulation::tycho_common::models::Chain::Ethereum,
        )
    }

    fn test_filter() -> ComponentFilter {
        ComponentFilter::with_tvl_range(0.0, 1.0)
    }

    fn unique_protocols<'a>(left: &'a [&'a str], right: &'a [&'a str]) -> BTreeSet<&'a str> {
        left.iter().chain(right.iter()).copied().collect()
    }

    #[test]
    fn native_stream_keeps_decode_skip_enabled() {
        assert!(decode_skip_state_failures(StreamDecodePolicy::Native));
    }

    #[test]
    fn vm_stream_keeps_decode_skip_enabled() {
        assert!(decode_skip_state_failures(StreamDecodePolicy::Vm));
    }

    #[test]
    fn unknown_native_protocol_returns_error() {
        let builder = test_builder();
        let filter = test_filter();
        let result = register_native_protocol(builder, "unknown_protocol", &filter);
        assert!(result.is_err());
        let Err(err) = result else {
            unreachable!("expected error for unknown native protocol");
        };
        assert!(err.to_string().contains("Unknown native protocol"));
    }

    #[test]
    fn unknown_vm_protocol_returns_error() {
        let builder = test_builder();
        let filter = test_filter();
        let result = register_vm_protocol(builder, "vm:unknown", &filter);
        assert!(result.is_err());
        let Err(err) = result else {
            unreachable!("expected error for unknown VM protocol");
        };
        assert!(err.to_string().contains("Unknown VM protocol"));
    }

    #[test]
    fn chain_profile_native_protocols_all_register_successfully() {
        let filter = test_filter();

        for protocol in unique_protocols(ETHEREUM_NATIVE_PROTOCOLS, BASE_NATIVE_PROTOCOLS) {
            let result = register_native_protocol(test_builder(), protocol, &filter);
            assert!(
                result.is_ok(),
                "expected native protocol {protocol} to register"
            );
        }
    }

    #[test]
    fn chain_profile_vm_protocols_all_register_successfully() {
        let filter = test_filter();

        for protocol in unique_protocols(ETHEREUM_VM_PROTOCOLS, BASE_VM_PROTOCOLS) {
            let result = register_vm_protocol(test_builder(), protocol, &filter);
            assert!(
                result.is_ok(),
                "expected VM protocol {protocol} to register"
            );
        }
    }
}
