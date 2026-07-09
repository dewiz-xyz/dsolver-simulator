use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Result};
use futures::StreamExt;
use simulator_core::broadcaster::BroadcasterBackend;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::models::tokens::TokenStore;
use tycho_simulation::{
    evm::{
        decoder::TychoStreamDecoder,
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
            uniswap_v4::hooks::hook_handler_creator::initialize_hook_handlers,
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
            liquorice::{client_builder::LiquoriceClientBuilder, state::LiquoriceState},
        },
        stream::RFQStreamBuilder,
    },
    tycho_client::feed::component_tracker::ComponentFilter,
    tycho_client::{
        feed::{BlockHeader, FeedMessage},
        stream::TychoStreamBuilder,
    },
    tycho_common::{
        models::{token::Token, Chain},
        Bytes,
    },
};

#[derive(Clone, Copy)]
enum StreamDecodePolicy {
    Native,
    Vm,
    Broadcaster,
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

pub async fn build_broadcaster_stream(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    tokens: Arc<TokenStore>,
    chain: Chain,
    protocols: &BroadcasterProtocols,
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
        decode_skip_state_failures(StreamDecodePolicy::Broadcaster),
        chain,
    );

    for protocol in &protocols.native {
        builder = register_native_protocol(builder, protocol, &tvl_filter)?;
    }

    for protocol in &protocols.vm {
        builder = register_vm_protocol(builder, protocol, &tvl_filter)?;
    }

    let snapshot = tokens.snapshot().await;
    let stream = builder.set_tokens(snapshot).await.build().await?;

    Ok(stream.map(|item| {
        item.map_err(|err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) })
    }))
}

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
        item.map_err(|err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) })
    }))
}

pub async fn build_broadcaster_subscription_decoder(
    tokens: Arc<TokenStore>,
    backend: BroadcasterBackend,
    protocols: &[String],
) -> Result<Arc<TychoStreamDecoder<BlockHeader>>> {
    let mut decoder = TychoStreamDecoder::new();
    decoder.skip_state_decode_failures(decode_skip_state_failures(match backend {
        BroadcasterBackend::Native => StreamDecodePolicy::Native,
        BroadcasterBackend::Vm => StreamDecodePolicy::Vm,
        BroadcasterBackend::Rfq => StreamDecodePolicy::Broadcaster,
    }));

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

pub async fn build_rfq_stream(
    tvl_add_threshold: f64,
    token_stores: RFQTokenStores,
    chain: Chain,
    protocols: &[String],
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

    if rfq_protocol_enabled(protocols, "rfq:bebop") {
        info!("Setting up Bebop RFQ client...\n");
        let (user, key) = (rfq_config.bebop_user.clone(), rfq_config.bebop_key.clone());
        let mut rfq_tokens_bebop = HashSet::new();
        for bebop_token_addr in token_stores.bebop.snapshot().await.keys().clone() {
            rfq_tokens_bebop.insert(bebop_token_addr.clone());
        }
        let bebop_client = BebopClientBuilder::new(chain, user, key)
            .tokens(rfq_tokens_bebop)
            .tvl_threshold(tvl_add_threshold)
            .build()
            .map_err(|err| anyhow::anyhow!("failed to create Bebop RFQ client: {err}"))?;
        rfq_builder = rfq_builder.add_client::<BebopState>("bebop", Box::new(bebop_client));
    }

    if rfq_protocol_enabled(protocols, "rfq:hashflow") {
        info!("Setting up Hashflow RFQ client...\n");
        let (user, key) = (
            rfq_config.hashflow_user.clone(),
            rfq_config.hashflow_key.clone(),
        );
        let mut rfq_tokens_hashflow = HashSet::new();
        for hashflow_token_addr in token_stores.hashflow.snapshot().await.keys() {
            rfq_tokens_hashflow.insert(hashflow_token_addr.clone());
        }
        let hashflow_client = HashflowClientBuilder::new(chain, user, key)
            .tokens(rfq_tokens_hashflow)
            .tvl_threshold(tvl_add_threshold)
            .poll_time(Duration::from_secs(5))
            .build()
            .map_err(|err| anyhow::anyhow!("failed to create Hashflow RFQ client: {err}"))?;
        rfq_builder =
            rfq_builder.add_client::<HashflowState>("hashflow", Box::new(hashflow_client));
    }

    if rfq_protocol_enabled(protocols, "rfq:liquorice") {
        info!("Setting up Liquorice RFQ client...\n");
        let (user, key) = (
            rfq_config.liquorice_user.clone(),
            rfq_config.liquorice_key.clone(),
        );
        let mut rfq_tokens_liquorice = HashSet::new();
        for liquorice_token_addr in token_stores.liquorice.snapshot().await.keys() {
            rfq_tokens_liquorice.insert(liquorice_token_addr.clone());
        }
        let liquorice_client = LiquoriceClientBuilder::new(chain, user, key)
            .tokens(rfq_tokens_liquorice)
            .tvl_threshold(tvl_add_threshold)
            .poll_time(Duration::from_secs(5))
            .build()
            .map_err(|err| anyhow::anyhow!("failed to create Liquorice RFQ client: {err}"))?;
        rfq_builder =
            rfq_builder.add_client::<LiquoriceState>("liquorice", Box::new(liquorice_client));
    }

    info!("Building RFQ Stream...\n");
    let (tx, /* mut */ rx) = mpsc::channel::<Update>(100);

    let decoder_tokens = rfq_decoder_tokens(
        &token_stores.tokens,
        &token_stores.bebop,
        &token_stores.hashflow,
        &token_stores.liquorice,
    )
    .await;
    rfq_builder = rfq_builder.set_tokens(decoder_tokens).await;
    tokio::spawn(rfq_builder.build(tx));
    info!("Connected to RFQs! Streaming live price levels...\n");

    // todo consider implementing register_rfq_protocol...

    Ok(ReceiverStream::new(rx).map(Ok).boxed())
}

fn rfq_protocol_enabled(protocols: &[String], protocol: &str) -> bool {
    protocols.iter().any(|configured| configured == protocol)
}

async fn rfq_decoder_tokens(
    tokens: &TokenStore,
    bebop_tokens: &TokenStore,
    hashflow_tokens: &TokenStore,
    liquorice_tokens: &TokenStore,
) -> HashMap<Bytes, Token> {
    let mut snapshot = tokens.snapshot().await;
    merge_missing_tokens(&mut snapshot, bebop_tokens.snapshot().await);
    merge_missing_tokens(&mut snapshot, hashflow_tokens.snapshot().await);
    merge_missing_tokens(&mut snapshot, liquorice_tokens.snapshot().await);
    snapshot
}

fn merge_missing_tokens(tokens: &mut HashMap<Bytes, Token>, extra: HashMap<Bytes, Token>) {
    for (address, token) in extra {
        tokens.entry(address).or_insert(token);
    }
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

fn register_native_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: &str,
) -> Result<()> {
    match protocol {
        "uniswap_v2" | "sushiswap_v2" => decoder.register_decoder::<UniswapV2State>(protocol),
        "pancakeswap_v2" => decoder.register_decoder::<PancakeswapV2State>(protocol),
        "uniswap_v3" | "pancakeswap_v3" => decoder.register_decoder::<UniswapV3State>(protocol),
        "uniswap_v4" => decoder.register_decoder::<UniswapV4State>(protocol),
        "ekubo_v2" => decoder.register_decoder::<EkuboState>(protocol),
        "fluid_v1" => {
            decoder.register_decoder::<FluidV1>(protocol);
            decoder.register_filter(protocol, fluid_v1_paused_pools_filter);
        }
        "rocketpool" => decoder.register_decoder::<RocketpoolState>(protocol),
        "ekubo_v3" => decoder.register_decoder::<EkuboV3State>(protocol),
        "aerodrome_slipstreams" => decoder.register_decoder::<AerodromeSlipstreamsState>(protocol),
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

    let builder = TychoStreamBuilder::new(tycho_url, chain.into())
        .timeout(15)
        .auth_key(Some(api_key.to_string()));

    (builder, tvl_filter)
}

fn decode_skip_state_failures(policy: StreamDecodePolicy) -> bool {
    matches!(
        policy,
        StreamDecodePolicy::Native | StreamDecodePolicy::Vm | StreamDecodePolicy::Broadcaster
    )
}

#[cfg(test)]
#[expect(
    clippy::panic,
    reason = "stream builder tests use explicit panics for invalid fixture setup"
)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        path::PathBuf,
        str::FromStr,
        sync::Arc,
        time::Duration,
    };

    use super::{
        build_broadcaster_stream, decode_skip_state_failures, merge_missing_tokens,
        register_native_protocol, register_vm_protocol, rfq_decoder_tokens, BroadcasterProtocols,
        StreamDecodePolicy,
    };
    use crate::config::{load_manifest_registries, resolve_chain_config, MANIFEST_PATH};
    use tycho_simulation::{
        evm::stream::ProtocolStreamBuilder,
        tycho_client::feed::component_tracker::ComponentFilter,
        tycho_common::{
            models::{token::Token, Chain},
            Bytes,
        },
    };

    use crate::models::tokens::TokenStore;

    fn test_builder() -> ProtocolStreamBuilder {
        ProtocolStreamBuilder::new(
            "localhost",
            tycho_simulation::tycho_common::models::Chain::Ethereum,
        )
    }

    fn test_filter() -> ComponentFilter {
        ComponentFilter::with_tvl_range(0.0, 1.0)
    }

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

    fn test_token(address: &str, symbol: &str) -> Token {
        let address = match Bytes::from_str(address) {
            Ok(address) => address,
            Err(err) => panic!("test token address must parse: {err}"),
        };
        Token::new(&address, symbol, 18, 0, &[], Chain::Ethereum, 100)
    }

    fn test_token_store(tokens: impl IntoIterator<Item = Token>) -> Arc<TokenStore> {
        let initial_tokens = tokens
            .into_iter()
            .map(|token| (token.address.clone(), token))
            .collect();
        Arc::new(TokenStore::new(
            initial_tokens,
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ))
    }

    #[test]
    fn rfq_protocol_enabled_matches_exact_protocol_name() {
        let protocols = vec![
            "rfq:bebop".to_string(),
            "rfq:hashflow".to_string(),
            "rfq:liquorice".to_string(),
        ];

        assert!(super::rfq_protocol_enabled(&protocols, "rfq:bebop"));
        assert!(super::rfq_protocol_enabled(&protocols, "rfq:hashflow"));
        assert!(super::rfq_protocol_enabled(&protocols, "rfq:liquorice"));
        assert!(!super::rfq_protocol_enabled(&protocols, "rfq:other"));
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
    fn broadcaster_stream_keeps_decode_skip_enabled() {
        assert!(decode_skip_state_failures(StreamDecodePolicy::Broadcaster));
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

    #[tokio::test]
    async fn broadcaster_stream_rejects_unknown_native_protocol() {
        let tokens = test_token_store([test_token(
            "0x0000000000000000000000000000000000000001",
            "TKNA",
        )]);
        let result = build_broadcaster_stream(
            "localhost",
            "key",
            1.0,
            1.0,
            tokens,
            Chain::Ethereum,
            &BroadcasterProtocols {
                native: vec!["unknown_protocol".to_string()],
                vm: Vec::new(),
            },
        )
        .await;

        assert!(result.is_err());
        let Err(err) = result else {
            unreachable!("expected unknown broadcaster protocol to fail");
        };
        assert!(err.to_string().contains("Unknown native protocol"));
    }

    #[test]
    fn manifest_native_protocols_all_register_successfully() {
        let filter = test_filter();
        let (ethereum_native_protocols, _) = chain_protocols(1);
        let (base_native_protocols, _) = chain_protocols(8453);

        for protocol in ethereum_native_protocols
            .iter()
            .chain(base_native_protocols.iter())
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
        {
            let result = register_native_protocol(test_builder(), protocol, &filter);
            assert!(
                result.is_ok(),
                "expected native protocol {protocol} to register"
            );
        }
    }

    #[test]
    fn manifest_vm_protocols_all_register_successfully() {
        let filter = test_filter();
        let (_, ethereum_vm_protocols) = chain_protocols(1);
        let (_, base_vm_protocols) = chain_protocols(8453);

        for protocol in ethereum_vm_protocols
            .iter()
            .chain(base_vm_protocols.iter())
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
        {
            let result = register_vm_protocol(test_builder(), protocol, &filter);
            assert!(
                result.is_ok(),
                "expected VM protocol {protocol} to register"
            );
        }
    }

    #[test]
    fn merge_missing_tokens_keeps_existing_entries() {
        let shared = test_token("0x0000000000000000000000000000000000000001", "BASE");
        let replacement = test_token("0x0000000000000000000000000000000000000001", "RFQ");
        let mut tokens = HashMap::from([(shared.address.clone(), shared.clone())]);

        merge_missing_tokens(
            &mut tokens,
            HashMap::from([(replacement.address.clone(), replacement)]),
        );

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[&shared.address].symbol, shared.symbol);
    }

    #[tokio::test]
    async fn rfq_decoder_tokens_include_rfq_only_assets() {
        let base = test_token("0x0000000000000000000000000000000000000001", "BASE");
        let bebop_only = test_token("0x0000000000000000000000000000000000000002", "BEBOP");
        let hashflow_only = test_token("0x0000000000000000000000000000000000000003", "HASH");
        let liquorice_only = test_token("0x0000000000000000000000000000000000000004", "LIQ");

        let tokens = test_token_store(vec![base.clone()]);
        let bebop_tokens = test_token_store(vec![bebop_only.clone()]);
        let hashflow_tokens = test_token_store(vec![hashflow_only.clone()]);
        let liquorice_tokens = test_token_store(vec![liquorice_only.clone()]);

        let merged =
            rfq_decoder_tokens(&tokens, &bebop_tokens, &hashflow_tokens, &liquorice_tokens).await;

        assert_eq!(merged.len(), 4);
        assert!(merged.contains_key(&base.address));
        assert!(merged.contains_key(&bebop_only.address));
        assert!(merged.contains_key(&hashflow_only.address));
        assert!(merged.contains_key(&liquorice_only.address));
    }
}
