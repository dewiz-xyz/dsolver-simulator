use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::StreamExt;
use simulator_core::broadcaster::BroadcasterBackend;
use simulator_core::models::protocol::ProtocolKind;
use simulator_replay::{DecoderConfig, ReplayBackend, ReplayDecoder};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::models::tokens::TokenStore;
use tycho_simulation::{
    tycho_client::feed::component_tracker::ComponentFilter,
    tycho_client::{
        feed::{BlockHeader, FeedMessage},
        stream::{RetryConfiguration, TychoStreamBuilder},
    },
    tycho_common::models::Chain,
};

pub(crate) const RFQ_QUOTE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const LIQUORICE_QUOTE_EXPIRY_SECS: u64 = 300;

pub async fn build_broadcaster_raw_stream(
    tycho_url: &str,
    api_key: &str,
    tvl_add_threshold: f64,
    tvl_keep_threshold: f64,
    chain: Chain,
    protocols: &BroadcasterProtocols,
) -> Result<(
    tokio::task::JoinHandle<()>,
    impl futures::Stream<
            Item = Result<
                FeedMessage<BlockHeader>,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
)> {
    let (mut builder, tvl_filter) = raw_base_builder(
        tycho_url,
        api_key,
        tvl_add_threshold,
        tvl_keep_threshold,
        chain,
    );

    for protocol in protocols.native.iter().chain(protocols.vm.iter()) {
        builder = builder.exchange(protocol.as_str(), tvl_filter.clone());
    }

    let (lifecycle_task, rx) = builder.build().await?;
    Ok((
        lifecycle_task,
        ReceiverStream::new(rx).map(|item| {
            item.map_err(
                |err| -> Box<dyn std::error::Error + Send + Sync + 'static> { Box::new(err) },
            )
        }),
    ))
}

pub async fn build_broadcaster_subscription_decoder(
    tokens: Arc<TokenStore>,
    backend: BroadcasterBackend,
    protocols: &[ProtocolKind],
) -> Result<Arc<ReplayDecoder>> {
    let config = DecoderConfig::for_backend(ReplayBackend::from(backend), protocols.to_vec(), 0)
        .with_chain(tokens.chain());
    Ok(Arc::new(
        ReplayDecoder::new(config, tokens.snapshot().await).await?,
    ))
}

#[derive(Clone)]
pub struct RFQConfig {
    pub native_key: String,
    pub metric_key: String,
    pub min_tvl_usd: f64,
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
    pub native: Vec<ProtocolKind>,
    pub vm: Vec<ProtocolKind>,
}

pub use super::rfq_stream::build_rfq_stream;

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
    use std::{
        collections::{BTreeSet, HashMap},
        path::PathBuf,
    };

    use crate::config::{load_manifest_registries, resolve_chain_config, MANIFEST_PATH};
    use simulator_core::models::protocol::ProtocolKind;
    use simulator_replay::{DecoderConfig, ReplayBackend, ReplayDecoder};
    use tycho_simulation::tycho_common::models::Chain;

    fn manifest_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../")
            .join(MANIFEST_PATH)
    }

    fn chain_protocols(chain_id: u64) -> (Vec<ProtocolKind>, Vec<ProtocolKind>) {
        let registries = load_manifest_registries(&manifest_path())
            .unwrap_or_else(|err| panic!("test manifest must load: {err}"));
        let resolved = resolve_chain_config(&registries, chain_id, None)
            .unwrap_or_else(|err| panic!("test manifest must resolve chain {chain_id}: {err}"));

        (
            resolved.chain_profile.broadcaster_native_protocols(),
            resolved.chain_profile.vm_protocols,
        )
    }

    #[tokio::test]
    async fn manifest_native_protocols_all_register_successfully() {
        let (ethereum_native_protocols, _) = chain_protocols(1);
        let (base_native_protocols, _) = chain_protocols(8453);
        let protocols = ethereum_native_protocols
            .iter()
            .chain(base_native_protocols.iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let result = ReplayDecoder::new(
            DecoderConfig::for_backend(ReplayBackend::Native, protocols, 0)
                .with_chain(Chain::Ethereum),
            HashMap::new(),
        )
        .await;

        assert!(result.is_ok(), "expected all native protocols to register");
    }

    #[tokio::test]
    async fn manifest_vm_protocols_all_register_successfully() {
        let (_, ethereum_vm_protocols) = chain_protocols(1);
        let (_, base_vm_protocols) = chain_protocols(8453);
        let protocols = ethereum_vm_protocols
            .iter()
            .chain(base_vm_protocols.iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let result = ReplayDecoder::new(
            DecoderConfig::for_backend(ReplayBackend::Vm, protocols, 0).with_chain(Chain::Ethereum),
            HashMap::new(),
        )
        .await;

        assert!(result.is_ok(), "expected all VM protocols to register");
    }
}
