use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use simulator_core::models::protocol::ProtocolKind;
use tycho_simulation::tycho_common::{models::Chain, Bytes};

use crate::models::erc4626::Erc4626PairPolicy;

pub(crate) const MANIFEST_PATH: &str = "simulator-manifest.toml";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackendKind {
    Native,
    Vm,
    Rfq,
}

impl BackendKind {
    fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestRegistries {
    chains: HashMap<u64, ChainRegistryEntry>,
    route_policies: HashMap<String, RoutePolicyRegistryEntry>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedChainConfig {
    pub(crate) chain_profile: ResolvedChainProfile,
    pub(crate) tycho_url: String,
    pub(crate) bebop_url: String,
    pub(crate) hashflow_filename: String,
    pub(crate) liquorice_url: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedChainProfile {
    pub(crate) chain: Chain,
    pub(crate) native_protocols: Vec<String>,
    pub(crate) vm_protocols: Vec<String>,
    pub(crate) rfq_protocols: Vec<String>,
    pub(crate) native_token_protocol_allowlist: Vec<String>,
    pub(crate) reset_allowance_tokens: HashMap<u64, HashSet<Bytes>>,
    pub(crate) erc4626_pair_policies: Vec<Erc4626PairPolicy>,
}

#[derive(Clone, Debug)]
struct ChainRegistryEntry {
    chain_id: u64,
    tycho_url: String,
    bebop_url: String,
    hashflow_filename: String,
    liquorice_url: Option<String>,
    native_protocols: Vec<String>,
    vm_protocols: Vec<String>,
    rfq_protocols: Vec<String>,
    route_policy_id: String,
}

#[derive(Clone, Debug)]
struct RoutePolicyRegistryEntry {
    native_token_protocol_allowlist: Vec<String>,
    reset_allowance_tokens: HashSet<Bytes>,
    erc4626_pair_policies: Vec<Erc4626PairPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManifest {
    protocols: Vec<RawProtocol>,
    route_policies: Vec<RawRoutePolicy>,
    chains: Vec<RawChain>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProtocol {
    id: String,
    backend: BackendKind,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoutePolicy {
    id: String,
    native_token_protocol_allowlist: Vec<String>,
    reset_allowance_tokens: Vec<String>,
    #[serde(default)]
    erc4626_pairs: Vec<RawErc4626PairPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawErc4626PairPolicy {
    asset_symbol: String,
    share_symbol: String,
    asset: String,
    share: String,
    allow_asset_to_share: bool,
    allow_share_to_asset: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChain {
    chain_id: u64,
    tycho_url: String,
    bebop_url: String,
    hashflow_filename: String,
    #[serde(default)]
    liquorice_url: Option<String>,
    native_protocols: Vec<String>,
    vm_protocols: Vec<String>,
    rfq_protocols: Vec<String>,
    route_policy: String,
}

pub(crate) fn load_manifest_registries(path: &Path) -> Result<ManifestRegistries> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest at {}", path.display()))?;
    parse_manifest_registries(&contents)
}

pub(crate) fn parse_manifest_registries(contents: &str) -> Result<ManifestRegistries> {
    let manifest: RawManifest =
        toml::from_str(contents).context("failed to parse simulator-manifest.toml")?;

    // Build the registries in dependency order so validation can stay strict and local.
    let protocols = validate_protocols(&manifest.protocols)?;
    let route_policies = validate_route_policies(&manifest.route_policies, &protocols)?;
    let chains = validate_chains(&manifest.chains, &protocols, &route_policies)?;

    Ok(ManifestRegistries {
        chains,
        route_policies,
    })
}

pub(crate) fn resolve_chain_config(
    registries: &ManifestRegistries,
    chain_id: u64,
    hashflow_filename_override: Option<String>,
) -> Result<ResolvedChainConfig> {
    let chain = registries.chains.get(&chain_id).ok_or_else(|| {
        anyhow!("Unsupported CHAIN_ID={chain_id}: not defined in simulator-manifest.toml")
    })?;
    let route_policy = registries
        .route_policies
        .get(&chain.route_policy_id)
        .ok_or_else(|| {
            anyhow!(
                "Chain {} references unknown route policy {}",
                chain.chain_id,
                chain.route_policy_id
            )
        })?;

    let mut reset_allowance_tokens = HashMap::new();
    // Route policies are shared in the manifest, but the runtime still consumes reset allowances
    // keyed by the active chain id.
    if !route_policy.reset_allowance_tokens.is_empty() {
        reset_allowance_tokens.insert(chain.chain_id, route_policy.reset_allowance_tokens.clone());
    }

    Ok(ResolvedChainConfig {
        chain_profile: ResolvedChainProfile {
            chain: chain_from_id(chain.chain_id)?,
            native_protocols: chain.native_protocols.clone(),
            vm_protocols: chain.vm_protocols.clone(),
            rfq_protocols: chain.rfq_protocols.clone(),
            native_token_protocol_allowlist: route_policy.native_token_protocol_allowlist.clone(),
            reset_allowance_tokens,
            erc4626_pair_policies: route_policy.erc4626_pair_policies.clone(),
        },
        tycho_url: chain.tycho_url.clone(),
        bebop_url: chain.bebop_url.clone(),
        hashflow_filename: hashflow_filename_override
            .unwrap_or_else(|| chain.hashflow_filename.clone()),
        liquorice_url: chain.liquorice_url.clone(),
    })
}

fn chain_from_id(chain_id: u64) -> Result<Chain> {
    supported_runtime_chains()
        .into_iter()
        .find(|chain| chain.id() == chain_id)
        .ok_or_else(|| anyhow!("chain {chain_id} is not supported by the current runtime"))
}

fn supported_runtime_chains() -> [Chain; 7] {
    [
        Chain::Ethereum,
        Chain::Starknet,
        Chain::ZkSync,
        Chain::Arbitrum,
        Chain::Base,
        Chain::Bsc,
        Chain::Unichain,
    ]
}

fn validate_protocols(protocols: &[RawProtocol]) -> Result<HashMap<String, BackendKind>> {
    let mut protocols_by_id = HashMap::new();

    for protocol in protocols {
        let protocol_id = required_string("protocol.id", &protocol.id)?;
        if ProtocolKind::from_sync_state_key(protocol_id).is_none() {
            bail!("unknown protocol id {protocol_id}");
        }
        if protocols_by_id
            .insert(protocol_id.to_string(), protocol.backend)
            .is_some()
        {
            bail!("duplicate protocol id {protocol_id}");
        }
    }

    Ok(protocols_by_id)
}

fn validate_route_policies(
    route_policies: &[RawRoutePolicy],
    protocols: &HashMap<String, BackendKind>,
) -> Result<HashMap<String, RoutePolicyRegistryEntry>> {
    let mut registry = HashMap::new();

    for policy in route_policies {
        let policy_id = required_string("route_policies.id", &policy.id)?;
        if registry.contains_key(policy_id) {
            bail!("duplicate route-policy id {policy_id}");
        }

        let mut native_token_protocol_allowlist =
            Vec::with_capacity(policy.native_token_protocol_allowlist.len());
        for protocol_id in &policy.native_token_protocol_allowlist {
            let protocol_id = required_string(
                &format!("route_policies[{policy_id}].native_token_protocol_allowlist"),
                protocol_id,
            )?;
            let backend = protocols.get(protocol_id).ok_or_else(|| {
                anyhow!("route policy {policy_id} references unknown protocol {protocol_id}")
            })?;
            if *backend != BackendKind::Native {
                bail!("route policy {policy_id} allowlists non-native protocol {protocol_id}");
            }
            native_token_protocol_allowlist.push(protocol_id.to_string());
        }

        let mut reset_allowance_tokens = HashSet::new();
        for token in &policy.reset_allowance_tokens {
            let address = parse_address(
                &format!("route_policies[{policy_id}].reset_allowance_tokens"),
                token,
            )?;
            if !reset_allowance_tokens.insert(address) {
                bail!("duplicate reset-allowance token in route policy {policy_id}");
            }
        }

        let mut erc4626_pair_policies = Vec::with_capacity(policy.erc4626_pairs.len());
        // ERC4626 direction rules live with the route policy because they affect routing behavior,
        // not whether the protocol exists for a chain at all.
        for pair in &policy.erc4626_pairs {
            let asset_symbol = required_string(
                &format!("route_policies[{policy_id}].erc4626_pairs.asset_symbol"),
                &pair.asset_symbol,
            )?;
            let share_symbol = required_string(
                &format!("route_policies[{policy_id}].erc4626_pairs.share_symbol"),
                &pair.share_symbol,
            )?;
            let asset = parse_address(
                &format!("route_policies[{policy_id}].erc4626_pairs.asset"),
                &pair.asset,
            )?;
            let share = parse_address(
                &format!("route_policies[{policy_id}].erc4626_pairs.share"),
                &pair.share,
            )?;
            if asset == share {
                bail!("route policy {policy_id} contains an ERC4626 pair with matching asset/share addresses");
            }
            if !pair.allow_asset_to_share && !pair.allow_share_to_asset {
                bail!(
                    "route policy {policy_id} contains an ERC4626 pair with no enabled directions"
                );
            }

            erc4626_pair_policies.push(Erc4626PairPolicy {
                asset_symbol: asset_symbol.to_string(),
                share_symbol: share_symbol.to_string(),
                asset,
                share,
                allow_asset_to_share: pair.allow_asset_to_share,
                allow_share_to_asset: pair.allow_share_to_asset,
            });
        }

        registry.insert(
            policy_id.to_string(),
            RoutePolicyRegistryEntry {
                native_token_protocol_allowlist,
                reset_allowance_tokens,
                erc4626_pair_policies,
            },
        );
    }

    Ok(registry)
}

fn validate_chains(
    chains: &[RawChain],
    protocols: &HashMap<String, BackendKind>,
    route_policies: &HashMap<String, RoutePolicyRegistryEntry>,
) -> Result<HashMap<u64, ChainRegistryEntry>> {
    let mut registry = HashMap::new();

    for chain in chains {
        if registry.contains_key(&chain.chain_id) {
            bail!("duplicate chain id {}", chain.chain_id);
        }
        let _runtime_chain = chain_from_id(chain.chain_id)?;

        let tycho_url = required_string(
            &format!("chains[{}].tycho_url", chain.chain_id),
            &chain.tycho_url,
        )?;
        let bebop_url = required_string(
            &format!("chains[{}].bebop_url", chain.chain_id),
            &chain.bebop_url,
        )?;
        let hashflow_filename = required_string(
            &format!("chains[{}].hashflow_filename", chain.chain_id),
            &chain.hashflow_filename,
        )?;
        let route_policy_id = required_string(
            &format!("chains[{}].route_policy", chain.chain_id),
            &chain.route_policy,
        )?;
        if !route_policies.contains_key(route_policy_id) {
            bail!(
                "chain {} references unknown route policy {}",
                chain.chain_id,
                route_policy_id
            );
        }

        let native_protocols = validate_backend_protocol_refs(
            chain.chain_id,
            BackendKind::Native,
            &chain.native_protocols,
            protocols,
        )?;
        let vm_protocols = validate_backend_protocol_refs(
            chain.chain_id,
            BackendKind::Vm,
            &chain.vm_protocols,
            protocols,
        )?;
        let rfq_protocols = validate_backend_protocol_refs(
            chain.chain_id,
            BackendKind::Rfq,
            &chain.rfq_protocols,
            protocols,
        )?;
        let liquorice_url =
            validate_liquorice_url(chain.chain_id, &rfq_protocols, &chain.liquorice_url)?;

        registry.insert(
            chain.chain_id,
            ChainRegistryEntry {
                chain_id: chain.chain_id,
                tycho_url: tycho_url.to_string(),
                bebop_url: bebop_url.to_string(),
                hashflow_filename: hashflow_filename.to_string(),
                liquorice_url,
                native_protocols,
                vm_protocols,
                rfq_protocols,
                route_policy_id: route_policy_id.to_string(),
            },
        );
    }

    Ok(registry)
}

fn validate_backend_protocol_refs(
    chain_id: u64,
    backend: BackendKind,
    protocol_ids: &[String],
    protocols: &HashMap<String, BackendKind>,
) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut normalized_protocol_ids = Vec::with_capacity(protocol_ids.len());
    // Chains can only point at globally declared protocols for the matching backend. That keeps a
    // typo or copy/paste mixup from silently turning into a weird runtime config.
    for protocol_id in protocol_ids {
        let protocol_id = required_string(
            &format!("chains[{chain_id}].{}_protocols", backend.label()),
            protocol_id,
        )?;
        if !seen.insert(protocol_id) {
            bail!(
                "chain {chain_id} lists protocol {protocol_id} more than once for backend {}",
                backend.label()
            );
        }
        let declared_backend = protocols
            .get(protocol_id)
            .ok_or_else(|| anyhow!("chain {chain_id} references unknown protocol {protocol_id}"))?;
        if *declared_backend != backend {
            bail!(
                "chain {chain_id} assigns protocol {protocol_id} to backend {} but manifest declares {}",
                backend.label(),
                declared_backend.label()
            );
        }
        normalized_protocol_ids.push(protocol_id.to_string());
    }
    Ok(normalized_protocol_ids)
}

fn validate_liquorice_url(
    chain_id: u64,
    rfq_protocols: &[String],
    raw_url: &Option<String>,
) -> Result<Option<String>> {
    let liquorice_enabled = rfq_protocols
        .iter()
        .any(|protocol| protocol == "rfq:liquorice");
    match raw_url {
        Some(url) => Ok(Some(
            required_string(&format!("chains[{chain_id}].liquorice_url"), url)?.to_string(),
        )),
        None if liquorice_enabled => {
            bail!("chain {chain_id} enables rfq:liquorice but does not define liquorice_url")
        }
        None => Ok(None),
    }
}

fn required_string<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(trimmed)
}

fn parse_address(field: &str, value: &str) -> Result<Bytes> {
    let bytes: Bytes = value
        .parse()
        .map_err(|_| anyhow!("invalid address in {field}: {value}"))?;
    if bytes.len() != 20 {
        bail!("invalid 20-byte address in {field}: {value}");
    }
    Ok(bytes)
}

impl<'de> Deserialize<'de> for BackendKind {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "native" => Ok(Self::Native),
            "vm" => Ok(Self::Vm),
            "rfq" => Ok(Self::Rfq),
            other => Err(serde::de::Error::custom(format!(
                "unknown backend kind {other}"
            ))),
        }
    }
}
