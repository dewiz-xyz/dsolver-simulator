use std::collections::{
    btree_map::Entry as BTreeEntry, hash_map::Entry as HashEntry, BTreeMap, HashMap,
};
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use reqwest::Client;
use tycho_simulation::tycho_common::{dto::ResponseAccount, Bytes};

use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterProtocolMessage, BroadcasterSnapshotSessionResponse,
};

use crate::models::broadcaster_urls::derive_broadcaster_http_url;
use crate::stream::StreamSupervisorConfig;

use super::processor::BroadcasterSubscriptionProcessor;
use super::BroadcasterSubscriptionControls;

const SNAPSHOT_DOWNLOAD_CONCURRENCY: usize = 4;

#[derive(Default)]
pub(super) struct RawSnapshotReassembly {
    messages: BTreeMap<String, BroadcasterProtocolMessage>,
}

impl RawSnapshotReassembly {
    pub(super) fn reset(&mut self) {
        self.messages.clear();
    }

    pub(super) fn push(&mut self, message: BroadcasterProtocolMessage) -> Result<()> {
        match self.messages.entry(message.protocol.clone()) {
            BTreeEntry::Vacant(entry) => {
                entry.insert(message);
            }
            BTreeEntry::Occupied(mut entry) => {
                merge_snapshot_protocol_message(entry.get_mut(), message)?;
            }
        }
        Ok(())
    }

    pub(super) fn take_messages(&mut self) -> Vec<BroadcasterProtocolMessage> {
        std::mem::take(&mut self.messages).into_values().collect()
    }
}

fn merge_snapshot_protocol_message(
    existing: &mut BroadcasterProtocolMessage,
    incoming: BroadcasterProtocolMessage,
) -> Result<()> {
    if existing.protocol != incoming.protocol {
        return Err(anyhow!(
            "broadcaster snapshot protocol mismatch: expected {}, got {}",
            existing.protocol,
            incoming.protocol
        ));
    }

    ensure_raw_snapshot_fragment_identity(existing, &incoming)?;
    ensure_raw_snapshot_fragment_conflicts(existing, &incoming)?;

    let mut merged_vm_storage = std::mem::take(&mut existing.message.snapshots.vm_storage);
    let mut incoming_message = incoming.message;
    let incoming_vm_storage = std::mem::take(&mut incoming_message.snapshots.vm_storage);
    merge_vm_storage(&mut merged_vm_storage, incoming_vm_storage)?;

    let mut merged_message = existing.message.clone().merge(incoming_message);
    merged_message.snapshots.vm_storage = merged_vm_storage;
    existing.message = merged_message;
    Ok(())
}

fn ensure_raw_snapshot_fragment_identity(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<()> {
    if existing.message.header != incoming.message.header {
        return Err(anyhow!(
            "broadcaster snapshot raw fragment header mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol,
            existing.message.header,
            incoming.message.header
        ));
    }

    if existing.sync_state != incoming.sync_state {
        return Err(anyhow!(
            "broadcaster snapshot raw fragment sync_state mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol,
            existing.sync_state,
            incoming.sync_state
        ));
    }

    Ok(())
}

fn ensure_raw_snapshot_fragment_conflicts(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<()> {
    ensure_no_duplicate_ids(
        &existing.protocol,
        &existing.message.snapshots.states,
        &incoming.message.snapshots.states,
        "snapshot state",
    )?;
    ensure_no_duplicate_ids(
        &existing.protocol,
        &existing.message.removed_components,
        &incoming.message.removed_components,
        "removed component",
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &existing.message.snapshots.states,
        &existing.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &incoming.message.snapshots.states,
        &incoming.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &existing.message.snapshots.states,
        &incoming.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &incoming.message.snapshots.states,
        &existing.message.removed_components,
    )?;

    Ok(())
}

fn ensure_no_duplicate_ids<Existing, Incoming>(
    protocol: &str,
    existing: &HashMap<String, Existing>,
    incoming: &HashMap<String, Incoming>,
    kind: &str,
) -> Result<()> {
    for component_id in incoming.keys() {
        if existing.contains_key(component_id) {
            return Err(anyhow!(
                "broadcaster snapshot raw fragment duplicate {kind} for protocol {protocol}: {component_id}"
            ));
        }
    }

    Ok(())
}

fn ensure_no_snapshot_removal_overlap<State, Removed>(
    protocol: &str,
    snapshots: &HashMap<String, State>,
    removals: &HashMap<String, Removed>,
) -> Result<()> {
    for component_id in snapshots.keys() {
        if removals.contains_key(component_id) {
            return Err(anyhow!(
                "broadcaster snapshot raw fragment snapshot/removal overlap for protocol {protocol}: {component_id}"
            ));
        }
    }

    Ok(())
}

fn merge_vm_storage(
    existing: &mut HashMap<Bytes, ResponseAccount>,
    incoming: HashMap<Bytes, ResponseAccount>,
) -> Result<()> {
    for (address, account) in incoming {
        match existing.entry(address.clone()) {
            HashEntry::Vacant(entry) => {
                entry.insert(account);
            }
            HashEntry::Occupied(mut entry) => {
                merge_vm_storage_account(&address, entry.get_mut(), account)?;
            }
        }
    }
    Ok(())
}

fn merge_vm_storage_account(
    address: &Bytes,
    existing: &mut ResponseAccount,
    incoming: ResponseAccount,
) -> Result<()> {
    ensure_vm_account_metadata_matches(address, existing, &incoming)?;
    for (slot, value) in incoming.slots {
        match existing.slots.entry(slot.clone()) {
            HashEntry::Vacant(entry) => {
                entry.insert(value);
            }
            HashEntry::Occupied(entry) if entry.get() == &value => {}
            HashEntry::Occupied(_) => {
                return Err(anyhow!(
                    "broadcaster snapshot VM storage slot mismatch for account {} slot {}",
                    address,
                    slot
                ));
            }
        }
    }
    Ok(())
}

#[expect(
    deprecated,
    reason = "creation_tx is deprecated but still part of the broadcaster wire DTO"
)]
fn ensure_vm_account_metadata_matches(
    address: &Bytes,
    existing: &ResponseAccount,
    incoming: &ResponseAccount,
) -> Result<()> {
    let mismatch = if existing.chain != incoming.chain {
        Some("chain")
    } else if existing.address != incoming.address {
        Some("address")
    } else if existing.title != incoming.title {
        Some("title")
    } else if existing.native_balance != incoming.native_balance {
        Some("native_balance")
    } else if existing.token_balances != incoming.token_balances {
        Some("token_balances")
    } else if existing.code != incoming.code {
        Some("code")
    } else if existing.code_hash != incoming.code_hash {
        Some("code_hash")
    } else if existing.balance_modify_tx != incoming.balance_modify_tx {
        Some("balance_modify_tx")
    } else if existing.code_modify_tx != incoming.code_modify_tx {
        Some("code_modify_tx")
    } else if existing.creation_tx != incoming.creation_tx {
        Some("creation_tx")
    } else {
        None
    };

    if let Some(field) = mismatch {
        return Err(anyhow!(
            "broadcaster snapshot VM storage metadata mismatch for account {} field {}",
            address,
            field
        ));
    }
    Ok(())
}

pub(super) async fn bootstrap_broadcaster_snapshot(
    client: &Client,
    broadcaster_url: &str,
    snapshot_sessions_url: &str,
    processor: &mut BroadcasterSubscriptionProcessor,
    controls: &BroadcasterSubscriptionControls,
    cfg: &StreamSupervisorConfig,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let session =
        create_broadcaster_snapshot_session(client, snapshot_sessions_url, cfg.readiness_stale)
            .await?;
    processor.set_bootstrap_redis_replay_boundary(session.redis_replay_boundary.clone());
    controls.stream_health().mark_started().await;

    {
        let mut payloads = futures::stream::iter(0..session.payload_count)
            .map(|index| {
                let session = session.clone();
                async move {
                    fetch_broadcaster_snapshot_payload(
                        client,
                        broadcaster_url,
                        &session,
                        index,
                        cfg.readiness_stale,
                    )
                    .await
                }
            })
            .buffered(SNAPSHOT_DOWNLOAD_CONCURRENCY);

        while let Some(envelope) = payloads.next().await {
            processor.observe(envelope?).await?;
        }
    }

    if !processor.bootstrap_complete() {
        return Err(anyhow!(
            "broadcaster HTTP snapshot session {} ended before bootstrap completed",
            session.session_id
        ));
    }

    Ok(session)
}

async fn create_broadcaster_snapshot_session(
    client: &Client,
    snapshot_sessions_url: &str,
    request_timeout: Duration,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let response = client
        .post(snapshot_sessions_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            anyhow!(
                "failed to create broadcaster snapshot session at {snapshot_sessions_url}: {error}"
            )
        })?;
    decode_success_json(
        response,
        snapshot_sessions_url,
        "create broadcaster snapshot session",
    )
    .await
}

async fn fetch_broadcaster_snapshot_payload(
    client: &Client,
    broadcaster_url: &str,
    session: &BroadcasterSnapshotSessionResponse,
    index: u32,
    request_timeout: Duration,
) -> Result<BroadcasterEnvelope> {
    let payload_url = derive_broadcaster_http_url(
        broadcaster_url,
        &broadcaster_snapshot_payload_path(session.session_id, index),
    )
    .map_err(|error| anyhow!("failed to derive broadcaster snapshot payload URL: {error}"))?;
    let response = client
        .get(&payload_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            anyhow!(
                "failed to fetch broadcaster snapshot payload {index} from {payload_url}: {error}"
            )
        })?;
    decode_success_json(response, &payload_url, "fetch broadcaster snapshot payload").await
}

pub(super) const BROADCASTER_SNAPSHOT_SESSIONS_PATH: &str = "snapshot-sessions";

fn broadcaster_snapshot_payload_path(session_id: u64, index: u32) -> String {
    format!("{BROADCASTER_SNAPSHOT_SESSIONS_PATH}/{session_id}/payloads/{index}")
}

async fn decode_success_json<T>(
    response: reqwest::Response,
    url: &str,
    operation: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("{operation} at {url} failed with HTTP {status}"));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| anyhow!("failed to read {operation} response from {url}: {error}"))?;
    serde_json::from_slice(&body)
        .map_err(|error| anyhow!("failed to decode {operation} response from {url}: {error}"))
}
