use std::collections::{
    btree_map::Entry as BTreeEntry, hash_map::Entry as HashEntry, BTreeMap, HashMap,
};

use thiserror::Error;
use tycho_simulation::tycho_common::{models::contract::Account, Bytes};

use simulator_core::broadcaster::BroadcasterProtocolMessage;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SnapshotReassemblyError {
    message: String,
}

impl SnapshotReassemblyError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct RawSnapshotReassembly {
    messages: BTreeMap<String, BroadcasterProtocolMessage>,
}

impl RawSnapshotReassembly {
    pub fn reset(&mut self) {
        self.messages.clear();
    }

    pub fn push(
        &mut self,
        message: BroadcasterProtocolMessage,
    ) -> Result<(), SnapshotReassemblyError> {
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

    pub fn take_messages(&mut self) -> Vec<BroadcasterProtocolMessage> {
        std::mem::take(&mut self.messages).into_values().collect()
    }
}

fn merge_snapshot_protocol_message(
    existing: &mut BroadcasterProtocolMessage,
    incoming: BroadcasterProtocolMessage,
) -> Result<(), SnapshotReassemblyError> {
    if existing.protocol != incoming.protocol {
        return Err(SnapshotReassemblyError::new(format!(
            "broadcaster snapshot protocol mismatch: expected {}, got {}",
            existing.protocol, incoming.protocol
        )));
    }

    ensure_raw_snapshot_fragment_identity(existing, &incoming)?;
    ensure_raw_snapshot_fragment_conflicts(existing, &incoming)?;

    let mut merged_vm_storage = std::mem::take(&mut existing.message.snapshots.vm_storage);
    let mut incoming_message = incoming.message;
    let incoming_vm_storage = std::mem::take(&mut incoming_message.snapshots.vm_storage);
    merge_vm_storage(&mut merged_vm_storage, incoming_vm_storage)?;
    let (incoming_new_tokens, incoming_dci_update) = incoming_message
        .deltas
        .as_mut()
        .map(|deltas| {
            (
                std::mem::take(&mut deltas.new_tokens),
                std::mem::take(&mut deltas.dci_update),
            )
        })
        .unwrap_or_default();

    let mut merged_message = existing.message.clone().merge(incoming_message);
    merged_message.snapshots.vm_storage = merged_vm_storage;
    if let Some(deltas) = merged_message.deltas.as_mut() {
        // Tycho's merge omits these fields, but snapshot fragments can carry all three.
        deltas.new_tokens.extend(incoming_new_tokens);
        for (component_id, entrypoints) in incoming_dci_update.new_entrypoints {
            deltas
                .dci_update
                .new_entrypoints
                .entry(component_id)
                .or_default()
                .extend(entrypoints);
        }
        for (entrypoint_id, params) in incoming_dci_update.new_entrypoint_params {
            deltas
                .dci_update
                .new_entrypoint_params
                .entry(entrypoint_id)
                .or_default()
                .extend(params);
        }
        deltas
            .dci_update
            .trace_results
            .extend(incoming_dci_update.trace_results);
    }
    existing.message = merged_message;
    Ok(())
}

fn ensure_raw_snapshot_fragment_identity(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<(), SnapshotReassemblyError> {
    if existing.message.header != incoming.message.header {
        return Err(SnapshotReassemblyError::new(format!(
            "broadcaster snapshot raw fragment header mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol, existing.message.header, incoming.message.header
        )));
    }

    if existing.sync_state != incoming.sync_state {
        return Err(SnapshotReassemblyError::new(format!(
            "broadcaster snapshot raw fragment sync_state mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol, existing.sync_state, incoming.sync_state
        )));
    }

    Ok(())
}

fn ensure_raw_snapshot_fragment_conflicts(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<(), SnapshotReassemblyError> {
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
) -> Result<(), SnapshotReassemblyError> {
    for component_id in incoming.keys() {
        if existing.contains_key(component_id) {
            return Err(SnapshotReassemblyError::new(format!(
                "broadcaster snapshot raw fragment duplicate {kind} for protocol {protocol}: {component_id}"
            )));
        }
    }

    Ok(())
}

fn ensure_no_snapshot_removal_overlap<State, Removed>(
    protocol: &str,
    snapshots: &HashMap<String, State>,
    removals: &HashMap<String, Removed>,
) -> Result<(), SnapshotReassemblyError> {
    for component_id in snapshots.keys() {
        if removals.contains_key(component_id) {
            return Err(SnapshotReassemblyError::new(format!(
                "broadcaster snapshot raw fragment snapshot/removal overlap for protocol {protocol}: {component_id}"
            )));
        }
    }

    Ok(())
}

fn merge_vm_storage(
    existing: &mut HashMap<Bytes, Account>,
    incoming: HashMap<Bytes, Account>,
) -> Result<(), SnapshotReassemblyError> {
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
    existing: &mut Account,
    incoming: Account,
) -> Result<(), SnapshotReassemblyError> {
    ensure_vm_account_metadata_matches(address, existing, &incoming)?;
    for (slot, value) in incoming.slots {
        match existing.slots.entry(slot.clone()) {
            HashEntry::Vacant(entry) => {
                entry.insert(value);
            }
            HashEntry::Occupied(entry) if entry.get() == &value => {}
            HashEntry::Occupied(_) => {
                return Err(SnapshotReassemblyError::new(format!(
                    "broadcaster snapshot VM storage slot mismatch for account {} slot {}",
                    address, slot
                )));
            }
        }
    }
    Ok(())
}

fn ensure_vm_account_metadata_matches(
    address: &Bytes,
    existing: &Account,
    incoming: &Account,
) -> Result<(), SnapshotReassemblyError> {
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
        return Err(SnapshotReassemblyError::new(format!(
            "broadcaster snapshot VM storage metadata mismatch for account {} field {}",
            address, field
        )));
    }
    Ok(())
}
