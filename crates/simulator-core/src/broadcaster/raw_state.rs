//! The raw market state the broadcaster keeps for each backend, as Tycho messages
//! with every later change folded in. The broadcaster applies live updates with
//! these rules, and a reader of stored updates applies the same rules to rebuild
//! the snapshot the broadcaster served at a past position.

use std::collections::{BTreeSet, HashMap, HashSet};

use thiserror::Error;
use tycho_simulation::{
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader,
    },
    tycho_common::{
        models::{
            blockchain::BlockAggregatedChanges,
            contract::{Account, AccountDelta},
            protocol::ProtocolComponentStateDelta,
            ChangeType,
        },
        Bytes,
    },
};

use super::BroadcasterProtocolMessage;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RawStateError {
    #[error("conflicting VM account {address} values at block {block_number}")]
    ConflictingVmAccount { address: Bytes, block_number: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub struct RawResidueGrowth {
    pub protocol: String,
    pub previous_count: usize,
    pub stats: RawCompactionStats,
}

/// Folds the protocol messages of one update partition into `messages`, the raw
/// state of that backend. The caller applies updates in stream order and checks
/// their continuity. On error `messages` may be partly updated and must be
/// discarded. Compaction drops `new_tokens`, so the token catalog comes from
/// elsewhere.
pub fn apply_raw_protocol_messages(
    messages: &mut Vec<BroadcasterProtocolMessage>,
    incoming: &[BroadcasterProtocolMessage],
) -> Result<Vec<RawResidueGrowth>, RawStateError> {
    let mut incoming = incoming.iter().collect::<Vec<_>>();
    incoming.sort_by(|left, right| left.protocol.cmp(&right.protocol));
    let mut growth = Vec::new();
    for message in incoming {
        growth.extend(merge_raw_message(messages, message.clone()));
        propagate_touched_vm_accounts(messages, message)?;
    }
    canonicalize_shared_vm_accounts(messages)?;
    Ok(growth)
}

pub fn merge_shared_vm_accounts(
    mut left: Account,
    right: Account,
    block_number: u64,
) -> Result<Account, RawStateError> {
    if !(left.chain == right.chain
        && left.address == right.address
        && left.native_balance == right.native_balance
        && left.code == right.code
        && left.code_hash == right.code_hash
        && left.balance_modify_tx == right.balance_modify_tx
        && left.code_modify_tx == right.code_modify_tx
        && left.creation_tx == right.creation_tx)
    {
        return Err(RawStateError::ConflictingVmAccount {
            address: left.address,
            block_number,
        });
    }
    merge_vm_account_map(&mut left.slots, right.slots);
    merge_vm_account_map(&mut left.token_balances, right.token_balances);
    left.title = deterministic_account_title(left.title, right.title);
    Ok(left)
}

fn merge_vm_account_map<V>(target: &mut HashMap<Bytes, V>, incoming: HashMap<Bytes, V>) {
    // Tycho's protocol-local account views can disagree on an overlapping value.
    // Sorted protocol order gives the shared VM database one stable winner.
    for (key, value) in incoming {
        target.entry(key).or_insert(value);
    }
}

fn deterministic_account_title(left: String, right: String) -> String {
    match (left.is_empty(), right.is_empty()) {
        (true, false) => right,
        (false, true) | (true, true) => left,
        (false, false) => left.min(right),
    }
}

fn propagate_touched_vm_accounts(
    messages: &mut [BroadcasterProtocolMessage],
    incoming: &BroadcasterProtocolMessage,
) -> Result<(), RawStateError> {
    let mut touched = incoming
        .message
        .snapshots
        .vm_storage
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut deleted = BTreeSet::new();
    if let Some(changes) = &incoming.message.deltas {
        touched.extend(changes.account_deltas.keys().cloned());
        touched.extend(changes.account_balances.keys().cloned());
        deleted.extend(
            changes
                .account_deltas
                .iter()
                .filter_map(|(address, update)| {
                    matches!(update.change_type(), ChangeType::Deletion).then_some(address.clone())
                }),
        );
    }

    for address in touched {
        if deleted.contains(&address) {
            for message in messages.iter_mut() {
                message.message.snapshots.vm_storage.remove(&address);
            }
            continue;
        }
        let mut same_block = messages
            .iter()
            .filter(|message| message.message.header.number == incoming.message.header.number)
            .filter_map(|message| {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .get(&address)
                    .cloned()
                    .map(|account| (message.protocol.as_str(), account))
            })
            .collect::<Vec<_>>();
        same_block.sort_by_key(|(protocol, _)| *protocol);
        let canonical = same_block
            .into_iter()
            .map(|(_, account)| account)
            .try_fold(None, |canonical, account| match canonical {
                Some(canonical) => {
                    merge_shared_vm_accounts(canonical, account, incoming.message.header.number)
                        .map(Some)
                }
                None => Ok(Some(account)),
            })?;
        if let Some(canonical) = canonical {
            for message in messages
                .iter_mut()
                .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
            {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .insert(address.clone(), canonical.clone());
            }
        }
    }
    Ok(())
}

fn canonicalize_shared_vm_accounts(
    messages: &mut [BroadcasterProtocolMessage],
) -> Result<(), RawStateError> {
    let addresses = messages
        .iter()
        .flat_map(|message| message.message.snapshots.vm_storage.keys().cloned())
        .collect::<BTreeSet<_>>();
    for address in addresses {
        let latest_block = messages
            .iter()
            .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
            .map(|message| message.message.header.number)
            .max()
            .unwrap_or_default();
        let mut latest = messages
            .iter()
            .filter(|message| message.message.header.number == latest_block)
            .filter_map(|message| {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .get(&address)
                    .cloned()
                    .map(|account| (message.protocol.as_str(), account))
            })
            .collect::<Vec<_>>();
        latest.sort_by_key(|(protocol, _)| *protocol);
        let canonical = latest.into_iter().map(|(_, account)| account).try_fold(
            None,
            |canonical, account| match canonical {
                Some(canonical) => {
                    merge_shared_vm_accounts(canonical, account, latest_block).map(Some)
                }
                None => Ok(Some(account)),
            },
        )?;
        let Some(canonical) = canonical else {
            continue;
        };
        for message in messages
            .iter_mut()
            .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
        {
            message
                .message
                .snapshots
                .vm_storage
                .insert(address.clone(), canonical.clone());
        }
    }
    Ok(())
}

fn merge_raw_message(
    messages: &mut Vec<BroadcasterProtocolMessage>,
    mut incoming: BroadcasterProtocolMessage,
) -> Option<RawResidueGrowth> {
    let growth = if let Some(existing_index) = messages
        .iter_mut()
        .position(|message| message.protocol == incoming.protocol)
    {
        // Moving the affected protocol out avoids cloning its full materialized state per block.
        let mut existing = messages.remove(existing_index);
        let previous_residue_count = raw_residue_entry_count(&existing.message);
        existing.sync_state = incoming.sync_state;
        let (incoming_new_tokens, incoming_dci_update) = incoming
            .message
            .deltas
            .as_mut()
            .map(|deltas| {
                (
                    std::mem::take(&mut deltas.new_tokens),
                    std::mem::take(&mut deltas.dci_update),
                )
            })
            .unwrap_or_default();
        let incoming_account_lifecycle = incoming
            .message
            .deltas
            .as_ref()
            .map(|deltas| {
                deltas
                    .account_deltas
                    .iter()
                    .filter(|(_, update)| {
                        matches!(
                            update.change_type(),
                            ChangeType::Creation | ChangeType::Deletion
                        )
                    })
                    .map(|(address, update)| (address.clone(), update.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        existing.message = existing.message.merge(incoming.message);
        if let Some(deltas) = existing.message.deltas.as_mut() {
            // BlockAggregatedChanges::merge omits new_tokens and dci_update, so preserve them explicitly.
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
            // Creation and deletion are lifecycle edges. The dependency's merge keeps an old
            // creation forever, so the newest edge has to win before materialization.
            deltas.account_deltas.extend(incoming_account_lifecycle);
        }
        let stats = compact_raw_state_sync_message(&mut existing.message);
        let growth = (stats.residual_entries > previous_residue_count).then(|| RawResidueGrowth {
            protocol: existing.protocol.clone(),
            previous_count: previous_residue_count,
            stats,
        });
        messages.push(existing);
        growth
    } else {
        let previous_residue_count = 0;
        let stats = compact_raw_state_sync_message(&mut incoming.message);
        let growth = (stats.residual_entries > previous_residue_count).then(|| RawResidueGrowth {
            protocol: incoming.protocol.clone(),
            previous_count: previous_residue_count,
            stats,
        });
        messages.push(incoming);
        growth
    };
    messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
    growth
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RawCompactionStats {
    pub folded_state_updates: usize,
    pub folded_component_balances: usize,
    pub folded_component_tvl: usize,
    pub folded_account_updates: usize,
    pub folded_account_balances: usize,
    pub residual_entries: usize,
}

fn compact_raw_state_sync_message(
    message: &mut StateSyncMessage<BlockHeader>,
) -> RawCompactionStats {
    let removed_components = std::mem::take(&mut message.removed_components);
    for component_id in removed_components.keys() {
        message.snapshots.states.remove(component_id);
        if let Some(deltas) = message.deltas.as_mut() {
            deltas.state_deltas.remove(component_id);
            deltas.component_balances.remove(component_id);
            deltas.component_tvl.remove(component_id);
            deltas.new_protocol_components.remove(component_id);
            deltas.deleted_protocol_components.remove(component_id);
        }
    }

    let mut stats = message
        .deltas
        .as_mut()
        .map(|deltas| fold_block_changes_into_snapshot(&mut message.snapshots, deltas))
        .unwrap_or_default();
    if message
        .deltas
        .as_ref()
        .is_some_and(|deltas| !block_changes_has_bootstrap_residue(deltas))
    {
        message.deltas = None;
    }
    stats.residual_entries = raw_residue_entry_count(message);
    stats
}

fn fold_block_changes_into_snapshot(
    snapshots: &mut Snapshot,
    deltas: &mut BlockAggregatedChanges,
) -> RawCompactionStats {
    let mut stats = RawCompactionStats::default();

    // Tokens come from /tokens/snapshot before the consumer builds its decoder.
    deltas.new_tokens.clear();

    for component_id in std::mem::take(&mut deltas.deleted_protocol_components).into_keys() {
        snapshots.states.remove(&component_id);
        deltas.state_deltas.remove(&component_id);
        deltas.component_balances.remove(&component_id);
        deltas.component_tvl.remove(&component_id);
        deltas.new_protocol_components.remove(&component_id);
        deltas.dci_update.new_entrypoints.remove(&component_id);
    }
    deltas
        .new_protocol_components
        .retain(|component_id, _| !snapshots.states.contains_key(component_id));

    deltas.state_deltas.retain(|component_id, delta| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        apply_protocol_delta_to_snapshot(component, std::mem::take(delta));
        stats.folded_state_updates += 1;
        false
    });

    deltas.component_balances.retain(|component_id, balances| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        component.state.balances.extend(
            std::mem::take(balances)
                .into_iter()
                .map(|(token, balance)| (token, balance.balance)),
        );
        stats.folded_component_balances += 1;
        false
    });

    deltas.component_tvl.retain(|component_id, tvl| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        component.component_tvl = Some(*tvl);
        stats.folded_component_tvl += 1;
        false
    });

    deltas.account_deltas.retain(|address, update| {
        if matches!(update.change_type(), ChangeType::Deletion) {
            snapshots.vm_storage.remove(address);
            deltas.account_balances.remove(address);
            stats.folded_account_updates += 1;
            return false;
        }
        if matches!(update.change_type(), ChangeType::Creation) {
            // A creation has no title or code hashes, so keep the latest operation for bootstrap.
            snapshots.vm_storage.remove(address);
            return true;
        }
        let Some(account) = snapshots.vm_storage.get_mut(address) else {
            return true;
        };
        fold_account_update_into_snapshot(account, update.clone());
        stats.folded_account_updates += 1;
        false
    });

    deltas.account_balances.retain(|address, balances| {
        let Some(account) = snapshots.vm_storage.get_mut(address) else {
            return true;
        };
        account.token_balances.extend(std::mem::take(balances));
        stats.folded_account_balances += 1;
        false
    });

    let active_entrypoint_ids = deltas
        .dci_update
        .new_entrypoints
        .values()
        .flat_map(|entrypoints| entrypoints.iter().map(|entrypoint| &entrypoint.external_id))
        .collect::<HashSet<_>>();
    deltas
        .dci_update
        .new_entrypoint_params
        .retain(|entrypoint_id, _| active_entrypoint_ids.contains(entrypoint_id));
    deltas
        .dci_update
        .trace_results
        .retain(|entrypoint_id, _| active_entrypoint_ids.contains(entrypoint_id));

    stats
}

fn apply_protocol_delta_to_snapshot(
    component: &mut ComponentWithState,
    delta: ProtocolComponentStateDelta,
) {
    for attribute in delta.deleted_attributes {
        component.state.attributes.remove(&attribute);
    }
    component.state.attributes.extend(delta.updated_attributes);
}

pub fn fold_account_update_into_snapshot(account: &mut Account, update: AccountDelta) {
    let code = update.code().clone();
    account.slots.extend(
        update
            .slots
            .into_iter()
            .map(|(slot, value)| (slot, value.unwrap_or_default())),
    );
    if let Some(balance) = update.balance {
        account.native_balance = balance;
    }
    if let Some(code) = code {
        account.code = code;
    }
}

fn block_changes_has_bootstrap_residue(deltas: &BlockAggregatedChanges) -> bool {
    !deltas.new_tokens.is_empty()
        || !deltas.account_deltas.is_empty()
        || !deltas.state_deltas.is_empty()
        || !deltas.new_protocol_components.is_empty()
        || !deltas.deleted_protocol_components.is_empty()
        || !deltas.component_balances.is_empty()
        || !deltas.account_balances.is_empty()
        || !deltas.component_tvl.is_empty()
        || !deltas.dci_update.new_entrypoints.is_empty()
        || !deltas.dci_update.new_entrypoint_params.is_empty()
        || !deltas.dci_update.trace_results.is_empty()
}

pub fn raw_residue_entry_count(message: &StateSyncMessage<BlockHeader>) -> usize {
    let delta_entries = message.deltas.as_ref().map_or(0, |deltas| {
        deltas.new_tokens.len()
            + deltas.account_deltas.len()
            + deltas.state_deltas.len()
            + deltas.new_protocol_components.len()
            + deltas.deleted_protocol_components.len()
            + deltas.component_balances.len()
            + deltas.account_balances.len()
            + deltas.component_tvl.len()
            + deltas.dci_update.new_entrypoints.len()
            + deltas.dci_update.new_entrypoint_params.len()
            + deltas.dci_update.trace_results.len()
    });
    delta_entries + message.removed_components.len()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use anyhow::{anyhow, Result};
    use tycho_simulation::{
        tycho_client::feed::{
            synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
            BlockHeader, SynchronizerState,
        },
        tycho_common::{
            dto::ResponseToken,
            models::{
                blockchain::{BlockAggregatedChanges, EntryPoint},
                contract::{Account, AccountBalance, AccountDelta},
                protocol::{
                    ComponentBalance, ProtocolComponent as DtoProtocolComponent,
                    ProtocolComponentState, ProtocolComponentStateDelta,
                },
                Chain as DtoChain, ChangeType,
            },
            Bytes as DtoBytes,
        },
    };

    use super::{
        compact_raw_state_sync_message, merge_raw_message, raw_residue_entry_count,
        RawCompactionStats, RawResidueGrowth,
    };
    use crate::broadcaster::BroadcasterProtocolMessage;

    #[test]
    fn raw_cache_compacts_protocol_updates_into_snapshot() {
        let component_id = "raw-1";
        let token = DtoBytes::from([17u8; 20]);
        let mut messages = Vec::new();
        merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "uniswap_v2",
                1,
                Snapshot {
                    states: HashMap::from([(
                        component_id.to_string(),
                        raw_component_with_state(component_id, 1),
                    )]),
                    vm_storage: HashMap::new(),
                },
                None,
                HashMap::new(),
            ),
        );

        for block_number in 2..=20 {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                component_id.to_string(),
                component_state_delta(
                    component_id,
                    [("version", DtoBytes::from([block_number as u8; 32]))],
                    (block_number == 20).then_some("large"),
                ),
            );
            changes.component_balances.insert(
                component_id.to_string(),
                HashMap::from([(
                    token.clone(),
                    component_balance(component_id, token.clone(), block_number as u8),
                )]),
            );
            changes
                .component_tvl
                .insert(component_id.to_string(), block_number as f64);
            let mut compacted = messages[0].message.clone();
            compacted.deltas = Some(changes.clone());
            let stats = compact_raw_state_sync_message(&mut compacted);
            assert_eq!(stats.folded_state_updates, 1);
            assert_eq!(stats.folded_component_balances, 1);
            assert_eq!(stats.folded_component_tvl, 1);
            merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "uniswap_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
        }

        let message = &messages[0];
        let component = &message.message.snapshots.states[component_id];
        assert_eq!(
            component.state.attributes["version"],
            DtoBytes::from([20u8; 32])
        );
        assert!(!component.state.attributes.contains_key("large"));
        assert_eq!(component.state.balances[&token], DtoBytes::from([20u8; 32]));
        assert_eq!(component.component_tvl, Some(20.0));
        assert!(message.message.deltas.is_none());
        assert!(message.message.removed_components.is_empty());
        assert_eq!(message.message.header.number, 20);
        assert!(matches!(
            message.sync_state,
            SynchronizerState::Ready(ref header) if header.number == 20
        ));
    }

    #[test]
    fn raw_cache_keeps_unfoldable_component_residue() {
        let component_id = "missing";
        let token = DtoBytes::from([18u8; 20]);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            component_id.to_string(),
            component_state_delta(component_id, [("version", DtoBytes::from([2u8; 32]))], []),
        );
        changes.component_balances.insert(
            component_id.to_string(),
            HashMap::from([(token.clone(), component_balance(component_id, token, 2))]),
        );
        changes.component_tvl.insert(component_id.to_string(), 2.0);
        let expected = changes.clone();
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot::default(),
            Some(changes),
            HashMap::new(),
        );

        let stats = compact_raw_state_sync_message(&mut message.message);

        assert_eq!(message.message.deltas, Some(expected));
        assert_eq!(stats.residual_entries, 3);
    }

    #[test]
    fn raw_cache_drops_process_history_not_needed_by_bootstrap() {
        let mut messages = Vec::new();
        for block_number in 1..=2 {
            let token_address = DtoBytes::from([block_number as u8; 20]);
            let mut changes = BlockAggregatedChanges::default();
            changes.new_tokens.insert(
                token_address.clone(),
                ResponseToken {
                    chain: DtoChain::Ethereum.into(),
                    address: token_address,
                    symbol: format!("T{block_number}"),
                    decimals: 18,
                    tax: 0,
                    gas: Vec::new(),
                    quality: 100,
                }
                .into(),
            );
            changes
                .dci_update
                .trace_results
                .insert(format!("trace-{block_number}"), Default::default());
            merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "vm:balancer_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
        }

        assert!(messages[0].message.deltas.is_none());
    }

    #[test]
    fn raw_cache_compacts_vm_updates_into_snapshot() {
        let address = DtoBytes::from([31u8; 20]);
        let token = DtoBytes::from([32u8; 20]);
        let mut message = raw_protocol_message_with_changes(
            "vm:balancer_v2",
            1,
            Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(
                    address.clone(),
                    raw_response_account(address.clone(), 0, 0),
                )]),
            },
            None,
            HashMap::new(),
        );
        let mut changes = BlockAggregatedChanges::default();
        changes.account_deltas.insert(
            address.clone(),
            account_update(
                address.clone(),
                ChangeType::Update,
                7,
                Some(DtoBytes::from([41u8; 32])),
            ),
        );
        changes.account_balances.insert(
            address.clone(),
            HashMap::from([(
                token.clone(),
                account_balance(address.clone(), token.clone(), 42),
            )]),
        );
        message.message.deltas = Some(changes);

        let stats = compact_raw_state_sync_message(&mut message.message);
        assert_eq!(stats.folded_account_updates, 1);
        assert_eq!(stats.folded_account_balances, 1);

        let account = &message.message.snapshots.vm_storage[&address];
        assert_eq!(
            account.slots[&DtoBytes::from([7u8; 32])],
            DtoBytes::from([8u8; 32])
        );
        assert_eq!(account.native_balance, DtoBytes::from([41u8; 32]));
        assert_eq!(
            account.token_balances[&token].balance,
            DtoBytes::from([42u8; 32])
        );
        assert!(message.message.deltas.is_none());
    }

    #[test]
    fn raw_cache_keeps_vm_updates_with_distinct_engine_semantics() -> Result<()> {
        let creation = DtoBytes::from([51u8; 20]);
        let deletion = DtoBytes::from([52u8; 20]);
        let unspecified = DtoBytes::from([53u8; 20]);
        let absent = DtoBytes::from([54u8; 20]);
        let mut vm_storage = HashMap::new();
        for address in [&creation, &deletion, &unspecified] {
            vm_storage.insert(address.clone(), raw_response_account(address.clone(), 0, 0));
        }
        let mut changes = BlockAggregatedChanges::default();
        for (address, change, seed) in [
            (creation.clone(), ChangeType::Creation, 1),
            (deletion.clone(), ChangeType::Deletion, 2),
            (
                unspecified.clone(),
                tycho_simulation::tycho_common::dto::ChangeType::Unspecified.into(),
                3,
            ),
            (absent.clone(), ChangeType::Update, 4),
        ] {
            changes
                .account_deltas
                .insert(address.clone(), account_update(address, change, seed, None));
        }
        let expected = HashMap::from([
            (creation.clone(), changes.account_deltas[&creation].clone()),
            (absent.clone(), changes.account_deltas[&absent].clone()),
        ]);
        let mut message = raw_protocol_message_with_changes(
            "vm:balancer_v2",
            2,
            Snapshot {
                states: HashMap::new(),
                vm_storage,
            },
            Some(changes),
            HashMap::new(),
        );

        compact_raw_state_sync_message(&mut message.message);

        let Some(deltas) = message.message.deltas else {
            return Err(anyhow!("expected VM update residue"));
        };
        assert_eq!(deltas.account_deltas, expected);
        Ok(())
    }

    #[test]
    fn raw_cache_account_creation_then_deletion_does_not_resurrect_on_bootstrap() {
        let address = DtoBytes::from([58u8; 20]);
        let mut creation = BlockAggregatedChanges::default();
        creation.account_deltas.insert(
            address.clone(),
            account_update(address.clone(), ChangeType::Creation, 1, None),
        );
        let mut messages = Vec::new();
        merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "vm:curve",
                10,
                Snapshot::default(),
                Some(creation),
                HashMap::new(),
            ),
        );

        let mut deletion = BlockAggregatedChanges::default();
        deletion.account_deltas.insert(
            address.clone(),
            account_update(address.clone(), ChangeType::Deletion, 2, None),
        );
        merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "vm:curve",
                11,
                Snapshot::default(),
                Some(deletion),
                HashMap::new(),
            ),
        );

        assert!(!messages[0]
            .message
            .snapshots
            .vm_storage
            .contains_key(&address));
        assert!(messages[0].message.deltas.is_none());
    }

    #[test]
    fn raw_cache_prunes_removed_components_for_fresh_bootstrap() {
        let component_id = "removed";
        let account_address = DtoBytes::from([61u8; 20]);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            component_id.to_string(),
            component_state_delta(component_id, [("version", DtoBytes::from([2u8; 32]))], []),
        );
        changes
            .component_balances
            .insert(component_id.to_string(), HashMap::new());
        changes.component_tvl.insert(component_id.to_string(), 2.0);
        changes.new_protocol_components.insert(
            component_id.to_string(),
            raw_component(component_id, "uniswap_v2", 2),
        );
        changes.deleted_protocol_components.insert(
            component_id.to_string(),
            raw_component(component_id, "uniswap_v2", 2),
        );
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot {
                states: HashMap::from([(
                    component_id.to_string(),
                    raw_component_with_state(component_id, 1),
                )]),
                vm_storage: HashMap::from([(
                    account_address.clone(),
                    raw_response_account(account_address.clone(), 0, 0),
                )]),
            },
            Some(changes),
            HashMap::from([(
                component_id.to_string(),
                raw_component(component_id, "uniswap_v2", 2),
            )]),
        );

        compact_raw_state_sync_message(&mut message.message);

        assert!(!message.message.snapshots.states.contains_key(component_id));
        assert!(message.message.deltas.is_none());
        assert!(message.message.removed_components.is_empty());
        assert!(message
            .message
            .snapshots
            .vm_storage
            .contains_key(&account_address));
    }

    #[test]
    fn raw_cache_prunes_deleted_component_entrypoints() -> Result<()> {
        let surviving_component = "surviving";
        let surviving_entrypoint = "entrypoint-surviving";
        let mut states = HashMap::new();
        let mut changes = BlockAggregatedChanges::default();
        for (component, entrypoint) in [
            ("deleted", "entrypoint-deleted"),
            (surviving_component, surviving_entrypoint),
        ] {
            states.insert(
                component.to_string(),
                raw_component_with_state(component, 1),
            );
            changes.dci_update.new_entrypoints.insert(
                component.to_string(),
                HashSet::from([EntryPoint::new(
                    entrypoint.to_string(),
                    DtoBytes::from([61u8; 20]),
                    "balanceOf(address)".to_string(),
                )]),
            );
            changes
                .dci_update
                .new_entrypoint_params
                .insert(entrypoint.to_string(), HashSet::new());
            changes
                .dci_update
                .trace_results
                .insert(entrypoint.to_string(), Default::default());
        }
        changes.deleted_protocol_components.insert(
            "deleted".to_string(),
            raw_component("deleted", "uniswap_v2", 2),
        );
        let surviving_entrypoints = changes.dci_update.new_entrypoints[surviving_component].clone();
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot {
                states,
                vm_storage: HashMap::new(),
            },
            Some(changes),
            HashMap::new(),
        );

        compact_raw_state_sync_message(&mut message.message);

        assert_eq!(
            message
                .message
                .snapshots
                .states
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            HashSet::from([surviving_component])
        );
        let deltas = message
            .message
            .deltas
            .as_ref()
            .ok_or_else(|| anyhow!("surviving entrypoints must remain"))?;
        assert!(deltas.deleted_protocol_components.is_empty());
        assert_eq!(
            deltas.dci_update.new_entrypoints,
            HashMap::from([(surviving_component.to_string(), surviving_entrypoints)])
        );
        assert_eq!(
            deltas.dci_update.new_entrypoint_params,
            HashMap::from([(surviving_entrypoint.to_string(), HashSet::new())])
        );
        assert_eq!(
            deltas.dci_update.trace_results,
            HashMap::from([(surviving_entrypoint.to_string(), Default::default())])
        );
        Ok(())
    }

    #[test]
    fn raw_cache_residue_count_does_not_grow_for_foldable_updates() {
        let component_id = "raw-1";
        let mut messages = Vec::new();
        merge_raw_message(
            &mut messages,
            raw_protocol_message_with_states(HashMap::from([(
                component_id.to_string(),
                raw_component_with_state(component_id, 1),
            )])),
        );

        for block_number in 11..=1_010 {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                component_id.to_string(),
                component_state_delta(
                    component_id,
                    [("version", DtoBytes::from([(block_number % 255) as u8; 32]))],
                    [],
                ),
            );
            merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "uniswap_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
            assert_eq!(super::raw_residue_entry_count(&messages[0].message), 0);
        }
    }

    #[test]
    fn raw_merge_reports_residue_growth_only_when_it_grows() {
        let residue = |block_number, value: u8| {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                "missing".to_string(),
                component_state_delta("missing", [("version", DtoBytes::from([value; 32]))], []),
            );
            raw_protocol_message_with_changes(
                "uniswap_v2",
                block_number,
                Snapshot::default(),
                Some(changes),
                HashMap::new(),
            )
        };
        let mut messages = Vec::new();

        let first = merge_raw_message(&mut messages, residue(1, 1));
        let second = merge_raw_message(&mut messages, residue(2, 2));

        assert_eq!(
            first,
            Some(RawResidueGrowth {
                protocol: "uniswap_v2".to_string(),
                previous_count: 0,
                stats: RawCompactionStats {
                    residual_entries: 1,
                    ..RawCompactionStats::default()
                },
            })
        );
        assert_eq!(second, None);
        assert_eq!(raw_residue_entry_count(&messages[0].message), 1);
    }

    fn block_header(number: u64, seed: u8) -> BlockHeader {
        BlockHeader {
            hash: DtoBytes::from(vec![seed; 32]),
            number,
            parent_hash: DtoBytes::from(vec![seed.saturating_add(1); 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        }
    }

    fn raw_protocol_message_with_states(
        states: HashMap<String, ComponentWithState>,
    ) -> BroadcasterProtocolMessage {
        BroadcasterProtocolMessage::new(
            "uniswap_v2",
            SynchronizerState::Started,
            StateSyncMessage {
                header: block_header(10, 1),
                snapshots: Snapshot {
                    states,
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
    }

    fn raw_protocol_message_with_changes(
        protocol: &str,
        block_number: u64,
        snapshots: Snapshot,
        deltas: Option<BlockAggregatedChanges>,
        removed_components: HashMap<String, DtoProtocolComponent>,
    ) -> BroadcasterProtocolMessage {
        let header = block_header(block_number, block_number as u8);
        BroadcasterProtocolMessage::new(
            protocol,
            SynchronizerState::Ready(header.clone()),
            StateSyncMessage {
                header,
                snapshots,
                deltas,
                removed_components,
            },
        )
    }

    fn component_state_delta(
        component_id: &str,
        attributes: impl IntoIterator<Item = (&'static str, DtoBytes)>,
        deleted_attributes: impl IntoIterator<Item = &'static str>,
    ) -> ProtocolComponentStateDelta {
        ProtocolComponentStateDelta::new(
            component_id,
            attributes
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect(),
            deleted_attributes.into_iter().map(str::to_string).collect(),
        )
    }

    fn account_update(
        address: DtoBytes,
        change: ChangeType,
        slot_seed: u8,
        balance: Option<DtoBytes>,
    ) -> AccountDelta {
        AccountDelta::new(
            DtoChain::Ethereum,
            address,
            HashMap::from([(
                DtoBytes::from([slot_seed; 32]),
                Some(DtoBytes::from([slot_seed.saturating_add(1); 32])),
            )]),
            balance,
            None,
            change,
        )
    }

    fn component_balance(component_id: &str, token: DtoBytes, balance: u8) -> ComponentBalance {
        ComponentBalance {
            token,
            balance: DtoBytes::from([balance; 32]),
            balance_float: f64::from(balance),
            modify_tx: DtoBytes::from([balance; 32]),
            component_id: component_id.to_string(),
        }
    }

    fn account_balance(account: DtoBytes, token: DtoBytes, balance: u8) -> AccountBalance {
        AccountBalance {
            account,
            token,
            balance: DtoBytes::from([balance; 32]),
            modify_tx: DtoBytes::from([balance; 32]),
        }
    }

    fn raw_component_with_state(component_id: &str, seed: u8) -> ComponentWithState {
        ComponentWithState {
            state: ProtocolComponentState {
                component_id: component_id.to_string(),
                attributes: HashMap::from([(
                    "large".to_string(),
                    DtoBytes::from(vec![seed; 1024]),
                )]),
                balances: HashMap::new(),
            },
            component: raw_component(component_id, "uniswap_v2", seed),
            component_tvl: Some(seed as f64),
            entrypoints: Vec::new(),
        }
    }

    fn raw_component(component_id: &str, protocol: &str, seed: u8) -> DtoProtocolComponent {
        DtoProtocolComponent {
            id: component_id.to_string(),
            protocol_system: protocol.to_string(),
            protocol_type_name: protocol.to_string(),
            chain: DtoChain::Ethereum,
            tokens: vec![DtoBytes::from([seed; 20]), DtoBytes::from([seed + 1; 20])],
            contract_addresses: Vec::new(),
            static_attributes: HashMap::new(),
            change: Default::default(),
            creation_tx: DtoBytes::from([seed; 32]),
            created_at: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        }
    }

    fn raw_response_account(
        address: DtoBytes,
        slot_count: usize,
        slot_value_size: usize,
    ) -> Account {
        let mut slots = HashMap::new();
        for index in 0..slot_count {
            let seed = index as u8;
            let mut slot_key = vec![0u8; 32];
            slot_key[24..].copy_from_slice(&(index as u64).to_be_bytes());
            slots.insert(
                DtoBytes::from(slot_key),
                DtoBytes::from(vec![seed.saturating_add(1); slot_value_size]),
            );
        }

        Account::new(
            DtoChain::Ethereum,
            address,
            "vm-account".to_string(),
            slots,
            DtoBytes::from([0u8; 32]),
            HashMap::new(),
            DtoBytes::from(vec![7u8; 128]),
            DtoBytes::from([8u8; 32]),
            DtoBytes::from([9u8; 32]),
            DtoBytes::from([10u8; 32]),
            None,
        )
    }
}
