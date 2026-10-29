-- Boundary slips used to store 0 when the request-time cursor was unknown, which made every
-- reader treat the gap as reaching back to the start of history. Lost messages come after the
-- last complete checkpoint in stream order, so its cursor is the floor. A ceiling under that
-- checkpoint means the stream went backwards, and then the floor stays 0. New rows get the same
-- floor from `record_gap` in store.rs.
UPDATE state_history.gaps g
SET from_block_number = COALESCE(
    (SELECT CASE WHEN c.block_number <= g.to_block_number THEN c.block_number ELSE 0 END
     FROM state_history.checkpoints c
     WHERE c.chain_id = g.chain_id
       AND c.status = 'complete'
       AND (c.generation, c.message_seq) < (g.generation, g.from_message_seq)
     ORDER BY c.generation DESC, c.message_seq DESC
     LIMIT 1),
    0)
WHERE g.reason = 'boundary_slip'
  AND g.from_block_number = 0
  AND g.to_block_number IS NOT NULL;

UPDATE state_history.gaps g
SET from_observed_at_ms = COALESCE(
    (SELECT CASE WHEN c.rfq_observed_at_ms <= g.to_observed_at_ms
                 THEN c.rfq_observed_at_ms ELSE 0 END
     FROM state_history.checkpoints c
     WHERE c.chain_id = g.chain_id
       AND c.status = 'complete'
       AND (c.generation, c.message_seq) < (g.generation, g.from_message_seq)
     ORDER BY c.generation DESC, c.message_seq DESC
     LIMIT 1),
    0)
WHERE g.reason = 'boundary_slip'
  AND g.from_observed_at_ms = 0
  AND g.to_observed_at_ms IS NOT NULL;
