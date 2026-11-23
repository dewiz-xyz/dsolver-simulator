-- prop_amm appears only in new broadcaster records. Previous RFQ records keep
-- their wire tag and timestamp. The two book feeds advance independently.
-- The additive columns preserve schema major 2 for a rolling deployment;
-- new processes check for this migration's column and validated constraint.
ALTER TABLE state_history.delta_backends
    DROP CONSTRAINT delta_backends_backend_check,
    ADD CONSTRAINT delta_backends_allow_prop_amm
        CHECK (backend IN ('native', 'vm', 'rfq', 'prop_amm'));

ALTER TABLE state_history.checkpoints
    ADD COLUMN prop_amm_observed_at_ms BIGINT;
