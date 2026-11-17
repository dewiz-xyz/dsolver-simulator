CREATE SCHEMA state_history;

CREATE TABLE state_history.schema_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    version INTEGER NOT NULL
);
INSERT INTO state_history.schema_meta (version) VALUES (1);

-- One row per accepted Update entry. (chain_id, generation, message_seq) is both the
-- idempotency key and the replay order.
CREATE TABLE state_history.deltas (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    generation BIGINT NOT NULL,
    message_seq BIGINT NOT NULL,
    block_number BIGINT,
    observed_at_ms BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, generation, message_seq)
);

-- One update can move native and VM at different heights and RFQ by provider timestamp;
-- range queries select on the partition cursor, never the envelope cursor.
CREATE TABLE state_history.delta_backends (
    delta_id BIGINT NOT NULL REFERENCES state_history.deltas (id) ON DELETE CASCADE,
    chain_id BIGINT NOT NULL,
    backend TEXT NOT NULL CHECK (backend IN ('native', 'vm', 'rfq')),
    generation BIGINT NOT NULL,
    message_seq BIGINT NOT NULL,
    block_number BIGINT,
    observed_at_ms BIGINT,
    PRIMARY KEY (delta_id, backend),
    CHECK (block_number IS NOT NULL OR observed_at_ms IS NOT NULL)
);
CREATE INDEX delta_backends_block_range ON state_history.delta_backends
    (chain_id, backend, block_number, generation, message_seq) WHERE block_number IS NOT NULL;
CREATE INDEX delta_backends_time_range ON state_history.delta_backends
    (chain_id, backend, observed_at_ms, generation, message_seq) WHERE observed_at_ms IS NOT NULL;

CREATE TABLE state_history.checkpoints (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    generation BIGINT NOT NULL,
    message_seq BIGINT NOT NULL,
    state_version BIGINT,
    kind TEXT NOT NULL CHECK (kind IN ('boundary', 'interval')),
    block_number BIGINT NOT NULL,
    rfq_observed_at_ms BIGINT,
    backends TEXT[] NOT NULL,
    s3_key TEXT,
    archive_sha256 TEXT,
    archive_bytes BIGINT,
    compressed_bytes BIGINT,
    token_s3_key TEXT,
    token_sha256 TEXT,
    token_count BIGINT,
    token_bytes BIGINT,
    status TEXT NOT NULL DEFAULT 'writing' CHECK (status IN ('writing', 'complete', 'failed')),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    -- A partial token reference is unusable: readers cannot verify an object key without its hash.
    -- Coupling to status makes the completion transaction the only writer of a reference.
    CONSTRAINT checkpoints_token_reference_all_or_none CHECK (
        num_nonnulls(token_s3_key, token_sha256, token_count, token_bytes) = 0
        OR (
            num_nonnulls(token_s3_key, token_sha256, token_count, token_bytes) = 4
            AND status = 'complete'
        )
    ),
    UNIQUE (chain_id, generation, message_seq, kind)
);
CREATE INDEX checkpoints_selection ON state_history.checkpoints
    (chain_id, generation DESC, message_seq DESC, block_number) WHERE status = 'complete';
CREATE INDEX checkpoints_boundaries ON state_history.checkpoints
    (chain_id, generation, message_seq) WHERE kind = 'boundary';

-- Writer-known losses only. Honest reporting, not proof machinery.
CREATE TABLE state_history.gaps (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    generation BIGINT NOT NULL,
    from_message_seq BIGINT NOT NULL,
    to_message_seq BIGINT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('queue_overflow', 'write_failed', 'writer_stopped', 'checkpoint_failed', 'boundary_slip')),
    from_block_number BIGINT,
    to_block_number BIGINT,
    from_observed_at_ms BIGINT,
    to_observed_at_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_message_seq <= to_message_seq),
    CONSTRAINT gaps_identity UNIQUE NULLS NOT DISTINCT
        (chain_id, generation, from_message_seq, to_message_seq, reason,
         from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms)
);
CREATE INDEX gaps_range ON state_history.gaps (chain_id, generation, from_message_seq, to_message_seq);

-- The block↔time↔gas join table Edson queries directly. Superseded only by a newer
-- (generation, message_seq) observation, so an old generation can't undo a reorg correction.
CREATE TABLE state_history.block_times (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    block_hash TEXT,
    parent_hash TEXT,
    base_fee_wei NUMERIC(78, 0),
    source_generation BIGINT NOT NULL,
    source_message_seq BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_number)
);
CREATE INDEX block_times_by_time ON state_history.block_times (chain_id, timestamp_ms, block_number);
