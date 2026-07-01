CREATE SCHEMA IF NOT EXISTS state_history;

CREATE TABLE state_history.delta_messages (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    stream_id TEXT NOT NULL,
    snapshot_id TEXT,
    message_seq BIGINT NOT NULL,
    redis_entry_id TEXT,
    kind TEXT NOT NULL,
    backend_scope TEXT[] NOT NULL,
    block_number BIGINT,
    observed_timestamp_ms BIGINT,
    payload_encoding TEXT NOT NULL,
    payload_compressed BYTEA NOT NULL,
    payload_hash TEXT NOT NULL,
    runtime_published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, stream_id, message_seq)
);

CREATE TABLE state_history.delta_backend_index (
    delta_id BIGINT NOT NULL REFERENCES state_history.delta_messages(id) ON DELETE CASCADE,
    chain_id BIGINT NOT NULL,
    backend TEXT NOT NULL,
    block_number BIGINT,
    observed_timestamp_ms BIGINT,
    message_seq BIGINT NOT NULL,
    gas_price_wei NUMERIC(78, 0),
    gas_price_source TEXT CHECK (gas_price_source = 'rpc_block'),
    gas_price_block_hash TEXT,
    gas_price_block_timestamp_secs BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (delta_id, backend),
    CHECK (
        (gas_price_wei IS NULL AND gas_price_source IS NULL)
        OR (gas_price_wei IS NOT NULL AND gas_price_source IS NOT NULL)
    )
);

CREATE INDEX state_history_delta_backend_block_idx
    ON state_history.delta_backend_index (chain_id, backend, block_number, message_seq)
    WHERE block_number IS NOT NULL;

CREATE INDEX state_history_delta_backend_timestamp_idx
    ON state_history.delta_backend_index (chain_id, backend, observed_timestamp_ms, message_seq)
    WHERE observed_timestamp_ms IS NOT NULL;

CREATE TABLE state_history.checkpoints (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    captured_at_timestamp_ms BIGINT NOT NULL,
    rfq_update_timestamp_ms BIGINT,
    stream_id TEXT NOT NULL,
    source_message_seq BIGINT NOT NULL,
    backend_scope TEXT[] NOT NULL,
    s3_bucket TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    payload_encoding TEXT NOT NULL,
    payload_hash TEXT,
    payload_bytes BIGINT,
    compressed_bytes BIGINT,
    status TEXT NOT NULL CHECK (status IN ('writing', 'complete', 'failed')),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (chain_id, block_number, captured_at_timestamp_ms, stream_id)
);

CREATE INDEX state_history_checkpoints_lookup_idx
    ON state_history.checkpoints (chain_id, block_number DESC, captured_at_timestamp_ms DESC)
    WHERE status = 'complete';

CREATE INDEX state_history_checkpoints_rfq_lookup_idx
    ON state_history.checkpoints (chain_id, block_number DESC, rfq_update_timestamp_ms DESC)
    WHERE status = 'complete' AND rfq_update_timestamp_ms IS NOT NULL;

CREATE TABLE state_history.checkpoint_backend_index (
    checkpoint_id BIGINT NOT NULL REFERENCES state_history.checkpoints(id) ON DELETE CASCADE,
    chain_id BIGINT NOT NULL,
    backend TEXT NOT NULL,
    block_number BIGINT,
    observed_timestamp_ms BIGINT,
    gas_price_wei NUMERIC(78, 0),
    gas_price_source TEXT CHECK (gas_price_source = 'rpc_block'),
    gas_price_block_hash TEXT,
    gas_price_block_timestamp_secs BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (checkpoint_id, backend),
    CHECK (
        block_number IS NOT NULL OR observed_timestamp_ms IS NOT NULL
    ),
    CHECK (
        (gas_price_wei IS NULL AND gas_price_source IS NULL)
        OR (gas_price_wei IS NOT NULL AND gas_price_source IS NOT NULL)
    )
);

CREATE INDEX state_history_checkpoint_backend_block_idx
    ON state_history.checkpoint_backend_index (chain_id, backend, block_number)
    WHERE block_number IS NOT NULL;

CREATE TABLE state_history.ingestion_gaps (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    stream_id TEXT NOT NULL,
    from_message_seq BIGINT NOT NULL,
    to_message_seq BIGINT NOT NULL,
    backend_scope TEXT[] NOT NULL,
    from_block_number BIGINT,
    to_block_number BIGINT,
    from_timestamp_ms BIGINT,
    to_timestamp_ms BIGINT,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_message_seq <= to_message_seq)
);

CREATE INDEX state_history_ingestion_gaps_stream_idx
    ON state_history.ingestion_gaps (chain_id, stream_id, from_message_seq, to_message_seq);
