ALTER TABLE state_history.deltas
    ADD COLUMN payload_format_version SMALLINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT deltas_payload_format_version_positive
        CHECK (payload_format_version > 0);

UPDATE state_history.schema_meta SET version = 2;
