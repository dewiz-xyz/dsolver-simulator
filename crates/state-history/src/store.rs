use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
use sqlx::{migrate::Migrator, Connection, Postgres, Transaction};

use crate::{
    BlockTimeObservation, CapturedState, DeltaEntry, GapRecord, StreamPosition, TokenSnapshotRef,
    DELTA_PAYLOAD_FORMAT_VERSION,
};

pub const STATE_HISTORY_SCHEMA_VERSION: i32 = 2;

const BLOCK_TIME_UPSERT: &str = r#"
    INSERT INTO state_history.block_times AS bt (chain_id, block_number, timestamp_ms, block_hash,
        parent_hash, base_fee_wei, source_generation, source_message_seq)
    VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8)
    ON CONFLICT (chain_id, block_number) DO UPDATE SET
        timestamp_ms = EXCLUDED.timestamp_ms, block_hash = EXCLUDED.block_hash,
        parent_hash = EXCLUDED.parent_hash,
        base_fee_wei = CASE
            WHEN EXCLUDED.block_hash IS NOT DISTINCT FROM bt.block_hash
                THEN COALESCE(EXCLUDED.base_fee_wei, bt.base_fee_wei)
            ELSE EXCLUDED.base_fee_wei
        END,
        source_generation = EXCLUDED.source_generation, source_message_seq = EXCLUDED.source_message_seq,
        updated_at = CASE WHEN (bt.timestamp_ms, bt.block_hash, bt.parent_hash)
            IS DISTINCT FROM (EXCLUDED.timestamp_ms, EXCLUDED.block_hash, EXCLUDED.parent_hash)
            THEN now() ELSE bt.updated_at END
    WHERE (EXCLUDED.source_generation, EXCLUDED.source_message_seq)
        > (bt.source_generation, bt.source_message_seq)
"#;

pub static STATE_HISTORY_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub const STATE_HISTORY_WRITER_ROLE: &str = "state_history_writer";
pub const STATE_HISTORY_OBSERVER_ROLE: &str = "state_history_observer";

pub struct StateHistoryStore {
    pool: sqlx::PgPool,
}

impl StateHistoryStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect(database_url)
            .await
            .context("failed to connect to the state history database")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    #[cfg(test)]
    pub(crate) fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn validate_schema(&self) -> anyhow::Result<()> {
        let version: i32 = sqlx::query_scalar("SELECT version FROM state_history.schema_meta")
            .fetch_one(&self.pool)
            .await
            .context("failed to read state history schema version")?;
        ensure!(
            version == STATE_HISTORY_SCHEMA_VERSION,
            "unsupported state history schema version {version}; expected {STATE_HISTORY_SCHEMA_VERSION}"
        );

        for table in [
            "state_history.schema_meta",
            "state_history.deltas",
            "state_history.delta_backends",
            "state_history.checkpoints",
            "state_history.gaps",
            "state_history.block_times",
        ] {
            let relation: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
                .bind(table)
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("failed to validate state history table {table}"))?;
            ensure!(
                relation.is_some(),
                "required state history table {table} does not exist"
            );
        }

        Ok(())
    }

    pub async fn latest_block_number(&self, chain_id: u64) -> anyhow::Result<Option<u64>> {
        let block_number: Option<i64> = sqlx::query_scalar(
            "SELECT max(block_number) FROM state_history.block_times WHERE chain_id = $1",
        )
        .bind(db_i64(chain_id, "block time chain_id")?)
        .fetch_one(&self.pool)
        .await
        .context("failed to read the latest state history block number")?;
        block_number
            .map(|value| u64::try_from(value).context("latest block number is negative"))
            .transpose()
    }

    pub async fn run_migrations(&self) -> anyhow::Result<()> {
        STATE_HISTORY_MIGRATOR
            .run(&self.pool)
            .await
            .context("failed to run state history migrations")?;
        Ok(())
    }

    pub async fn validate_migration_catalog(&self) -> anyhow::Result<()> {
        let applied: Vec<(i64, String, bool, Vec<u8>)> = sqlx::query_as(
            "SELECT version, description, success, checksum FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to read the SQLx migration catalog")?;
        let expected = STATE_HISTORY_MIGRATOR
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
            .collect::<Vec<_>>();

        ensure!(
            applied.len() == expected.len(),
            "state history migration catalog has {} entries; expected {}",
            applied.len(),
            expected.len()
        );
        for ((version, description, success, checksum), migration) in applied.iter().zip(expected) {
            ensure!(
                *version == migration.version,
                "state history migration catalog version mismatch: expected {}, got {version}",
                migration.version
            );
            ensure!(
                *success,
                "state history migration {version} is marked unsuccessful"
            );
            ensure!(
                description == migration.description.as_ref(),
                "state history migration {version} description mismatch: expected {}, got {description}",
                migration.description
            );
            ensure!(
                checksum.as_slice() == migration.checksum.as_ref(),
                "state history migration {version} checksum does not match the embedded migration"
            );
        }
        Ok(())
    }

    pub async fn provision_runtime_roles(&self, writer_password: &str) -> anyhow::Result<()> {
        ensure!(
            !writer_password.is_empty(),
            "state history writer password is empty"
        );
        let rds_iam_exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_iam')")
                .fetch_one(&self.pool)
                .await
                .context("failed to verify the RDS IAM database role")?;
        ensure!(
            rds_iam_exists,
            "required PostgreSQL role rds_iam does not exist"
        );

        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to start state history role transaction")?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('state-history-runtime-roles', 0))",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to lock state history role provisioning")?;
        create_runtime_roles(&mut transaction).await?;
        ensure_runtime_roles_are_restricted(&mut transaction).await?;
        set_runtime_writer_password(&mut transaction, writer_password).await?;
        apply_runtime_role_grants(&mut transaction).await?;

        transaction
            .commit()
            .await
            .context("failed to commit state history runtime roles")?;
        self.validate_runtime_roles().await?;
        Ok(())
    }

    async fn validate_runtime_roles(&self) -> anyhow::Result<()> {
        let roles_valid: bool = sqlx::query_scalar(
            r#"
            SELECT
                writer.rolcanlogin AND NOT writer.rolsuper AND NOT writer.rolcreatedb
                    AND NOT writer.rolcreaterole AND NOT writer.rolreplication
                    AND NOT writer.rolbypassrls AND writer.rolinherit
                    AND observer.rolcanlogin AND NOT observer.rolsuper
                    AND NOT observer.rolcreatedb AND NOT observer.rolcreaterole
                    AND NOT observer.rolreplication AND NOT observer.rolbypassrls
                    AND observer.rolinherit
                    AND COALESCE('default_transaction_read_only=on' = ANY(observer.rolconfig), false)
                    AND pg_has_role('state_history_observer', 'rds_iam', 'MEMBER')
            FROM pg_roles AS writer
            CROSS JOIN pg_roles AS observer
            WHERE writer.rolname = 'state_history_writer'
              AND observer.rolname = 'state_history_observer'
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to validate state history role attributes")?;
        ensure!(
            roles_valid,
            "state history runtime role attributes are invalid"
        );

        let table_privileges_valid: bool = sqlx::query_scalar(
            r#"
            SELECT COALESCE(bool_and(
                has_table_privilege('state_history_writer', format('%I.%I', schemaname, tablename), 'SELECT')
                AND has_table_privilege('state_history_writer', format('%I.%I', schemaname, tablename), 'INSERT')
                AND has_table_privilege('state_history_writer', format('%I.%I', schemaname, tablename), 'UPDATE')
                AND NOT has_table_privilege('state_history_writer', format('%I.%I', schemaname, tablename), 'DELETE')
                AND has_table_privilege('state_history_observer', format('%I.%I', schemaname, tablename), 'SELECT')
                AND NOT has_table_privilege('state_history_observer', format('%I.%I', schemaname, tablename), 'INSERT')
                AND NOT has_table_privilege('state_history_observer', format('%I.%I', schemaname, tablename), 'UPDATE')
                AND NOT has_table_privilege('state_history_observer', format('%I.%I', schemaname, tablename), 'DELETE')
            ), false)
            FROM pg_tables
            WHERE schemaname = 'state_history'
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to validate state history table privileges")?;
        ensure!(
            table_privileges_valid,
            "state history table privileges are invalid"
        );

        let other_privileges_valid: bool = sqlx::query_scalar(
            r#"
            SELECT
                has_database_privilege('state_history_writer', current_database(), 'CONNECT')
                AND has_database_privilege('state_history_observer', current_database(), 'CONNECT')
                AND has_schema_privilege('state_history_writer', 'state_history', 'USAGE')
                AND has_schema_privilege('state_history_observer', 'state_history', 'USAGE')
                AND has_table_privilege('state_history_observer', 'public._sqlx_migrations', 'SELECT')
                AND NOT has_table_privilege('state_history_writer', 'public._sqlx_migrations', 'SELECT')
                AND NOT EXISTS (
                    SELECT 1
                    FROM pg_sequences
                    WHERE schemaname = 'state_history'
                      AND NOT has_sequence_privilege(
                          'state_history_writer',
                          format('%I.%I', schemaname, sequencename),
                          'USAGE'
                      )
                )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to validate state history schema privileges")?;
        ensure!(
            other_privileges_valid,
            "state history schema privileges are invalid"
        );
        Ok(())
    }

    pub async fn insert_delta(
        conn: &mut sqlx::PgConnection,
        delta: &DeltaEntry,
        base_fee_by_block: &BTreeMap<u64, String>,
    ) -> anyhow::Result<DeltaInsertOutcome> {
        let position = delta.position();
        let chain_id = db_i64(delta.chain_id(), "delta chain_id")?;
        let generation = db_i64(position.generation, "delta generation")?;
        let message_seq = db_i64(position.message_seq, "delta message_seq")?;
        let observed_at_ms = db_i64(delta.observed_at_ms(), "delta observed_at_ms")?;
        let block_number = delta
            .backends()
            .iter()
            .filter_map(|cursor| cursor.block_number)
            .max()
            .map(|value| db_i64(value, "delta block_number"))
            .transpose()?;
        let payload_sha256 = hex::encode(Sha256::digest(delta.payload_json().as_bytes()));
        let payload = zstd::stream::encode_all(delta.payload_json().as_bytes(), 3)
            .context("failed to compress delta payload")?;

        let inserted_id: Option<i64> = sqlx::query_scalar(
            "INSERT INTO state_history.deltas
                (chain_id, generation, message_seq, block_number, observed_at_ms, payload,
                 payload_sha256, payload_format_version)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (chain_id, generation, message_seq) DO NOTHING
             RETURNING id",
        )
        .bind(chain_id)
        .bind(generation)
        .bind(message_seq)
        .bind(block_number)
        .bind(observed_at_ms)
        .bind(payload)
        .bind(&payload_sha256)
        .bind(DELTA_PAYLOAD_FORMAT_VERSION)
        .fetch_optional(&mut *conn)
        .await
        .context("failed to insert state history delta")?;

        let (delta_id, outcome) = if let Some(id) = inserted_id {
            (id, DeltaInsertOutcome::Inserted(id))
        } else {
            let id = validate_existing_delta(
                conn,
                delta,
                chain_id,
                generation,
                message_seq,
                &payload_sha256,
            )
            .await?;
            (id, DeltaInsertOutcome::Duplicate)
        };

        for cursor in delta.backends() {
            sqlx::query(
                "INSERT INTO state_history.delta_backends
                    (delta_id, chain_id, backend, generation, message_seq, block_number, observed_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT DO NOTHING",
            )
            .bind(delta_id)
            .bind(chain_id)
            .bind(cursor.backend.as_str())
            .bind(generation)
            .bind(message_seq)
            .bind(
                cursor
                    .block_number
                    .map(|value| db_i64(value, "delta backend block_number"))
                    .transpose()?,
            )
            .bind(
                cursor
                    .observed_at_ms
                    .map(|value| db_i64(value, "delta backend observed_at_ms"))
                    .transpose()?,
            )
            .execute(&mut *conn)
            .await
            .context("failed to insert delta backend cursor")?;
        }

        upsert_block_times(
            conn,
            delta.chain_id(),
            delta.block_times(),
            position,
            base_fee_by_block,
        )
        .await?;

        Ok(outcome)
    }

    pub async fn record_gap(conn: &mut sqlx::PgConnection, gap: &GapRecord) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO state_history.gaps
                (chain_id, generation, from_message_seq, to_message_seq, reason,
                 from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT ON CONSTRAINT gaps_identity DO NOTHING",
        )
        .bind(db_i64(gap.chain_id, "gap chain_id")?)
        .bind(db_i64(gap.generation, "gap generation")?)
        .bind(db_i64(gap.from_message_seq, "gap from_message_seq")?)
        .bind(db_i64(gap.to_message_seq, "gap to_message_seq")?)
        .bind(gap.reason.as_str())
        .bind(
            gap.from_block_number
                .map(|value| db_i64(value, "gap from_block_number"))
                .transpose()?,
        )
        .bind(
            gap.to_block_number
                .map(|value| db_i64(value, "gap to_block_number"))
                .transpose()?,
        )
        .bind(
            gap.from_observed_at_ms
                .map(|value| db_i64(value, "gap from_observed_at_ms"))
                .transpose()?,
        )
        .bind(
            gap.to_observed_at_ms
                .map(|value| db_i64(value, "gap to_observed_at_ms"))
                .transpose()?,
        )
        .execute(conn)
        .await
        .context("failed to record state history gap")?;
        Ok(())
    }

    pub async fn create_checkpoint(
        conn: &mut sqlx::PgConnection,
        capture: &CapturedState,
    ) -> anyhow::Result<i64> {
        let position = capture.position();
        let backends = capture
            .backends()
            .iter()
            .map(|backend| backend.as_str())
            .collect::<Vec<_>>();
        let id = sqlx::query_scalar(
            "INSERT INTO state_history.checkpoints
                (chain_id, generation, message_seq, state_version, kind, block_number,
                 rfq_observed_at_ms, backends, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'writing')
             RETURNING id",
        )
        .bind(db_i64(capture.chain_id(), "checkpoint chain_id")?)
        .bind(db_i64(position.generation, "checkpoint generation")?)
        .bind(db_i64(position.message_seq, "checkpoint message_seq")?)
        .bind(
            capture
                .state_version()
                .map(|value| db_i64(value, "checkpoint state_version"))
                .transpose()?,
        )
        .bind(capture.kind().as_str())
        .bind(db_i64(capture.block_number(), "checkpoint block_number")?)
        .bind(
            capture
                .rfq_observed_at_ms()
                .map(|value| db_i64(value, "checkpoint rfq_observed_at_ms"))
                .transpose()?,
        )
        .bind(backends)
        .fetch_one(conn)
        .await
        .context("failed to create state history checkpoint")?;
        Ok(id)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn complete_checkpoint(
        conn: &mut sqlx::PgConnection,
        id: i64,
        s3_key: &str,
        archive: &EncodedArchiveInfo,
        token_reference: Option<&TokenSnapshotRef>,
        block_times: &[BlockTimeObservation],
        source: StreamPosition,
        chain_id: u64,
    ) -> anyhow::Result<()> {
        let token_count = token_reference
            .map(|reference| db_i64(reference.token_count, "checkpoint token_count"))
            .transpose()?;
        let token_bytes = token_reference
            .map(|reference| db_i64(reference.token_bytes, "checkpoint token_bytes"))
            .transpose()?;
        let mut transaction = conn
            .begin()
            .await
            .context("failed to start checkpoint completion transaction")?;
        let result = sqlx::query(
            "UPDATE state_history.checkpoints
             SET s3_key = $2, archive_sha256 = $3, archive_bytes = $4,
                 compressed_bytes = $5, token_s3_key = $6, token_sha256 = $7,
                 token_count = $8, token_bytes = $9, status = 'complete', error = NULL,
                 completed_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(s3_key)
        .bind(&archive.sha256)
        .bind(db_i64(archive.archive_bytes, "checkpoint archive_bytes")?)
        .bind(db_i64(
            archive.compressed_bytes,
            "checkpoint compressed_bytes",
        )?)
        .bind(token_reference.map(|reference| reference.s3_key.as_str()))
        .bind(token_reference.map(|reference| reference.sha256.as_str()))
        .bind(token_count)
        .bind(token_bytes)
        .execute(&mut *transaction)
        .await
        .context("failed to complete state history checkpoint")?;
        ensure!(
            result.rows_affected() == 1,
            "state history checkpoint {id} does not exist"
        );

        upsert_block_times(
            &mut transaction,
            chain_id,
            block_times,
            source,
            &BTreeMap::new(),
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit checkpoint completion transaction")?;
        Ok(())
    }

    pub async fn fail_checkpoint(pool: &sqlx::PgPool, id: i64, error: &str) -> anyhow::Result<()> {
        // A completed checkpoint can fail later, but a failed manifest must never expose a token reference.
        let result = sqlx::query(
            "UPDATE state_history.checkpoints
             SET token_s3_key = NULL, token_sha256 = NULL, token_count = NULL,
                 token_bytes = NULL, status = 'failed', error = $2, completed_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(error)
        .execute(pool)
        .await
        .context("failed to mark state history checkpoint as failed")?;
        ensure!(
            result.rows_affected() == 1,
            "state history checkpoint {id} does not exist"
        );
        Ok(())
    }
}

async fn create_runtime_roles(transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'state_history_writer') THEN
                CREATE ROLE state_history_writer LOGIN;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'state_history_observer') THEN
                CREATE ROLE state_history_observer LOGIN;
            END IF;
        END
        $$
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create state history runtime roles")?;
    Ok(())
}

async fn ensure_runtime_roles_are_restricted(
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // RDS administrators cannot clear superuser-only attributes, so reject an elevated existing
    // role before changing its credentials or grants. Fresh roles already use these safe defaults.
    let roles_restricted: bool = sqlx::query_scalar(
        r#"
        SELECT
            NOT writer.rolsuper AND NOT writer.rolcreatedb AND NOT writer.rolcreaterole
                AND NOT writer.rolreplication AND NOT writer.rolbypassrls
                AND NOT observer.rolsuper AND NOT observer.rolcreatedb
                AND NOT observer.rolcreaterole AND NOT observer.rolreplication
                AND NOT observer.rolbypassrls
        FROM pg_roles AS writer
        CROSS JOIN pg_roles AS observer
        WHERE writer.rolname = 'state_history_writer'
          AND observer.rolname = 'state_history_observer'
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to validate state history runtime role restrictions")?;
    ensure!(
        roles_restricted,
        "state history runtime roles have privileged attributes"
    );
    Ok(())
}

async fn set_runtime_writer_password(
    transaction: &mut Transaction<'_, Postgres>,
    writer_password: &str,
) -> anyhow::Result<()> {
    // Keep the password bound as data. PostgreSQL utility statements cannot accept a password
    // parameter directly, so the fixed PL/pgSQL block quotes it on the server.
    sqlx::query("SELECT set_config('state_history.writer_password', $1, true)")
        .bind(writer_password)
        .execute(&mut **transaction)
        .await
        .context("failed to stage the state history writer password")?;
    sqlx::query(
        r#"
        DO $$
        BEGIN
            EXECUTE format(
                'ALTER ROLE state_history_writer LOGIN PASSWORD %L',
                current_setting('state_history.writer_password')
            );
        END
        $$
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to set the state history writer password")?;
    sqlx::query("SELECT set_config('state_history.writer_password', '', true)")
        .execute(&mut **transaction)
        .await
        .context("failed to clear the staged state history writer password")?;
    Ok(())
}

async fn apply_runtime_role_grants(
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for statement in [
        "ALTER ROLE state_history_writer LOGIN INHERIT",
        "ALTER ROLE state_history_writer SET default_transaction_read_only = off",
        "ALTER ROLE state_history_observer LOGIN PASSWORD NULL INHERIT",
        "ALTER ROLE state_history_observer SET default_transaction_read_only = on",
        "GRANT rds_iam TO state_history_observer",
        "REVOKE ALL ON SCHEMA state_history FROM state_history_writer, state_history_observer",
        "REVOKE ALL ON ALL TABLES IN SCHEMA state_history FROM state_history_writer, state_history_observer",
        "REVOKE ALL ON ALL SEQUENCES IN SCHEMA state_history FROM state_history_writer, state_history_observer",
        "REVOKE ALL ON TABLE public._sqlx_migrations FROM state_history_writer, state_history_observer",
        "GRANT USAGE ON SCHEMA state_history TO state_history_writer, state_history_observer",
        "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA state_history TO state_history_writer",
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA state_history TO state_history_writer",
        "GRANT SELECT ON ALL TABLES IN SCHEMA state_history TO state_history_observer",
        "GRANT USAGE ON SCHEMA public TO state_history_observer",
        "GRANT SELECT ON TABLE public._sqlx_migrations TO state_history_observer",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA state_history REVOKE ALL ON TABLES FROM state_history_writer, state_history_observer",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA state_history REVOKE ALL ON SEQUENCES FROM state_history_writer, state_history_observer",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA state_history GRANT SELECT, INSERT, UPDATE ON TABLES TO state_history_writer",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA state_history GRANT USAGE ON SEQUENCES TO state_history_writer",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA state_history GRANT SELECT ON TABLES TO state_history_observer",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to apply state history role grant: {statement}"))?;
    }

    let connect_grant: String = sqlx::query_scalar(
        "SELECT format('GRANT CONNECT ON DATABASE %I TO state_history_writer, state_history_observer', current_database())",
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to prepare state history database connect grant")?;
    sqlx::query(&connect_grant)
        .execute(&mut **transaction)
        .await
        .context("failed to grant state history database access")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaInsertOutcome {
    Inserted(i64),
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedArchiveInfo {
    pub sha256: String,
    pub archive_bytes: u64,
    pub compressed_bytes: u64,
}

async fn validate_existing_delta(
    connection: &mut sqlx::PgConnection,
    delta: &DeltaEntry,
    chain_id: i64,
    generation: i64,
    message_seq: i64,
    payload_sha256: &str,
) -> anyhow::Result<i64> {
    let (id, stored_sha256, stored_format): (i64, String, i16) = sqlx::query_as(
        "SELECT id, payload_sha256, payload_format_version
         FROM state_history.deltas
         WHERE chain_id = $1 AND generation = $2 AND message_seq = $3",
    )
    .bind(chain_id)
    .bind(generation)
    .bind(message_seq)
    .fetch_one(connection)
    .await
    .context("failed to read conflicting state history delta")?;
    let position = delta.position();
    ensure!(
        stored_sha256 == payload_sha256,
        "delta content drift at chain_id={}, generation={}, message_seq={}",
        delta.chain_id(),
        position.generation,
        position.message_seq
    );
    ensure!(
        stored_format == DELTA_PAYLOAD_FORMAT_VERSION,
        "delta format drift at chain_id={}, generation={}, message_seq={}: stored {}, writer {}",
        delta.chain_id(),
        position.generation,
        position.message_seq,
        stored_format,
        DELTA_PAYLOAD_FORMAT_VERSION
    );
    Ok(id)
}

async fn upsert_block_times(
    conn: &mut sqlx::PgConnection,
    chain_id: u64,
    block_times: &[BlockTimeObservation],
    source: StreamPosition,
    base_fee_by_block: &BTreeMap<u64, String>,
) -> anyhow::Result<()> {
    let chain_id = db_i64(chain_id, "block time chain_id")?;
    let generation = db_i64(source.generation, "block time source_generation")?;
    let message_seq = db_i64(source.message_seq, "block time source_message_seq")?;
    for observation in block_times {
        sqlx::query(BLOCK_TIME_UPSERT)
            .bind(chain_id)
            .bind(db_i64(observation.block_number, "block time block_number")?)
            .bind(db_i64(observation.timestamp_ms, "block time timestamp_ms")?)
            .bind(observation.block_hash.as_deref())
            .bind(observation.parent_hash.as_deref())
            .bind(
                base_fee_by_block
                    .get(&observation.block_number)
                    .map(String::as_str),
            )
            .bind(generation)
            .bind(message_seq)
            .execute(&mut *conn)
            .await
            .context("failed to upsert block time observation")?;
    }
    Ok(())
}

fn db_i64(value: u64, field: &str) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds PostgreSQL BIGINT"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sqlx::PgPool;

    use super::*;
    use crate::{
        Backend, BlockTimeObservation, CapturedState, CheckpointKind, DeltaBackendCursor,
        DeltaEntry, GapReason, GapRecord, StreamPosition,
    };

    #[test]
    fn embedded_migration_catalog_keeps_initial_migration_and_adds_schema_version_two() {
        let migrations = STATE_HISTORY_MIGRATOR
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
            .collect::<Vec<_>>();

        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version, 20_260_728_000_100);
        assert_eq!(migrations[0].description, "create state history");
        assert_eq!(
            hex::encode(migrations[0].checksum.as_ref()),
            "7e6f16f9c3faa11685000d13138297e01c39d192d239ac149edf24bfb51826300e6535b6e655398461e03171c6308884"
        );
        assert_eq!(migrations[1].version, 20_260_813_000_100);
        assert_eq!(
            migrations[1].description,
            "add delta payload format version"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn insert_delta_is_idempotent_and_rejects_content_drift(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let delta = test_delta(8453, 1, 5); // helper: one native cursor at block 100, one block_time row
        let mut conn = pool.acquire().await?;
        assert!(matches!(
            StateHistoryStore::insert_delta(&mut conn, &delta, &BTreeMap::new()).await?,
            DeltaInsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            StateHistoryStore::insert_delta(&mut conn, &delta, &BTreeMap::new()).await?,
            DeltaInsertOutcome::Duplicate
        ));
        let drifted = test_delta_with_payload(8453, 1, 5, r#"{"different":true}"#);
        assert!(
            StateHistoryStore::insert_delta(&mut conn, &drifted, &BTreeMap::new())
                .await
                .is_err()
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn block_times_supersede_only_by_newer_source(pool: PgPool) -> anyhow::Result<()> {
        // gen 2 writes block 100 with hash A; a late gen-1 write with hash B must NOT overwrite;
        // a gen-3 write with hash C must overwrite.
        let mut conn = pool.acquire().await?;
        let generation_two = test_delta_with_block_hash(8453, 2, 5, "A");
        StateHistoryStore::insert_delta(&mut conn, &generation_two, &BTreeMap::new()).await?;
        assert_eq!(block_time_source(&pool).await?, ("A".to_owned(), 2));

        let late_generation_one = test_delta_with_block_hash(8453, 1, 6, "B");
        StateHistoryStore::insert_delta(&mut conn, &late_generation_one, &BTreeMap::new()).await?;
        assert_eq!(block_time_source(&pool).await?, ("A".to_owned(), 2));

        let generation_three = test_delta_with_block_hash(8453, 3, 1, "C");
        StateHistoryStore::insert_delta(&mut conn, &generation_three, &BTreeMap::new()).await?;
        assert_eq!(block_time_source(&pool).await?, ("C".to_owned(), 3));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn changed_block_hash_clears_missing_replacement_fee(pool: PgPool) -> anyhow::Result<()> {
        let mut connection = pool.acquire().await?;
        let original = test_delta_with_block_hash(8453, 1, 1, "old-hash");
        let base_fees = BTreeMap::from([(100, "100".to_owned())]);
        StateHistoryStore::insert_delta(&mut connection, &original, &base_fees).await?;

        let replacement = test_delta_with_block_hash(8453, 1, 2, "new-hash");
        StateHistoryStore::insert_delta(&mut connection, &replacement, &BTreeMap::new()).await?;

        assert_eq!(block_time_fee(&pool).await?, ("new-hash".to_owned(), None));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn unchanged_block_hash_preserves_existing_fee(pool: PgPool) -> anyhow::Result<()> {
        let mut connection = pool.acquire().await?;
        let original = test_delta_with_block_hash(8453, 1, 1, "same-hash");
        let base_fees = BTreeMap::from([(100, "100".to_owned())]);
        StateHistoryStore::insert_delta(&mut connection, &original, &base_fees).await?;

        let replacement = test_delta_with_block_hash(8453, 1, 2, "same-hash");
        StateHistoryStore::insert_delta(&mut connection, &replacement, &BTreeMap::new()).await?;

        assert_eq!(
            block_time_fee(&pool).await?,
            ("same-hash".to_owned(), Some("100".to_owned()))
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn gap_identity_makes_retries_noops(pool: PgPool) -> anyhow::Result<()> {
        let gap = test_gap(8453, 1, 10, 12, GapReason::QueueOverflow);
        let mut conn = pool.acquire().await?;
        StateHistoryStore::record_gap(&mut conn, &gap).await?;
        StateHistoryStore::record_gap(&mut conn, &gap).await?;
        assert_eq!(count_gaps(&pool).await?, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_token_reference_check_rejects_partial_columns(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let capture = test_capture(8453, 4, 20, CheckpointKind::Interval);
        let mut connection = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
        drop(connection);

        let error = sqlx::query(
            "UPDATE state_history.checkpoints
             SET token_s3_key = $2
             WHERE id = $1",
        )
        .bind(id)
        .bind("state-history/chain=8453/tokens/partial.zst")
        .execute(&pool)
        .await
        .err()
        .context("partial token reference unexpectedly satisfied the CHECK constraint")?;

        assert_eq!(
            error
                .as_database_error()
                .and_then(|database_error| database_error.constraint()),
            Some("checkpoints_token_reference_all_or_none")
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_token_reference_check_rejects_incomplete_status(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let capture = test_capture(8453, 4, 21, CheckpointKind::Interval);
        let mut connection = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
        drop(connection);

        let error = sqlx::query(
            "UPDATE state_history.checkpoints
             SET token_s3_key = $2, token_sha256 = $3, token_count = 1, token_bytes = 64
             WHERE id = $1",
        )
        .bind(id)
        .bind("state-history/chain=8453/tokens/writing.zst")
        .bind("writing-sha256")
        .execute(&pool)
        .await
        .err()
        .context("token reference on a writing row unexpectedly satisfied the CHECK constraint")?;

        assert_eq!(
            error
                .as_database_error()
                .and_then(|database_error| database_error.constraint()),
            Some("checkpoints_token_reference_all_or_none")
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_lifecycle_writing_complete_failed(pool: PgPool) -> anyhow::Result<()> {
        // create → status 'writing'; complete_checkpoint writes s3 fields + block_times in one tx;
        // a second create at same (chain, gen, seq, kind) errors (UNIQUE); fail_checkpoint records error text.
        let capture = test_capture(8453, 4, 20, CheckpointKind::Interval);
        let mut conn = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut conn, &capture).await?;
        assert_eq!(checkpoint_status(&pool, id).await?, "writing");

        let archive = EncodedArchiveInfo {
            sha256: "archive-sha256".to_owned(),
            archive_bytes: 1_024,
            compressed_bytes: 512,
        };
        let block_times = vec![test_block_time("checkpoint")];
        StateHistoryStore::complete_checkpoint(
            &mut conn,
            id,
            "checkpoints/8453/4/20.tar.zst",
            &archive,
            None,
            &block_times,
            StreamPosition {
                generation: 4,
                message_seq: 20,
            },
            8453,
        )
        .await?;
        let completed = sqlx::query_as::<_, (String, String, String, i64, i64)>(
            "SELECT status, s3_key, archive_sha256, archive_bytes, compressed_bytes
             FROM state_history.checkpoints WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            completed,
            (
                "complete".to_owned(),
                "checkpoints/8453/4/20.tar.zst".to_owned(),
                "archive-sha256".to_owned(),
                1_024,
                512,
            )
        );
        assert_eq!(
            block_time_source(&pool).await?,
            ("checkpoint".to_owned(), 4)
        );
        assert!(StateHistoryStore::create_checkpoint(&mut conn, &capture)
            .await
            .is_err());

        let failed_capture = test_capture(8453, 4, 21, CheckpointKind::Interval);
        let failed_id = StateHistoryStore::create_checkpoint(&mut conn, &failed_capture).await?;
        drop(conn);
        StateHistoryStore::fail_checkpoint(&pool, failed_id, "archive upload failed").await?;
        let failed = sqlx::query_as::<_, (String, String)>(
            "SELECT status, error FROM state_history.checkpoints WHERE id = $1",
        )
        .bind(failed_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            failed,
            ("failed".to_owned(), "archive upload failed".to_owned())
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn checkpoint_updates_reject_unknown_ids(pool: PgPool) -> anyhow::Result<()> {
        let archive = EncodedArchiveInfo {
            sha256: "archive-sha256".to_owned(),
            archive_bytes: 1_024,
            compressed_bytes: 512,
        };
        let mut connection = pool.acquire().await?;
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            i64::MAX,
            "checkpoints/missing.tar.zst",
            &archive,
            None,
            &[test_block_time("missing")],
            StreamPosition {
                generation: 4,
                message_seq: 20,
            },
            8453,
        )
        .await
        .expect_err("completing an unknown checkpoint must fail");
        let block_time_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM state_history.block_times")
                .fetch_one(&pool)
                .await?;
        assert_eq!(block_time_count, 0);

        StateHistoryStore::fail_checkpoint(&pool, i64::MAX, "missing")
            .await
            .expect_err("failing an unknown checkpoint must fail");
        Ok(())
    }

    fn test_delta(chain_id: u64, generation: u64, message_seq: u64) -> DeltaEntry {
        test_delta_with_payload(chain_id, generation, message_seq, r#"{"update":true}"#)
    }

    fn test_delta_with_payload(
        chain_id: u64,
        generation: u64,
        message_seq: u64,
        payload: &str,
    ) -> DeltaEntry {
        test_delta_with_payload_and_block_hash(chain_id, generation, message_seq, payload, "A")
    }

    fn test_delta_with_block_hash(
        chain_id: u64,
        generation: u64,
        message_seq: u64,
        block_hash: &str,
    ) -> DeltaEntry {
        test_delta_with_payload_and_block_hash(
            chain_id,
            generation,
            message_seq,
            &format!(r#"{{"generation":{generation},"message_seq":{message_seq}}}"#),
            block_hash,
        )
    }

    #[expect(clippy::expect_used)]
    fn test_delta_with_payload_and_block_hash(
        chain_id: u64,
        generation: u64,
        message_seq: u64,
        payload: &str,
        block_hash: &str,
    ) -> DeltaEntry {
        DeltaEntry::new(
            chain_id,
            StreamPosition {
                generation,
                message_seq,
            },
            1_000,
            payload.to_owned(),
            vec![DeltaBackendCursor {
                backend: Backend::Native,
                block_number: Some(100),
                observed_at_ms: None,
            }],
            vec![test_block_time(block_hash)],
        )
        .expect("test delta should be valid")
    }

    fn test_block_time(block_hash: &str) -> BlockTimeObservation {
        BlockTimeObservation {
            block_number: 100,
            timestamp_ms: 1_000,
            block_hash: Some(block_hash.to_owned()),
            parent_hash: Some("parent".to_owned()),
        }
    }

    fn test_gap(
        chain_id: u64,
        generation: u64,
        from_message_seq: u64,
        to_message_seq: u64,
        reason: GapReason,
    ) -> GapRecord {
        GapRecord {
            chain_id,
            generation,
            from_message_seq,
            to_message_seq,
            reason,
            from_block_number: None,
            to_block_number: None,
            from_observed_at_ms: None,
            to_observed_at_ms: None,
        }
    }

    #[expect(clippy::expect_used)]
    fn test_capture(
        chain_id: u64,
        generation: u64,
        message_seq: u64,
        kind: CheckpointKind,
    ) -> CapturedState {
        CapturedState::new(
            chain_id,
            StreamPosition {
                generation,
                message_seq,
            },
            Some(42),
            kind,
            100,
            None,
            vec![Backend::Native],
            vec![r#"{"state":true}"#.to_owned()],
            vec![test_block_time("capture")],
        )
        .expect("test capture should be valid")
    }

    async fn block_time_source(pool: &PgPool) -> anyhow::Result<(String, i64)> {
        Ok(sqlx::query_as(
            "SELECT block_hash, source_generation
             FROM state_history.block_times
             WHERE chain_id = 8453 AND block_number = 100",
        )
        .fetch_one(pool)
        .await?)
    }

    async fn block_time_fee(pool: &PgPool) -> anyhow::Result<(String, Option<String>)> {
        Ok(sqlx::query_as(
            "SELECT block_hash, base_fee_wei::text
             FROM state_history.block_times
             WHERE chain_id = 8453 AND block_number = 100",
        )
        .fetch_one(pool)
        .await?)
    }

    async fn count_gaps(pool: &PgPool) -> anyhow::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM state_history.gaps")
                .fetch_one(pool)
                .await?,
        )
    }

    async fn checkpoint_status(pool: &PgPool, id: i64) -> anyhow::Result<String> {
        Ok(
            sqlx::query_scalar("SELECT status FROM state_history.checkpoints WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await?,
        )
    }
}
