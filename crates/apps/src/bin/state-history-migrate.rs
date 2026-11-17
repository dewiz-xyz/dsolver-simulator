use std::env;

use anyhow::{ensure, Context};
use serde_json::json;
use state_history::{
    StateHistoryStore, STATE_HISTORY_MIGRATOR, STATE_HISTORY_OBSERVER_ROLE,
    STATE_HISTORY_SCHEMA_VERSION, STATE_HISTORY_WRITER_ROLE,
};

const DATABASE_URL_ENV: &str = "STATE_HISTORY_MIGRATION_DATABASE_URL";
const WRITER_PASSWORD_ENV: &str = "STATE_HISTORY_WRITER_PASSWORD";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = required_secret(DATABASE_URL_ENV)?;
    let writer_password = required_secret(WRITER_PASSWORD_ENV)?;
    let store = StateHistoryStore::connect(&database_url).await?;

    store.run_migrations().await?;
    store.validate_migration_catalog().await?;
    store.validate_schema().await?;
    store.provision_runtime_roles(&writer_password).await?;

    let migration_count = STATE_HISTORY_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .count();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "state_history_migration_complete",
            "migrationCatalogEntries": migration_count,
            "schemaVersion": STATE_HISTORY_SCHEMA_VERSION,
            "writerRole": STATE_HISTORY_WRITER_ROLE,
            "observerRole": STATE_HISTORY_OBSERVER_ROLE,
        }))?
    );
    Ok(())
}

fn required_secret(name: &str) -> anyhow::Result<String> {
    let value = env::var(name)
        .with_context(|| format!("required environment variable {name} is missing"))?;
    ensure!(
        !value.trim().is_empty(),
        "required environment variable {name} is empty"
    );
    Ok(value)
}
