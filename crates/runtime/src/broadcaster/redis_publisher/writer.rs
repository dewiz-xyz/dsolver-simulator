use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use serde_json::Value;

use simulator_core::broadcaster::{BroadcasterRedisSnapshotPointer, BroadcasterRedisStreamEntry};

pub trait RedisStreamWriter: Send + Sync {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>>;

    fn set_snapshot_pointer<'a>(
        &'a self,
        snapshot_key: &'a str,
        pointer: &'a BroadcasterRedisSnapshotPointer,
    ) -> BoxFuture<'a, Result<()>>;
}

pub struct TokioRedisStreamWriter {
    connection: redis::aio::ConnectionManager,
}

impl TokioRedisStreamWriter {
    pub async fn connect(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("failed to create Redis client from BROADCASTER_REDIS_URL")?;
        let connection = client
            .get_connection_manager()
            .await
            .context("failed to connect to broadcaster Redis")?;
        Ok(Self { connection })
    }
}

impl RedisStreamWriter for TokioRedisStreamWriter {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let entry_id = redis_entry_id(entry)?;
            let fields = redis_entry_fields(entry)?;
            let mut command = redis::cmd("XADD");
            command.arg(stream_key).arg(&entry_id);
            for (field, value) in fields {
                command.arg(field).arg(value);
            }
            let mut connection = self.connection.clone();
            match command.query_async::<String>(&mut connection).await {
                Ok(entry_id) => Ok(entry_id),
                Err(error) => {
                    if redis_stream_entry_exists(&mut connection, stream_key, &entry_id).await? {
                        Ok(entry_id)
                    } else {
                        Err(error).context("Redis XADD failed")
                    }
                }
            }
        })
    }

    fn set_snapshot_pointer<'a>(
        &'a self,
        snapshot_key: &'a str,
        pointer: &'a BroadcasterRedisSnapshotPointer,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let pointer_json = serde_json::to_string(pointer)
                .context("failed to serialize Redis snapshot pointer")?;
            let mut connection = self.connection.clone();
            redis::cmd("SET")
                .arg(snapshot_key)
                .arg(pointer_json)
                .query_async(&mut connection)
                .await
                .context("Redis snapshot pointer SET failed")
        })
    }
}

pub(super) fn redis_entry_id(entry: &BroadcasterRedisStreamEntry) -> Result<String> {
    let generation = entry
        .stream_id
        .rsplit_once('-')
        .map(|(_, generation)| generation)
        .ok_or_else(|| anyhow!("Redis broadcaster stream_id is missing generation"))?;
    let generation = generation.parse::<u64>().with_context(|| {
        format!("Redis broadcaster stream_id has invalid generation: {generation}")
    })?;
    Ok(format!("{generation}-{}", entry.message_seq))
}

async fn redis_stream_entry_exists(
    connection: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    entry_id: &str,
) -> Result<bool> {
    let entries = redis::cmd("XRANGE")
        .arg(stream_key)
        .arg(entry_id)
        .arg(entry_id)
        .arg("COUNT")
        .arg(1)
        .query_async::<Vec<redis::Value>>(connection)
        .await
        .context("Redis XRANGE failed while checking ambiguous XADD result")?;
    Ok(!entries.is_empty())
}

fn redis_entry_fields(entry: &BroadcasterRedisStreamEntry) -> Result<Vec<(String, String)>> {
    let Value::Object(fields) =
        serde_json::to_value(entry).context("failed to serialize Redis stream entry")?
    else {
        return Err(anyhow!("Redis stream entry did not serialize as an object"));
    };

    fields
        .into_iter()
        .map(|(field, value)| {
            let value = match value {
                Value::String(value) => value,
                Value::Number(value) => value.to_string(),
                Value::Bool(value) => value.to_string(),
                Value::Null => String::new(),
                Value::Array(_) | Value::Object(_) => serde_json::to_string(&value)
                    .context("failed to serialize nested Redis stream field")?,
            };
            Ok((field, value))
        })
        .collect()
}
