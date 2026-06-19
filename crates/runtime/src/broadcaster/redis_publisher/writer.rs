use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use redis::streams::{StreamInfoStreamReply, StreamRangeReply};
use serde_json::Value;

use simulator_core::broadcaster::BroadcasterRedisStreamEntry;

pub trait RedisStreamWriter: Send + Sync {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        maxlen: Option<u64>,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>>;
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

    pub async fn next_generation(&self, stream_key: &str) -> Result<u64> {
        let mut connection = self.connection.clone();
        let reply = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream_key)
            .query_async::<StreamInfoStreamReply>(&mut connection)
            .await;
        next_generation_from_xinfo_reply(reply)
    }
}

impl RedisStreamWriter for TokioRedisStreamWriter {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        maxlen: Option<u64>,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let entry_id = redis_entry_id(entry)?;
            let fields = redis_entry_fields(entry)?;
            let command = redis_xadd_command_from_fields(stream_key, maxlen, &entry_id, &fields);
            let mut connection = self.connection.clone();
            match command.query_async::<String>(&mut connection).await {
                Ok(entry_id) => Ok(entry_id),
                Err(error) => {
                    if redis_stream_entry_matches(&mut connection, stream_key, &entry_id, &fields)
                        .await?
                    {
                        Ok(entry_id)
                    } else {
                        Err(error).context("Redis XADD failed")
                    }
                }
            }
        })
    }
}

fn next_generation_from_xinfo_reply(
    reply: std::result::Result<StreamInfoStreamReply, redis::RedisError>,
) -> Result<u64> {
    match reply {
        Ok(reply) => next_generation_after_entry_id(&reply.last_generated_id),
        Err(error) if redis_stream_missing_key(&error) => Ok(1),
        Err(error) => Err(anyhow!(error).context("Redis XINFO STREAM failed")),
    }
}

pub(super) fn next_generation_after_entry_id(entry_id: &str) -> Result<u64> {
    let generation = entry_id
        .split_once('-')
        .map(|(generation, _)| generation)
        .ok_or_else(|| anyhow!("Redis stream last-generated-id is invalid: {entry_id}"))?
        .parse::<u64>()
        .with_context(|| format!("Redis stream last-generated-id is invalid: {entry_id}"))?;
    generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("Redis broadcaster generation overflow"))
}

fn redis_stream_missing_key(error: &redis::RedisError) -> bool {
    error
        .detail()
        .is_some_and(|detail| detail.contains("no such key"))
}

#[cfg(test)]
pub(super) fn redis_xadd_command(
    stream_key: &str,
    maxlen: Option<u64>,
    entry: &BroadcasterRedisStreamEntry,
) -> Result<redis::Cmd> {
    let entry_id = redis_entry_id(entry)?;
    let fields = redis_entry_fields(entry)?;
    Ok(redis_xadd_command_from_fields(
        stream_key, maxlen, &entry_id, &fields,
    ))
}

fn redis_xadd_command_from_fields(
    stream_key: &str,
    maxlen: Option<u64>,
    entry_id: &str,
    fields: &[(String, String)],
) -> redis::Cmd {
    let mut command = redis::cmd("XADD");
    command.arg(stream_key);
    if let Some(maxlen) = maxlen {
        command.arg("MAXLEN").arg("~").arg(maxlen);
    }
    command.arg(entry_id);
    for (field, value) in fields {
        command.arg(field).arg(value);
    }
    command
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

async fn redis_stream_entry_matches(
    connection: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    entry_id: &str,
    expected_fields: &[(String, String)],
) -> Result<bool> {
    let reply = redis::cmd("XRANGE")
        .arg(stream_key)
        .arg(entry_id)
        .arg(entry_id)
        .arg("COUNT")
        .arg(1)
        .query_async::<StreamRangeReply>(connection)
        .await
        .context("Redis XRANGE failed while checking XADD result")?;
    redis_stream_entry_matches_reply(&reply, entry_id, expected_fields)
}

pub(super) fn redis_stream_entry_matches_reply(
    reply: &StreamRangeReply,
    entry_id: &str,
    expected_fields: &[(String, String)],
) -> Result<bool> {
    let Some(existing) = reply.ids.first() else {
        return Ok(false);
    };
    if reply.ids.len() != 1
        || existing.id != entry_id
        || existing.map.len() != expected_fields.len()
    {
        return Ok(false);
    }

    for (field, expected_value) in expected_fields {
        let Some(value) = existing.map.get(field) else {
            return Ok(false);
        };
        let actual_value = redis::from_redis_value::<String>(value.clone())
            .with_context(|| format!("Redis XRANGE returned invalid value for field {field}"))?;
        if actual_value != *expected_value {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn redis_entry_fields(
    entry: &BroadcasterRedisStreamEntry,
) -> Result<Vec<(String, String)>> {
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
