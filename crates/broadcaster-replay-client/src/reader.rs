use std::time::Duration;

use redis::streams::{StreamId, StreamInfoStreamReply, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use simulator_core::broadcaster::BroadcasterRedisStreamEntry;

use crate::error::{BroadcasterReplayClientError, Result};

const REDIS_CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);
const REDIS_INSPECTION_TIMEOUT: Duration = Duration::from_secs(3);
const REDIS_BLOCKING_READ_TIMEOUT_MARGIN: Duration = Duration::from_secs(2);

pub(crate) struct TokioRedisStreamReader {
    blocking_read_connection: redis::aio::ConnectionManager,
    inspection_connection: redis::aio::ConnectionManager,
}

impl TokioRedisStreamReader {
    pub(crate) async fn connect(redis_url: &str, block_ms: u64) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|error| BroadcasterReplayClientError::redis_connect(error.to_string()))?;
        let blocking_read_connection = redis::aio::ConnectionManager::new_with_config(
            client.clone(),
            connection_manager_config(blocking_read_timeout(block_ms)),
        )
        .await
        .map_err(|error| BroadcasterReplayClientError::redis_connect(error.to_string()))?;
        let inspection_connection = redis::aio::ConnectionManager::new_with_config(
            client,
            connection_manager_config(REDIS_INSPECTION_TIMEOUT),
        )
        .await
        .map_err(|error| BroadcasterReplayClientError::redis_connect(error.to_string()))?;
        Ok(Self {
            blocking_read_connection,
            inspection_connection,
        })
    }

    pub(crate) async fn read_after(
        &self,
        stream_key: &str,
        after_entry_id: &str,
        block_ms: u64,
        read_count: u64,
    ) -> Result<Vec<RedisStreamMessage>> {
        let options = StreamReadOptions::default()
            .block(block_ms as usize)
            .count(read_count as usize);
        let mut connection = self.blocking_read_connection.clone();
        let reply = connection
            .xread_options(&[stream_key], &[after_entry_id], &options)
            .await;
        redis_xread_messages(stream_key, reply)
    }

    pub(crate) async fn stream_info(&self, stream_key: &str) -> Result<Option<RedisStreamInfo>> {
        let mut connection = self.inspection_connection.clone();
        let reply = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream_key)
            .query_async::<StreamInfoStreamReply>(&mut connection)
            .await;
        redis_stream_info(reply)
    }
}

fn connection_manager_config(response_timeout: Duration) -> redis::aio::ConnectionManagerConfig {
    redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Some(REDIS_CONNECTION_TIMEOUT))
        .set_response_timeout(Some(response_timeout))
}

pub(crate) fn blocking_read_timeout(block_ms: u64) -> Duration {
    Duration::from_millis(block_ms).saturating_add(REDIS_BLOCKING_READ_TIMEOUT_MARGIN)
}

#[derive(Debug, Clone)]
pub(crate) struct RedisStreamMessage {
    pub(crate) entry_id: String,
    pub(crate) entry: BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedisStreamInfo {
    pub(crate) last_generated_entry_id: String,
    pub(crate) first_entry_id: Option<String>,
    pub(crate) last_entry_id: Option<String>,
}

fn redis_stream_messages(
    expected_stream_key: &str,
    reply: StreamReadReply,
) -> Result<Vec<RedisStreamMessage>> {
    let mut messages = Vec::new();
    for key in reply.keys {
        if key.key != expected_stream_key {
            return Err(BroadcasterReplayClientError::redis_decode(format!(
                "Redis XREAD returned stream key {}, expected {}",
                key.key, expected_stream_key
            )));
        }
        for stream_id in key.ids {
            messages.push(redis_stream_message(stream_id)?);
        }
    }
    Ok(messages)
}

pub(crate) fn redis_xread_messages(
    expected_stream_key: &str,
    reply: std::result::Result<Option<StreamReadReply>, redis::RedisError>,
) -> Result<Vec<RedisStreamMessage>> {
    match reply {
        Ok(Some(reply)) => redis_stream_messages(expected_stream_key, reply),
        Ok(None) => Ok(Vec::new()),
        Err(error) if redis_transport_failure(&error) => Err(
            BroadcasterReplayClientError::redis_read_transport(error.to_string()),
        ),
        Err(error) => Err(BroadcasterReplayClientError::redis_read(error.to_string())),
    }
}

fn redis_stream_info(
    reply: std::result::Result<StreamInfoStreamReply, redis::RedisError>,
) -> Result<Option<RedisStreamInfo>> {
    match reply {
        Ok(reply) => redis_stream_info_from_reply(reply).map(Some),
        Err(error) if redis_stream_missing_key(&error) => Ok(None),
        Err(error) if redis_transport_failure(&error) => Err(
            BroadcasterReplayClientError::redis_inspect_transport(error.to_string()),
        ),
        Err(error) => Err(BroadcasterReplayClientError::redis_inspect(
            error.to_string(),
        )),
    }
}

fn redis_stream_info_from_reply(reply: StreamInfoStreamReply) -> Result<RedisStreamInfo> {
    let first_entry_id = redis_stream_info_entry_id(reply.length, reply.first_entry.id)?;
    let last_entry_id = redis_stream_info_entry_id(reply.length, reply.last_entry.id)?;
    Ok(RedisStreamInfo {
        last_generated_entry_id: reply.last_generated_id,
        first_entry_id,
        last_entry_id,
    })
}

fn redis_stream_info_entry_id(length: usize, entry_id: String) -> Result<Option<String>> {
    match (length, entry_id.is_empty()) {
        (0, _) => Ok(None),
        (_, false) => Ok(Some(entry_id)),
        (_, true) => Err(BroadcasterReplayClientError::redis_inspect(
            "Redis XINFO STREAM omitted a retained entry id",
        )),
    }
}

fn redis_stream_missing_key(error: &redis::RedisError) -> bool {
    error
        .detail()
        .is_some_and(|detail| detail.contains("no such key"))
}

fn redis_transport_failure(error: &redis::RedisError) -> bool {
    if matches!(error.kind(), redis::ErrorKind::AuthenticationFailed) {
        return false;
    }

    !matches!(error.retry_method(), redis::RetryMethod::NoRetry)
}

fn redis_stream_message(stream_id: StreamId) -> Result<RedisStreamMessage> {
    let entry_id = stream_id.id;
    let mut value = serde_json::Map::new();
    for (field, redis_value) in stream_id.map {
        let field_value: String = redis::from_redis_value(redis_value).map_err(|error| {
            BroadcasterReplayClientError::redis_decode(format!(
                "Redis stream field {field} is not a string: {error}"
            ))
        })?;
        value.insert(field, serde_json::Value::String(field_value));
    }
    let entry = serde_json::from_value(serde_json::Value::Object(value))
        .map_err(|error| BroadcasterReplayClientError::redis_decode(error.to_string()))?;
    Ok(RedisStreamMessage { entry_id, entry })
}
