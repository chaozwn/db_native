use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use duckdb::{
    types::{TimeUnit, Value as DuckValue, ValueRef},
    Connection,
};
use napi::{bindgen_prelude::ReadableStream, Env};
use napi_derive::napi;
use serde_json::{Map, Number, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::{into_napi_error, state_error, DbNativeResult},
    resolve_query_limit, resolve_stream_batch_size,
    save::{SaveResult, StreamingSaveWriter},
    ColumnMeta, QueryBatch, QueryResult,
};

#[napi]
pub struct DuckDbDriver {
    connection: Arc<Mutex<Option<Connection>>>,
}

#[napi]
impl DuckDbDriver {
    #[napi(constructor)]
    pub fn new() -> Self {
        let connection = Connection::open_in_memory()
            .unwrap_or_else(|error| panic!("Failed to open in-memory DuckDB connection: {error}"));
        apply_runtime_settings(&connection).unwrap_or_else(|error| {
            panic!("Failed to initialize DuckDB runtime settings: {error}")
        });

        Self {
            connection: Arc::new(Mutex::new(Some(connection))),
        }
    }

    #[napi]
    pub fn query(&self, sql: String, limit: Option<u32>) -> DbNativeResult<QueryResult> {
        self.query_internal(sql, Some(resolve_query_limit(limit)))
    }

    #[napi(js_name = "queryStream")]
    pub fn query_stream(
        &self,
        env: Env,
        sql: String,
        batch_size: Option<u32>,
        limit: Option<u32>,
    ) -> DbNativeResult<ReadableStream<'static, QueryBatch>> {
        let connection = Arc::clone(&self.connection);
        let batch_size = resolve_stream_batch_size(batch_size);
        let limit = resolve_query_limit(limit);
        let (sender, receiver) = mpsc::channel::<DbNativeResult<QueryBatch>>(2);

        std::thread::spawn(move || {
            if let Err(error) =
                stream_duckdb_query_to_channel(connection, sql, batch_size, limit, sender.clone())
            {
                let _ = sender.blocking_send(Err(error));
            }
        });

        ReadableStream::new(&env, ReceiverStream::new(receiver)).map_err(into_napi_error)
    }

    #[napi(js_name = "configureS3")]
    pub fn configure_s3(
        &self,
        endpoint: String,
        access_key_id: String,
        access_key_secret: String,
    ) -> DbNativeResult<()> {
        let guard = self
            .connection
            .lock()
            .map_err(|_| state_error("DuckDB connection lock is poisoned"))?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| state_error("DuckDB connection is already closed"))?;

        connection
            .execute_batch("INSTALL httpfs; LOAD httpfs;")
            .map_err(into_napi_error)?;

        let (endpoint, use_ssl) = normalize_endpoint(endpoint.trim());
        let sql = format!(
            "SET s3_endpoint = '{endpoint}';\
       SET s3_access_key_id = '{access_key_id}';\
       SET s3_secret_access_key = '{access_key_secret}';\
       SET s3_url_style = 'path';\
       SET s3_use_ssl = {use_ssl};",
            endpoint = escape_sql_literal(endpoint),
            access_key_id = escape_sql_literal(access_key_id.trim()),
            access_key_secret = escape_sql_literal(access_key_secret.trim()),
            use_ssl = if use_ssl { "true" } else { "false" },
        );

        connection
            .execute_batch(sql.as_str())
            .map_err(into_napi_error)
    }

    #[napi]
    pub fn close(&self) -> DbNativeResult<()> {
        let mut guard = self
            .connection
            .lock()
            .map_err(|_| state_error("DuckDB connection lock is poisoned"))?;
        guard.take();
        Ok(())
    }

    #[napi]
    pub fn save(
        &self,
        sql: String,
        file_type: String,
        path: String,
        mode: Option<String>,
    ) -> DbNativeResult<SaveResult> {
        let batch_size = resolve_stream_batch_size(None);
        let mut writer = StreamingSaveWriter::new(&file_type, &path, mode.as_deref())?;
        let guard = self
            .connection
            .lock()
            .map_err(|_| state_error("DuckDB connection lock is poisoned"))?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| state_error("DuckDB connection is already closed"))?;
        let mut statement = connection.prepare(sql.as_str()).map_err(into_napi_error)?;
        let mut rows = statement.query([]).map_err(into_napi_error)?;
        let statement_ref = rows
            .as_ref()
            .ok_or_else(|| state_error("DuckDB statement metadata is unavailable"))?;
        let columns = (0..statement_ref.column_count())
            .map(|index| ColumnMeta {
                name: statement_ref
                    .column_name(index)
                    .map(|name| name.to_string())
                    .unwrap_or_else(|_| format!("column_{index}")),
                data_type: format!("{:?}", statement_ref.column_type(index)),
            })
            .collect::<Vec<_>>();
        let mut result_rows = Vec::with_capacity(batch_size);

        loop {
            let Some(row) = rows.next().map_err(into_napi_error)? else {
                break;
            };
            let mut object = Map::with_capacity(columns.len());

            for (index, column) in columns.iter().enumerate() {
                let value = row.get_ref(index).map_err(into_napi_error)?;
                object.insert(column.name.clone(), value_ref_to_json(value));
            }

            result_rows.push(Value::Object(object));

            if result_rows.len() >= batch_size {
                writer.write_batch(&columns, &result_rows)?;
                result_rows.clear();
            }
        }

        writer.write_batch(&columns, &result_rows)?;
        writer.finish()
    }
}

fn stream_duckdb_query_to_channel(
    connection: Arc<Mutex<Option<Connection>>>,
    sql: String,
    batch_size: usize,
    limit: usize,
    sender: mpsc::Sender<DbNativeResult<QueryBatch>>,
) -> DbNativeResult<()> {
    let guard = connection
        .lock()
        .map_err(|_| state_error("DuckDB connection lock is poisoned"))?;
    let connection = guard
        .as_ref()
        .ok_or_else(|| state_error("DuckDB connection is already closed"))?;
    let mut statement = connection.prepare(sql.as_str()).map_err(into_napi_error)?;
    let mut rows = statement.query([]).map_err(into_napi_error)?;
    let statement_ref = rows
        .as_ref()
        .ok_or_else(|| state_error("DuckDB statement metadata is unavailable"))?;
    let columns = (0..statement_ref.column_count())
        .map(|index| ColumnMeta {
            name: statement_ref
                .column_name(index)
                .map(|name| name.to_string())
                .unwrap_or_else(|_| format!("column_{index}")),
            data_type: format!("{:?}", statement_ref.column_type(index)),
        })
        .collect::<Vec<_>>();
    let mut result_rows = Vec::with_capacity(batch_size);
    let mut emitted = false;
    let mut sent_rows = 0usize;

    while sent_rows < limit {
        let Some(row) = rows.next().map_err(into_napi_error)? else {
            break;
        };
        let mut object = Map::with_capacity(columns.len());

        for (index, column) in columns.iter().enumerate() {
            let value = row.get_ref(index).map_err(into_napi_error)?;
            object.insert(column.name.clone(), value_ref_to_json(value));
        }

        result_rows.push(Value::Object(object));
        sent_rows += 1;

        if result_rows.len() >= batch_size {
            let batch = QueryBatch {
                columns: columns.clone(),
                rows: std::mem::take(&mut result_rows),
            };
            emitted = true;
            if sender.blocking_send(Ok(batch)).is_err() {
                return Ok(());
            }
        }
    }

    if !result_rows.is_empty() || (!emitted && !columns.is_empty()) {
        let batch = QueryBatch {
            columns,
            rows: result_rows,
        };
        let _ = sender.blocking_send(Ok(batch));
    }

    Ok(())
}

impl DuckDbDriver {
    fn query_internal(&self, sql: String, limit: Option<usize>) -> DbNativeResult<QueryResult> {
        let started_at = Instant::now();
        let guard = self
            .connection
            .lock()
            .map_err(|_| state_error("DuckDB connection lock is poisoned"))?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| state_error("DuckDB connection is already closed"))?;
        let limit = limit.unwrap_or(usize::MAX);

        let mut statement = connection.prepare(sql.as_str()).map_err(into_napi_error)?;
        let mut rows = statement.query([]).map_err(into_napi_error)?;
        let statement_ref = rows
            .as_ref()
            .ok_or_else(|| state_error("DuckDB statement metadata is unavailable"))?;
        let columns = (0..statement_ref.column_count())
            .map(|index| ColumnMeta {
                name: statement_ref
                    .column_name(index)
                    .map(|name| name.to_string())
                    .unwrap_or_else(|_| format!("column_{index}")),
                data_type: format!("{:?}", statement_ref.column_type(index)),
            })
            .collect::<Vec<_>>();
        let mut result_rows = Vec::new();

        while result_rows.len() < limit {
            let Some(row) = rows.next().map_err(into_napi_error)? else {
                break;
            };
            let mut object = Map::with_capacity(columns.len());

            for (index, column) in columns.iter().enumerate() {
                let value = row.get_ref(index).map_err(into_napi_error)?;
                object.insert(column.name.clone(), value_ref_to_json(value));
            }

            result_rows.push(Value::Object(object));
        }

        Ok(QueryResult {
            columns,
            rows: result_rows,
            execution_time_ms: started_at.elapsed().as_millis() as i64,
        })
    }
}

fn apply_runtime_settings(connection: &Connection) -> DbNativeResult<()> {
    if let Ok(memory_limit) = std::env::var("DUCKDB_MEMORY_LIMIT") {
        let memory_limit = memory_limit.trim();
        if !memory_limit.is_empty() {
            let sql = format!("SET memory_limit = '{}';", escape_sql_literal(memory_limit),);
            connection
                .execute_batch(sql.as_str())
                .map_err(into_napi_error)?;
        }
    }

    if let Ok(threads) = std::env::var("DUCKDB_THREADS") {
        let threads = threads.trim();
        if !threads.is_empty() {
            let threads = threads
                .parse::<u64>()
                .map_err(|error| state_error(format!("Invalid DUCKDB_THREADS value: {error}")))?;
            let sql = format!("SET threads = {threads};");
            connection
                .execute_batch(sql.as_str())
                .map_err(into_napi_error)?;
        }
    }

    Ok(())
}

fn normalize_endpoint(endpoint: &str) -> (&str, bool) {
    if let Some(value) = endpoint.strip_prefix("http://") {
        return (value, false);
    }
    if let Some(value) = endpoint.strip_prefix("https://") {
        return (value, true);
    }
    (endpoint, true)
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn value_ref_to_json(value: ValueRef<'_>) -> Value {
    duck_value_to_json(value.to_owned())
}

fn duck_value_to_json(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(value) => Value::Bool(value),
        DuckValue::TinyInt(value) => Value::Number(Number::from(value)),
        DuckValue::SmallInt(value) => Value::Number(Number::from(value)),
        DuckValue::Int(value) => Value::Number(Number::from(value)),
        DuckValue::BigInt(value) => Value::Number(Number::from(value)),
        DuckValue::HugeInt(value) => Value::String(value.to_string()),
        DuckValue::UTinyInt(value) => Value::Number(Number::from(value)),
        DuckValue::USmallInt(value) => Value::Number(Number::from(value)),
        DuckValue::UInt(value) => Value::Number(Number::from(value)),
        DuckValue::UBigInt(value) => Value::Number(Number::from(value)),
        DuckValue::Float(value) => f64_to_json(value as f64),
        DuckValue::Double(value) => f64_to_json(value),
        DuckValue::Decimal(value) => Value::String(value.to_string()),
        DuckValue::Timestamp(unit, value) => Value::String(format_timestamp(unit, value)),
        DuckValue::Text(value) => Value::String(value),
        DuckValue::Blob(value) => Value::String(format!("0x{}", encode_hex(&value))),
        DuckValue::Date32(value) => Value::String(format!("date32:{value}")),
        DuckValue::Time64(unit, value) => Value::String(format_time64(unit, value)),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => Value::Object(
            [
                ("months".to_string(), Value::Number(Number::from(months))),
                ("days".to_string(), Value::Number(Number::from(days))),
                ("nanos".to_string(), Value::Number(Number::from(nanos))),
            ]
            .into_iter()
            .collect(),
        ),
        DuckValue::List(values) | DuckValue::Array(values) => {
            Value::Array(values.into_iter().map(duck_value_to_json).collect())
        }
        DuckValue::Enum(value) => Value::String(value),
        DuckValue::Struct(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), duck_value_to_json(value.clone())))
                .collect(),
        ),
        DuckValue::Map(values) => Value::Array(
            values
                .iter()
                .map(|(key, value)| {
                    Value::Object(
                        [
                            ("key".to_string(), duck_value_to_json(key.clone())),
                            ("value".to_string(), duck_value_to_json(value.clone())),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect(),
        ),
        DuckValue::Union(value) => duck_value_to_json(*value),
    }
}

fn format_timestamp(unit: TimeUnit, value: i64) -> String {
    format!("timestamp({unit:?}, {value})")
}

fn format_time64(unit: TimeUnit, value: i64) -> String {
    format!("time64({unit:?}, {value})")
}

fn f64_to_json(value: f64) -> Value {
    match Number::from_f64(value) {
        Some(number) => Value::Number(number),
        None => Value::String(value.to_string()),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duckdb_streaming_query_batches_and_honors_limit() {
        let connection = Connection::open_in_memory().expect("duckdb in-memory connection");
        apply_runtime_settings(&connection).expect("runtime settings should apply");
        let connection = Arc::new(Mutex::new(Some(connection)));
        let (sender, mut receiver) = mpsc::channel(4);

        stream_duckdb_query_to_channel(
            connection,
            "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')) AS t(id, name) ORDER BY id"
                .to_string(),
            2,
            3,
            sender,
        )
        .expect("streaming query should succeed");

        let batches = std::iter::from_fn(|| receiver.blocking_recv())
            .map(|batch| batch.expect("stream batch should be successful"))
            .collect::<Vec<_>>();

        assert_eq!(batches.len(), 2, "expected two emitted batches");
        assert_eq!(batches[0].rows.len(), 2, "first batch should be full");
        assert_eq!(batches[1].rows.len(), 1, "second batch should honor limit");

        let first_row = batches[0].rows[0]
            .as_object()
            .expect("first batch row should be an object");
        let last_row = batches[1].rows[0]
            .as_object()
            .expect("last batch row should be an object");

        assert_eq!(
            first_row.get("id").and_then(|value| value.as_i64()),
            Some(1)
        );
        assert_eq!(
            first_row.get("name").and_then(|value| value.as_str()),
            Some("alice")
        );
        assert_eq!(last_row.get("id").and_then(|value| value.as_i64()), Some(3));
        assert_eq!(
            last_row.get("name").and_then(|value| value.as_str()),
            Some("carol")
        );
    }
}
