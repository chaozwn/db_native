use std::{sync::Mutex, time::Instant};

use futures_util::TryStreamExt;
use napi::{bindgen_prelude::ReadableStream, Env};
use napi_derive::napi;
use serde_json::{Map, Number, Value};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow},
    types::{
        chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc},
        BigDecimal, Decimal, JsonValue, Uuid,
    },
    Column, Executor, Row, TypeInfo, ValueRef,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::{into_napi_error, state_error, DbNativeResult},
    resolve_query_limit, resolve_stream_batch_size,
    save::{spawn_streaming_save_worker, SaveResult},
    ColumnMeta, QueryBatch, QueryResult,
};

struct MySqlProtocolDriver {
    pool: Mutex<Option<MySqlPool>>,
    driver_label: &'static str,
}

impl MySqlProtocolDriver {
    async fn connect(
        driver_label: &'static str,
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    ) -> DbNativeResult<Self> {
        let options = MySqlConnectOptions::new()
            .host(&host)
            .port(port)
            .database(&database)
            .username(&username)
            .password(&password)
            .charset("utf8mb4");

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(into_napi_error)?;

        Ok(Self {
            pool: Mutex::new(Some(pool)),
            driver_label,
        })
    }

    async fn query(&self, sql: String, limit: Option<u32>) -> DbNativeResult<QueryResult> {
        self.query_internal(sql, Some(resolve_query_limit(limit)))
            .await
    }

    fn query_stream(
        &self,
        env: Env,
        sql: String,
        batch_size: Option<u32>,
        limit: Option<u32>,
    ) -> DbNativeResult<ReadableStream<'static, QueryBatch>> {
        let pool = self.pool()?;
        let batch_size = resolve_stream_batch_size(batch_size);
        let limit = resolve_query_limit(limit);
        let (sender, receiver) = mpsc::channel::<DbNativeResult<QueryBatch>>(2);

        tokio::spawn(async move {
            if let Err(error) =
                stream_mysql_query_to_channel(pool, sql, batch_size, limit, sender.clone()).await
            {
                let _ = sender.send(Err(error)).await;
            }
        });

        ReadableStream::new(&env, ReceiverStream::new(receiver)).map_err(into_napi_error)
    }

    async fn close(&self) -> DbNativeResult<()> {
        let pool = {
            let mut guard = self
                .pool
                .lock()
                .map_err(|_| state_error("MySQL connection lock is poisoned"))?;
            guard.take()
        };

        if let Some(pool) = pool {
            pool.close().await;
        }
        Ok(())
    }

    async fn save(
        &self,
        sql: String,
        file_type: String,
        path: String,
        mode: Option<String>,
    ) -> DbNativeResult<SaveResult> {
        let pool = self.pool()?;
        let batch_size = resolve_stream_batch_size(None);
        let (sender, result_receiver) = spawn_streaming_save_worker(file_type, path, mode);
        if let Err(error) =
            stream_mysql_query_to_channel(pool, sql, batch_size, usize::MAX, sender.clone()).await
        {
            let _ = sender.send(Err(error)).await;
        }
        drop(sender);
        result_receiver
            .await
            .map_err(|_| state_error("MySQL streaming save worker was cancelled"))?
    }

    fn pool(&self) -> DbNativeResult<MySqlPool> {
        let guard = self
            .pool
            .lock()
            .map_err(|_| state_error("MySQL connection lock is poisoned"))?;
        guard.as_ref().cloned().ok_or_else(|| {
            state_error(format!(
                "{} connection is already closed",
                self.driver_label
            ))
        })
    }

    async fn query_internal(
        &self,
        sql: String,
        limit: Option<usize>,
    ) -> DbNativeResult<QueryResult> {
        let pool = self.pool()?;
        let started_at = Instant::now();
        let limit = limit.unwrap_or(usize::MAX);

        let describe = pool.describe(sql.as_str()).await.ok();
        let mut rows = Vec::new();
        let mut stream = sqlx::query(sql.as_str()).fetch(&pool);

        while rows.len() < limit {
            match stream.try_next().await.map_err(into_napi_error)? {
                Some(row) => rows.push(row),
                None => break,
            }
        }

        let columns = describe
            .map(|metadata| {
                metadata
                    .columns()
                    .iter()
                    .map(|column| ColumnMeta {
                        name: column.name().to_string(),
                        data_type: column.type_info().name().to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| columns_from_rows(&rows));

        let rows = rows.iter().map(row_to_json).collect::<Vec<_>>();

        Ok(QueryResult {
            columns,
            rows,
            execution_time_ms: started_at.elapsed().as_millis() as i64,
        })
    }
}

async fn stream_mysql_query_to_channel(
    pool: MySqlPool,
    sql: String,
    batch_size: usize,
    limit: usize,
    sender: mpsc::Sender<DbNativeResult<QueryBatch>>,
) -> DbNativeResult<()> {
    let describe = pool.describe(sql.as_str()).await.ok();
    let mut columns = describe.map(columns_from_describe);
    let mut stream = sqlx::query(sql.as_str()).fetch(&pool);
    let mut rows = Vec::with_capacity(batch_size);
    let mut emitted = false;
    let mut sent_rows = 0usize;

    while sent_rows < limit {
        let Some(row) = stream.try_next().await.map_err(into_napi_error)? else {
            break;
        };

        if columns.is_none() {
            columns = Some(columns_from_rows(std::slice::from_ref(&row)));
        }

        rows.push(row_to_json(&row));
        sent_rows += 1;

        if rows.len() >= batch_size {
            let batch = QueryBatch {
                columns: columns.clone().unwrap_or_default(),
                rows: std::mem::take(&mut rows),
            };
            emitted = true;
            if sender.send(Ok(batch)).await.is_err() {
                return Ok(());
            }
        }
    }

    let columns = columns.unwrap_or_default();
    if !rows.is_empty() || (!emitted && !columns.is_empty()) {
        let batch = QueryBatch { columns, rows };
        let _ = sender.send(Ok(batch)).await;
    }

    Ok(())
}

fn columns_from_describe(metadata: sqlx::Describe<sqlx::MySql>) -> Vec<ColumnMeta> {
    metadata
        .columns()
        .iter()
        .map(|column| ColumnMeta {
            name: column.name().to_string(),
            data_type: column.type_info().name().to_string(),
        })
        .collect()
}

macro_rules! define_mysql_protocol_driver {
    ($name:ident, $label:literal) => {
        #[napi]
        pub struct $name {
            inner: MySqlProtocolDriver,
        }

        #[napi]
        impl $name {
            #[napi]
            pub async fn connect(
                host: String,
                port: u16,
                database: String,
                username: String,
                password: String,
            ) -> DbNativeResult<$name> {
                Ok(Self {
                    inner: MySqlProtocolDriver::connect(
                        $label, host, port, database, username, password,
                    )
                    .await?,
                })
            }

            #[napi]
            pub async fn query(
                &self,
                sql: String,
                limit: Option<u32>,
            ) -> DbNativeResult<QueryResult> {
                self.inner.query(sql, limit).await
            }

            #[napi(js_name = "queryStream")]
            pub fn query_stream(
                &self,
                env: Env,
                sql: String,
                batch_size: Option<u32>,
                limit: Option<u32>,
            ) -> DbNativeResult<ReadableStream<'static, QueryBatch>> {
                self.inner.query_stream(env, sql, batch_size, limit)
            }

            #[napi]
            pub async fn close(&self) -> DbNativeResult<()> {
                self.inner.close().await
            }

            #[napi]
            pub async fn save(
                &self,
                sql: String,
                file_type: String,
                path: String,
                mode: Option<String>,
            ) -> DbNativeResult<SaveResult> {
                self.inner.save(sql, file_type, path, mode).await
            }
        }
    };
}

define_mysql_protocol_driver!(MysqlDriver, "MySQL");
define_mysql_protocol_driver!(DorisDriver, "Doris");
define_mysql_protocol_driver!(StarRocksDriver, "StarRocks");

fn columns_from_rows(rows: &[MySqlRow]) -> Vec<ColumnMeta> {
    rows.first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|column| ColumnMeta {
                    name: column.name().to_string(),
                    data_type: column.type_info().name().to_string(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn row_to_json(row: &MySqlRow) -> Value {
    let mut object = Map::with_capacity(row.len());

    for (index, column) in row.columns().iter().enumerate() {
        object.insert(column.name().to_string(), value_from_row(row, index));
    }

    Value::Object(object)
}

fn value_from_row(row: &MySqlRow, index: usize) -> Value {
    if let Ok(raw) = row.try_get_raw(index) {
        if raw.is_null() {
            return Value::Null;
        }
    }

    if let Ok(value) = row.try_get::<i8, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<i16, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<i32, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<u8, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<u16, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<u32, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<u64, _>(index) {
        return Value::Number(Number::from(value));
    }
    if let Ok(value) = row.try_get::<bool, _>(index) {
        return Value::Bool(value);
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return f64_to_json(value);
    }
    if let Ok(value) = row.try_get::<Decimal, _>(index) {
        return Value::String(value.to_string());
    }
    if let Ok(value) = row.try_get::<BigDecimal, _>(index) {
        return Value::String(value.to_string());
    }
    if let Ok(value) = row.try_get::<JsonValue, _>(index) {
        return value;
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Value::String(value);
    }
    if let Ok(value) = row.try_get::<Vec<u8>, _>(index) {
        return Value::String(format!("0x{}", encode_hex(&value)));
    }
    if let Ok(value) = row.try_get::<NaiveDate, _>(index) {
        return Value::String(value.to_string());
    }
    if let Ok(value) = row.try_get::<NaiveTime, _>(index) {
        return Value::String(value.to_string());
    }
    if let Ok(value) = row.try_get::<NaiveDateTime, _>(index) {
        return Value::String(value.to_string());
    }
    if let Ok(value) = row.try_get::<DateTime<Utc>, _>(index) {
        return Value::String(value.to_rfc3339());
    }
    if let Ok(value) = row.try_get::<Uuid, _>(index) {
        return Value::String(value.to_string());
    }

    Value::String(format!(
        "<unsupported:{}>",
        row.columns()[index].type_info().name()
    ))
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
