use std::{sync::Mutex, time::Instant};

use clickhouse::Client;
use napi::{bindgen_prelude::ReadableStream, Env};
use napi_derive::napi;
use serde_json::{Map, Value};
use tokio::{io::AsyncBufReadExt, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::{into_napi_error, state_error, DbNativeResult},
    resolve_query_limit, resolve_stream_batch_size,
    save::{spawn_streaming_save_worker, SaveResult},
    ColumnMeta, QueryBatch, QueryResult,
};

#[napi]
pub struct ClickHouseDriver {
    client: Mutex<Option<Client>>,
}

#[napi]
impl ClickHouseDriver {
    #[napi]
    pub async fn connect(
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    ) -> DbNativeResult<ClickHouseDriver> {
        let mut client = Client::default()
            .with_url(normalize_clickhouse_url(&host, port))
            .with_user(username)
            .with_password(password)
            .with_option("wait_end_of_query", "1");

        if !database.trim().is_empty() {
            client = client.with_database(database.trim());
        }

        Ok(ClickHouseDriver {
            client: Mutex::new(Some(client)),
        })
    }

    #[napi]
    pub async fn query(&self, sql: String, limit: Option<u32>) -> DbNativeResult<QueryResult> {
        self.query_internal(sql, Some(resolve_query_limit(limit)))
            .await
    }

    #[napi(js_name = "queryStream")]
    pub fn query_stream(
        &self,
        env: Env,
        sql: String,
        batch_size: Option<u32>,
        limit: Option<u32>,
    ) -> DbNativeResult<ReadableStream<'static, QueryBatch>> {
        let client = self.client()?;
        let batch_size = resolve_stream_batch_size(batch_size);
        let limit = resolve_query_limit(limit);
        let (sender, receiver) = mpsc::channel::<DbNativeResult<QueryBatch>>(2);

        tokio::spawn(async move {
            if let Err(error) =
                stream_clickhouse_query_to_channel(client, sql, batch_size, limit, sender.clone())
                    .await
            {
                let _ = sender.send(Err(error)).await;
            }
        });

        ReadableStream::new(&env, ReceiverStream::new(receiver)).map_err(into_napi_error)
    }

    #[napi]
    pub async fn close(&self) -> DbNativeResult<()> {
        let mut guard = self
            .client
            .lock()
            .map_err(|_| state_error("ClickHouse connection lock is poisoned"))?;
        guard.take();
        Ok(())
    }

    #[napi]
    pub async fn save(
        &self,
        sql: String,
        file_type: String,
        path: String,
        mode: Option<String>,
    ) -> DbNativeResult<SaveResult> {
        let client = self.client()?;
        let batch_size = resolve_stream_batch_size(None);
        let (sender, result_receiver) = spawn_streaming_save_worker(file_type, path, mode);
        if let Err(error) =
            stream_clickhouse_query_to_channel(client, sql, batch_size, usize::MAX, sender.clone())
                .await
        {
            let _ = sender.send(Err(error)).await;
        }
        drop(sender);
        result_receiver
            .await
            .map_err(|_| state_error("ClickHouse streaming save worker was cancelled"))?
    }
}

async fn stream_clickhouse_query_to_channel(
    client: Client,
    sql: String,
    batch_size: usize,
    limit: usize,
    sender: mpsc::Sender<DbNativeResult<QueryBatch>>,
) -> DbNativeResult<()> {
    let mut lines = client
        .query(sql.as_str())
        .fetch_bytes("JSONCompactEachRowWithNamesAndTypes")
        .map_err(into_napi_error)?
        .lines();
    let mut column_names: Option<Vec<String>> = None;
    let mut columns: Option<Vec<ColumnMeta>> = None;
    let mut rows = Vec::with_capacity(batch_size);
    let mut emitted = false;
    let mut sent_rows = 0usize;

    while sent_rows < limit {
        let Some(line) = lines.next_line().await.map_err(into_napi_error)? else {
            break;
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if column_names.is_none() {
            column_names = Some(parse_string_array(line, "column names")?);
            continue;
        }

        if columns.is_none() {
            let names = column_names.as_deref().unwrap_or(&[]);
            let column_types = parse_string_array(line, "column types")?;
            columns = Some(build_columns(names.to_vec(), column_types)?);
            continue;
        }

        let values = parse_value_array(line)?;
        rows.push(values_to_row(
            column_names.as_deref().unwrap_or(&[]),
            values,
        )?);
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

impl ClickHouseDriver {
    fn client(&self) -> DbNativeResult<Client> {
        let guard = self
            .client
            .lock()
            .map_err(|_| state_error("ClickHouse connection lock is poisoned"))?;
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| state_error("ClickHouse connection is already closed"))
    }

    async fn query_internal(
        &self,
        sql: String,
        limit: Option<usize>,
    ) -> DbNativeResult<QueryResult> {
        let client = self.client()?;
        let started_at = Instant::now();
        let limit = limit.unwrap_or(usize::MAX);
        let mut parser = ClickHouseResponseParser::new(limit);
        let mut lines = client
            .query(sql.as_str())
            .fetch_bytes("JSONCompactEachRowWithNamesAndTypes")
            .map_err(into_napi_error)?
            .lines();

        loop {
            match lines.next_line().await.map_err(into_napi_error)? {
                Some(line) => {
                    parser.ingest_line(&line)?;
                    if parser.is_complete_for_limit() {
                        break;
                    }
                }
                None => break,
            }
        }

        let (columns, rows) = parser.finish()?;

        Ok(QueryResult {
            columns,
            rows,
            execution_time_ms: started_at.elapsed().as_millis() as i64,
        })
    }
}

struct ClickHouseResponseParser {
    limit: usize,
    column_names: Option<Vec<String>>,
    column_types: Option<Vec<String>>,
    rows: Vec<Value>,
}

impl ClickHouseResponseParser {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            column_names: None,
            column_types: None,
            rows: Vec::new(),
        }
    }

    fn ingest_line(&mut self, line: &str) -> DbNativeResult<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }

        if self.column_names.is_none() {
            self.column_names = Some(parse_string_array(line, "column names")?);
            return Ok(());
        }

        if self.column_types.is_none() {
            let column_types = parse_string_array(line, "column types")?;
            let column_names = self
                .column_names
                .as_ref()
                .ok_or_else(|| state_error("ClickHouse response is missing column names"))?;

            if column_names.len() != column_types.len() {
                return Err(state_error(format!(
                    "ClickHouse response has {} column names but {} column types",
                    column_names.len(),
                    column_types.len(),
                )));
            }

            self.column_types = Some(column_types);
            return Ok(());
        }

        if self.rows.len() >= self.limit {
            return Ok(());
        }

        let column_names = self
            .column_names
            .as_ref()
            .ok_or_else(|| state_error("ClickHouse response is missing column names"))?;
        let values = parse_value_array(line)?;
        self.rows.push(values_to_row(column_names, values)?);
        Ok(())
    }

    fn is_complete_for_limit(&self) -> bool {
        self.column_names.is_some() && self.column_types.is_some() && self.rows.len() >= self.limit
    }

    fn finish(self) -> DbNativeResult<(Vec<ColumnMeta>, Vec<Value>)> {
        match (self.column_names, self.column_types) {
            (None, None) => Ok((Vec::new(), Vec::new())),
            (Some(column_names), Some(column_types)) => {
                Ok((build_columns(column_names, column_types)?, self.rows))
            }
            _ => Err(state_error(
                "Incomplete ClickHouse response header while parsing query result",
            )),
        }
    }
}

fn build_columns(
    column_names: Vec<String>,
    column_types: Vec<String>,
) -> DbNativeResult<Vec<ColumnMeta>> {
    if column_names.len() != column_types.len() {
        return Err(state_error(format!(
            "ClickHouse response has {} column names but {} column types",
            column_names.len(),
            column_types.len(),
        )));
    }

    Ok(column_names
        .into_iter()
        .zip(column_types)
        .map(|(name, data_type)| ColumnMeta { name, data_type })
        .collect())
}

fn values_to_row(column_names: &[String], values: Vec<Value>) -> DbNativeResult<Value> {
    if column_names.len() != values.len() {
        return Err(state_error(format!(
            "ClickHouse row has {} values but header defines {} columns",
            values.len(),
            column_names.len(),
        )));
    }

    Ok(Value::Object(
        column_names
            .iter()
            .cloned()
            .zip(values)
            .collect::<Map<String, Value>>(),
    ))
}

fn parse_string_array(line: &str, context: &str) -> DbNativeResult<Vec<String>> {
    serde_json::from_str(line).map_err(|error| {
        state_error(format!(
            "Failed to parse ClickHouse {context} JSON array: {error}",
        ))
    })
}

fn parse_value_array(line: &str) -> DbNativeResult<Vec<Value>> {
    serde_json::from_str(line).map_err(|error| {
        state_error(format!(
            "Failed to parse ClickHouse row JSON array: {error}"
        ))
    })
}

fn normalize_clickhouse_url(host: &str, port: u16) -> String {
    let host = host.trim().trim_end_matches('/');
    if host.contains("://") {
        return host.to_string();
    }

    let host = if host.contains(':') && !host.starts_with('[') && !host.ends_with(']') {
        format!("[{host}]")
    } else {
        host.to_string()
    };

    format!("http://{host}:{port}")
}

#[cfg(test)]
mod tests {
    use super::{normalize_clickhouse_url, ClickHouseResponseParser};

    #[test]
    fn parser_builds_rows_with_limit() {
        let mut parser = ClickHouseResponseParser::new(2);

        parser
            .ingest_line(r#"["id","name","score"]"#)
            .expect("column names should parse");
        parser
            .ingest_line(r#"["UInt64","String","Float64"]"#)
            .expect("column types should parse");
        parser
            .ingest_line(r#"[1,"alice",98.5]"#)
            .expect("first row should parse");
        parser
            .ingest_line(r#"[2,"bob",87.0]"#)
            .expect("second row should parse");
        parser
            .ingest_line(r#"[3,"carol",91.2]"#)
            .expect("extra row should be ignored after limit");

        let (columns, rows) = parser.finish().expect("parser should finish");

        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "alice");
        assert_eq!(rows[1]["score"], 87.0);
    }

    #[test]
    fn parser_supports_empty_result_set() {
        let mut parser = ClickHouseResponseParser::new(1000);
        parser
            .ingest_line(r#"["id","name"]"#)
            .expect("column names should parse");
        parser
            .ingest_line(r#"["UInt64","String"]"#)
            .expect("column types should parse");

        let (columns, rows) = parser.finish().expect("parser should finish");

        assert_eq!(columns.len(), 2);
        assert!(rows.is_empty());
    }

    #[test]
    fn normalizes_plain_host_to_http_url() {
        assert_eq!(
            normalize_clickhouse_url("localhost", 8123),
            "http://localhost:8123",
        );
        assert_eq!(
            normalize_clickhouse_url("https://demo.clickhouse.cloud:8443/", 8123),
            "https://demo.clickhouse.cloud:8443",
        );
    }
}
