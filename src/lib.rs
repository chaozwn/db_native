mod drivers;
mod error;
mod save;

pub use drivers::{
    ClickHouseDriver, DorisDriver, DuckDbDriver, MysqlDriver, PostgresDriver, StarRocksDriver,
    SupabaseDriver,
};
pub use save::SaveResult;

use napi_derive::napi;

pub(crate) const DEFAULT_QUERY_LIMIT: u32 = 1000;
pub(crate) const DEFAULT_STREAM_BATCH_SIZE: u32 = 200;

pub(crate) fn resolve_query_limit(limit: Option<u32>) -> usize {
    limit.unwrap_or(DEFAULT_QUERY_LIMIT) as usize
}

pub(crate) fn resolve_stream_batch_size(batch_size: Option<u32>) -> usize {
    batch_size.unwrap_or(DEFAULT_STREAM_BATCH_SIZE).max(1) as usize
}

#[napi(object)]
#[derive(Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
}

#[napi(object)]
pub struct QueryBatch {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<serde_json::Value>,
}

#[napi(object)]
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<serde_json::Value>,
    pub execution_time_ms: i64,
}
