use std::{
    env,
    fs::{self, File},
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use chrono::{SecondsFormat, Utc};
use napi_derive::napi;
use serde::Serialize;
use uuid::Uuid;

use crate::error::{into_napi_error, state_error, DbNativeResult};

#[napi(object)]
#[derive(Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub path: String,
}

#[derive(Clone)]
pub(crate) struct SessionRecorder {
    inner: Arc<Mutex<SessionState>>,
}

struct SessionState {
    document: SessionDocument,
    path: PathBuf,
    next_view_index: u64,
}

#[derive(Serialize)]
struct SessionDocument {
    session_id: String,
    created_at: String,
    updated_at: String,
    views: Vec<SessionViewRecord>,
}

#[derive(Serialize)]
struct SessionViewRecord {
    view_name: String,
    operation: String,
    sql: String,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<i64>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl SessionRecorder {
    pub(crate) fn new() -> DbNativeResult<Self> {
        let session_id = Uuid::new_v4().to_string();
        let created_at = now_timestamp();
        let session_dir = session_base_dir()?.join(&session_id);
        fs::create_dir_all(&session_dir).map_err(into_napi_error)?;

        let path = session_dir.join("session.json");
        let document = SessionDocument {
            session_id,
            created_at: created_at.clone(),
            updated_at: created_at,
            views: Vec::new(),
        };
        write_document(&path, &document)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(SessionState {
                document,
                path,
                next_view_index: 1,
            })),
        })
    }

    pub(crate) fn info(&self) -> DbNativeResult<SessionInfo> {
        let state = self
            .inner
            .lock()
            .map_err(|_| state_error("Session recorder lock is poisoned"))?;

        Ok(SessionInfo {
            session_id: state.document.session_id.clone(),
            path: state.path.to_string_lossy().into_owned(),
        })
    }

    pub(crate) fn record_success(
        &self,
        view_name: Option<String>,
        operation: &str,
        sql: &str,
        row_count: Option<i64>,
    ) -> DbNativeResult<()> {
        self.record(view_name, operation, sql, row_count, "success", None)
    }

    pub(crate) fn record_error(
        &self,
        view_name: Option<String>,
        operation: &str,
        sql: &str,
        error: String,
    ) -> DbNativeResult<()> {
        self.record(view_name, operation, sql, None, "error", Some(error))
    }

    fn record(
        &self,
        view_name: Option<String>,
        operation: &str,
        sql: &str,
        row_count: Option<i64>,
        status: &str,
        error: Option<String>,
    ) -> DbNativeResult<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| state_error("Session recorder lock is poisoned"))?;
        let created_at = now_timestamp();
        let view_name = resolve_view_name(&mut state, view_name);

        state.document.updated_at = created_at.clone();
        state.document.views.push(SessionViewRecord {
            view_name,
            operation: operation.to_string(),
            sql: sql.to_string(),
            created_at,
            row_count,
            status: status.to_string(),
            error,
        });

        write_document(&state.path, &state.document)
    }
}

fn resolve_view_name(state: &mut SessionState, view_name: Option<String>) -> String {
    if let Some(view_name) = view_name.map(|value| value.trim().to_string()) {
        if !view_name.is_empty() {
            return view_name;
        }
    }

    let view_name = format!("view_{}", state.next_view_index);
    state.next_view_index += 1;
    view_name
}

fn session_base_dir() -> DbNativeResult<PathBuf> {
    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| state_error("Unable to resolve home directory for db-native session"))?;

    Ok(PathBuf::from(home).join(".db-native"))
}

fn write_document(path: &PathBuf, document: &SessionDocument) -> DbNativeResult<()> {
    let temp_path = path.with_file_name("session.json.tmp");
    let file = File::create(&temp_path).map_err(into_napi_error)?;
    serde_json::to_writer_pretty(BufWriter::new(file), document).map_err(into_napi_error)?;
    fs::rename(&temp_path, path).map_err(into_napi_error)
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
