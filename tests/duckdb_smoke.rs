use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use db_native::DuckDbDriver;

fn temp_csv_path() -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();

    env::temp_dir().join(format!("db_native_duckdb_smoke_{suffix}.csv"))
}

fn cleanup_session(session_path: &Path) {
    if let Some(session_dir) = session_path.parent() {
        let _ = fs::remove_dir_all(session_dir);
    }
}

#[test]
fn duckdb_queries_memory_and_csv() {
    let driver = DuckDbDriver::new();
    let session_path = PathBuf::from(
        driver
            .get_session()
            .expect("session info should be available")
            .path,
    );

    let ping = driver
        .query(
            "SELECT 1 AS ping, 'duckdb' AS engine".to_string(),
            None,
            None,
        )
        .expect("simple duckdb query should succeed");

    assert_eq!(ping.rows.len(), 1, "expected one row from ping query");

    let row = ping
        .rows
        .first()
        .and_then(|value| value.as_object())
        .expect("first row should be a JSON object");

    assert_eq!(row.get("ping").and_then(|value| value.as_i64()), Some(1));
    assert_eq!(
        row.get("engine").and_then(|value| value.as_str()),
        Some("duckdb")
    );

    let csv_path = temp_csv_path();
    fs::write(&csv_path, "id,name,score\n1,alice,98\n2,bob,87\n")
        .expect("csv fixture should be written");

    let escaped_path = csv_path.to_string_lossy().replace('\'', "''");
    let csv_query =
        format!("SELECT id, name, score FROM read_csv_auto('{escaped_path}') ORDER BY id");

    let csv_result = driver
        .query(csv_query, None, None)
        .expect("duckdb csv query should succeed");

    assert_eq!(csv_result.rows.len(), 2, "expected two rows from csv query");

    let first_row = csv_result
        .rows
        .first()
        .and_then(|value| value.as_object())
        .expect("first csv row should be a JSON object");

    assert_eq!(
        first_row.get("id").and_then(|value| value.as_i64()),
        Some(1)
    );
    assert_eq!(
        first_row.get("name").and_then(|value| value.as_str()),
        Some("alice")
    );
    assert_eq!(
        first_row.get("score").and_then(|value| value.as_i64()),
        Some(98)
    );

    let limited = driver
        .query(
            "SELECT * FROM (VALUES (1), (2), (3)) AS t(id) ORDER BY id".to_string(),
            Some(2),
            None,
        )
        .expect("limited duckdb query should succeed");

    assert_eq!(limited.rows.len(), 2, "limit should cap duckdb rows");

    fs::remove_file(&csv_path).expect("csv fixture should be removed");

    driver.close().expect("duckdb close should succeed");
    cleanup_session(&session_path);
}

#[test]
fn duckdb_records_session_views() {
    let driver = DuckDbDriver::new();
    let session = driver
        .get_session()
        .expect("session info should be available");
    let session_path = PathBuf::from(&session.path);

    assert!(
        session_path.exists(),
        "session file should be created when the driver is initialized",
    );

    let result = driver
        .query(
            "SELECT 42 AS answer".to_string(),
            None,
            Some("step_1".to_string()),
        )
        .expect("tracked query should succeed");

    assert_eq!(result.rows.len(), 1);

    let error = driver.query(
        "SELECT * FROM definitely_missing_table".to_string(),
        None,
        Some("step_error".to_string()),
    );
    assert!(error.is_err(), "failing query should return an error");

    let session_json = fs::read_to_string(&session_path).expect("session file should be readable");
    let document: serde_json::Value =
        serde_json::from_str(&session_json).expect("session file should be valid JSON");
    let views = document["views"]
        .as_array()
        .expect("session views should be an array");

    assert_eq!(
        document["session_id"].as_str(),
        Some(session.session_id.as_str())
    );
    assert_eq!(views.len(), 2);
    assert_eq!(views[0]["view_name"].as_str(), Some("step_1"));
    assert_eq!(views[0]["operation"].as_str(), Some("query"));
    assert_eq!(views[0]["row_count"].as_i64(), Some(1));
    assert_eq!(views[0]["status"].as_str(), Some("success"));
    assert_eq!(views[1]["view_name"].as_str(), Some("step_error"));
    assert_eq!(views[1]["status"].as_str(), Some("error"));
    assert!(views[1]["error"].as_str().is_some());

    driver.close().expect("duckdb close should succeed");
    cleanup_session(&session_path);
}
