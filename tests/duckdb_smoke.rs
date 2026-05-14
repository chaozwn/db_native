use std::{
    env, fs,
    path::PathBuf,
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

#[test]
fn duckdb_queries_memory_and_csv() {
    let driver = DuckDbDriver::new();

    let ping = driver
        .query("SELECT 1 AS ping, 'duckdb' AS engine".to_string(), None)
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
        .query(csv_query, None)
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
        )
        .expect("limited duckdb query should succeed");

    assert_eq!(limited.rows.len(), 2, "limit should cap duckdb rows");

    fs::remove_file(&csv_path).expect("csv fixture should be removed");

    driver.close().expect("duckdb close should succeed");
}
