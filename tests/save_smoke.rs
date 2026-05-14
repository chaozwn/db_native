use std::{
    env, fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use db_native::DuckDbDriver;

fn temp_dir() -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();

    env::temp_dir().join(format!("db_native_save_smoke_{suffix}"))
}

#[test]
fn duckdb_save_supports_all_formats() {
    let driver = DuckDbDriver::new();
    let output_dir = temp_dir();
    fs::create_dir_all(&output_dir).expect("output directory should be created");

    let sql = "SELECT * FROM (VALUES (1, 'alice', 98), (2, 'bob', 87)) AS t(id, name, score)";

    let csv_path = output_dir.join("result.csv");
    let json_path = output_dir.join("result.json");
    let parquet_path = output_dir.join("result.parquet");
    let orc_path = output_dir.join("result.orc");
    let excel_path = output_dir.join("result.xlsx");

    driver
        .save(
            sql.to_string(),
            "csv".to_string(),
            csv_path.to_string_lossy().into_owned(),
            None,
        )
        .expect("csv save should succeed");
    driver
        .save(
            sql.to_string(),
            "json".to_string(),
            json_path.to_string_lossy().into_owned(),
            None,
        )
        .expect("json save should succeed");
    driver
        .save(
            sql.to_string(),
            "parquet".to_string(),
            parquet_path.to_string_lossy().into_owned(),
            None,
        )
        .expect("parquet save should succeed");
    driver
        .save(
            sql.to_string(),
            "orc".to_string(),
            orc_path.to_string_lossy().into_owned(),
            None,
        )
        .expect("orc save should succeed");
    driver
        .save(
            sql.to_string(),
            "excel".to_string(),
            excel_path.to_string_lossy().into_owned(),
            None,
        )
        .expect("excel save should succeed");

    let csv = fs::read_to_string(&csv_path).expect("csv file should be readable");
    assert!(csv.contains("id,name,score"));
    assert!(csv.contains("alice"));

    let json = fs::read_to_string(&json_path).expect("json file should be readable");
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&json).expect("json export should be valid JSON");
    assert_eq!(rows.len(), 2);

    let parquet = fs::read(&parquet_path).expect("parquet file should be readable");
    assert_eq!(&parquet[..4], b"PAR1");

    let orc = fs::read(&orc_path).expect("orc file should be readable");
    assert_eq!(&orc[..3], b"ORC");

    let excel = fs::read(&excel_path).expect("xlsx file should be readable");
    assert_eq!(&excel[..2], b"PK");

    driver.close().expect("duckdb close should succeed");
    fs::remove_dir_all(&output_dir).expect("output directory should be removed");
}

#[test]
fn duckdb_save_append_supports_csv_and_json() {
    let driver = DuckDbDriver::new();
    let output_dir = temp_dir();
    fs::create_dir_all(&output_dir).expect("output directory should be created");

    let csv_path = output_dir.join("append.csv");
    let json_path = output_dir.join("append.json");

    driver
        .save(
            "SELECT * FROM (VALUES (1, 'alice')) AS t(id, name)".to_string(),
            "csv".to_string(),
            csv_path.to_string_lossy().into_owned(),
            Some("overwrite".to_string()),
        )
        .expect("csv overwrite save should succeed");
    driver
        .save(
            "SELECT * FROM (VALUES (2, 'bob')) AS t(id, name)".to_string(),
            "csv".to_string(),
            csv_path.to_string_lossy().into_owned(),
            Some("append".to_string()),
        )
        .expect("csv append save should succeed");

    driver
        .save(
            "SELECT * FROM (VALUES (1, 'alice')) AS t(id, name)".to_string(),
            "json".to_string(),
            json_path.to_string_lossy().into_owned(),
            Some("overwrite".to_string()),
        )
        .expect("json overwrite save should succeed");
    driver
        .save(
            "SELECT * FROM (VALUES (2, 'bob')) AS t(id, name)".to_string(),
            "json".to_string(),
            json_path.to_string_lossy().into_owned(),
            Some("append".to_string()),
        )
        .expect("json append save should succeed");

    let csv = fs::read_to_string(&csv_path).expect("csv file should be readable");
    assert_eq!(csv.lines().filter(|line| *line == "id,name").count(), 1);
    assert!(csv.contains("1,alice"));
    assert!(csv.contains("2,bob"));

    let json = fs::read_to_string(&json_path).expect("json file should be readable");
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&json).expect("json export should be valid JSON");
    assert_eq!(rows.len(), 2);

    driver.close().expect("duckdb close should succeed");
    fs::remove_dir_all(&output_dir).expect("output directory should be removed");
}

#[test]
fn duckdb_save_append_rejects_existing_parquet() {
    let driver = DuckDbDriver::new();
    let output_dir = temp_dir();
    fs::create_dir_all(&output_dir).expect("output directory should be created");

    let parquet_path = output_dir.join("append.parquet");

    driver
        .save(
            "SELECT * FROM (VALUES (1, 'alice')) AS t(id, name)".to_string(),
            "parquet".to_string(),
            parquet_path.to_string_lossy().into_owned(),
            Some("overwrite".to_string()),
        )
        .expect("parquet overwrite save should succeed");

    let error = match driver.save(
        "SELECT * FROM (VALUES (2, 'bob')) AS t(id, name)".to_string(),
        "parquet".to_string(),
        parquet_path.to_string_lossy().into_owned(),
        Some("append".to_string()),
    ) {
        Ok(_) => panic!("parquet append should fail"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("Append mode is not supported for parquet exports"),
        "unexpected error: {error}",
    );

    driver.close().expect("duckdb close should succeed");
    fs::remove_dir_all(&output_dir).expect("output directory should be removed");
}
