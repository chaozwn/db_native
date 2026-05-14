use std::env;

use db_native::StarRocksDriver;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

#[tokio::test]
#[ignore = "requires TEST_STARROCKS_* environment variables and a live StarRocks instance"]
async fn starrocks_connects_and_queries() {
    let host = required_env("TEST_STARROCKS_HOST");
    let port = required_env("TEST_STARROCKS_PORT")
        .parse::<u16>()
        .expect("TEST_STARROCKS_PORT must be a valid u16");
    let username = required_env("TEST_STARROCKS_USERNAME");
    let password = required_env("TEST_STARROCKS_PASSWORD");
    let database = required_env("TEST_STARROCKS_DATABASE");

    let driver = StarRocksDriver::connect(host, port, database.clone(), username, password)
        .await
        .expect("starrocks connect should succeed");

    let ping = driver
        .query(
            "SELECT 1 AS ping, DATABASE() AS database_name".to_string(),
            None,
            None,
        )
        .await
        .expect("simple starrocks query should succeed");

    assert_eq!(ping.rows.len(), 1, "expected one row from ping query");

    let row = ping
        .rows
        .first()
        .and_then(|value| value.as_object())
        .expect("first row should be a JSON object");

    assert_eq!(row.get("ping").and_then(|value| value.as_i64()), Some(1));
    assert_eq!(
        row.get("database_name").and_then(|value| value.as_str()),
        Some(database.as_str())
    );

    let tables = driver
        .query("SHOW TABLES".to_string(), None, None)
        .await
        .expect("SHOW TABLES should succeed");

    assert!(
        !tables.columns.is_empty(),
        "SHOW TABLES should return at least one column",
    );

    let limited = driver
        .query(
            "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n".to_string(),
            Some(2),
            None,
        )
        .await
        .expect("limited starrocks query should succeed");

    assert_eq!(limited.rows.len(), 2, "limit should cap starrocks rows");

    driver
        .close()
        .await
        .expect("starrocks close should succeed");
}
