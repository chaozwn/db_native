use std::env;

use db_native::ClickHouseDriver;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

#[tokio::test]
#[ignore = "requires TEST_CLICKHOUSE_* environment variables and a live ClickHouse instance"]
async fn clickhouse_connects_and_queries() {
    let host = required_env("TEST_CLICKHOUSE_HOST");
    let port = required_env("TEST_CLICKHOUSE_PORT")
        .parse::<u16>()
        .expect("TEST_CLICKHOUSE_PORT must be a valid u16");
    let username = required_env("TEST_CLICKHOUSE_USERNAME");
    let password = required_env("TEST_CLICKHOUSE_PASSWORD");
    let database = required_env("TEST_CLICKHOUSE_DATABASE");

    let driver = ClickHouseDriver::connect(host, port, database.clone(), username, password)
        .await
        .expect("clickhouse connect should succeed");

    let ping = driver
        .query(
            "SELECT 1 AS ping, currentDatabase() AS database_name".to_string(),
            None,
            None,
        )
        .await
        .expect("simple clickhouse query should succeed");

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
        .query(
            "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name LIMIT 5"
                .to_string(),
            None,
            None,
        )
        .await
        .expect("table listing query should succeed");

    assert!(
        !tables.columns.is_empty(),
        "table listing query should return at least one column",
    );

    let limited = driver
        .query(
            "SELECT number AS n FROM numbers(3) ORDER BY n".to_string(),
            Some(2),
            None,
        )
        .await
        .expect("limited clickhouse query should succeed");

    assert_eq!(limited.rows.len(), 2, "limit should cap clickhouse rows");

    driver
        .close()
        .await
        .expect("clickhouse close should succeed");
}
