use std::env;

use db_native::PostgresDriver;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_* environment variables and a live PostgreSQL instance"]
async fn postgres_connects_and_queries() {
    let host = required_env("TEST_POSTGRES_HOST");
    let port = required_env("TEST_POSTGRES_PORT")
        .parse::<u16>()
        .expect("TEST_POSTGRES_PORT must be a valid u16");
    let username = required_env("TEST_POSTGRES_USERNAME");
    let password = required_env("TEST_POSTGRES_PASSWORD");
    let database = required_env("TEST_POSTGRES_DATABASE");
    let schema = required_env("TEST_POSTGRES_SCHEMA");

    let driver = PostgresDriver::connect(
        host,
        port,
        database.clone(),
        Some(schema.clone()),
        username,
        password,
    )
    .await
    .expect("postgres connect should succeed");

    let ping = driver
        .query(
            "SELECT 1 AS ping, current_database() AS database_name, current_schema() AS schema_name"
                .to_string(),
            None,
        )
        .await
        .expect("simple postgres query should succeed");

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
    assert_eq!(
        row.get("schema_name").and_then(|value| value.as_str()),
        Some(schema.as_str())
    );

    let tables = driver
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY table_name LIMIT 5"
                .to_string(),
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
            "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n".to_string(),
            Some(2),
        )
        .await
        .expect("limited postgres query should succeed");

    assert_eq!(limited.rows.len(), 2, "limit should cap postgres rows");

    driver.close().await.expect("postgres close should succeed");
}
