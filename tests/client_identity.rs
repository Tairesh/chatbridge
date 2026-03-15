//! Integration tests for client identity resolution.
//! Requires DATABASE_URL and REDIS_URL env vars.

use chatbridge::model::ProviderKind;
use sqlx::PgPool;
use uuid::Uuid;

async fn setup_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL required for integration tests");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("failed to connect");
    sqlx::migrate!().run(&pool).await.expect("migration failed");
    pool
}

#[tokio::test]
async fn telegram_client_upsert_creates_and_updates() {
    let pool = setup_pool().await;
    let ext_id = Uuid::new_v4().to_string();

    // First upsert — creates
    let id1 = chatbridge::db::upsert_client(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Telegram,
        &ext_id,
        Some("John Doe"),
        Some("johndoe"),
    )
    .await
    .unwrap();

    // Second upsert — same external_id returns same id, updates name
    let id2 = chatbridge::db::upsert_client(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Telegram,
        &ext_id,
        Some("John Updated"),
        Some("johndoe"),
    )
    .await
    .unwrap();

    assert_eq!(id1, id2);

    // Verify lookup
    let client = chatbridge::db::find_client_by_external_id(&pool, ProviderKind::Telegram, &ext_id)
        .await
        .unwrap()
        .expect("client should exist");

    assert_eq!(client.id, id1);
    assert_eq!(client.name.as_deref(), Some("John Updated"));
    assert_eq!(client.username.as_deref(), Some("johndoe"));
    assert_eq!(client.provider, "telegram");

    // Cleanup
    sqlx::query("DELETE FROM clients WHERE id = $1")
        .bind(id1)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn instagram_client_upsert_with_null_name() {
    let pool = setup_pool().await;
    let ext_id = Uuid::new_v4().to_string();

    let id = Uuid::new_v4();
    let returned_id = chatbridge::db::upsert_client(&pool, id, ProviderKind::Instagram, &ext_id, None, None)
        .await
        .unwrap();

    assert_eq!(returned_id, id);

    let client = chatbridge::db::find_client_by_external_id(&pool, ProviderKind::Instagram, &ext_id)
        .await
        .unwrap()
        .expect("client should exist");

    assert!(client.name.is_none());
    assert!(client.username.is_none());

    // Cleanup
    sqlx::query("DELETE FROM clients WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
}
