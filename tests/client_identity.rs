//! Integration tests for client identity resolution.
//! Requires DATABASE_URL and REDIS_URL env vars.

mod common;

use chatbridge::model::ProviderKind;
use common::TestClient;
use uuid::Uuid;

#[tokio::test]
async fn telegram_client_upsert_creates_and_updates() {
    let pool = common::setup_pool().await;
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

    let _guard = TestClient { id: id1 };

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
}

#[tokio::test]
async fn instagram_client_upsert_with_null_name() {
    let pool = common::setup_pool().await;
    let ext_id = Uuid::new_v4().to_string();

    let id = Uuid::new_v4();
    let returned_id =
        chatbridge::db::upsert_client(&pool, id, ProviderKind::Instagram, &ext_id, None, None)
            .await
            .unwrap();

    let _guard = TestClient { id: returned_id };

    assert_eq!(returned_id, id);

    let client =
        chatbridge::db::find_client_by_external_id(&pool, ProviderKind::Instagram, &ext_id)
            .await
            .unwrap()
            .expect("client should exist");

    assert!(client.name.is_none());
    assert!(client.username.is_none());
}
