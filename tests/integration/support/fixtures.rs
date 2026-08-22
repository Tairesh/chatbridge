//! Rows the tests need in place before they start.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::{TestChannel, TestChat, TestClient};

/// Insert a channel row. `config` is the provider-specific settings blob.
pub async fn insert_test_channel(
    pool: &PgPool,
    provider: &str,
    external_key: &str,
    config: serde_json::Value,
) -> TestChannel {
    let channel_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO channels (id, provider, name, external_key, config)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(channel_id)
    .bind(provider)
    .bind(format!("{provider}:{external_key}"))
    .bind(external_key)
    .bind(config)
    .execute(pool)
    .await
    .unwrap();
    TestChannel { id: channel_id }
}

pub async fn insert_test_instagram_channel(pool: &PgPool) -> (TestChannel, String) {
    let user_id = format!("test_{}", Uuid::new_v4());
    let guard = insert_test_channel(
        pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "test_token"}),
    )
    .await;
    (guard, user_id)
}

pub async fn insert_test_telegram_channel(pool: &PgPool, bot_secret: &str) -> TestChannel {
    // external_key is the numeric bot id, matching the runtime convention.
    let bot_id = (Uuid::new_v4().as_u128() as u64).to_string();
    let bot_token = format!("{bot_id}:test");
    insert_test_channel(
        pool,
        "telegram",
        &bot_id,
        serde_json::json!({"bot_token": bot_token, "bot_secret": bot_secret}),
    )
    .await
}

pub async fn insert_test_widget_channel(pool: &PgPool, widget_id: &str) -> TestChannel {
    insert_test_channel(pool, "widget", widget_id, serde_json::json!({})).await
}

pub async fn insert_test_client(pool: &PgPool, name: &str) -> TestClient {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO clients (id, provider, name) VALUES ($1, 'widget', $2)")
        .bind(id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    TestClient { id }
}

pub async fn insert_test_chat(
    pool: &PgPool,
    client_id: Uuid,
    channel_id: Uuid,
    status: &str,
    created_at: DateTime<Utc>,
) -> TestChat {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO chats (id, client_id, channel_id, status, created_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(client_id)
    .bind(channel_id)
    .bind(status)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
    TestChat { id }
}

/// A channel, a client and their chat, for tests that only care about messages.
///
/// The client needs no guard of its own: `TestChannel`'s drop deletes the channel's
/// messages and chats and then every client they referenced.
pub async fn seed_chat(pool: &PgPool, tag: &str) -> (TestChannel, Uuid, Uuid) {
    let channel = insert_test_channel(
        pool,
        "instagram",
        &format!("{tag}_{}", Uuid::new_v4().simple()),
        serde_json::json!({"access_token": "t"}),
    )
    .await;
    let client_id = chatbridge::db::create_client(pool).await.unwrap();
    let chat_id = chatbridge::db::find_or_create_chat(pool, client_id, channel.id)
        .await
        .unwrap();
    (channel, client_id, chat_id)
}

/// Insert one message on `chat_id` and return its row id.
pub async fn seed_message(
    pool: &PgPool,
    channel_id: Uuid,
    chat_id: Uuid,
    sender_id: Option<Uuid>,
    sender_type: &str,
    external_message_id: &str,
) -> Uuid {
    let msg = chatbridge::model::NewMessage {
        external_message_id: external_message_id.to_owned(),
        channel_id,
        // The chat is passed explicitly to `insert_message` below, so nothing here
        // has a conversation to name.
        conversation: None,
        sender_id,
        sender_type: sender_type.to_owned(),
        provider: chatbridge::model::ProviderKind::Instagram,
        event: chatbridge::model::EventKind::Message,
        text: Some("body".into()),
        raw: serde_json::json!({}),
    };
    chatbridge::db::insert_message(pool, &msg, Some(chat_id))
        .await
        .unwrap()
        .expect("seed message must insert")
        .id
}
