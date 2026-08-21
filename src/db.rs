use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::model::{NewMessage, ProviderKind};

pub async fn init_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(30)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(database_url)
        .await
        .expect("failed to connect to Postgres")
}

pub async fn run_migrations(pool: &PgPool) {
    sqlx::migrate!()
        .run(pool)
        .await
        .expect("failed to run migrations");
}

#[derive(Debug, Clone, FromRow)]
pub struct InstagramChannel {
    pub id: Uuid,
    pub user_id: String,
    pub access_token: String,
}

pub async fn find_instagram_channel_by_user_id(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<InstagramChannel>, sqlx::Error> {
    sqlx::query_as::<_, InstagramChannel>(
        "SELECT id, user_id, access_token FROM instagram_channels WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Clone, FromRow)]
pub struct TelegramChannel {
    pub id: Uuid,
    pub bot_token: String,
    pub bot_secret: String,
}

pub async fn find_telegram_channel_by_id(
    pool: &PgPool,
    channel_id: Uuid,
) -> Result<Option<TelegramChannel>, sqlx::Error> {
    sqlx::query_as::<_, TelegramChannel>(
        "SELECT id, bot_token, bot_secret FROM telegram_channels WHERE id = $1",
    )
    .bind(channel_id)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Clone, FromRow)]
pub struct WidgetChannel {
    pub id: Uuid,
    pub widget_id: String,
}

pub async fn find_widget_channel_by_widget_id(
    pool: &PgPool,
    widget_id: &str,
) -> Result<Option<WidgetChannel>, sqlx::Error> {
    sqlx::query_as::<_, WidgetChannel>(
        "SELECT id, widget_id FROM widget_channels WHERE widget_id = $1",
    )
    .bind(widget_id)
    .fetch_optional(pool)
    .await
}

pub async fn create_client(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as("INSERT INTO clients DEFAULT VALUES RETURNING id")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

#[derive(Debug, Clone, FromRow)]
pub struct Client {
    pub id: Uuid,
    pub provider: String,
    pub external_id: Option<String>,
    pub name: Option<String>,
    pub username: Option<String>,
    pub updated_at: DateTime<Utc>,
}

pub async fn upsert_client(
    pool: &PgPool,
    id: Uuid,
    provider: ProviderKind,
    external_id: &str,
    name: Option<&str>,
    username: Option<&str>,
) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as(
        "INSERT INTO clients (id, provider, external_id, name, username, updated_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (provider, external_id) WHERE external_id IS NOT NULL
         DO UPDATE SET name = $4, username = $5, updated_at = now()
         RETURNING id",
    )
    .bind(id)
    .bind(provider.to_string())
    .bind(external_id)
    .bind(name)
    .bind(username)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

pub async fn find_client_by_external_id(
    pool: &PgPool,
    provider: ProviderKind,
    external_id: &str,
) -> Result<Option<Client>, sqlx::Error> {
    sqlx::query_as::<_, Client>(
        "SELECT id, provider, external_id, name, username, updated_at
         FROM clients
         WHERE provider = $1 AND external_id = $2",
    )
    .bind(provider.to_string())
    .bind(external_id)
    .fetch_optional(pool)
    .await
}

pub async fn find_client_by_uuid(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Option<Client>, sqlx::Error> {
    sqlx::query_as::<_, Client>(
        "SELECT id, provider, external_id, name, username, updated_at
         FROM clients WHERE id = $1",
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await
}

pub async fn find_or_create_chat(
    pool: &PgPool,
    sender_id: Uuid,
    channel_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let existing: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM chats WHERE client_id = $1 AND channel_id = $2 AND status = 'new'",
    )
    .bind(sender_id)
    .bind(channel_id)
    .fetch_optional(pool)
    .await?;

    if let Some((chat_id,)) = existing {
        return Ok(chat_id);
    }

    match sqlx::query_as::<_, (Uuid,)>(
        "INSERT INTO chats (client_id, channel_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(sender_id)
    .bind(channel_id)
    .fetch_one(pool)
    .await
    {
        Ok((chat_id,)) => Ok(chat_id),
        Err(sqlx::Error::Database(ref e)) if e.is_unique_violation() => {
            let (chat_id,): (Uuid,) = sqlx::query_as(
                "SELECT id FROM chats WHERE client_id = $1 AND channel_id = $2 AND status = 'new'",
            )
            .bind(sender_id)
            .bind(channel_id)
            .fetch_one(pool)
            .await?;
            Ok(chat_id)
        }
        Err(e) => Err(e),
    }
}

/// The client's most recent chat on a channel, whatever its status.
///
/// Unlike `find_or_create_chat` this does NOT filter on `status = 'new'`: a client
/// returning to a closed conversation should still see its history. Creates nothing.
#[derive(Debug, Clone, FromRow)]
pub struct LastChat {
    pub id: Uuid,
    pub status: String,
}

pub async fn find_last_chat(
    pool: &PgPool,
    client_id: Uuid,
    channel_id: Uuid,
) -> Result<Option<LastChat>, sqlx::Error> {
    sqlx::query_as::<_, LastChat>(
        "SELECT id, status FROM chats
         WHERE client_id = $1 AND channel_id = $2
         ORDER BY created_at DESC
         LIMIT 1",
    )
    .bind(client_id)
    .bind(channel_id)
    .fetch_optional(pool)
    .await
}

// ── DB return types (decoupled from model event structs) ────────────

#[derive(Debug, Clone, FromRow)]
pub struct DbMessage {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub sender_id: Option<Uuid>,
    pub sender_type: String,
    pub text: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct DbEdit {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub sender_id: Option<Uuid>,
    pub sender_type: String,
    pub text: Option<String>,
    pub edited_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct DbRead {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub sender_id: Option<Uuid>,
    pub sender_type: String,
}

pub async fn insert_message(
    pool: &PgPool,
    msg: &NewMessage,
    chat_id: Option<Uuid>,
) -> Result<Option<DbMessage>, sqlx::Error> {
    sqlx::query_as::<_, DbMessage>(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_id, sender_type, text, raw)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (channel_id, external_message_id) DO NOTHING
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type, text, status, created_at",
    )
    .bind(chat_id)
    .bind(&msg.external_message_id)
    .bind(msg.channel_id)
    .bind(msg.sender_id)
    .bind(&msg.sender_type)
    .bind(&msg.text)
    .bind(&msg.raw)
    .fetch_optional(pool)
    .await
}

pub async fn edit_message(
    pool: &PgPool,
    channel_id: Uuid,
    external_message_id: &str,
    text: Option<&str>,
) -> Result<Option<DbEdit>, sqlx::Error> {
    sqlx::query_as::<_, DbEdit>(
        "UPDATE messages SET text = $3, edited_at = now()
         WHERE channel_id = $1 AND external_message_id = $2
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type, text, edited_at",
    )
    .bind(channel_id)
    .bind(external_message_id)
    .bind(text)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ChatSummary {
    pub chat_id: Uuid,
    pub chat_status: String,
    pub chat_created_at: DateTime<Utc>,
    pub client_id: Uuid,
    pub client_name: Option<String>,
    pub client_provider: String,
    pub last_message_text: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_message_sender_type: Option<String>,
    pub last_message_sender_name: Option<String>,
}

pub async fn list_active_chats(pool: &PgPool) -> Result<Vec<ChatSummary>, sqlx::Error> {
    sqlx::query_as::<_, ChatSummary>(
        "SELECT
             c.id        AS chat_id,
             c.status    AS chat_status,
             c.created_at AS chat_created_at,
             cl.id       AS client_id,
             cl.name     AS client_name,
             cl.provider AS client_provider,
             lm.text     AS last_message_text,
             lm.created_at AS last_message_at,
             lm.sender_type AS last_message_sender_type,
             CASE
                 WHEN lm.sender_type = 'operator' THEN op.name
                 ELSE scl.name
             END AS last_message_sender_name
         FROM chats c
         JOIN clients cl ON cl.id = c.client_id
         LEFT JOIN LATERAL (
             SELECT m.text, m.created_at, m.sender_type, m.sender_id
             FROM messages m
             WHERE m.chat_id = c.id
             ORDER BY m.created_at DESC
             LIMIT 1
         ) lm ON true
         LEFT JOIN operators op ON lm.sender_type = 'operator' AND op.id = lm.sender_id
         LEFT JOIN clients scl ON lm.sender_type = 'client' AND scl.id = lm.sender_id
         WHERE c.status = 'new'
         ORDER BY COALESCE(lm.created_at, c.created_at) DESC",
    )
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ChatMessage {
    pub id: Uuid,
    pub external_message_id: String,
    pub sender_id: Option<Uuid>,
    pub sender_type: String,
    pub sender_name: Option<String>,
    pub text: Option<String>,
    pub status: String,
    pub edited_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

pub async fn chat_exists(pool: &PgPool, chat_id: Uuid) -> Result<bool, sqlx::Error> {
    let row: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM chats WHERE id = $1")
        .bind(chat_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

pub async fn get_chat_messages(
    pool: &PgPool,
    chat_id: Uuid,
) -> Result<Vec<ChatMessage>, sqlx::Error> {
    sqlx::query_as::<_, ChatMessage>(
        "SELECT m.id, m.external_message_id, m.sender_id, m.sender_type,
                CASE
                    WHEN m.sender_type = 'operator' THEN op.name
                    ELSE cl.name
                END AS sender_name,
                m.text, m.status, m.edited_at, m.created_at
         FROM messages m
         LEFT JOIN operators op ON m.sender_type = 'operator' AND op.id = m.sender_id
         LEFT JOIN clients   cl ON m.sender_type = 'client'   AND cl.id = m.sender_id
         WHERE m.chat_id = $1
         ORDER BY m.created_at ASC
         LIMIT 100",
    )
    .bind(chat_id)
    .fetch_all(pool)
    .await
}

pub async fn mark_messages_read(
    pool: &PgPool,
    channel_id: Uuid,
    external_message_id: &str,
    reader_type: &str,
) -> Result<Vec<DbRead>, sqlx::Error> {
    sqlx::query_as::<_, DbRead>(
        "UPDATE messages SET status = 'read'
         WHERE chat_id = (
             SELECT chat_id FROM messages WHERE channel_id = $1 AND external_message_id = $2
         )
         AND created_at <= (
             SELECT created_at FROM messages WHERE channel_id = $1 AND external_message_id = $2
         )
         AND status = 'new'
         AND sender_type != $3
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type",
    )
    .bind(channel_id)
    .bind(external_message_id)
    .bind(reader_type)
    .fetch_all(pool)
    .await
}

/// Mark messages as read by target message UUID (for WS read receipts).
pub async fn mark_messages_read_by_id(
    pool: &PgPool,
    message_id: Uuid,
    reader_type: &str,
) -> Result<Vec<DbRead>, sqlx::Error> {
    sqlx::query_as::<_, DbRead>(
        "UPDATE messages SET status = 'read'
         WHERE chat_id = (SELECT chat_id FROM messages WHERE id = $1)
         AND created_at <= (SELECT created_at FROM messages WHERE id = $1)
         AND status = 'new'
         AND sender_type != $2
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type",
    )
    .bind(message_id)
    .bind(reader_type)
    .fetch_all(pool)
    .await
}

// ── Operator queries ────────────────────────────────────────────────

#[derive(Debug, Clone, FromRow)]
pub struct Operator {
    pub id: Uuid,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
}

pub async fn create_operator(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as("INSERT INTO operators DEFAULT VALUES RETURNING id")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

pub async fn find_operator_by_id(
    pool: &PgPool,
    operator_id: Uuid,
) -> Result<Option<Operator>, sqlx::Error> {
    sqlx::query_as::<_, Operator>("SELECT id, name, created_at FROM operators WHERE id = $1")
        .bind(operator_id)
        .fetch_optional(pool)
        .await
}

// ── Chat info for cache ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ChatInfo {
    pub client_id: Uuid,
    pub channel_id: Uuid,
}

pub async fn find_chat_info(pool: &PgPool, chat_id: Uuid) -> Result<Option<ChatInfo>, sqlx::Error> {
    let row: Option<(Uuid, Uuid)> =
        sqlx::query_as("SELECT client_id, channel_id FROM chats WHERE id = $1")
            .bind(chat_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(client_id, channel_id)| ChatInfo {
        client_id,
        channel_id,
    }))
}

pub async fn find_channel_provider(
    pool: &PgPool,
    channel_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as("SELECT provider FROM channels WHERE id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(p,)| p))
}
