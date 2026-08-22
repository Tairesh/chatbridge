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

/// A row of `channels`. `config` is the provider-specific settings blob; it
/// carries no provider tag, so read it by matching on `provider`.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct Channel {
    pub id: Uuid,
    pub provider: String,
    pub name: String,
    pub external_key: String,
    pub config: serde_json::Value,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// The `channels` columns, in `Channel`'s field order. A macro rather than a
/// `const` so it can be spliced into a statement with `concat!`: sqlx 0.9 takes
/// only `&'static str`, so every query here stays a compile-time literal and no
/// SQL is ever built at runtime.
macro_rules! channel_columns {
    () => {
        "id, provider, name, external_key, config, deleted_at, created_at"
    };
}

/// Hot path: a channel that is not deleted. Used by the webhook and WS handlers.
pub async fn find_live_channel_by_id(
    pool: &PgPool,
    channel_id: Uuid,
) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels WHERE id = $1 AND deleted_at IS NULL"
    ))
    .bind(channel_id)
    .fetch_optional(pool)
    .await
}

/// Hot path: route an inbound event to a channel by the provider's identity.
pub async fn find_live_channel_by_external_key(
    pool: &PgPool,
    provider: ProviderKind,
    external_key: &str,
) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels
         WHERE provider = $1 AND external_key = $2 AND deleted_at IS NULL"
    ))
    .bind(provider.to_string())
    .bind(external_key)
    .fetch_optional(pool)
    .await
}

/// Any state, including soft-deleted. For CRUD and for resolving a 409 body.
pub async fn find_channel_by_id(
    pool: &PgPool,
    channel_id: Uuid,
) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels WHERE id = $1"
    ))
    .bind(channel_id)
    .fetch_optional(pool)
    .await
}

/// Any state, including soft-deleted. Resolves which channel a unique
/// violation collided with.
pub async fn find_channel_by_external_key(
    pool: &PgPool,
    provider: ProviderKind,
    external_key: &str,
) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels WHERE provider = $1 AND external_key = $2"
    ))
    .bind(provider.to_string())
    .bind(external_key)
    .fetch_optional(pool)
    .await
}

/// Every channel, live ones first. Deleted channels are listed too — the
/// settings panel shows them dimmed rather than pretending they are gone.
pub async fn list_channels(pool: &PgPool) -> Result<Vec<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels
         ORDER BY deleted_at NULLS FIRST, created_at DESC"
    ))
    .fetch_all(pool)
    .await
}

/// Live channels of one provider. Small result sets by design — the caller filters
/// on the config blob in Rust rather than casting JSONB in SQL.
pub async fn list_live_channels_by_provider(
    pool: &PgPool,
    provider: ProviderKind,
) -> Result<Vec<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "SELECT ",
        channel_columns!(),
        " FROM channels
         WHERE provider = $1 AND deleted_at IS NULL"
    ))
    .bind(provider.to_string())
    .fetch_all(pool)
    .await
}

/// The caller supplies the id so that it can build the webhook URL before the
/// row exists.
pub async fn insert_channel(
    pool: &PgPool,
    id: Uuid,
    provider: ProviderKind,
    name: &str,
    external_key: &str,
    config: &serde_json::Value,
) -> Result<Channel, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "INSERT INTO channels (id, provider, name, external_key, config)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING ",
        channel_columns!()
    ))
    .bind(id)
    .bind(provider.to_string())
    .bind(name)
    .bind(external_key)
    .bind(config)
    .fetch_one(pool)
    .await
}

/// Write a channel identified by the provider's own identity, creating it or
/// refreshing the one that is already there.
///
/// `name` is deliberately absent from `DO UPDATE`: the channel may have been
/// renamed by an operator, and re-connecting the same account must not undo that.
/// Clearing `deleted_at` is the auto-restore — logging in is an explicit "I want
/// this account", and a popup has nowhere to ask a follow-up question.
///
/// One statement, so the unique index stays the sole arbiter and two simultaneous
/// logins for the same account cannot both insert.
///
/// Returns `(channel, inserted)`. `xmax = 0` is Postgres's own answer to "did this
/// row come from the INSERT branch", and it is the *only* trustworthy one: a
/// `SELECT` before the upsert can be overtaken by a concurrent insert, and the
/// caller uses this flag to decide whether a failure afterwards may physically
/// delete the row. Getting it wrong deletes somebody else's live channel.
pub async fn upsert_channel_by_external_key(
    pool: &PgPool,
    id: Uuid,
    provider: ProviderKind,
    name: &str,
    external_key: &str,
    config: &serde_json::Value,
) -> Result<(Channel, bool), sqlx::Error> {
    use sqlx::{FromRow, Row};

    let row = sqlx::query(concat!(
        "INSERT INTO channels (id, provider, name, external_key, config)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (provider, external_key) DO UPDATE
             SET config = EXCLUDED.config, deleted_at = NULL
         RETURNING ",
        channel_columns!(),
        ", (xmax = 0) AS inserted"
    ))
    .bind(id)
    .bind(provider.to_string())
    .bind(name)
    .bind(external_key)
    .bind(config)
    .fetch_one(pool)
    .await?;

    let inserted: bool = row.try_get("inserted")?;
    Ok((Channel::from_row(&row)?, inserted))
}

/// `None` arguments leave their column untouched. `restore` clears `deleted_at`.
pub async fn update_channel(
    pool: &PgPool,
    id: Uuid,
    name: Option<&str>,
    external_key: Option<&str>,
    config: Option<&serde_json::Value>,
    restore: bool,
) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "UPDATE channels SET
             name         = COALESCE($2, name),
             external_key = COALESCE($3, external_key),
             config       = COALESCE($4, config),
             deleted_at   = CASE WHEN $5 THEN NULL ELSE deleted_at END
         WHERE id = $1
         RETURNING ",
        channel_columns!()
    ))
    .bind(id)
    .bind(name)
    .bind(external_key)
    .bind(config)
    .bind(restore)
    .fetch_optional(pool)
    .await
}

pub async fn soft_delete_channel(pool: &PgPool, id: Uuid) -> Result<Option<Channel>, sqlx::Error> {
    sqlx::query_as::<_, Channel>(concat!(
        "UPDATE channels SET deleted_at = COALESCE(deleted_at, now())
         WHERE id = $1
         RETURNING ",
        channel_columns!()
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
}

/// Physically remove a channel row. This exists for exactly ONE caller: rolling
/// back a create whose `setWebhook` failed. It is safe there and only there,
/// because the row is milliseconds old, so no `chats` or `messages` row can
/// reference it yet. Everything a user can reach uses `soft_delete_channel`.
pub async fn hard_delete_channel(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
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
    pub status: crate::model::MessageStatus,
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
         JOIN channels ch ON ch.id = c.channel_id
         LEFT JOIN LATERAL (
             SELECT m.text, m.created_at, m.sender_type, m.sender_id
             FROM messages m
             WHERE m.chat_id = c.id
             ORDER BY m.created_at DESC
             LIMIT 1
         ) lm ON true
         LEFT JOIN operators op ON lm.sender_type = 'operator' AND op.id = lm.sender_id
         LEFT JOIN clients scl ON lm.sender_type = 'client' AND scl.id = lm.sender_id
         WHERE c.status = 'new' AND ch.deleted_at IS NULL
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
    pub status: crate::model::MessageStatus,
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
        "UPDATE messages SET status = $4
         WHERE chat_id = (
             SELECT chat_id FROM messages WHERE channel_id = $1 AND external_message_id = $2
         )
         AND created_at <= (
             SELECT created_at FROM messages WHERE channel_id = $1 AND external_message_id = $2
         )
         AND status IN ('new', 'delivered')
         AND sender_type != $3
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type",
    )
    .bind(channel_id)
    .bind(external_message_id)
    .bind(reader_type)
    .bind(crate::model::MessageStatus::Read)
    .fetch_all(pool)
    .await
}

/// Mark everything the other side has unread in this customer's active chat.
///
/// The fallback for a read receipt whose anchor we do not have: the receipt overtook
/// the id adoption, or the message was sent from the provider's own app before we
/// stored it. There is deliberately no upper time bound — comparing our `created_at`
/// with the provider's clock is comparing two clocks, and a read means "everything up
/// to here", so with no "here" the honest reading is "everything so far".
pub async fn mark_chat_read(
    pool: &PgPool,
    channel_id: Uuid,
    client_id: Uuid,
    reader_type: &str,
) -> Result<Vec<DbRead>, sqlx::Error> {
    sqlx::query_as::<_, DbRead>(
        // The subquery returns at most one row: idx_chats_active is unique on
        // (client_id, channel_id) where status = 'new'.
        "UPDATE messages SET status = $4
         WHERE chat_id = (
             SELECT id FROM chats
             WHERE client_id = $2 AND channel_id = $1 AND status = 'new'
         )
         AND channel_id = $1
         AND status IN ('new', 'delivered')
         AND sender_type != $3
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type",
    )
    .bind(channel_id)
    .bind(client_id)
    .bind(reader_type)
    .bind(crate::model::MessageStatus::Read)
    .fetch_all(pool)
    .await
}

/// Attach the provider's id to a row stored under a local one.
///
/// Returns `false` when the row is gone. Read receipts and edits arrive keyed by the
/// *provider's* id, so until this runs every receipt for an operator's reply resolves
/// to nothing.
pub async fn adopt_external_message_id(
    pool: &PgPool,
    message_id: Uuid,
    new: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Step one is not optional: without it, a delivery task whose row was deleted
    // would go on to delete whatever else holds `new`.
    let Some((channel_id,)): Option<(Uuid,)> =
        sqlx::query_as("SELECT channel_id FROM messages WHERE id = $1 FOR UPDATE")
            .bind(message_id)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(false);
    };

    // An echo of this very message can arrive before we get here. Both rows then
    // claim the provider's id and only one can keep it: ours, which carries the
    // operator. `id <> $3` keeps a repeated adoption from deleting its own row.
    let duplicate: Option<(Uuid,)> = sqlx::query_as(
        "DELETE FROM messages
         WHERE channel_id = $1 AND external_message_id = $2 AND id <> $3
         RETURNING id",
    )
    .bind(channel_id)
    .bind(new)
    .bind(message_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((dropped,)) = duplicate {
        tracing::warn!(
            %channel_id, %dropped, %new,
            "an echo of this reply arrived before its provider id was stored; \
             dropping the duplicate row"
        );
    }

    sqlx::query("UPDATE messages SET external_message_id = $2 WHERE id = $1")
        .bind(message_id)
        .bind(new)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(true)
}

/// Mark a message the provider accepted.
///
/// `status = 'new'` guards it against the receipt that beat the adoption: a customer
/// with the thread open can read a reply before its delivery task gets this far, and
/// `read` must not fall back to `delivered`.
pub async fn mark_message_delivered(pool: &PgPool, message_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE messages SET status = $2 WHERE id = $1 AND status = 'new'")
        .bind(message_id)
        .bind(crate::model::MessageStatus::Delivered)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Mark a message the provider refused to deliver.
///
/// `status = 'new'` guards it: a row somebody already read cannot then become
/// undelivered. Returns whether anything changed.
pub async fn mark_message_failed(pool: &PgPool, message_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE messages SET status = $2 WHERE id = $1 AND status = 'new'")
        .bind(message_id)
        .bind(crate::model::MessageStatus::Failed)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Mark messages as read by target message UUID (for WS read receipts).
pub async fn mark_messages_read_by_id(
    pool: &PgPool,
    message_id: Uuid,
    reader_type: &str,
) -> Result<Vec<DbRead>, sqlx::Error> {
    sqlx::query_as::<_, DbRead>(
        "UPDATE messages SET status = $3
         WHERE chat_id = (SELECT chat_id FROM messages WHERE id = $1)
         AND created_at <= (SELECT created_at FROM messages WHERE id = $1)
         AND status IN ('new', 'delivered')
         AND sender_type != $2
         RETURNING id, external_message_id, channel_id, chat_id, sender_id, sender_type",
    )
    .bind(message_id)
    .bind(reader_type)
    .bind(crate::model::MessageStatus::Read)
    .fetch_all(pool)
    .await
}

// ── Operator queries ────────────────────────────────────────────────

#[derive(Debug, Clone, FromRow)]
pub struct Operator {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Create an operator with a name.
///
/// The id is generated here rather than by the database because the name is derived
/// from it, and a column default cannot reference its own row. There is no login yet,
/// so nobody gets to choose the name — but "Operator 3f2a" beats a blank, and it stays
/// the same for as long as the row does.
pub async fn create_operator(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::new_v4();
    let name = format!("Operator {}", &id.simple().to_string()[..4]);
    sqlx::query("INSERT INTO operators (id, name) VALUES ($1, $2)")
        .bind(id)
        .bind(&name)
        .execute(pool)
        .await?;
    Ok(id)
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
