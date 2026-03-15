use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::model::ProviderKind;

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

pub async fn find_client_by_id(pool: &PgPool, client_id: Uuid) -> Result<bool, sqlx::Error> {
    let row: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM clients WHERE id = $1")
        .bind(client_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
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
