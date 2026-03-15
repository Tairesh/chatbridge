use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

pub async fn init_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(10)
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
    // pub access_token: String,
}

pub async fn find_instagram_channel_by_user_id(
    pool: &PgPool,
    user_id: &str,
) -> Result<Option<InstagramChannel>, sqlx::Error> {
    sqlx::query_as::<_, InstagramChannel>(
        "SELECT id, user_id FROM instagram_channels WHERE user_id = $1",
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
