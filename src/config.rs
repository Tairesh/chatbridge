use std::sync::Arc;

use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::cache::{ChannelCache, ChatCache, ClientCache, OperatorCache};
use crate::registry::ClientRegistry;

pub struct AppState {
    pub config: AppConfig,
    pub db: PgPool,
    pub redis: ConnectionManager,
    pub cache: Arc<ChannelCache>,
    pub client_cache: Arc<ClientCache>,
    pub operator_cache: Arc<OperatorCache>,
    pub chat_cache: Arc<ChatCache>,
    pub registry: ClientRegistry,
    pub shutdown: CancellationToken,
}

#[derive(Clone)]
pub struct AppConfig {
    pub instagram_verify_token: String,
    pub instagram_app_secret: String,
    pub redis_url: String,
    pub app_jwt_secret: String,
    /// Public origin of this deployment, without a trailing slash. Used to build
    /// webhook URLs for `setWebhook` and the `endpoint` field of a channel.
    pub public_base_url: String,
    /// Telegram Bot API origin. Overridable so a local run can point at a fake
    /// Bot API. Integration tests do not use this path — they build `AppConfig`
    /// directly and pass their mock's URL in.
    pub telegram_api_base: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            instagram_verify_token: std::env::var("INSTAGRAM_VERIFY_TOKEN")
                .expect("INSTAGRAM_VERIFY_TOKEN must be set"),
            instagram_app_secret: std::env::var("INSTAGRAM_APP_SECRET")
                .expect("INSTAGRAM_APP_SECRET must be set"),
            redis_url: std::env::var("REDIS_URL").expect("REDIS_URL must be set"),
            app_jwt_secret: std::env::var("APP_JWT_SECRET")
                .expect("APP_JWT_SECRET must be set"),
            public_base_url: std::env::var("PUBLIC_BASE_URL")
                .expect("PUBLIC_BASE_URL must be set")
                .trim_end_matches('/')
                .to_owned(),
            telegram_api_base: std::env::var("TELEGRAM_API_BASE")
                .unwrap_or_else(|_| "https://api.telegram.org".into()),
        }
    }
}
