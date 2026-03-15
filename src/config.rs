use std::sync::Arc;

use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::cache::ChannelCache;
use crate::registry::ClientRegistry;

pub struct AppState {
    pub config: AppConfig,
    pub db: PgPool,
    pub redis: ConnectionManager,
    pub cache: Arc<ChannelCache>,
    pub registry: ClientRegistry,
    pub shutdown: CancellationToken,
}

#[derive(Clone)]
pub struct AppConfig {
    pub port: u16,
    pub meta_verify_token: String,
    pub instagram_app_secret: String,
    pub redis_url: String,
    pub widget_jwt_secret: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3000),
            meta_verify_token: std::env::var("META_VERIFY_TOKEN")
                .expect("META_VERIFY_TOKEN must be set"),
            instagram_app_secret: std::env::var("INSTAGRAM_APP_SECRET")
                .expect("INSTAGRAM_APP_SECRET must be set"),
            redis_url: std::env::var("REDIS_URL").expect("REDIS_URL must be set"),
            widget_jwt_secret: std::env::var("WIDGET_JWT_SECRET")
                .expect("WIDGET_JWT_SECRET must be set"),
        }
    }
}
