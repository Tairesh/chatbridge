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

/// Meta serves the Instagram OAuth flow from three different hosts. They are
/// grouped so a local run can point all of them at one fake API with a single
/// environment variable.
#[derive(Clone, Debug)]
pub struct InstagramEndpoints {
    /// Where the browser is sent to authorize. `https://www.instagram.com`.
    pub authorize: String,
    /// Where the authorization code is exchanged. `https://api.instagram.com`.
    pub api: String,
    /// Everything else, version segment included. v26.0 is current, released
    /// 2026-07-29. `https://graph.instagram.com/v26.0`.
    pub graph: String,
}

impl Default for InstagramEndpoints {
    fn default() -> Self {
        Self {
            authorize: "https://www.instagram.com".into(),
            api: "https://api.instagram.com".into(),
            graph: "https://graph.instagram.com/v26.0".into(),
        }
    }
}

impl InstagramEndpoints {
    /// Point every host at one base. The real `graph` value carries a `/v26.0`
    /// version segment and an override does not, so a mock serves the bare paths.
    pub fn single(base: &str) -> Self {
        let base = base.trim_end_matches('/').to_owned();
        Self {
            authorize: base.clone(),
            api: base.clone(),
            graph: base,
        }
    }

    pub fn from_env() -> Self {
        match std::env::var("INSTAGRAM_API_BASE") {
            Ok(base) => Self::single(&base),
            Err(_) => Self::default(),
        }
    }
}

#[derive(Clone)]
pub struct AppConfig {
    pub instagram_verify_token: String,
    pub instagram_app_secret: String,
    /// Public id of the *Instagram* app — not the Meta app's id from
    /// Settings → Basic. Pasting the Meta value fails with non-obvious
    /// authentication errors.
    pub instagram_app_id: String,
    pub instagram: InstagramEndpoints,
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
            instagram_app_id: std::env::var("INSTAGRAM_APP_ID")
                .expect("INSTAGRAM_APP_ID must be set"),
            instagram: InstagramEndpoints::from_env(),
            redis_url: std::env::var("REDIS_URL").expect("REDIS_URL must be set"),
            app_jwt_secret: std::env::var("APP_JWT_SECRET").expect("APP_JWT_SECRET must be set"),
            public_base_url: std::env::var("PUBLIC_BASE_URL")
                .expect("PUBLIC_BASE_URL must be set")
                .trim_end_matches('/')
                .to_owned(),
            telegram_api_base: std::env::var("TELEGRAM_API_BASE")
                .unwrap_or_else(|_| "https://api.telegram.org".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instagram_endpoints_default_to_the_real_meta_hosts() {
        let ep = InstagramEndpoints::default();
        assert_eq!(ep.authorize, "https://www.instagram.com");
        assert_eq!(ep.api, "https://api.instagram.com");
        assert_eq!(ep.graph, "https://graph.instagram.com/v26.0");
    }

    #[test]
    fn a_single_base_collapses_all_three_hosts() {
        // One override is enough for a fake API because the paths beneath the three
        // hosts do not collide: /oauth/authorize, /oauth/access_token, /access_token.
        let ep = InstagramEndpoints::single("http://127.0.0.1:9");
        assert_eq!(ep.authorize, "http://127.0.0.1:9");
        assert_eq!(ep.api, "http://127.0.0.1:9");
        assert_eq!(ep.graph, "http://127.0.0.1:9");
    }

    #[test]
    fn a_single_base_loses_its_trailing_slash() {
        assert_eq!(InstagramEndpoints::single("http://x/").graph, "http://x");
    }
}
