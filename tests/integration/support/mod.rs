//! Test infrastructure: pools, app state, and the server under test.
#![allow(dead_code)]

pub mod fixtures;
pub mod http;
pub mod mocks;
pub mod ws;

pub use fixtures::*;
pub use http::*;
pub use mocks::*;
pub use ws::*;

use std::sync::Arc;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use chatbridge::cache::{ChatCache, ClientCache, OperatorCache};
use chatbridge::config::{AppConfig, AppState};
use chatbridge::registry::ClientRegistry;
use chatbridge::routes;

pub const TEST_VERIFY_TOKEN: &str = "test_verify_token";

pub const TEST_APP_SECRET: &str = "test_app_secret";

pub const TEST_JWT_SECRET: &str = "test-jwt-secret-at-least-32-bytes!!";

pub fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

// `setup_pool` lives in `common`, shared with tests/client_identity.rs. It is not
// re-exported here: two globs offering the same name make every call site ambiguous.

pub async fn setup_redis() -> redis::aio::ConnectionManager {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let client = redis::Client::open(url.as_str()).expect("invalid REDIS_URL");
    redis::aio::ConnectionManager::new(client)
        .await
        .expect("failed to connect to Redis")
}

pub const TEST_PUBLIC_BASE_URL: &str = "https://test.example.com";

/// Port 1 is never listening and needs no DNS lookup, so any provider call made
/// by a test that forgot to pass a mock fails instantly and locally instead of
/// reaching out to the real API. The test suite must make zero outbound requests.
pub const NO_TELEGRAM: &str = "http://127.0.0.1:1";

pub const NO_INSTAGRAM: &str = "http://127.0.0.1:1";

pub const TEST_APP_ID: &str = "1234567890";

pub async fn build_state(db: PgPool) -> Arc<AppState> {
    build_state_full(db, NO_TELEGRAM.into(), NO_INSTAGRAM.into()).await
}

/// A test that needs Telegram.
pub async fn build_state_with(db: PgPool, telegram_api_base: String) -> Arc<AppState> {
    build_state_full(db, telegram_api_base, NO_INSTAGRAM.into()).await
}

/// A test that needs Instagram.
pub async fn build_state_ig(db: PgPool, instagram_base: String) -> Arc<AppState> {
    build_state_full(db, NO_TELEGRAM.into(), instagram_base).await
}

pub async fn build_state_full(
    db: PgPool,
    telegram_api_base: String,
    instagram_base: String,
) -> Arc<AppState> {
    let redis = setup_redis().await;
    Arc::new(AppState {
        config: AppConfig {
            instagram_verify_token: TEST_VERIFY_TOKEN.into(),
            instagram_app_secret: TEST_APP_SECRET.into(),
            instagram_app_id: TEST_APP_ID.into(),
            instagram: chatbridge::config::InstagramEndpoints::single(&instagram_base),
            redis_url: "redis://localhost:6379".into(),
            app_jwt_secret: TEST_JWT_SECRET.into(),
            public_base_url: TEST_PUBLIC_BASE_URL.into(),
            telegram_api_base,
        },
        db,
        redis,
        cache: Arc::new(Default::default()),
        client_cache: Arc::new(ClientCache::new()),
        operator_cache: Arc::new(OperatorCache::new()),
        chat_cache: Arc::new(ChatCache::new()),
        registry: ClientRegistry::new(),
        shutdown: CancellationToken::new(),
    })
}

/// Start the app on a random port and return the address.
pub async fn spawn_app(state: Arc<AppState>) -> std::net::SocketAddr {
    chatbridge::listener::spawn_message_listener(state.clone()).await;
    let app = routes::build(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}
