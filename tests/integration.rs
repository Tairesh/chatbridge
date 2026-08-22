mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use tokio_tungstenite::tungstenite;
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::cache::{ChatCache, ClientCache, OperatorCache};
use chatbridge::config::{AppConfig, AppState};
use chatbridge::model::ProviderKind;
use chatbridge::registry::ClientRegistry;
use chatbridge::routes;
use common::{TestChannel, TestChannelKey, TestChat, TestClient, TestOperator};
use tokio_util::sync::CancellationToken;

const TEST_VERIFY_TOKEN: &str = "test_verify_token";
const TEST_APP_SECRET: &str = "test_app_secret";
const TEST_JWT_SECRET: &str = "test-jwt-secret-at-least-32-bytes!!";

/// Insert a channel row. `config` is the provider-specific settings blob.
async fn insert_test_channel(
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

async fn insert_test_instagram_channel(pool: &PgPool) -> (TestChannel, String) {
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

async fn insert_test_telegram_channel(pool: &PgPool, bot_secret: &str) -> TestChannel {
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

async fn insert_test_widget_channel(pool: &PgPool, widget_id: &str) -> TestChannel {
    insert_test_channel(pool, "widget", widget_id, serde_json::json!({})).await
}

async fn insert_test_client(pool: &PgPool, name: &str) -> TestClient {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO clients (id, provider, name) VALUES ($1, 'widget', $2)")
        .bind(id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    TestClient { id }
}

async fn insert_test_chat(
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

// --- Test helpers ---

/// Wait for a Redis message on the `incoming_messages` channel that matches the given channel_id.
/// Skips messages from other channels (concurrent tests).
async fn wait_for_redis_msg(
    stream: &mut (impl futures_util::Stream<Item = redis::Msg> + Unpin),
    expected_channel_id: Uuid,
) -> serde_json::Value {
    use futures_util::StreamExt;
    let deadline = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            panic!("timed out waiting for Redis message for channel {expected_channel_id}");
        }
        let msg = tokio::time::timeout(remaining, stream.next())
            .await
            .expect("timed out waiting for Redis message")
            .unwrap();
        let payload: String = msg.get_payload().unwrap();
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        if value["channel_id"] == expected_channel_id.to_string() {
            return value;
        }
    }
}

fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

async fn setup_pool() -> PgPool {
    common::setup_pool().await
}

async fn setup_redis() -> redis::aio::ConnectionManager {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let client = redis::Client::open(url.as_str()).expect("invalid REDIS_URL");
    redis::aio::ConnectionManager::new(client)
        .await
        .expect("failed to connect to Redis")
}

const TEST_PUBLIC_BASE_URL: &str = "https://test.example.com";

/// Port 1 is never listening and needs no DNS lookup, so any provider call made
/// by a test that forgot to pass a mock fails instantly and locally instead of
/// reaching out to the real API. The test suite must make zero outbound requests.
const NO_TELEGRAM: &str = "http://127.0.0.1:1";
const NO_INSTAGRAM: &str = "http://127.0.0.1:1";
const TEST_APP_ID: &str = "1234567890";

async fn build_state(db: PgPool) -> Arc<AppState> {
    build_state_full(db, NO_TELEGRAM.into(), NO_INSTAGRAM.into()).await
}

/// A test that needs Telegram.
async fn build_state_with(db: PgPool, telegram_api_base: String) -> Arc<AppState> {
    build_state_full(db, telegram_api_base, NO_INSTAGRAM.into()).await
}

/// A test that needs Instagram.
async fn build_state_ig(db: PgPool, instagram_base: String) -> Arc<AppState> {
    build_state_full(db, NO_TELEGRAM.into(), instagram_base).await
}

async fn build_state_full(
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
async fn spawn_app(state: Arc<AppState>) -> std::net::SocketAddr {
    chatbridge::listener::spawn_message_listener(state.clone()).await;
    let app = routes::build(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

// --- Meta verify (GET) ---

#[tokio::test]
async fn meta_verify_valid() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/webhook/instagram?hub.mode=subscribe&hub.verify_token={TEST_VERIFY_TOKEN}&hub.challenge=challenge_123"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"challenge_123");
}

#[tokio::test]
async fn meta_verify_invalid_token() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/webhook/instagram?hub.mode=subscribe&hub.verify_token=wrong&hub.challenge=x")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn meta_verify_wrong_mode() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/webhook/instagram?hub.mode=unsubscribe&hub.verify_token={TEST_VERIFY_TOKEN}&hub.challenge=x"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// --- Instagram ingest (POST) ---

#[tokio::test]
async fn instagram_ingest_valid_signature() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "time": 1773347860136_i64,
            "id": "17841448717199999",
            "messaging": [{
                "sender": {"id": "836189122827510"},
                "recipient": {"id": "17841448717199999"},
                "timestamp": 1773347859458_i64,
                "message": {"mid": "aWdf_test", "text": "hello"}
            }]
        }]
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let sig = sign_body(TEST_APP_SECRET, &body_bytes);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/webhook/instagram")
                .header("content-type", "application/json")
                .header("X-Hub-Signature-256", format!("sha256={sig}"))
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn instagram_ingest_invalid_signature() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let body = b"some payload";

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/webhook/instagram")
                .header("content-type", "application/json")
                .header("X-Hub-Signature-256", "sha256=bad_signature")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn instagram_ingest_missing_signature() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/webhook/instagram")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// --- Instagram ingest with channel lookup ---

#[tokio::test]
async fn instagram_ingest_with_channel_lookup() {
    let pool = setup_pool().await;
    let (_guard, user_id) = insert_test_instagram_channel(&pool).await;
    let sender_id = Uuid::new_v4().simple().to_string();

    let app = routes::build(build_state(pool.clone()).await);

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "time": 1773347860136_i64,
            "id": &user_id,
            "messaging": [{
                "sender": {"id": &sender_id},
                "recipient": {"id": &user_id},
                "timestamp": 1773347859458_i64,
                "message": {"mid": "aWdf_lookup", "text": "hello"}
            }]
        }]
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let sig = sign_body(TEST_APP_SECRET, &body_bytes);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/webhook/instagram")
                .header("content-type", "application/json")
                .header("X-Hub-Signature-256", format!("sha256={sig}"))
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Give the background task a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Clean up the client created by the background task
    let _client_guard =
        chatbridge::db::find_client_by_external_id(&pool, ProviderKind::Instagram, &sender_id)
            .await
            .ok()
            .flatten()
            .map(|c| TestClient { id: c.id });
}

// --- Telegram ingest (POST) ---

#[tokio::test]
async fn telegram_ingest_valid() {
    let pool = setup_pool().await;
    let bot_secret = "test_bot_secret";
    let guard = insert_test_telegram_channel(&pool, bot_secret).await;
    let sender_id = Uuid::new_v4().as_u128() as i64;

    let app = routes::build(build_state(pool.clone()).await);

    let body = serde_json::json!({
        "update_id": 100,
        "message": {
            "message_id": 42,
            "date": 1700000000,
            "from": {"id": sender_id, "first_name": "Test"},
            "chat": {"id": 123, "type": "private"},
            "text": "hello bot"
        }
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{}", guard.id))
                .header("content-type", "application/json")
                .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Wait for background client upsert to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Clean up the client created by the background task
    let _client_guard = chatbridge::db::find_client_by_external_id(
        &pool,
        ProviderKind::Telegram,
        &sender_id.to_string(),
    )
    .await
    .ok()
    .flatten()
    .map(|c| TestClient { id: c.id });
}

#[tokio::test]
async fn telegram_ingest_invalid_secret() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "correct_secret").await;

    let app = routes::build(build_state(pool.clone()).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{}", guard.id))
                .header("content-type", "application/json")
                .header("X-Telegram-Bot-Api-Secret-Token", "wrong_secret")
                .body(Body::from(r#"{"update_id":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn telegram_ingest_unknown_channel() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let fake_id = Uuid::new_v4();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{fake_id}"))
                .header("content-type", "application/json")
                .header("X-Telegram-Bot-Api-Secret-Token", "whatever")
                .body(Body::from(r#"{"update_id":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// --- WebSocket widget tests ---

use chatbridge::cache::ChannelCache;
use futures_util::{SinkExt, StreamExt};

/// Connect to a WS endpoint and consume the initial auth message.
/// Returns the websocket stream, the JWT token, and a cleanup guard for the client row.
async fn ws_connect(
    addr: std::net::SocketAddr,
    widget_id: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
    TestClient,
) {
    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // First message should be auth
    let resp = ws.next().await.unwrap().unwrap();
    let auth: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(auth["action"], "auth");
    let token = auth["token"].as_str().unwrap().to_string();
    let client_id = chatbridge::jwt::verify(&token, TEST_JWT_SECRET.as_bytes())
        .expect("auth token should be a valid JWT");
    let client_guard = TestClient { id: client_id };

    (ws, token, client_guard)
}

#[tokio::test]
async fn ws_connect_and_receive_ack() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send a valid message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "550e8400-e29b-41d4-a716-446655440000", "text": "Hello", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();

    // Receive ACK
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");
    assert_eq!(ack["message_id"], "550e8400-e29b-41d4-a716-446655440000");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_message_with_attachments() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    let attachment_id = Uuid::new_v4();
    let msg = serde_json::json!({
        "action": "send",
        "mid": "550e8400-e29b-41d4-a716-446655440000",
        "text": "See attached",
        "attachments": [attachment_id.to_string()]
    });
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");
    assert_eq!(ack["message_id"], "550e8400-e29b-41d4-a716-446655440000");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_invalid_json_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send invalid JSON
    ws.send(tungstenite::Message::Text("not json".into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");
    assert!(err["reason"].as_str().unwrap().contains("invalid message"));

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_missing_text_field_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Valid JSON but missing required "text" field
    ws.send(tungstenite::Message::Text(r#"{"attachments": []}"#.into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_missing_message_id_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Valid JSON with "text" field but without "mid" field
    ws.send(tungstenite::Message::Text(r#"{"text": "Hello"}"#.into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_unknown_widget_id_rejects() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/nonexistent_widget");
    let result = tokio_tungstenite::connect_async(&url).await;

    // Server should respond with non-101 status (404), causing connection failure
    assert!(result.is_err());
}

#[tokio::test]
async fn ws_multiple_messages_get_individual_acks() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    let mut seen_ids = std::collections::HashSet::new();

    for i in 0..3 {
        let mid = Uuid::new_v4();
        let msg = serde_json::json!({"action": "send", "text": format!("msg {i}"), "mid": mid.to_string()});
        ws.send(tungstenite::Message::Text(msg.to_string().into()))
            .await
            .unwrap();

        let resp = ws.next().await.unwrap().unwrap();
        let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
        assert_eq!(ack["action"], "ack");
        // Each ACK should have a unique message_id
        let mid = ack["message_id"].as_str().unwrap().to_string();
        assert!(seen_ids.insert(mid), "duplicate message_id");
    }

    assert_eq!(seen_ids.len(), 3);

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_continues_after_bad_message() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send bad message
    ws.send(tungstenite::Message::Text("bad".into()))
        .await
        .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");

    // Connection should still be alive — send valid message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "still here", "mid": "550e8400-e29b-41d4-a716-446655440000"}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_publishes_to_redis() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "redis test", "mid": "550e8400-e29b-41d4-a716-446655440000"}"#.into(),
    ))
    .await
    .unwrap();

    // Consume the ACK
    let _ = ws.next().await.unwrap().unwrap();

    // Check Redis received the published message
    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["type"], "message");
    assert_eq!(internal["text"], "redis test");
    assert_eq!(internal["status"], "new");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn instagram_publishes_to_redis() {
    let pool = setup_pool().await;
    let (guard, user_id) = insert_test_instagram_channel(&pool).await;
    let sender_id = Uuid::new_v4().simple().to_string();

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "time": 1773347860136_i64,
            "id": &user_id,
            "messaging": [{
                "sender": {"id": &sender_id},
                "recipient": {"id": &user_id},
                "timestamp": 1773347859458_i64,
                "message": {"mid": "aWdf_redis", "text": "redis ig test"}
            }]
        }]
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let sig = sign_body(TEST_APP_SECRET, &body_bytes);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/webhook/instagram"))
        .header("content-type", "application/json")
        .header("X-Hub-Signature-256", format!("sha256={sig}"))
        .body(body_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["type"], "message");

    // Clean up the client created by the background task
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _client_guard =
        chatbridge::db::find_client_by_external_id(&pool, ProviderKind::Instagram, &sender_id)
            .await
            .ok()
            .flatten()
            .map(|c| TestClient { id: c.id });
}

#[tokio::test]
async fn telegram_publishes_to_redis() {
    let pool = setup_pool().await;
    let bot_secret = "test_bot_secret_redis";
    let guard = insert_test_telegram_channel(&pool, bot_secret).await;
    let sender_id = Uuid::new_v4().as_u128() as i64;

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let body = serde_json::json!({
        "update_id": 100,
        "message": {
            "message_id": 42,
            "date": 1700000000,
            "from": {"id": sender_id, "first_name": "Test"},
            "chat": {"id": 123, "type": "private"},
            "text": "redis tg test"
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/webhook/telegram/{}", guard.id))
        .header("content-type", "application/json")
        .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
        .body(serde_json::to_vec(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["type"], "message");

    // Clean up the client created by the background task
    let _client_guard = chatbridge::db::find_client_by_external_id(
        &pool,
        ProviderKind::Telegram,
        &sender_id.to_string(),
    )
    .await
    .ok()
    .flatten()
    .map(|c| TestClient { id: c.id });
}

// --- WebSocket edit action tests ---

#[tokio::test]
async fn ws_edit_message_returns_ack() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send original message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "660e8400-e29b-41d4-a716-446655440001", "text": "Helo", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");
    assert_eq!(ack["message_id"], "660e8400-e29b-41d4-a716-446655440001");

    // Edit the message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "edit", "mid": "660e8400-e29b-41d4-a716-446655440001", "text": "Hello"}"#
            .into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");
    assert_eq!(ack["message_id"], "660e8400-e29b-41d4-a716-446655440001");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_edit_publishes_edit_event_to_redis() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send original message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "770e8400-e29b-41d4-a716-446655440002", "text": "Helo", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();
    let _ = ws.next().await.unwrap().unwrap();

    // Consume the send event from Redis
    let _ = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;

    // Edit the message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "edit", "mid": "770e8400-e29b-41d4-a716-446655440002", "text": "Hello"}"#
            .into(),
    ))
    .await
    .unwrap();
    let _ = ws.next().await.unwrap().unwrap();

    // Check Redis received the edit event
    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["type"], "edit");
    assert_eq!(internal["text"], "Hello");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_unknown_action_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send message with unknown action
    ws.send(tungstenite::Message::Text(
        r#"{"action": "delete", "mid": "880e8400-e29b-41d4-a716-446655440003", "text": "x"}"#
            .into(),
    ))
    .await
    .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");
    assert!(
        err["reason"]
            .as_str()
            .unwrap()
            .contains("invalid message: unknown variant `delete`")
    );

    // Connection should still be alive
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "990e8400-e29b-41d4-a716-446655440004", "text": "still alive"}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_invalid_mid_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send message with arbitrary string as mid — should be rejected
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "arbitrary-string", "text": "Hello"}"#.into(),
    ))
    .await
    .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["action"], "error");
    assert!(err["reason"].as_str().unwrap().contains("invalid message"));

    // Connection should still be alive after rejected mid
    let valid_mid = Uuid::new_v4();
    let msg = serde_json::json!({"action": "send", "mid": valid_mid.to_string(), "text": "ok"});
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");

    ws.close(None).await.unwrap();
}

// --- Cache + invalidation tests ---

#[tokio::test]
async fn cache_lookup_by_external_key_and_invalidation() {
    let pool = setup_pool().await;
    let widget_id = format!("cache_test_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    let cache = Arc::new(ChannelCache::new());

    // First lookup — cache miss, loads from DB
    let ch = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);

    // Delete from DB — a cached entry must still be served
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    cache.invalidate(guard.id);

    let after = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap();
    assert!(after.is_none(), "None after invalidation + DB delete");
}

#[tokio::test]
async fn cache_lookup_by_id_and_invalidation() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "cache_secret").await;

    let cache = Arc::new(ChannelCache::new());

    let ch = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);
    assert_eq!(ch.config["bot_secret"], "cache_secret");

    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    cache.invalidate(guard.id);
    assert!(
        cache
            .get_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cache_invalidate_by_id_clears_the_external_key_index() {
    let pool = setup_pool().await;
    let widget_id = format!("cache_test_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    let cache = Arc::new(ChannelCache::new());
    // Warm both maps through the key lookup, then evict by id only.
    cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("channel should exist");

    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    cache.invalidate(guard.id);

    assert!(
        cache
            .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
            .await
            .unwrap()
            .is_none(),
        "invalidating by id must also drop the secondary index entry"
    );
}

#[tokio::test]
async fn cache_invalidation_via_redis_pubsub() {
    let pool = setup_pool().await;
    let bot_secret = "redis_inv_secret";
    let guard = insert_test_telegram_channel(&pool, bot_secret).await;

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let cache = Arc::new(ChannelCache::new());

    // Populate cache
    let ch = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.config["bot_secret"], bot_secret);

    // Start invalidation listener
    chatbridge::cache::spawn_invalidation_listener(
        &redis_url,
        cache.clone(),
        Arc::new(ClientCache::new()),
        Arc::new(OperatorCache::new()),
        Arc::new(ChatCache::new()),
    )
    .await;

    // Delete from DB so we can detect cache eviction
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    // Publish invalidation via Redis
    let mut redis = setup_redis().await;
    redis::AsyncCommands::publish::<_, _, ()>(
        &mut redis,
        chatbridge::cache::INVALIDATION_CHANNEL,
        format!("channel:{}", guard.id),
    )
    .await
    .unwrap();

    // Give the listener a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cache should be evicted, DB is empty → None
    let after = cache.get_channel_by_id(&pool, guard.id).await.unwrap();
    assert!(
        after.is_none(),
        "should be None after Redis pubsub invalidation"
    );
}

#[tokio::test]
async fn cache_invalidation_does_not_affect_other_channels() {
    let pool = setup_pool().await;
    let guard_a = insert_test_telegram_channel(&pool, "secret_a").await;
    let guard_b = insert_test_telegram_channel(&pool, "secret_b").await;

    let cache = Arc::new(ChannelCache::new());

    // Populate both
    cache
        .get_channel_by_id(&pool, guard_a.id)
        .await
        .unwrap()
        .unwrap();
    cache
        .get_channel_by_id(&pool, guard_b.id)
        .await
        .unwrap()
        .unwrap();

    // Invalidate only A
    cache.invalidate(guard_a.id);

    // B should still be cached even if we delete it from DB
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard_b.id)
        .execute(&pool)
        .await
        .unwrap();
    let b = cache
        .get_channel_by_id(&pool, guard_b.id)
        .await
        .unwrap()
        .expect("channel B should still be cached");
    assert_eq!(b.config["bot_secret"], "secret_b");
}

#[tokio::test]
async fn instagram_rejects_non_instagram_object() {
    use chatbridge::provider::WebhookProvider;
    use chatbridge::provider::instagram::InstagramProvider;

    fn test_provider() -> InstagramProvider {
        InstagramProvider::new(
            TEST_APP_SECRET,
            NO_INSTAGRAM,
            Arc::new(ChannelCache::new()),
            Arc::new(ClientCache::new()),
        )
    }

    let pool = setup_pool().await;
    let provider = test_provider();

    let body = serde_json::json!({
        "object": "page",
        "entry": [{
            "time": 1773347860136_i64,
            "id": "12345",
            "messaging": []
        }]
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();

    let redis = setup_redis().await;
    let err = provider.parse(&body_bytes, &pool, redis).await.unwrap_err();
    assert!(err.to_string().contains("unexpected object: page"));
}

// --- JWT client identity tests ---

#[tokio::test]
async fn ws_returns_auth_on_first_connect() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (_ws, token, _client) = ws_connect(addr, &widget_id).await;

    // Token should be a valid JWT
    let client_id = chatbridge::jwt::verify(&token, TEST_JWT_SECRET.as_bytes());
    assert!(client_id.is_some(), "token should be a valid JWT");
}

#[tokio::test]
async fn ws_reconnect_with_token_skips_auth() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // First connect — get token
    let (mut ws1, token, _client) = ws_connect(addr, &widget_id).await;
    ws1.close(None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Reconnect with token — should NOT get an auth message
    let url = format!(
        "ws://{addr}/ws/{widget_id}?token={}",
        urlencoding::encode(&token)
    );
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send a message — first response should be ack, not auth
    ws2.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "550e8400-e29b-41d4-a716-446655440000", "text": "Hello"}"#
            .into(),
    ))
    .await
    .unwrap();

    let resp = ws2.next().await.unwrap().unwrap();
    let msg: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(
        msg["action"], "ack",
        "returning client should not get auth message"
    );

    ws2.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_invalid_token_gets_new_auth() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // Connect with garbage token
    let url = format!("ws://{addr}/ws/{widget_id}?token=garbage.invalid.token");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Should get a fresh auth message
    let resp = ws.next().await.unwrap().unwrap();
    let auth: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(auth["action"], "auth");
    let token = auth["token"].as_str().unwrap();
    assert!(token.contains('.'), "should be a JWT");
    let _client = TestClient {
        id: chatbridge::jwt::verify(token, TEST_JWT_SECRET.as_bytes()).unwrap(),
    };

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_redis_message_includes_client_id() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis channel before connecting
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, token, _client) = ws_connect(addr, &widget_id).await;
    let client_id = chatbridge::jwt::verify(&token, TEST_JWT_SECRET.as_bytes()).unwrap();

    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "redis client test", "mid": "550e8400-e29b-41d4-a716-446655440000"}"#.into(),
    ))
    .await
    .unwrap();

    // Consume ACK
    let _ = ws.next().await.unwrap().unwrap();

    // Check Redis message has client_id
    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["sender"]["id"], client_id.to_string());

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_multi_tab_same_token_both_work() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // First tab — get token
    let (mut ws1, token, _client) = ws_connect(addr, &widget_id).await;

    // Second tab — connect with the same token
    let url = format!(
        "ws://{addr}/ws/{widget_id}?token={}",
        urlencoding::encode(&token)
    );
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Both tabs should work independently
    let mid1 = Uuid::new_v4();
    let msg1 = serde_json::json!({"action": "send", "mid": mid1.to_string(), "text": "from tab 1"});
    ws1.send(tungstenite::Message::Text(msg1.to_string().into()))
        .await
        .unwrap();
    let resp1 = ws1.next().await.unwrap().unwrap();
    let ack1: serde_json::Value = serde_json::from_str(resp1.to_text().unwrap()).unwrap();
    assert_eq!(ack1["action"], "ack");

    let mid2 = Uuid::new_v4();
    let msg2 = serde_json::json!({"action": "send", "mid": mid2.to_string(), "text": "from tab 2"});
    ws2.send(tungstenite::Message::Text(msg2.to_string().into()))
        .await
        .unwrap();
    // This tab may or may not receive a connect-time chat event: the handshake
    // completes before the server task runs find_last_chat, so whether a chat
    // exists by then depends on how tab 1's message interleaves.
    let ack2 = loop {
        let resp2 = ws2.next().await.unwrap().unwrap();
        let frame: serde_json::Value = serde_json::from_str(resp2.to_text().unwrap()).unwrap();
        if frame["action"] == "chat" {
            continue;
        }
        break frame;
    };
    assert_eq!(ack2["action"], "ack");

    ws1.close(None).await.unwrap();
    ws2.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_valid_token_deleted_client_gets_new_auth() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // First connect — get token and client_id
    // Don't use the TestClient guard — we delete this client manually below.
    let (mut ws1, token, first_client) = ws_connect(addr, &widget_id).await;
    let client_id = first_client.id;
    // Defuse the guard — we'll delete it ourselves.
    std::mem::forget(first_client);
    ws1.close(None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Delete the client from DB
    sqlx::query("DELETE FROM clients WHERE id = $1")
        .bind(client_id)
        .execute(&pool)
        .await
        .unwrap();

    // Reconnect with old token — client is gone, should get new auth
    let url = format!(
        "ws://{addr}/ws/{widget_id}?token={}",
        urlencoding::encode(&token)
    );
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let resp = ws2.next().await.unwrap().unwrap();
    let auth: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(auth["action"], "auth");

    // New token should have a different client_id
    let new_client_id =
        chatbridge::jwt::verify(auth["token"].as_str().unwrap(), TEST_JWT_SECRET.as_bytes())
            .unwrap();
    assert_ne!(new_client_id, client_id, "should be a new client");
    let _client = TestClient { id: new_client_id };

    ws2.close(None).await.unwrap();
}

// ---------------------------------------------------------------------------
// Telegram client reuse
// ---------------------------------------------------------------------------

#[tokio::test]
async fn telegram_ingest_reuses_client_id() {
    let pool = setup_pool().await;
    let state = build_state(pool.clone()).await;

    let bot_secret = "reuse_secret";
    let _ch = insert_test_telegram_channel(&pool, bot_secret).await;
    let channel_id = _ch.id;

    let telegram_user_id = 99887766_i64;
    let body = serde_json::json!({
        "update_id": 200,
        "message": {
            "message_id": 50,
            "date": 1700000000,
            "from": {"id": telegram_user_id, "first_name": "Reuse"},
            "chat": {"id": telegram_user_id, "type": "private"},
            "text": "first message"
        }
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();

    let app = routes::build(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{channel_id}"))
                .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
                .header("content-type", "application/json")
                .body(Body::from(body_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Wait for background upsert
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify client was created
    let client = chatbridge::db::find_client_by_external_id(
        &pool,
        chatbridge::model::ProviderKind::Telegram,
        &telegram_user_id.to_string(),
    )
    .await
    .unwrap()
    .expect("client should exist after first message");

    let _client_guard = TestClient { id: client.id };
    let first_client_id = client.id;

    // Send second message from same user
    let body2 = serde_json::json!({
        "update_id": 201,
        "message": {
            "message_id": 51,
            "date": 1700000001,
            "from": {"id": telegram_user_id, "first_name": "Reuse"},
            "chat": {"id": telegram_user_id, "type": "private"},
            "text": "second message"
        }
    });
    let body2_bytes = serde_json::to_vec(&body2).unwrap();

    let app2 = routes::build(state.clone());
    let resp2 = app2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{channel_id}"))
                .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
                .header("content-type", "application/json")
                .body(Body::from(body2_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // Wait for background processing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify same client_id — no duplicate
    let client2 = chatbridge::db::find_client_by_external_id(
        &pool,
        chatbridge::model::ProviderKind::Telegram,
        &telegram_user_id.to_string(),
    )
    .await
    .unwrap()
    .expect("client should still exist");

    assert_eq!(
        first_client_id, client2.id,
        "second message should reuse the same client_id"
    );
}

// ---------------------------------------------------------------------------
// Client cache invalidation via Redis pub/sub
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_cache_invalidated_via_redis_pubsub() {
    let pool = setup_pool().await;
    let mut redis = setup_redis().await;
    let client_cache = Arc::new(chatbridge::cache::ClientCache::new());

    // Insert a client
    let client_id = Uuid::new_v4();
    chatbridge::db::upsert_client(
        &pool,
        client_id,
        chatbridge::model::ProviderKind::Telegram,
        "invalidation_test_user",
        Some("Old Name"),
        None,
    )
    .await
    .unwrap();
    let _client_guard = TestClient { id: client_id };

    // Populate cache via read-through
    let cached = client_cache
        .get_client(
            &pool,
            chatbridge::model::ProviderKind::Telegram,
            "invalidation_test_user",
        )
        .await
        .unwrap();
    assert_eq!(cached.unwrap().name.as_deref(), Some("Old Name"));

    // Start invalidation listener
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let channel_cache = Arc::new(chatbridge::cache::ChannelCache::new());
    chatbridge::cache::spawn_invalidation_listener(
        &redis_url,
        channel_cache,
        client_cache.clone(),
        Arc::new(OperatorCache::new()),
        Arc::new(ChatCache::new()),
    )
    .await;

    // Give listener time to subscribe
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish invalidation
    chatbridge::cache::publish_invalidation(&mut redis, "client", client_id).await;

    // Wait for invalidation to propagate
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Update DB with new name
    chatbridge::db::upsert_client(
        &pool,
        client_id,
        chatbridge::model::ProviderKind::Telegram,
        "invalidation_test_user",
        Some("New Name"),
        None,
    )
    .await
    .unwrap();

    // Cache should have been cleared — next read-through returns fresh data
    let refreshed = client_cache
        .get_client(
            &pool,
            chatbridge::model::ProviderKind::Telegram,
            "invalidation_test_user",
        )
        .await
        .unwrap();
    assert_eq!(
        refreshed.unwrap().name.as_deref(),
        Some("New Name"),
        "cache should return fresh DB data after invalidation"
    );
}

// --- Chat and message persistence tests ---

#[tokio::test]
async fn find_or_create_chat_creates_new() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("chat_test_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };

    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();

    let _chat_guard = common::TestChat { id: chat_id };

    // Calling again returns the same chat
    let chat_id2 = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    assert_eq!(chat_id, chat_id2);
}

#[tokio::test]
async fn insert_message_returns_incoming_with_id() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("msg_test_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };

    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    let _chat_guard = common::TestChat { id: chat_id };

    let new_msg = NewMessage {
        external_message_id: "widget:test-mid".into(),
        channel_id: channel.id,
        sender_id: Some(client_id),
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("hello".into()),
        raw: serde_json::json!({"action": "send", "text": "hello"}),
    };

    let incoming = chatbridge::db::insert_message(&pool, &new_msg, Some(chat_id))
        .await
        .unwrap()
        .expect("should not be a duplicate");

    let _msg_guard = common::TestMessage { id: incoming.id };

    assert_eq!(incoming.external_message_id, "widget:test-mid");
    assert_eq!(incoming.channel_id, channel.id);
    assert_eq!(incoming.chat_id, Some(chat_id));
    assert_eq!(incoming.sender_id, Some(client_id));
    assert_eq!(incoming.text.as_deref(), Some("hello"));
    assert_eq!(incoming.status, "new");
}

#[tokio::test]
async fn insert_message_without_sender_has_no_chat() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("nosender_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "instagram:mid_orphan".into(),
        channel_id: channel.id,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Instagram,
        event: EventKind::Message,
        text: Some("orphan msg".into()),
        raw: serde_json::json!({}),
    };

    let incoming = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap()
        .expect("should not be a duplicate");

    let _msg_guard = common::TestMessage { id: incoming.id };

    assert!(incoming.chat_id.is_none());
    assert!(incoming.sender_id.is_none());
}

#[tokio::test]
async fn ws_message_creates_chat_and_sets_chat_id() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("incoming_messages").await.unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut ws, token, _client) = ws_connect(addr, &widget_id).await;
    let client_id = chatbridge::jwt::verify(&token, TEST_JWT_SECRET.as_bytes()).unwrap();

    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "chat test", "mid": "550e8400-e29b-41d4-a716-446655440000"}"#
            .into(),
    ))
    .await
    .unwrap();

    // Consume ACK
    let _ = ws.next().await.unwrap().unwrap();

    // Check Redis message has non-null chat_id and correct sender_id
    let internal = wait_for_redis_msg(&mut pubsub_stream, guard.id).await;
    assert_eq!(internal["type"], "message");
    assert_eq!(
        internal["sender"]["id"],
        client_id.to_string(),
        "sender.id should be set for widget messages"
    );
    assert!(
        !internal["chat_id"].is_null(),
        "chat_id should not be null for widget messages, got: {internal}"
    );

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn edit_message_updates_text_and_edited_at() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("edit_test_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "widget:edit-target".into(),
        channel_id: channel.id,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("original".into()),
        raw: serde_json::json!({}),
    };
    let incoming = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap()
        .unwrap();
    let _msg_guard = common::TestMessage { id: incoming.id };

    let edit =
        chatbridge::db::edit_message(&pool, channel.id, "widget:edit-target", Some("updated"))
            .await
            .unwrap()
            .expect("message should exist");

    assert_eq!(edit.id, incoming.id);
    assert_eq!(edit.text.as_deref(), Some("updated"));
    assert!(edit.edited_at >= incoming.created_at);
}

#[tokio::test]
async fn edit_message_unknown_returns_none() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("edit_miss_{}", Uuid::new_v4())).await;

    let result =
        chatbridge::db::edit_message(&pool, channel.id, "widget:nonexistent", Some("text"))
            .await
            .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn mark_messages_read_watermark() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("read_test_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };
    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    let _chat_guard = common::TestChat { id: chat_id };

    let mut msg_guards = Vec::new();
    for i in 1..=3 {
        let new_msg = NewMessage {
            external_message_id: format!("widget:read-{i}"),
            channel_id: channel.id,
            sender_id: Some(client_id),
            sender_type: "client".into(),
            provider: ProviderKind::Widget,
            event: EventKind::Message,
            text: Some(format!("msg {i}")),
            raw: serde_json::json!({}),
        };
        let incoming = chatbridge::db::insert_message(&pool, &new_msg, Some(chat_id))
            .await
            .unwrap()
            .unwrap();
        msg_guards.push(common::TestMessage { id: incoming.id });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Mark read up to message 2 (watermark) — should mark messages 1 and 2
    let reads = chatbridge::db::mark_messages_read(&pool, channel.id, "widget:read-2", "operator")
        .await
        .unwrap();
    assert_eq!(reads.len(), 2, "should mark messages 1 and 2 as read");

    // Message 3 should still be 'new'
    let reads_again =
        chatbridge::db::mark_messages_read(&pool, channel.id, "widget:read-3", "operator")
            .await
            .unwrap();
    assert_eq!(reads_again.len(), 1, "only message 3 should be newly read");
}

#[tokio::test]
async fn mark_messages_read_unknown_returns_empty() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("read_miss_{}", Uuid::new_v4())).await;

    let reads =
        chatbridge::db::mark_messages_read(&pool, channel.id, "widget:nonexistent", "operator")
            .await
            .unwrap();
    assert!(reads.is_empty());
}

#[tokio::test]
async fn insert_message_dedup_returns_none() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel =
        insert_test_widget_channel(&pool, &format!("dedup_test_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "widget:dedup-mid".into(),
        channel_id: channel.id,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("first".into()),
        raw: serde_json::json!({}),
    };

    let first = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap();
    assert!(first.is_some());
    let _msg_guard = common::TestMessage {
        id: first.unwrap().id,
    };

    let second = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap();
    assert!(second.is_none(), "duplicate should return None");
}

// --- Operator endpoints ---

/// Connect to the operator WebSocket, wait for auth, return stream + operator_id + cleanup guard.
async fn operator_ws_connect(
    addr: std::net::SocketAddr,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
    TestOperator,
) {
    let url = format!("ws://{addr}/ws/operator");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let auth = wait_for_ws_msg(&mut ws).await;
    assert_eq!(auth["action"], "auth");
    let operator_id = auth["operator_id"].as_str().unwrap().to_string();
    let guard = TestOperator {
        id: Uuid::parse_str(&operator_id).unwrap(),
    };
    (ws, operator_id, guard)
}

/// Wait for the next text message on a WebSocket stream with a 5s timeout.
async fn wait_for_ws_msg(
    ws: &mut (
             impl futures_util::Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin
         ),
) -> serde_json::Value {
    use futures_util::StreamExt;
    let deadline = std::time::Duration::from_secs(5);
    let msg = tokio::time::timeout(deadline, async {
        loop {
            match ws.next().await {
                Some(Ok(tungstenite::Message::Text(text))) => {
                    return serde_json::from_str::<serde_json::Value>(&text).unwrap();
                }
                Some(Ok(_)) => continue, // skip pings, pongs, etc.
                Some(Err(e)) => panic!("ws error: {e}"),
                None => panic!("ws stream ended unexpectedly"),
            }
        }
    })
    .await
    .expect("timed out waiting for operator ws message");
    msg
}

#[tokio::test]
async fn operator_get_chats_empty() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/chats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let chats: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(chats.is_array());
    // May contain chats from other tests running in parallel, that's ok
}

#[tokio::test]
async fn operator_get_chats_with_active_chat() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // Connect widget and send a message to create a chat
    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;
    let mid = Uuid::new_v4();
    ws.send(tungstenite::Message::Text(
        serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hello operator", "attachments": []}).to_string().into(),
    ))
    .await
    .unwrap();

    // Wait for ACK
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");

    // Small delay for background persist
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // GET /api/chats
    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{addr}/api/chats"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let chats: Vec<serde_json::Value> = resp.json().await.unwrap();

    // Find our chat (filter by channel_id from the guard)
    let our_chat = chats
        .iter()
        .find(|c| c["last_message_text"] == "hello operator")
        .expect("our chat should appear in active chats");

    assert_eq!(our_chat["chat_status"], "new");
    assert_eq!(our_chat["client_provider"], "widget");
    assert!(our_chat["last_message_at"].is_string());

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn operator_get_chat_messages() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // Send two messages
    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;
    for text in &["first message", "second message"] {
        let mid = Uuid::new_v4();
        ws.send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": text, "attachments": []}).to_string().into(),
        ))
        .await
        .unwrap();
        // Wait for ACK
        let resp = ws.next().await.unwrap().unwrap();
        let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
        assert_eq!(ack["action"], "ack");
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Get chat_id from /api/chats
    let http = reqwest::Client::new();
    let chats: Vec<serde_json::Value> = http
        .get(format!("http://{addr}/api/chats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let our_chat = chats
        .iter()
        .find(|c| c["last_message_text"] == "second message")
        .expect("our chat should exist");
    let chat_id = our_chat["chat_id"].as_str().unwrap();

    // GET /api/chats/{chat_id}
    let resp = http
        .get(format!("http://{addr}/api/chats/{chat_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let messages: Vec<serde_json::Value> = resp.json().await.unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["text"], "first message");
    assert_eq!(messages[1]["text"], "second message");
    // Verify ascending order
    assert!(
        messages[0]["created_at"].as_str().unwrap() <= messages[1]["created_at"].as_str().unwrap()
    );

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn operator_get_chat_messages_unknown_chat_returns_404() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;
    let addr = spawn_app(state).await;

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{addr}/api/chats/{}", Uuid::new_v4()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn operator_ws_receives_widget_message() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // Connect operator WS first so it's subscribed before the message
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    // Connect widget and send a message
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;
    let mid = Uuid::new_v4();
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hello from widget", "attachments": []}).to_string().into(),
        ))
        .await
        .unwrap();

    // Wait for widget ACK
    let resp = widget_ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");

    // Read from operator WS — may need to skip events from other channels
    let deadline = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            panic!(
                "timed out waiting for operator ws message for channel {}",
                channel_guard.id
            );
        }
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out");
        if event["channel_id"] == channel_guard.id.to_string() {
            assert_eq!(event["type"], "message");
            assert_eq!(event["text"], "hello from widget");
            break;
        }
    }

    // Also verify /api/chats shows the new chat
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let http = reqwest::Client::new();
    let chats: Vec<serde_json::Value> = http
        .get(format!("http://{addr}/api/chats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        chats
            .iter()
            .any(|c| c["last_message_text"] == "hello from widget"),
        "chat should appear in /api/chats"
    );

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

#[tokio::test]
async fn operator_ws_receives_edit_event() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Send a message
    let mid = Uuid::new_v4();
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "original", "attachments": []}).to_string().into(),
        ))
        .await
        .unwrap();

    // Wait for widget ACK
    let resp = widget_ws.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(resp.to_text().unwrap()).unwrap()["action"],
        "ack"
    );

    // Wait for message event on operator WS (skip other channels)
    let deadline = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for message event");
        if event["channel_id"] == channel_guard.id.to_string() && event["type"] == "message" {
            break;
        }
    }

    // Now send an edit
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "edit", "mid": mid.to_string(), "text": "edited text"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    // Wait for edit ACK
    let resp = widget_ws.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(resp.to_text().unwrap()).unwrap()["action"],
        "ack"
    );

    // Wait for edit event on operator WS
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for edit event");
        if event["channel_id"] == channel_guard.id.to_string() && event["type"] == "edit" {
            assert_eq!(event["text"], "edited text");
            assert!(event["edited_at"].is_string());
            break;
        }
    }

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

// --- Two-way chat tests ---

#[tokio::test]
async fn operator_sends_message_to_widget_client() {
    use std::time::Duration;

    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // Connect operator WS (gets auth with new operator_id)
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    // Connect widget client (gets auth with token)
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Widget sends a message (creates chat)
    let mid = Uuid::new_v4();
    let msg = serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hello from client", "attachments": []});
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&msg).unwrap().into(),
        ))
        .await
        .unwrap();

    // Widget receives ACK
    let ack = wait_for_ws_msg(&mut widget_ws).await;
    assert_eq!(ack["action"], "ack");

    // Operator should receive the message — filter by our channel
    let deadline = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let chat_id;
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let op_event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for operator message");
        if op_event["channel_id"] == channel_guard.id.to_string() && op_event["type"] == "message" {
            assert_eq!(op_event["text"], "hello from client");
            chat_id = op_event["chat_id"].as_str().unwrap().to_string();
            break;
        }
    }

    // Operator sends reply
    let reply_mid = Uuid::new_v4();
    let reply = serde_json::json!({
        "action": "send",
        "chat_id": chat_id,
        "mid": reply_mid.to_string(),
        "text": "hello from operator"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();

    // Operator gets ack — skip non-ack messages from other channels
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let msg = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for ack");
        if msg["action"] == "ack" {
            break;
        }
    }

    // Widget receives operator message
    let widget_event = wait_for_ws_msg(&mut widget_ws).await;
    assert_eq!(widget_event["type"], "message");
    assert_eq!(widget_event["text"], "hello from operator");
    assert_eq!(widget_event["sender"]["type"], "operator");

    // Verify chat history shows both messages
    tokio::time::sleep(Duration::from_millis(100)).await;
    let resp = reqwest::get(format!("http://{addr}/api/chats/{chat_id}"))
        .await
        .unwrap();
    let messages: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(messages.len() >= 2);

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

#[tokio::test]
async fn operator_edit_reaches_widget_client() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Widget sends a message (creates chat)
    let mid = Uuid::new_v4();
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hi", "attachments": []})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let _ack = wait_for_ws_msg(&mut widget_ws).await;

    // Operator receives message — skip events from other channels
    let deadline = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    let chat_id;
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for message event on operator ws");
        if event["channel_id"] == channel_guard.id.to_string() && event["type"] == "message" {
            chat_id = event["chat_id"].as_str().unwrap().to_string();
            break;
        }
    }

    // Operator sends a reply
    let reply_mid = Uuid::new_v4();
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({
                "action": "send",
                "chat_id": chat_id,
                "mid": reply_mid.to_string(),
                "text": "original reply"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    // Wait for operator ack (skip non-ack messages from other channels)
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let msg = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for ack on operator ws");
        if msg["action"] == "ack" {
            break;
        }
    }

    // Widget receives the reply
    let _widget_msg = wait_for_ws_msg(&mut widget_ws).await;

    // Operator sends edit
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({
                "action": "edit",
                "chat_id": chat_id,
                "mid": reply_mid.to_string(),
                "text": "edited reply"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    // Wait for edit ack (skip non-ack messages)
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let msg = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for edit ack");
        if msg["action"] == "ack" {
            break;
        }
    }

    // Widget receives edit event
    let edit_event = wait_for_ws_msg(&mut widget_ws).await;
    assert_eq!(edit_event["type"], "edit");
    assert_eq!(edit_event["text"], "edited reply");
    assert_eq!(edit_event["sender"]["type"], "operator");

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

#[tokio::test]
async fn widget_read_receipt_reaches_operator() {
    use std::time::Duration;

    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Widget sends a message (creates chat)
    let mid = Uuid::new_v4();
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hi", "attachments": []})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let _ack = wait_for_ws_msg(&mut widget_ws).await;

    // Operator receives message — skip events from other channels
    let deadline = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let chat_id;
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for message event on operator ws");
        if event["channel_id"] == channel_guard.id.to_string() && event["type"] == "message" {
            chat_id = event["chat_id"].as_str().unwrap().to_string();
            break;
        }
    }

    // Operator sends a reply
    let reply_mid = Uuid::new_v4();
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({
                "action": "send",
                "chat_id": chat_id,
                "mid": reply_mid.to_string(),
                "text": "operator reply"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    // Wait for operator ack
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let msg = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for ack");
        if msg["action"] == "ack" {
            break;
        }
    }

    // Widget receives operator message
    let op_msg = wait_for_ws_msg(&mut widget_ws).await;
    assert_eq!(op_msg["type"], "message");
    let message_id = op_msg["id"].as_str().unwrap().to_string();

    // Widget sends read receipt for operator's message
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "read", "mid": message_id})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    // Operator should receive the read event
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for read event on operator ws");
        if event["type"] == "read" && event["channel_id"] == channel_guard.id.to_string() {
            assert_eq!(
                event["external_message_id"],
                format!("operator:{reply_mid}")
            );
            break;
        }
    }

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

#[tokio::test]
async fn operator_read_receipt_reaches_widget() {
    use std::time::Duration;

    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;
    let (mut widget_ws, _token, _client) = ws_connect(addr, &widget_id).await;

    // Widget sends a message
    let mid = Uuid::new_v4();
    widget_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "read me", "attachments": []})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let _ack = wait_for_ws_msg(&mut widget_ws).await;

    // Operator receives message
    let deadline = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let (chat_id, message_id);
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for message event on operator ws");
        if event["channel_id"] == channel_guard.id.to_string() && event["type"] == "message" {
            chat_id = event["chat_id"].as_str().unwrap().to_string();
            message_id = event["id"].as_str().unwrap().to_string();
            break;
        }
    }

    // Operator sends read receipt for client's message
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::json!({
                "action": "read",
                "chat_id": chat_id,
                "mid": message_id
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    // Widget should receive the read event
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut widget_ws))
            .await
            .expect("timed out waiting for read event on widget ws");
        if event["type"] == "read" {
            assert_eq!(event["external_message_id"], format!("widget:{mid}"));
            break;
        }
    }

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

// --- find_last_chat ---

#[tokio::test]
async fn find_last_chat_returns_closed_chat() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Archived Client").await;
    let chat = insert_test_chat(&pool, client.id, channel.id, "closed", Utc::now()).await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap()
        .expect("a closed chat must still be returned");

    assert_eq!(found.id, chat.id);
    assert_eq!(found.status, "closed");
}

#[tokio::test]
async fn find_last_chat_returns_newest_of_several() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Returning Client").await;

    // Two 'new' chats are impossible — idx_chats_active forbids them.
    let old = insert_test_chat(
        &pool,
        client.id,
        channel.id,
        "closed",
        Utc::now() - chrono::Duration::hours(2),
    )
    .await;
    let recent = insert_test_chat(&pool, client.id, channel.id, "new", Utc::now()).await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap()
        .expect("chat should exist");

    assert_eq!(found.id, recent.id);
    assert_ne!(found.id, old.id);
    assert_eq!(found.status, "new");
}

#[tokio::test]
async fn find_last_chat_returns_none_when_no_chats() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Fresh Client").await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap();

    assert!(found.is_none(), "a client with no chats yields None");
}

// --- get_chat_messages sender_name ---

#[tokio::test]
async fn get_chat_messages_includes_sender_name() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Named Client").await;
    let chat = insert_test_chat(&pool, client.id, channel.id, "new", Utc::now()).await;

    let operator_id = Uuid::new_v4();
    sqlx::query("INSERT INTO operators (id, name) VALUES ($1, 'Named Operator')")
        .bind(operator_id)
        .execute(&pool)
        .await
        .unwrap();
    let _operator = TestOperator { id: operator_id };

    sqlx::query(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_id, sender_type, text, raw)
         VALUES ($1, $2, $3, $4, 'client', 'from the client', '{}'::jsonb)",
    )
    .bind(chat.id)
    .bind(format!("widget:{}", Uuid::new_v4()))
    .bind(channel.id)
    .bind(client.id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_id, sender_type, text, raw, created_at)
         VALUES ($1, $2, $3, $4, 'operator', 'from the operator', '{}'::jsonb, now() + interval '1 second')",
    )
    .bind(chat.id)
    .bind(format!("operator:{}", Uuid::new_v4()))
    .bind(channel.id)
    .bind(operator_id)
    .execute(&pool)
    .await
    .unwrap();

    let messages = chatbridge::db::get_chat_messages(&pool, chat.id)
        .await
        .unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].sender_type, "client");
    assert_eq!(messages[0].sender_name.as_deref(), Some("Named Client"));
    assert_eq!(messages[1].sender_type, "operator");
    assert_eq!(messages[1].sender_name.as_deref(), Some("Named Operator"));
}

#[tokio::test]
async fn ws_returning_client_receives_chat_event() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // First connect: new client, gets auth, sends one message so a chat is created.
    let (mut ws1, token, _client) = ws_connect(addr, &widget_id).await;
    let mid = Uuid::new_v4();
    let msg = serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "first"});
    ws1.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
    let ack = ws1.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(ack.to_text().unwrap()).unwrap();
    assert_eq!(
        ack["action"], "ack",
        "message must be persisted before reconnect"
    );
    ws1.close(None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Reconnect with the token: no auth, first frame is the chat event.
    let url = format!(
        "ws://{addr}/ws/{widget_id}?token={}",
        urlencoding::encode(&token)
    );
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    // Timeout, not a bare await: with no chat event the server stays silent for the
    // full 300s idle period, and a hung test is far less useful than a failed one.
    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws2.next())
        .await
        .expect("no frame within 5s — the server sent nothing on connect")
        .unwrap()
        .unwrap();
    let event: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();

    assert_eq!(event["action"], "chat");
    assert_eq!(event["status"], "new");
    let chat_id: Uuid = event["chat_id"].as_str().unwrap().parse().unwrap();
    let (chat_channel,): (Uuid,) = sqlx::query_as("SELECT channel_id FROM chats WHERE id = $1")
        .bind(chat_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(chat_channel, channel.id, "chat must belong to this channel");

    ws2.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_new_client_receives_no_chat_event() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let _guard = insert_test_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    // ws_connect already asserts the first frame is auth. A client with no chat must
    // get nothing after it, so the next frame is the ack for the message we send.
    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;
    let mid = Uuid::new_v4();
    let msg = serde_json::json!({"action": "send", "mid": mid.to_string(), "text": "hello"});
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let event: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(
        event["action"], "ack",
        "a client with no chat must not receive a chat event"
    );

    ws.close(None).await.unwrap();
}

// --- Channel CRUD queries ---

#[tokio::test]
async fn channel_insert_and_find_live() {
    let pool = setup_pool().await;
    let id = Uuid::new_v4();
    let key = format!("chan_{}", Uuid::new_v4());
    let created = chatbridge::db::insert_channel(
        &pool,
        id,
        ProviderKind::Widget,
        "My widget",
        &key,
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    let _guard = TestChannel { id };

    assert_eq!(created.id, id);
    assert_eq!(created.provider, "widget");
    assert_eq!(created.name, "My widget");
    assert_eq!(created.external_key, key);
    assert!(created.deleted_at.is_none());

    let by_id = chatbridge::db::find_live_channel_by_id(&pool, id)
        .await
        .unwrap()
        .expect("live channel by id");
    assert_eq!(by_id.external_key, key);

    let by_key =
        chatbridge::db::find_live_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .expect("live channel by key");
    assert_eq!(by_key.id, id);
}

#[tokio::test]
async fn channel_soft_delete_hides_from_live_queries_only() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    let deleted = chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap()
        .expect("row returned");
    assert!(deleted.deleted_at.is_some());

    assert!(
        chatbridge::db::find_live_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none(),
        "soft-deleted channel must be invisible to the live query"
    );
    assert!(
        chatbridge::db::find_live_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        chatbridge::db::find_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_some(),
        "the any-state query must still see it"
    );
    assert!(
        chatbridge::db::find_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn channel_insert_duplicate_external_key_is_unique_violation() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let _guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    let err = chatbridge::db::insert_channel(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Widget,
        "dup",
        &key,
        &serde_json::json!({}),
    )
    .await
    .expect_err("second insert on the same identity must fail");

    match err {
        sqlx::Error::Database(ref e) => assert!(e.is_unique_violation()),
        other => panic!("expected a unique violation, got {other:?}"),
    }
}

#[tokio::test]
async fn channel_insert_duplicate_key_conflicts_even_when_deleted() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let err = chatbridge::db::insert_channel(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Widget,
        "dup",
        &key,
        &serde_json::json!({}),
    )
    .await
    .expect_err("the identity stays taken after a soft delete");

    match err {
        sqlx::Error::Database(ref e) => assert!(e.is_unique_violation()),
        other => panic!("expected a unique violation, got {other:?}"),
    }
}

#[tokio::test]
async fn channel_update_renames_restores_and_rewrites_config() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &key,
        serde_json::json!({"bot_token": "old"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let updated = chatbridge::db::update_channel(
        &pool,
        guard.id,
        Some("Renamed"),
        None,
        Some(&serde_json::json!({"bot_token": "new"})),
        true,
    )
    .await
    .unwrap()
    .expect("row returned");

    assert_eq!(updated.name, "Renamed");
    assert!(updated.deleted_at.is_none(), "restore clears deleted_at");
    assert_eq!(updated.config["bot_token"], "new");
}

#[tokio::test]
async fn channel_update_leaves_untouched_fields_alone() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({"keep": true})).await;

    let updated =
        chatbridge::db::update_channel(&pool, guard.id, Some("Only a rename"), None, None, false)
            .await
            .unwrap()
            .expect("row returned");

    assert_eq!(updated.name, "Only a rename");
    assert_eq!(updated.external_key, key, "external_key untouched");
    assert_eq!(updated.config["keep"], true, "config untouched");
}

#[tokio::test]
async fn channel_list_puts_live_channels_first() {
    let pool = setup_pool().await;
    let live_key = format!("chan_live_{}", Uuid::new_v4());
    let dead_key = format!("chan_dead_{}", Uuid::new_v4());
    let live = insert_test_channel(&pool, "widget", &live_key, serde_json::json!({})).await;
    let dead = insert_test_channel(&pool, "widget", &dead_key, serde_json::json!({})).await;
    chatbridge::db::soft_delete_channel(&pool, dead.id)
        .await
        .unwrap();

    let all = chatbridge::db::list_channels(&pool).await.unwrap();
    let live_pos = all
        .iter()
        .position(|c| c.id == live.id)
        .expect("live listed");
    let dead_pos = all
        .iter()
        .position(|c| c.id == dead.id)
        .expect("deleted listed");
    assert!(
        live_pos < dead_pos,
        "live channels must sort before deleted ones"
    );
}

#[tokio::test]
async fn channel_hard_delete_removes_the_row() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    chatbridge::db::hard_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    assert!(
        chatbridge::db::find_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none()
    );
    // The identity is free again, which is the whole point of the create rollback.
    let reused = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;
    assert_ne!(reused.id, guard.id);
}

// --- Channel REST API ---

#[tokio::test]
async fn get_channels_lists_live_and_deleted_with_endpoints() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let live = insert_test_widget_channel(&pool, &widget_id).await;
    let dead_id = format!("api_dead_{}", Uuid::new_v4());
    let dead = insert_test_widget_channel(&pool, &dead_id).await;
    chatbridge::db::soft_delete_channel(&pool, dead.id)
        .await
        .unwrap();

    let state = build_state(pool.clone()).await;
    let app = routes::build(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    let live_row = list
        .iter()
        .find(|c| c["id"] == live.id.to_string())
        .expect("live channel listed");
    assert_eq!(live_row["provider"], "widget");
    assert_eq!(live_row["external_key"], widget_id);
    assert_eq!(live_row["deleted_at"], serde_json::Value::Null);
    assert_eq!(
        live_row["endpoint"],
        format!("wss://test.example.com/ws/{widget_id}")
    );

    let dead_row = list
        .iter()
        .find(|c| c["id"] == dead.id.to_string())
        .expect("deleted channel is listed too, not hidden");
    assert!(dead_row["deleted_at"].is_string());
}

#[tokio::test]
async fn get_channels_returns_telegram_secrets_in_full() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "api_secret").await;

    let state = build_state(pool.clone()).await;
    let app = routes::build(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    let row = list
        .iter()
        .find(|c| c["id"] == guard.id.to_string())
        .expect("channel listed");
    // Deliberate: the panel shows and edits keys. See docs/tech_debt.md.
    assert_eq!(row["config"]["bot_secret"], "api_secret");
    assert!(row["config"]["bot_token"].as_str().unwrap().contains(':'));
    assert_eq!(
        row["endpoint"],
        format!("https://test.example.com/webhook/telegram/{}", guard.id)
    );
}

async fn post_json(
    state: Arc<AppState>,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    request_json(state, "POST", uri, Some(body)).await
}

async fn request_json(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let app = routes::build(state);
    let builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(b) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

async fn request_raw(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let app = routes::build(state);
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// Every request the mock saw: `(method, path, query)`.
type Requests = Arc<std::sync::Mutex<Vec<(String, String, String)>>>;

/// One server for all three Meta hosts. `responses` is keyed by the request path
/// with its leading slash stripped, e.g. "me" or "me/subscribed_apps"; anything
/// unlisted answers `{"success": true}`.
///
/// It **records** every request, because the permissive default is a trap: a test
/// that only asserts on the database passes just as happily when the production
/// code never made the call at all. Any test whose name claims a provider call
/// happened has to assert against this log.
async fn spawn_mock_instagram_recording(responses: serde_json::Value) -> (String, Requests) {
    use axum::Router;
    use axum::extract::Request as AxumRequest;
    use axum::routing::any;

    let responses = Arc::new(responses);
    let seen: Requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = seen.clone();

    let app = Router::new().fallback(any(move |req: AxumRequest| {
        let responses = responses.clone();
        let recorder = recorder.clone();
        async move {
            let path = req.uri().path().trim_start_matches('/').to_owned();
            let query = req.uri().query().unwrap_or_default().to_owned();
            recorder
                .lock()
                .unwrap()
                .push((req.method().to_string(), path.clone(), query));
            let body = responses
                .get(path.as_str())
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"success": true}));
            axum::Json(body)
        }
    }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), seen)
}

/// For tests that do not need the request log.
async fn spawn_mock_instagram(responses: serde_json::Value) -> String {
    spawn_mock_instagram_recording(responses).await.0
}

/// Did the mock see a subscribe carrying every field we mean to subscribe to?
///
/// reqwest percent-encodes the comma in a query value, so the expected list is
/// matched on `%2C`.
fn subscribed_all_fields(requests: &Requests) -> bool {
    let wanted = "subscribed_fields=messages%2Cmessage_edit%2Cmessage_reactions%2Cmessaging_seen";
    requests
        .lock()
        .unwrap()
        .iter()
        .any(|(method, path, query)| {
            method == "POST" && path == "me/subscribed_apps" && query.contains(wanted)
        })
}

fn saw(requests: &Requests, method: &str, path: &str) -> bool {
    requests
        .lock()
        .unwrap()
        .iter()
        .any(|(m, p, _)| m == method && p == path)
}

/// A mock that walks the whole happy path: code → short → long → profile → subscribe.
fn instagram_login_ok(user_id: &str, username: &str) -> serde_json::Value {
    serde_json::json!({
        "oauth/access_token": {"access_token": "short_lived", "user_id": user_id},
        "access_token": {"access_token": "long_lived", "token_type": "bearer", "expires_in": 5_183_944},
        "me": {"user_id": user_id, "username": username, "id": user_id},
        "me/subscribed_apps": {"success": true},
    })
}

/// Drive the callback the way the browser would, with a state this deployment signed.
async fn oauth_callback(state: Arc<AppState>, channel_id: Option<Uuid>) -> (StatusCode, String) {
    let token = chatbridge::oauth::sign_state(
        TEST_JWT_SECRET.as_bytes(),
        chatbridge::model::ProviderKind::Instagram,
        channel_id,
    );
    let (status, _, body) = request_raw(
        state,
        "GET",
        &format!("/api/oauth/instagram/callback?code=AQB123&state={token}"),
    )
    .await;
    (status, body)
}

#[tokio::test]
async fn oauth_providers_lists_instagram_with_its_redirect_uri() {
    let pool = setup_pool().await;
    let state = build_state(pool.clone()).await;

    let (status, body) = request_json(state, "GET", "/api/oauth/providers", None).await;

    assert_eq!(status, StatusCode::OK);
    let list = body.as_array().unwrap();
    assert_eq!(list.len(), 1, "only instagram has an OAuth login today");
    assert_eq!(list[0]["provider"], "instagram");
    assert_eq!(list[0]["label"], "Instagram");
    assert_eq!(list[0]["start_path"], "/api/oauth/instagram/start");
    // The panel shows this so it can be pasted into the Meta dashboard.
    assert_eq!(
        list[0]["redirect_uri"],
        format!("{TEST_PUBLIC_BASE_URL}/api/oauth/instagram/callback")
    );
}

#[tokio::test]
async fn oauth_start_redirects_to_the_provider_with_a_signed_state() {
    let pool = setup_pool().await;
    let state = build_state_ig(pool.clone(), "http://mock.test".into()).await;

    let (status, headers, _) = request_raw(state, "GET", "/api/oauth/instagram/start").await;

    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    let location = headers["location"].to_str().unwrap();
    assert!(
        location.starts_with("http://mock.test/oauth/authorize?"),
        "{location}"
    );
    assert!(
        location.contains(&format!("client_id={TEST_APP_ID}")),
        "{location}"
    );

    // The state must verify against the app secret and carry no pinned channel.
    let url = reqwest::Url::parse(location).unwrap();
    let token = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .expect("state parameter");
    let claims = chatbridge::oauth::verify_state(
        TEST_JWT_SECRET.as_bytes(),
        &token,
        chatbridge::model::ProviderKind::Instagram,
    )
    .unwrap();
    assert_eq!(claims.ch, None);
}

#[tokio::test]
async fn oauth_start_pins_the_channel_the_reconnect_button_came_from() {
    let pool = setup_pool().await;
    let state = build_state_ig(pool.clone(), "http://mock.test".into()).await;
    let channel_id = Uuid::new_v4();

    let (status, headers, _) = request_raw(
        state,
        "GET",
        &format!("/api/oauth/instagram/start?channel_id={channel_id}"),
    )
    .await;

    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    let url = reqwest::Url::parse(headers["location"].to_str().unwrap()).unwrap();
    let token = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .unwrap();
    let claims = chatbridge::oauth::verify_state(
        TEST_JWT_SECRET.as_bytes(),
        &token,
        chatbridge::model::ProviderKind::Instagram,
    )
    .unwrap();
    assert_eq!(claims.ch, Some(channel_id));
}

#[tokio::test]
async fn oauth_start_is_404_for_a_provider_without_a_login() {
    let pool = setup_pool().await;
    let state = build_state(pool.clone()).await;

    let (status, _, _) = request_raw(state.clone(), "GET", "/api/oauth/telegram/start").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, _) = request_raw(state, "GET", "/api/oauth/nonsense/start").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn oauth_callback_creates_a_channel_and_subscribes_it() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let (api, requests) =
        spawn_mock_instagram_recording(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    // Taken before the first assertion: this test creates a real row, and a failing
    // assertion below would otherwise leak it into the shared database — which is
    // exactly what happened once while checking the subscribe assertion can fail.
    let _key_guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the callback is a page, never a 4xx"
    );
    assert!(body.contains("\"ok\":true"), "{body}");
    assert!(body.contains("\"created\":true"), "{body}");
    // Without this the test passes with the entire subscribe call deleted, because
    // the mock answers unlisted paths with {"success": true} and the row is written
    // either way.
    assert!(
        subscribed_all_fields(&requests),
        "no subscribe reached the provider: {:?}",
        requests.lock().unwrap()
    );

    let row = sqlx::query_as::<_, (Uuid, String, String, serde_json::Value)>(
        "SELECT id, name, external_key, config FROM channels
         WHERE provider = 'instagram' AND external_key = $1",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let _guard = TestChannel { id: row.0 };

    assert_eq!(
        row.1, "@yourbiz",
        "the name defaults to the account's handle"
    );
    assert_eq!(
        row.3["access_token"], "long_lived",
        "the short-lived token is never stored"
    );
    assert_eq!(row.3["username"], "yourbiz");
    assert!(
        row.3["token_expires_at"].is_string(),
        "the 60-day expiry is recorded so the refresher can find it"
    );
}

#[tokio::test]
async fn oauth_callback_on_a_live_channel_replaces_the_token_and_keeps_the_name() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;
    sqlx::query("UPDATE channels SET name = 'Support' WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"created\":false"), "{body}");

    let (name, config) = sqlx::query_as::<_, (String, serde_json::Value)>(
        "SELECT name, config FROM channels WHERE id = $1",
    )
    .bind(guard.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        name, "Support",
        "a login must not undo an operator's rename"
    );
    assert_eq!(config["access_token"], "long_lived");
}

#[tokio::test]
async fn oauth_callback_restores_a_deleted_channel() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    let (api, requests) =
        spawn_mock_instagram_recording(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"restored\":true"), "{body}");
    assert!(
        subscribed_all_fields(&requests),
        "a restored channel has to be subscribed again"
    );

    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        deleted_at.is_none(),
        "a login is an explicit 'I want this account'"
    );
}

#[tokio::test]
async fn oauth_callback_refuses_a_different_account_when_a_channel_is_pinned() {
    let pool = setup_pool().await;
    let ours = format!("ig_{}", Uuid::new_v4().simple());
    let theirs = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &ours,
        serde_json::json!({"access_token": "ours"}),
    )
    .await;

    let api = spawn_mock_instagram(instagram_login_ok(&theirs, "someone_else")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _stranger = TestChannelKey::instagram(&theirs);

    let (status, body) = oauth_callback(state, Some(guard.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("someone_else"),
        "the page names the account: {body}"
    );

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        config["access_token"], "ours",
        "the pinned channel is untouched"
    );

    let stranger: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&theirs)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        stranger, 0,
        "and no channel is created for the other account"
    );
}

#[tokio::test]
async fn oauth_callback_rolls_back_a_new_channel_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    // Every subscribe attempt fails, including the per-field probes.
    responses["me/subscribed_apps"] = serde_json::json!({
        "error": {"message": "Application does not have permission", "code": 10}
    });
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;

    // If the rollback regresses, the assertion below fires *and* the row survives; the
    // guard is what stops it from poisoning the shared database for every later test.
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "a row that never worked must not occupy this account's identity forever"
    );
}

#[tokio::test]
async fn oauth_callback_keeps_an_existing_channel_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;

    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    responses["me/subscribed_apps"] =
        serde_json::json!({"error": {"message": "temporarily unavailable", "code": 2}});
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"ok\":true"),
        "the token is still an improvement: {body}"
    );
    assert!(body.contains("\"warning\""), "{body}");

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "long_lived");
}

#[tokio::test]
async fn oauth_callback_writes_nothing_for_a_tampered_state() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, _, body) = request_raw(
        state,
        "GET",
        "/api/oauth/instagram/callback?code=AQB123&state=not.a.jwt",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn oauth_callback_reports_a_cancelled_login_without_writing() {
    let pool = setup_pool().await;
    // No mock at all: a cancelled login must not reach the provider.
    let state = build_state(pool.clone()).await;

    let (status, _, body) = request_raw(
        state,
        "GET",
        "/api/oauth/instagram/callback?error=access_denied&error_description=User+denied",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(body.contains("User denied"), "{body}");
}

#[tokio::test]
async fn oauth_callback_refuses_a_pinned_channel_that_no_longer_exists() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    // Deleted between opening the popup and finishing the login.
    let (status, body) = oauth_callback(state, Some(Uuid::new_v4())).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0, "a pin that cannot be honoured writes nothing");
}

#[tokio::test]
async fn oauth_callback_refuses_a_pinned_channel_of_another_provider() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let telegram = insert_test_telegram_channel(&pool, "s").await;
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, Some(telegram.id)).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("telegram"),
        "the page says what the channel is: {body}"
    );
}

#[tokio::test]
async fn oauth_callback_writes_nothing_when_the_code_exchange_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    responses["oauth/access_token"] = serde_json::json!({
        "error": {"message": "This authorization code has been used.", "code": 100}
    });
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("authorization code"),
        "Meta's own message survives: {body}"
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn post_channel_creates_a_widget_channel() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id, "name": "Acme site"}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    assert_eq!(body["provider"], "widget");
    assert_eq!(body["name"], "Acme site");
    assert_eq!(body["external_key"], widget_id);
    assert_eq!(body["config"], serde_json::json!({}));
    assert_eq!(
        body["endpoint"],
        format!("wss://test.example.com/ws/{widget_id}")
    );

    let stored = chatbridge::db::find_live_channel_by_id(&pool, id)
        .await
        .unwrap();
    assert!(stored.is_some(), "row persisted");
}

#[tokio::test]
async fn post_channel_defaults_widget_name_to_the_widget_id() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let _guard = TestChannel {
        id: body["id"].as_str().unwrap().parse().unwrap(),
    };
    assert_eq!(body["name"], widget_id);
}

#[tokio::test]
async fn post_channel_rejects_a_widget_id_that_breaks_the_route() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": "has spaces/and-slash"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_channel_duplicate_widget_id_conflicts_with_channel_exists() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let existing = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], existing.id.to_string());
    assert_eq!(body["deleted_at"], serde_json::Value::Null);
}

#[tokio::test]
async fn post_channel_on_a_deleted_identity_conflicts_with_channel_deleted() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let existing = insert_test_widget_channel(&pool, &widget_id).await;
    chatbridge::db::soft_delete_channel(&pool, existing.id)
        .await
        .unwrap();
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_deleted");
    assert_eq!(body["channel_id"], existing.id.to_string());
    assert!(
        body["deleted_at"].is_string(),
        "the panel shows when it was deleted"
    );
}

/// `request_json` parses the body, and an `AppError` renders as plain text — so a 4xx
/// comes back as `Null` and its message is lost. This keeps the text.
async fn request_text(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, String) {
    let app = routes::build(state);
    let builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(b) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn post_instagram_channel_derives_its_identity_from_the_token() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"success": true},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "instagram", "access_token": "IGQ_token"}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    // The whole point of the manual path is that it produces a working channel rather
    // than a silent one, so the subscribe is the assertion that matters.
    assert!(
        subscribed_all_fields(&requests),
        "the manual path must subscribe too"
    );

    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    // The operator types only the token; /me is the authority on who it belongs to.
    assert_eq!(body["external_key"], user_id);
    assert_eq!(body["name"], "@yourbiz");
    assert_eq!(body["config"]["access_token"], "IGQ_token");
    assert_eq!(body["config"]["username"], "yourbiz");
    assert!(
        body["config"]["token_expires_at"].is_null(),
        "a pasted token has no known expiry; the refresher fills it in"
    );
    assert_eq!(
        body["endpoint"],
        "https://test.example.com/webhook/instagram"
    );
}

#[tokio::test]
async fn post_instagram_channel_rejects_an_empty_token() {
    let pool = setup_pool().await;
    // No mock: validation must fail before any network call.
    let state = build_state(pool.clone()).await;

    let (status, body) = request_text(
        state,
        "POST",
        "/api/channels",
        Some(serde_json::json!({"provider": "instagram", "access_token": "  "})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Asserting the status alone proves nothing: with the guard removed, `fetch_profile`
    // hits the unreachable default base, and that failure is also mapped to a 400.
    assert!(body.contains("must not be empty"), "{body}");
}

#[tokio::test]
async fn post_instagram_channel_rolls_back_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"error": {"message": "no permission", "code": 10}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "instagram", "access_token": "IGQ_token"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "the manual path must not leave a silent channel behind"
    );
}

#[tokio::test]
async fn patch_instagram_channel_drops_the_stale_expiry() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({
            "access_token": "old",
            "token_expires_at": "2026-10-19T09:00:00Z",
            "username": "yourbiz"
        }),
    )
    .await;
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"success": true},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "new"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["config"]["access_token"], "new");
    assert!(
        body["config"]["token_expires_at"].is_null(),
        "the old expiry described the token that was just replaced"
    );
    assert_eq!(body["config"]["username"], "yourbiz");
    assert!(
        subscribed_all_fields(&requests),
        "a new token has to be armed, or the whole Rearm branch could be deleted unnoticed"
    );
}

#[tokio::test]
async fn patch_instagram_channel_refuses_a_token_from_another_account() {
    let pool = setup_pool().await;
    let ours = format!("ig_{}", Uuid::new_v4().simple());
    let theirs = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &ours,
        serde_json::json!({"access_token": "ours"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me": {"user_id": theirs, "username": "someone_else"},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "theirs"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "ours");
}

#[tokio::test]
async fn delete_instagram_channel_unsubscribes() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({})).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _, _) = request_raw(state, "DELETE", &format!("/api/channels/{}", guard.id)).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    // Unsubscribing is best effort, so a failure is invisible from the outside — the
    // request log is the only way to tell "it failed" from "it never happened".
    assert!(
        saw(&requests, "DELETE", "me/subscribed_apps"),
        "the account has to be unsubscribed: {:?}",
        requests.lock().unwrap()
    );
}

#[tokio::test]
async fn delete_instagram_channel_survives_a_failing_unsubscribe() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "revoked"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"error": {"message": "Invalid OAuth access token.", "code": 190}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _, _) = request_raw(state, "DELETE", &format!("/api/channels/{}", guard.id)).await;

    // A channel whose token was revoked must stay deletable.
    assert_eq!(status, StatusCode::NO_CONTENT);
    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(deleted_at.is_some());
}

#[tokio::test]
async fn patch_does_not_subscribe_a_channel_that_stays_deleted() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "old"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    // Editing a deleted channel's token is allowed; subscribing it is not. Meta would
    // start delivering events for a channel the app rejects, and report the failures
    // against something the operator believes is gone.
    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "new"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        !saw(&requests, "POST", "me/subscribed_apps"),
        "a channel that stays deleted must not be armed"
    );
}

#[tokio::test]
async fn restore_instagram_channel_resubscribes() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    // Subscribing fails, so a restore that forgot to re-subscribe would pass this
    // test silently; the 502 is what proves the call happened.
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"error": {"message": "nope", "code": 10}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({"restore": true})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        deleted_at.is_none(),
        "the row is already committed; only the subscription failed"
    );
}

#[tokio::test]
async fn a_first_instagram_message_from_a_new_sender_creates_a_chat() {
    // The first message of a conversation is the only one that decides whether the
    // conversation appears in the inbox at all. Client resolution used to insert the
    // client row in a spawned task, so `persist_and_publish` found no client, left
    // `sender_id` NULL and created no chat — every new customer's opening message was
    // orphaned, and the second one silently repaired it.
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let sender = format!("igsid_{}", Uuid::new_v4().simple());
    let channel = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    // No profile endpoint: the lookup is allowed to fail, the chat must appear anyway.
    let api = spawn_mock_instagram(serde_json::json!({})).await;
    let state = build_state_ig(pool.clone(), api).await;

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": sender},
                "recipient": {"id": user_id},
                "timestamp": 1_787_416_333_085i64,
                "message": {"mid": format!("mid_{}", Uuid::new_v4().simple()), "text": "first ever"}
            }]
        }]
    });
    let raw = serde_json::to_vec(&body).unwrap();
    let signature = sign_body(TEST_APP_SECRET, &raw);

    let app = routes::build(state);
    let request = Request::builder()
        .method("POST")
        .uri("/webhook/instagram")
        .header("content-type", "application/json")
        .header("X-Hub-Signature-256", format!("sha256={signature}"))
        .body(Body::from(raw))
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Ingestion is spawned, so poll rather than sleep a fixed amount.
    let mut chat: Option<Uuid> = None;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        chat = sqlx::query_scalar("SELECT chat_id FROM messages WHERE channel_id = $1")
            .bind(channel.id)
            .fetch_optional(&pool)
            .await
            .unwrap()
            .flatten();
        if chat.is_some() {
            break;
        }
    }

    let client_id: Option<Uuid> =
        sqlx::query_scalar("SELECT sender_id FROM messages WHERE channel_id = $1")
            .bind(channel.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let _client_guard = client_id.map(|id| TestClient { id });

    assert!(
        client_id.is_some(),
        "the message must carry its sender, not NULL"
    );
    assert!(
        chat.is_some(),
        "the first message from a new sender has to create a chat"
    );
}

#[tokio::test]
async fn an_operator_reply_reaches_instagram() {
    use std::time::Duration;

    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let igsid = format!("igsid_{}", Uuid::new_v4().simple());
    let channel = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me/messages": {"message_id": "mid_out", "recipient_id": igsid},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;
    let addr = spawn_app(state).await;

    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    // An inbound message first: it is what creates the client and the chat, and the
    // client's external_id is the IGSID the reply has to be addressed to.
    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": igsid},
                "recipient": {"id": user_id},
                "message": {"mid": format!("in_{}", Uuid::new_v4().simple()), "text": "customer asks"}
            }]
        }]
    });
    let raw = serde_json::to_vec(&body).unwrap();
    let signature = sign_body(TEST_APP_SECRET, &raw);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/webhook/instagram"))
        .header("content-type", "application/json")
        .header("X-Hub-Signature-256", format!("sha256={signature}"))
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Wait for the chat, then reply into it.
    let deadline = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let chat_id;
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(&mut op_ws))
            .await
            .expect("timed out waiting for the inbound message");
        if event["channel_id"] == channel.id.to_string() && event["type"] == "message" {
            chat_id = event["chat_id"].as_str().unwrap().to_string();
            break;
        }
    }

    let reply = serde_json::json!({
        "action": "send",
        "chat_id": chat_id,
        "mid": Uuid::new_v4().to_string(),
        "text": "operator answers"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();

    // Delivery is spawned after the Ack, so poll the mock's request log.
    let mut sent = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if saw(&requests, "POST", "me/messages") {
            sent = true;
            break;
        }
    }
    assert!(
        sent,
        "the operator's reply never reached Instagram: {:?}",
        requests.lock().unwrap()
    );

    // Meta's own id has to replace the local one, or every read receipt for this
    // reply resolves to nothing: `mark_messages_read` anchors on
    // (channel_id, external_message_id) and the receipt carries Meta's id.
    let mut external_id = String::new();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        external_id = sqlx::query_scalar(
            "SELECT external_message_id FROM messages
             WHERE channel_id = $1 AND sender_type = 'operator'",
        )
        .bind(channel.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if external_id.starts_with("instagram:") {
            break;
        }
    }
    assert_eq!(
        external_id, "instagram:mid_out",
        "the outbound row must adopt the id Meta returned"
    );
}

/// Load a channel and its parsed config so a test can drive one refresh directly.
async fn channel_for_refresh(
    pool: &PgPool,
    id: Uuid,
) -> (chatbridge::db::Channel, chatbridge::model::InstagramConfig) {
    let channel = chatbridge::db::find_live_channel_by_id(pool, id)
        .await
        .unwrap()
        .unwrap();
    let config = serde_json::from_value(channel.config.clone()).unwrap();
    (channel, config)
}

#[tokio::test]
async fn the_refresher_replaces_an_expiring_token() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({
            "access_token": "about_to_die",
            "token_expires_at": (chrono::Utc::now() + chrono::TimeDelta::days(2)).to_rfc3339(),
            "username": "yourbiz"
        }),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "access_token": "fresh", "token_type": "bearer", "expires_in": 5_183_944
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    // `refresh_channel`, not `refresh_due_tokens`: the pass walks every live Instagram
    // channel in the shared database and would rewrite rows that other tests are
    // asserting on, failing them at random depending on scheduling.
    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap();

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "fresh");
    assert_eq!(
        config["username"], "yourbiz",
        "the handle survives a refresh"
    );
    let expires_at = config["token_expires_at"].as_str().unwrap();
    let parsed = chrono::DateTime::parse_from_rfc3339(expires_at).unwrap();
    assert!(
        parsed > chrono::Utc::now() + chrono::TimeDelta::days(50),
        "the new expiry should be ~60 days out, got {expires_at}"
    );
}

#[tokio::test]
async fn the_refresher_fills_in_a_missing_expiry() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "pasted_by_hand"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "access_token": "fresh", "token_type": "bearer", "expires_in": 5_183_944
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    assert!(
        chatbridge::refresh::is_due(&config, chrono::Utc::now()),
        "an unknown expiry has to be due, or a hand-pasted token never acquires one"
    );
    chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap();

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        config["token_expires_at"].is_string(),
        "a hand-pasted token acquires a real expiry on the first pass"
    );
}

#[tokio::test]
async fn a_rejected_refresh_leaves_the_stored_token_alone() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "already_expired"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "error": {"message": "Error validating access token", "code": 190}
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    let err = chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap_err();
    assert!(err.contains("Error validating access token"), "{err}");

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        config["access_token"], "already_expired",
        "a failed refresh must not overwrite the stored token"
    );
}

/// Spawn a fake Bot API that answers `/bot<token>/<method>` from `responses`,
/// defaulting to `{"ok":true,"result":{}}`.
async fn spawn_mock_telegram(responses: serde_json::Value) -> String {
    use axum::Router;
    use axum::extract::Path;
    use axum::routing::any;

    let responses = Arc::new(responses);
    let app = Router::new().route(
        "/bot{token}/{method}",
        any(move |Path((_token, method)): Path<(String, String)>| {
            let responses = responses.clone();
            async move {
                let body = responses
                    .get(method.as_str())
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"ok": true, "result": {}}));
                axum::Json(body)
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn get_me_ok(bot_id: i64) -> serde_json::Value {
    serde_json::json!({"ok": true, "result": {
        "id": bot_id, "is_bot": true, "first_name": "Acme", "username": "acme_bot"
    }})
}

#[tokio::test]
async fn post_telegram_channel_registers_the_webhook_and_stores_a_secret() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAtoken")}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    // external_key is the bot id from getMe, not a parsed token prefix.
    assert_eq!(body["external_key"], bot_id.to_string());
    // The name defaults to the bot's @username.
    assert_eq!(body["name"], "@acme_bot");
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAtoken"));
    let secret = body["config"]["bot_secret"].as_str().unwrap();
    assert_eq!(secret.len(), 32, "32 hex chars from a v4 UUID");
    assert!(secret.bytes().all(|b| b.is_ascii_alphanumeric()));
    assert_eq!(
        body["endpoint"],
        format!("https://test.example.com/webhook/telegram/{id}")
    );
}

#[tokio::test]
async fn post_telegram_channel_rejects_a_token_telegram_refuses() {
    let pool = setup_pool().await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "Unauthorized"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": "123456789:AAbad"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_telegram_channel_rejects_a_malformed_token_without_calling_telegram() {
    let pool = setup_pool().await;
    // getMe is wired to fail loudly: reaching it would mean validation was skipped.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "should never be called"},
    }))
    .await;
    let state = build_state_with(pool, api).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": "not-a-token"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_telegram_channel_rolls_the_row_back_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook: HTTPS url must be provided"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = post_json(
        state.clone(),
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAtoken")}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);

    // The rollback is a HARD delete, so the identity is free and a retry is a
    // clean create rather than a 409 on a channel that never worked.
    let leftover = chatbridge::db::find_channel_by_external_key(
        &pool,
        ProviderKind::Telegram,
        &bot_id.to_string(),
    )
    .await
    .unwrap();
    assert!(leftover.is_none(), "no row may survive a failed setWebhook");
}

#[tokio::test]
async fn post_telegram_channel_conflicts_before_touching_the_webhook() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let existing = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "old_secret"}),
    )
    .await;

    // setWebhook is wired to fail: if the handler called it, the test would see 502
    // instead of 409, which is exactly the webhook-hijack ordering bug.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "must not be reached"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], existing.id.to_string());

    // The live channel's stored secret is untouched.
    let stored = chatbridge::db::find_channel_by_id(&pool, existing.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.config["bot_secret"], "old_secret");
}

#[tokio::test]
async fn patch_channel_renames_without_contacting_the_provider() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "patch_secret").await;
    // getMe fails loudly: a plain rename must not call Telegram at all.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "must not be reached"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({"name": "Renamed"})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Renamed");
    assert_eq!(
        body["config"]["bot_secret"], "patch_secret",
        "config untouched"
    );
}

#[tokio::test]
async fn patch_channel_unknown_id_returns_404() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", Uuid::new_v4()),
        Some(serde_json::json!({"name": "x"})),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_channel_cannot_change_the_provider() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": "123456789:AA"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_channel_moves_a_widget_id_and_conflicts_when_taken() {
    let pool = setup_pool().await;
    let first = format!("api_a_{}", Uuid::new_v4());
    let second = format!("api_b_{}", Uuid::new_v4());
    let moving = insert_test_widget_channel(&pool, &first).await;
    let blocker = insert_test_widget_channel(&pool, &second).await;
    let state = build_state(pool.clone()).await;

    // Free key — accepted, and the endpoint follows the new key.
    let free = format!("api_c_{}", Uuid::new_v4());
    let (status, body) = request_json(
        state.clone(),
        "PATCH",
        &format!("/api/channels/{}", moving.id),
        Some(serde_json::json!({"spec": {"provider": "widget", "widget_id": free}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["external_key"], free);
    assert_eq!(
        body["endpoint"],
        format!("wss://test.example.com/ws/{free}")
    );

    // Taken key — 409 naming the blocking channel.
    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", moving.id),
        Some(serde_json::json!({"spec": {"provider": "widget", "widget_id": second}})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], blocker.id.to_string());
}

#[tokio::test]
async fn patch_channel_rotates_a_telegram_token_for_the_same_bot() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "kept_secret"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAnew"));
    assert_eq!(
        body["config"]["bot_secret"], "kept_secret",
        "the webhook secret survives a token rotation"
    );
}

#[tokio::test]
async fn patch_channel_refuses_a_token_belonging_to_another_bot() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let other_bot_id = bot_id + 1;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({"getMe": get_me_ok(other_bot_id)})).await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{other_bot_id}:AAother")}
        })),
    )
    .await;

    // Repointing a channel at a different bot is not an edit: chats and messages
    // hang off this channel id.
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_channel_restores_and_rewrites_the_submitted_fields() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAdead"), "bot_secret": "old"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "restore": true,
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAfresh")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted_at"], serde_json::Value::Null);
    assert_eq!(
        body["config"]["bot_token"],
        format!("{bot_id}:AAfresh"),
        "restore writes the freshly entered token, not the dead one"
    );
    assert_eq!(body["id"], guard.id.to_string(), "the original id is kept");
}

#[tokio::test]
async fn patch_channel_returns_502_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn patch_channel_drops_the_cache_even_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    // Warm the cache with the pre-PATCH config.
    state
        .cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();

    let (status, _) = request_json(
        state.clone(),
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    // The UPDATE committed before setWebhook failed, so the cache must not keep
    // serving the old config — the webhook handler verifies secrets against it.
    let cached = state
        .cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel still live");
    assert_eq!(cached.config["bot_token"], format!("{bot_id}:AAnew"));
}

#[tokio::test]
async fn patch_channel_does_not_arm_the_webhook_of_a_channel_that_stays_deleted() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    // setWebhook is wired to fail: reaching it would turn this into a 502.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "must not be reached"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "editing a deleted channel is allowed"
    );
    assert!(body["deleted_at"].is_string(), "and it stays deleted");
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAnew"));
}

#[tokio::test]
async fn delete_channel_soft_deletes_and_is_idempotent() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state.clone(),
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let row = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("the row survives — history is kept");
    let first_deleted_at = row.deleted_at.expect("deleted_at set");

    // A repeat delete answers 204 as well and does not move the timestamp.
    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let again = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.deleted_at, Some(first_deleted_at));
}

#[tokio::test]
async fn delete_channel_unknown_id_is_still_204() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;
    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn delete_channel_succeeds_even_when_delete_webhook_fails() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "del_secret").await;
    // A revoked token makes deleteWebhook fail; the channel must still be deletable.
    let api = spawn_mock_telegram(serde_json::json!({
        "deleteWebhook": {"ok": false, "description": "Unauthorized"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    let row = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.deleted_at.is_some());
}

#[tokio::test]
async fn deleted_channel_is_invisible_to_the_hot_path() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state.clone(),
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // The handler invalidated the cache, so the next read-through misses the DB filter.
    assert!(
        state
            .cache
            .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
            .await
            .unwrap()
            .is_none()
    );

    // And the widget cannot connect any more.
    let addr = spawn_app(state).await;
    let url = format!("ws://{addr}/ws/{widget_id}");
    assert!(
        tokio_tungstenite::connect_async(&url).await.is_err(),
        "a deleted widget channel must refuse the upgrade"
    );
}

#[tokio::test]
async fn list_active_chats_excludes_chats_of_a_deleted_channel() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Deleted Channel Customer").await;
    let chat_id = chatbridge::db::find_or_create_chat(&pool, client.id, channel.id)
        .await
        .unwrap();
    let _chat = TestChat { id: chat_id };

    let before = chatbridge::db::list_active_chats(&pool).await.unwrap();
    assert!(
        before.iter().any(|c| c.chat_id == chat_id),
        "the chat is in the inbox while the channel is live"
    );

    chatbridge::db::soft_delete_channel(&pool, channel.id)
        .await
        .unwrap();

    let after = chatbridge::db::list_active_chats(&pool).await.unwrap();
    assert!(
        !after.iter().any(|c| c.chat_id == chat_id),
        "a deleted channel's chats must leave the inbox — they are unanswerable"
    );
}

#[tokio::test]
async fn connection_status_reports_a_registered_telegram_webhook() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AA"), "bot_secret": "s"}),
    )
    .await;
    let expected = format!("https://test.example.com/webhook/telegram/{}", guard.id);
    let api = spawn_mock_telegram(serde_json::json!({
        "getWebhookInfo": {"ok": true, "result": {
            "url": expected, "pending_update_count": 0
        }},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "GET",
        &format!("/api/channels/{}/connection", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
    assert_eq!(body["details"]["registered_url"], expected);
    assert_eq!(body["details"]["expected_url"], expected);
    assert_eq!(
        body["details"]["last_error_message"],
        serde_json::Value::Null
    );
    assert!(body["summary"].as_str().unwrap().contains("registered at"));
}

#[tokio::test]
async fn connection_status_reports_a_hijacked_telegram_webhook() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AA"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getWebhookInfo": {"ok": true, "result": {
            "url": "https://someone-else.example.com/webhook/telegram/other",
            "pending_update_count": 12,
            "last_error_date": 1700000000,
            "last_error_message": "wrong response from webhook: 404"
        }},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "GET",
        &format!("/api/channels/{}/connection", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], false);
    assert_eq!(body["details"]["pending_update_count"], 12);
    assert!(
        body["details"]["last_error_message"]
            .as_str()
            .unwrap()
            .contains("404")
    );
}

#[tokio::test]
async fn connection_register_reregisters_a_telegram_webhook() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AA"), "bot_secret": "s"}),
    )
    .await;
    let expected = format!("https://test.example.com/webhook/telegram/{}", guard.id);
    let api = spawn_mock_telegram(serde_json::json!({
        "setWebhook": {"ok": true, "result": true},
        "getWebhookInfo": {"ok": true, "result": {
            "url": expected, "pending_update_count": 0
        }},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "POST",
        &format!("/api/channels/{}/connection", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["ok"], true,
        "re-register then report in one round trip"
    );
}

#[tokio::test]
async fn connection_status_is_404_for_a_deleted_channel() {
    let pool = setup_pool().await;
    let deleted = insert_test_telegram_channel(&pool, "s").await;
    chatbridge::db::soft_delete_channel(&pool, deleted.id)
        .await
        .unwrap();
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state,
        "GET",
        &format!("/api/channels/{}/connection", deleted.id),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a deleted channel has nothing to manage; restore it instead"
    );
}

#[tokio::test]
async fn connection_status_for_a_widget_channel_is_ok_and_says_why() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let widget = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, body) = request_json(
        state.clone(),
        "GET",
        &format!("/api/channels/{}/connection", widget.id),
        None,
    )
    .await;

    // Not a 400: the endpoint tells the truth rather than refusing. The panel simply
    // does not show the button for widgets.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
    assert!(
        body["summary"]
            .as_str()
            .unwrap()
            .contains("register nothing")
    );

    // POST is a no-op rather than a 400, so the panel never has to special-case it.
    let (status, body) = request_json(
        state,
        "POST",
        &format!("/api/channels/{}/connection", widget.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn connection_status_reports_an_instagram_subscription_and_expiry() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let expires_at = chrono::Utc::now() + chrono::TimeDelta::days(58);
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({
            "access_token": "tok",
            "token_expires_at": expires_at.to_rfc3339(),
        }),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"data": [
            {"subscribed_fields": ["messages", "message_edit"]}
        ]},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "GET",
        &format!("/api/channels/{}/connection", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
    assert_eq!(body["details"]["subscribed_fields"][0], "messages");
    // num_days truncates, so 58 days minus a few microseconds reads as 57.
    assert_eq!(body["details"]["expires_in_days"], 57);
    let summary = body["summary"].as_str().unwrap();
    assert!(summary.contains("messages, message_edit"), "{summary}");
}

#[tokio::test]
async fn connection_status_is_not_ok_when_instagram_has_no_subscription() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"data": []},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "GET",
        &format!("/api/channels/{}/connection", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], false, "no subscription means no events arrive");
    assert!(body["summary"].as_str().unwrap().contains("Not subscribed"));
}
