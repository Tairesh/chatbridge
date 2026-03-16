mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use tokio_tungstenite::tungstenite;
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::cache::ClientCache;
use chatbridge::config::{AppConfig, AppState};
use chatbridge::model::ProviderKind;
use chatbridge::registry::ClientRegistry;
use chatbridge::routes;
use common::{TestChannel, TestClient};
use tokio_util::sync::CancellationToken;

const TEST_VERIFY_TOKEN: &str = "test_verify_token";
const TEST_APP_SECRET: &str = "test_app_secret";
const TEST_JWT_SECRET: &str = "test-jwt-secret-at-least-32-bytes!!";

async fn insert_test_instagram_channel(pool: &PgPool) -> (TestChannel, String) {
    let channel_id = Uuid::new_v4();
    let user_id = format!("test_{channel_id}");
    sqlx::query("INSERT INTO instagram_channels (id, user_id, access_token) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&user_id)
        .bind("test_token")
        .execute(pool)
        .await
        .unwrap();
    let guard = TestChannel {
        table: "instagram_channels",
        id: channel_id,
    };
    (guard, user_id)
}

async fn insert_test_telegram_channel(pool: &PgPool, bot_secret: &str) -> TestChannel {
    let channel_id = Uuid::new_v4();
    let bot_token = format!("test:{channel_id}");
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&bot_token)
        .bind(bot_secret)
        .execute(pool)
        .await
        .unwrap();
    TestChannel {
        table: "telegram_channels",
        id: channel_id,
    }
}

async fn insert_test_widget_channel(pool: &PgPool, widget_id: &str) -> TestChannel {
    let channel_id = Uuid::new_v4();
    sqlx::query("INSERT INTO widget_channels (id, widget_id) VALUES ($1, $2)")
        .bind(channel_id)
        .bind(widget_id)
        .execute(pool)
        .await
        .unwrap();
    TestChannel {
        table: "widget_channels",
        id: channel_id,
    }
}

// --- Test helpers ---

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

async fn build_state(db: PgPool) -> Arc<AppState> {
    let redis = setup_redis().await;
    Arc::new(AppState {
        config: AppConfig {
            meta_verify_token: TEST_VERIFY_TOKEN.into(),
            instagram_app_secret: TEST_APP_SECRET.into(),
            redis_url: "redis://localhost:6379".into(),
            widget_jwt_secret: TEST_JWT_SECRET.into(),
        },
        db,
        redis,
        cache: Arc::new(Default::default()),
        client_cache: Arc::new(ClientCache::new()),
        registry: ClientRegistry::new(),
        shutdown: CancellationToken::new(),
    })
}

/// Start the app on a random port and return the address.
async fn spawn_app(state: Arc<AppState>) -> std::net::SocketAddr {
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
    pubsub
        .subscribe(format!("widget:{}", guard.id))
        .await
        .unwrap();
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
    let redis_msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for Redis message")
        .unwrap();

    let payload: String = redis_msg.get_payload().unwrap();
    let internal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(internal["provider"], "Widget");
    assert_eq!(internal["channel_id"], guard.id.to_string());
    assert_eq!(
        internal["raw"]["mid"],
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(internal["raw"]["text"], "redis test");

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
    pubsub
        .subscribe(format!("instagram:{}", guard.id))
        .await
        .unwrap();
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

    let redis_msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for Redis message")
        .unwrap();

    let payload: String = redis_msg.get_payload().unwrap();
    let internal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(internal["provider"], "Instagram");
    assert_eq!(internal["channel_id"], guard.id.to_string());

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
    pubsub
        .subscribe(format!("telegram:{}", guard.id))
        .await
        .unwrap();
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

    let redis_msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for Redis message")
        .unwrap();

    let payload: String = redis_msg.get_payload().unwrap();
    let internal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(internal["provider"], "Telegram");
    assert_eq!(internal["channel_id"], guard.id.to_string());

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
    pubsub
        .subscribe(format!("widget:{}", guard.id))
        .await
        .unwrap();
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
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for send Redis message");

    // Edit the message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "edit", "mid": "770e8400-e29b-41d4-a716-446655440002", "text": "Hello"}"#
            .into(),
    ))
    .await
    .unwrap();
    let _ = ws.next().await.unwrap().unwrap();

    // Check Redis received the edit event
    let redis_msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for edit Redis message")
        .unwrap();

    let payload: String = redis_msg.get_payload().unwrap();
    let internal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(internal["event"], "Edit");
    assert_eq!(
        internal["raw"]["mid"],
        "770e8400-e29b-41d4-a716-446655440002"
    );
    assert_eq!(internal["raw"]["text"], "Hello");
    assert_eq!(internal["raw"]["action"], "edit");

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
async fn cache_instagram_lookup_and_invalidation() {
    let pool = setup_pool().await;
    let (guard, user_id) = insert_test_instagram_channel(&pool).await;

    let cache = Arc::new(ChannelCache::new());

    // First lookup — cache miss, loads from DB
    let ch = cache
        .get_instagram_channel(&pool, &user_id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);

    // Delete from DB — cache should still return the channel
    sqlx::query("DELETE FROM instagram_channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_instagram_channel(&pool, &user_id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    // Invalidate the specific channel
    cache.invalidate(guard.id);

    // Now cache is empty, lookup goes to DB — channel is gone
    let after = cache.get_instagram_channel(&pool, &user_id).await.unwrap();
    assert!(
        after.is_none(),
        "should be None after invalidation + DB delete"
    );
}

#[tokio::test]
async fn cache_telegram_lookup_and_invalidation() {
    let pool = setup_pool().await;
    let bot_secret = "cache_test_secret";
    let guard = insert_test_telegram_channel(&pool, bot_secret).await;

    let cache = Arc::new(ChannelCache::new());

    // First lookup — cache miss, loads from DB
    let ch = cache
        .get_telegram_channel(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.bot_secret, bot_secret);

    // Delete from DB
    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_telegram_channel(&pool, guard.id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.bot_secret, bot_secret);

    // Invalidate
    cache.invalidate(guard.id);

    let after = cache.get_telegram_channel(&pool, guard.id).await.unwrap();
    assert!(
        after.is_none(),
        "should be None after invalidation + DB delete"
    );
}

#[tokio::test]
async fn cache_widget_lookup_and_invalidation() {
    let pool = setup_pool().await;
    let widget_id = format!("cache_test_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    let cache = Arc::new(ChannelCache::new());

    // First lookup — cache miss, loads from DB
    let ch = cache
        .get_widget_channel(&pool, &widget_id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);

    // Delete from DB
    sqlx::query("DELETE FROM widget_channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_widget_channel(&pool, &widget_id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    // Invalidate
    cache.invalidate(guard.id);

    let after = cache.get_widget_channel(&pool, &widget_id).await.unwrap();
    assert!(
        after.is_none(),
        "should be None after invalidation + DB delete"
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
        .get_telegram_channel(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.bot_secret, bot_secret);

    // Start invalidation listener
    chatbridge::cache::spawn_invalidation_listener(
        &redis_url,
        cache.clone(),
        Arc::new(ClientCache::new()),
    )
    .await;

    // Delete from DB so we can detect cache eviction
    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
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
    let after = cache.get_telegram_channel(&pool, guard.id).await.unwrap();
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
        .get_telegram_channel(&pool, guard_a.id)
        .await
        .unwrap()
        .unwrap();
    cache
        .get_telegram_channel(&pool, guard_b.id)
        .await
        .unwrap()
        .unwrap();

    // Invalidate only A
    cache.invalidate(guard_a.id);

    // B should still be cached even if we delete it from DB
    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
        .bind(guard_b.id)
        .execute(&pool)
        .await
        .unwrap();
    let b = cache
        .get_telegram_channel(&pool, guard_b.id)
        .await
        .unwrap()
        .expect("channel B should still be cached");
    assert_eq!(b.bot_secret, "secret_b");
}

#[tokio::test]
async fn instagram_rejects_non_instagram_object() {
    use chatbridge::provider::WebhookProvider;
    use chatbridge::provider::instagram::InstagramProvider;

    fn test_provider() -> InstagramProvider {
        InstagramProvider::new(
            TEST_APP_SECRET,
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
    pubsub
        .subscribe(format!("widget:{}", guard.id))
        .await
        .unwrap();
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
    let redis_msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub_stream.next())
        .await
        .expect("timed out waiting for Redis message")
        .unwrap();

    let payload: String = redis_msg.get_payload().unwrap();
    let internal: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(internal["client_id"], client_id.to_string());

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
    let resp2 = ws2.next().await.unwrap().unwrap();
    let ack2: serde_json::Value = serde_json::from_str(resp2.to_text().unwrap()).unwrap();
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
    chatbridge::cache::spawn_invalidation_listener(&redis_url, channel_cache, client_cache.clone())
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
