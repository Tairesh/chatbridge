use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use tokio_tungstenite::tungstenite;
use tower::ServiceExt;
use uuid::Uuid;

use webhook::config::{AppConfig, AppState};
use webhook::routes;

const TEST_VERIFY_TOKEN: &str = "test_verify_token";
const TEST_APP_SECRET: &str = "test_app_secret";

fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

async fn setup_pool() -> PgPool {
    let url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let pool = PgPool::connect(&url)
        .await
        .expect("failed to connect to test DB");
    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("failed to run migrations");
    pool
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
            port: 3000,
            meta_verify_token: TEST_VERIFY_TOKEN.into(),
            instagram_app_secret: TEST_APP_SECRET.into(),
            redis_url: "redis://localhost:6379".into(),
        },
        db,
        redis,
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

    // Insert a test channel with unique user_id
    let channel_id = Uuid::new_v4();
    let user_id = format!("test_{channel_id}");
    sqlx::query("INSERT INTO instagram_channels (id, user_id, access_token) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&user_id)
        .bind("test_token")
        .execute(&pool)
        .await
        .unwrap();

    let app = routes::build(build_state(pool.clone()).await);

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "time": 1773347860136_i64,
            "id": &user_id,
            "messaging": [{
                "sender": {"id": "836189122827510"},
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

    // Cleanup
    sqlx::query("DELETE FROM instagram_channels WHERE id = $1")
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
}

// --- Telegram ingest (POST) ---

#[tokio::test]
async fn telegram_ingest_valid() {
    let pool = setup_pool().await;

    let channel_id = Uuid::new_v4();
    let bot_secret = "test_bot_secret";
    let bot_token = format!("test:{channel_id}");
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&bot_token)
        .bind(bot_secret)
        .execute(&pool)
        .await
        .unwrap();

    let app = routes::build(build_state(pool.clone()).await);

    let body = serde_json::json!({
        "update_id": 100,
        "message": {
            "message_id": 42,
            "date": 1700000000,
            "from": {"id": 123, "first_name": "Test"},
            "chat": {"id": 123, "type": "private"},
            "text": "hello bot"
        }
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{channel_id}"))
                .header("content-type", "application/json")
                .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cleanup
    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn telegram_ingest_invalid_secret() {
    let pool = setup_pool().await;

    let channel_id = Uuid::new_v4();
    let bot_token = format!("test:{channel_id}");
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&bot_token)
        .bind("correct_secret")
        .execute(&pool)
        .await
        .unwrap();

    let app = routes::build(build_state(pool.clone()).await);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/webhook/telegram/{channel_id}"))
                .header("content-type", "application/json")
                .header("X-Telegram-Bot-Api-Secret-Token", "wrong_secret")
                .body(Body::from(r#"{"update_id":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Cleanup
    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
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

use futures_util::{SinkExt, StreamExt};

async fn insert_widget_channel(pool: &PgPool, widget_id: &str) -> Uuid {
    let channel_id = Uuid::new_v4();
    sqlx::query("INSERT INTO widget_channels (id, widget_id) VALUES ($1, $2)")
        .bind(channel_id)
        .bind(widget_id)
        .execute(pool)
        .await
        .unwrap();
    channel_id
}

async fn delete_widget_channel(pool: &PgPool, channel_id: Uuid) {
    sqlx::query("DELETE FROM widget_channels WHERE id = $1")
        .bind(channel_id)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn ws_connect_and_receive_ack() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send a valid message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "msg-123456", "text": "Hello", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();

    // Receive ACK
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");
    assert_eq!(ack["message_id"], "msg-123456");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_message_with_attachments() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let attachment_id = Uuid::new_v4();
    let msg = serde_json::json!({
        "action": "send",
        "mid": "msg-123456",
        "text": "See attached",
        "attachments": [attachment_id.to_string()]
    });
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");
    assert_eq!(ack["message_id"], "msg-123456");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_invalid_json_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send invalid JSON
    ws.send(tungstenite::Message::Text("not json".into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["status"], "error");
    assert!(err["reason"].as_str().unwrap().contains("invalid message"));

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_missing_text_field_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Valid JSON but missing required "text" field
    ws.send(tungstenite::Message::Text(r#"{"attachments": []}"#.into()))
        .await
        .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["status"], "error");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_missing_message_id_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Valid JSON with "text" field but without "mid" field
    ws.send(tungstenite::Message::Text(r#"{"text": "Hello"}"#.into()))
        .await
    .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["status"], "error");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
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
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let mut seen_ids = std::collections::HashSet::new();

    for i in 0..3 {
        let msg = serde_json::json!({"action": "send", "text": format!("msg {i}"), "mid": format!("msg-{i}")});
        ws.send(tungstenite::Message::Text(msg.to_string().into()))
            .await
            .unwrap();

        let resp = ws.next().await.unwrap().unwrap();
        let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
        assert_eq!(ack["status"], "ok");
        // Each ACK should have a unique message_id
        let mid = ack["message_id"].as_str().unwrap().to_string();
        assert!(seen_ids.insert(mid), "duplicate message_id");
    }

    assert_eq!(seen_ids.len(), 3);

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_continues_after_bad_message() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send bad message
    ws.send(tungstenite::Message::Text("bad".into()))
        .await
        .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["status"], "error");

    // Connection should still be alive — send valid message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "still here", "mid": "msg-123456"}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_publishes_to_redis() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub
        .subscribe(format!("widget:{channel_id}"))
        .await
        .unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "text": "redis test", "mid": "msg-123456"}"#.into(),
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
    assert_eq!(internal["channel_id"], channel_id.to_string());
    assert_eq!(internal["raw"]["mid"], "msg-123456");
    assert_eq!(internal["raw"]["text"], "redis test");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn instagram_publishes_to_redis() {
    let pool = setup_pool().await;

    let channel_id = Uuid::new_v4();
    let user_id = format!("test_{channel_id}");
    sqlx::query("INSERT INTO instagram_channels (id, user_id, access_token) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&user_id)
        .bind("test_token")
        .execute(&pool)
        .await
        .unwrap();

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub
        .subscribe(format!("instagram:{channel_id}"))
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
                "sender": {"id": "836189122827510"},
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
    assert_eq!(internal["channel_id"], channel_id.to_string());

    sqlx::query("DELETE FROM instagram_channels WHERE id = $1")
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn telegram_publishes_to_redis() {
    let pool = setup_pool().await;

    let channel_id = Uuid::new_v4();
    let bot_secret = "test_bot_secret_redis";
    let bot_token = format!("test_redis:{channel_id}");
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind(&bot_token)
        .bind(bot_secret)
        .execute(&pool)
        .await
        .unwrap();

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub
        .subscribe(format!("telegram:{channel_id}"))
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
            "from": {"id": 123, "first_name": "Test"},
            "chat": {"id": 123, "type": "private"},
            "text": "redis tg test"
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/webhook/telegram/{channel_id}"))
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
    assert_eq!(internal["channel_id"], channel_id.to_string());

    sqlx::query("DELETE FROM telegram_channels WHERE id = $1")
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
}

// --- WebSocket edit action tests ---

#[tokio::test]
async fn ws_edit_message_returns_ack() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send original message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "msg-edit-1", "text": "Helo", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");
    assert_eq!(ack["message_id"], "msg-edit-1");

    // Edit the message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "edit", "mid": "msg-edit-1", "text": "Hello"}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");
    assert_eq!(ack["message_id"], "msg-edit-1");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_edit_publishes_edit_event_to_redis() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    // Subscribe to Redis channel before sending
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let sub_client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
    pubsub
        .subscribe(format!("widget:{channel_id}"))
        .await
        .unwrap();
    let mut pubsub_stream = pubsub.on_message();

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send original message
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "msg-redis-edit", "text": "Helo", "attachments": []}"#
            .into(),
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
        r#"{"action": "edit", "mid": "msg-redis-edit", "text": "Hello"}"#.into(),
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
    assert_eq!(internal["raw"]["mid"], "msg-redis-edit");
    assert_eq!(internal["raw"]["text"], "Hello");
    assert_eq!(internal["raw"]["action"], "edit");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}

#[tokio::test]
async fn ws_unknown_action_returns_error() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel_id = insert_widget_channel(&pool, &widget_id).await;

    let state = build_state(pool.clone()).await;
    let addr = spawn_app(state).await;

    let url = format!("ws://{addr}/ws/{widget_id}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Send message with unknown action
    ws.send(tungstenite::Message::Text(
        r#"{"action": "delete", "mid": "msg-1", "text": "x"}"#.into(),
    ))
    .await
    .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let err: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(err["status"], "error");
    assert!(err["reason"].as_str().unwrap().contains("unknown action"));

    // Connection should still be alive
    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "msg-2", "text": "still alive"}"#.into(),
    ))
    .await
    .unwrap();
    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["status"], "ok");

    ws.close(None).await.unwrap();
    delete_widget_channel(&pool, channel_id).await;
}
