use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
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

fn build_state(db: PgPool) -> Arc<AppState> {
    Arc::new(AppState {
        config: AppConfig {
            port: 3000,
            meta_verify_token: TEST_VERIFY_TOKEN.into(),
            instagram_app_secret: TEST_APP_SECRET.into(),
        },
        db,
    })
}

// --- Meta verify (GET) ---

#[tokio::test]
async fn meta_verify_valid() {
    let pool = setup_pool().await;
    let app = routes::build(build_state(pool));

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
    let app = routes::build(build_state(pool));

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
    let app = routes::build(build_state(pool));

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
    let app = routes::build(build_state(pool));

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
    let app = routes::build(build_state(pool));

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
    let app = routes::build(build_state(pool));

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

    // Insert a test channel
    let channel_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO instagram_channels (id, instagram_user_id, user_id, access_token) VALUES ($1, $2, $3, $4)"
    )
    .bind(channel_id)
    .bind("17841448717100001")
    .bind("test_user")
    .bind("test_token")
    .execute(&pool)
    .await
    .unwrap();

    let app = routes::build(build_state(pool.clone()));

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "time": 1773347860136_i64,
            "id": "17841448717100001",
            "messaging": [{
                "sender": {"id": "836189122827510"},
                "recipient": {"id": "17841448717100001"},
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
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind("123456:ABC-DEF")
        .bind(bot_secret)
        .execute(&pool)
        .await
        .unwrap();

    let app = routes::build(build_state(pool.clone()));

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
    sqlx::query("INSERT INTO telegram_channels (id, bot_token, bot_secret) VALUES ($1, $2, $3)")
        .bind(channel_id)
        .bind("123456:INVALID-TEST")
        .bind("correct_secret")
        .execute(&pool)
        .await
        .unwrap();

    let app = routes::build(build_state(pool.clone()));

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
    let app = routes::build(build_state(pool));

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
