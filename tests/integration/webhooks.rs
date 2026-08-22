use crate::common::*;
use crate::support::*;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::cache::{ChannelCache, ClientCache};
use chatbridge::model::ProviderKind;
use chatbridge::routes;

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

#[tokio::test]
async fn two_telegram_clients_can_both_send_message_id_one() {
    // Telegram numbers messages per chat, so every new conversation with a bot starts
    // near 1. Keyed on the message id alone, the second person's first message
    // collides with the first person's and ON CONFLICT DO NOTHING erases it — at
    // debug level, so nothing above debug ever says a message was lost.
    let pool = setup_pool().await;
    let bot_secret = "collision_secret";
    let channel = insert_test_telegram_channel(&pool, bot_secret).await;
    let app = routes::build(build_state(pool.clone()).await);

    for (chat_id, sender_id) in [(1001i64, 5001i64), (2002i64, 6002i64)] {
        let body = serde_json::json!({
            "update_id": chat_id,
            "message": {
                "message_id": 1,
                "date": 1700000000,
                "from": {"id": sender_id, "first_name": "Test"},
                "chat": {"id": chat_id, "type": "private"},
                "text": "first message"
            }
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/webhook/telegram/{}", channel.id))
                    .header("content-type", "application/json")
                    .header("X-Telegram-Bot-Api-Secret-Token", bot_secret)
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Ingestion is spawned, so poll rather than sleep a fixed amount.
    let mut stored: Vec<String> = Vec::new();
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        stored = sqlx::query_scalar(
            "SELECT external_message_id FROM messages WHERE channel_id = $1
             ORDER BY external_message_id",
        )
        .bind(channel.id)
        .fetch_all(&pool)
        .await
        .unwrap();
        if stored.len() == 2 {
            break;
        }
    }

    assert_eq!(
        stored,
        vec!["telegram:1001:1".to_owned(), "telegram:2002:1".to_owned()],
        "both first messages have to be stored under distinct ids"
    );
}
