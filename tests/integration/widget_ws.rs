use crate::common::*;
use crate::support::*;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

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

#[tokio::test]
async fn the_ack_names_the_row_the_message_was_stored_as() {
    // The panel and the widget both anchor read and edit events on this id. Without
    // it the optimistic bubble has no stable identity: its external id is replaced
    // the moment the provider answers.
    let pool = setup_pool().await;
    let widget_id = format!("ack_row_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;

    let addr = spawn_app(build_state(pool.clone()).await).await;
    let (mut ws, _token, _client) = ws_connect(addr, &widget_id).await;

    ws.send(tungstenite::Message::Text(
        r#"{"action": "send", "mid": "550e8400-e29b-41d4-a716-446655440001", "text": "Hello", "attachments": []}"#.into(),
    ))
    .await
    .unwrap();

    let resp = ws.next().await.unwrap().unwrap();
    let ack: serde_json::Value = serde_json::from_str(resp.to_text().unwrap()).unwrap();
    assert_eq!(ack["action"], "ack");
    assert!(ack["id"].is_string(), "the ack has to name the row: {ack}");

    let stored: Uuid = sqlx::query_scalar("SELECT id FROM messages WHERE channel_id = $1")
        .bind(channel.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ack["id"].as_str().unwrap(), stored.to_string());

    ws.close(None).await.unwrap();
}
