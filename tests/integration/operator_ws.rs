use crate::common::*;
use crate::support::*;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::routes;

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
async fn a_telegram_reply_adopts_the_id_the_bot_api_returned() {
    // Without this the reply keeps its local `operator:<uuid>` and no later event can
    // ever be matched to it.
    let pool = setup_pool().await;
    let api = spawn_mock_telegram(serde_json::json!({
        "sendMessage": {"ok": true, "result": {"message_id": 77, "chat": {"id": 4242}}}
    }))
    .await;
    let channel = insert_test_telegram_channel(&pool, "adopt_secret").await;
    let client_id = chatbridge::db::upsert_client(
        &pool,
        Uuid::new_v4(),
        chatbridge::model::ProviderKind::Telegram,
        "4242",
        Some("Customer"),
        None,
    )
    .await
    .unwrap();
    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();

    let addr = spawn_app(build_state_with(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

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

    // Delivery is spawned after the ack, so poll for the adoption. The row itself may
    // also be missing on the first pass: nothing here waits for the ack, so `None` is
    // "not stored yet" and has to keep the loop going rather than fail it.
    let mut external_id: Option<String> = None;
    for _ in 0..40 {
        external_id = sqlx::query_scalar(
            "SELECT external_message_id FROM messages
             WHERE channel_id = $1 AND sender_type = 'operator'",
        )
        .bind(channel.id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        if external_id
            .as_deref()
            .is_some_and(|id| id.starts_with("telegram:"))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(external_id.as_deref(), Some("telegram:4242:77"));
}

#[tokio::test]
async fn a_new_operator_is_not_anonymous() {
    // `name` was read in two places and written in none, so every operator row had
    // NULL forever and every outbound bubble was authorless.
    let pool = setup_pool().await;
    let id = chatbridge::db::create_operator(&pool).await.unwrap();
    let _guard = TestOperator { id };

    let operator = chatbridge::db::find_operator_by_id(&pool, id)
        .await
        .unwrap()
        .expect("the operator we just created");
    assert!(
        operator.name.starts_with("Operator "),
        "expected a derived name, got {:?}",
        operator.name
    );
}
