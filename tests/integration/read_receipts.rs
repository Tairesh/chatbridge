use crate::common;
use crate::common::*;
use crate::support::*;

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

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
            conversation: Some(chatbridge::model::Conversation::Customer(client_id)),
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
            // `widget:<client_id>:<mid>` — the client id is what keeps two visitors
            // sending the same browser-generated mid from erasing each other.
            let external = event["external_message_id"].as_str().unwrap();
            assert!(
                external.starts_with("widget:") && external.ends_with(&format!(":{mid}")),
                "the read has to name the message the widget sent: {external}"
            );
            break;
        }
    }

    widget_ws.close(None).await.unwrap();
    op_ws.close(None).await.unwrap();
}

/// One message's status, for tests that assert on the row rather than on a count.
async fn status_of(pool: &sqlx::PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// `created_at` is the watermark, and Postgres resolves it finely enough that two
/// inserts in the same millisecond would be ordered arbitrarily.
async fn tick() {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
}

#[tokio::test]
async fn a_read_receipt_marks_its_anchor_as_well_as_everything_older() {
    // "Mark everything older" would leave the message the customer actually tapped
    // showing as unread. The watermark test next door only counts rows, so the
    // anchor's own status is asserted here directly.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "anchor").await;
    let older = seed_message(&pool, channel.id, chat_id, None, "operator", "instagram:m1").await;
    tick().await;
    let anchor = seed_message(&pool, channel.id, chat_id, None, "operator", "instagram:m2").await;
    tick().await;
    let newer = seed_message(&pool, channel.id, chat_id, None, "operator", "instagram:m3").await;

    let reads = chatbridge::db::mark_messages_read(&pool, channel.id, "instagram:m2", "client")
        .await
        .unwrap();
    assert_eq!(reads.len(), 2);

    assert_eq!(status_of(&pool, older).await, "read");
    assert_eq!(
        status_of(&pool, anchor).await,
        "read",
        "the anchor itself was read too"
    );
    assert_eq!(status_of(&pool, newer).await, "new");
}

#[tokio::test]
async fn a_reader_never_marks_their_own_messages() {
    let pool = setup_pool().await;
    let (channel, client_id, chat_id) = seed_chat(&pool, "own").await;
    let theirs = seed_message(
        &pool,
        channel.id,
        chat_id,
        Some(client_id),
        "client",
        "instagram:c1",
    )
    .await;
    tick().await;
    seed_message(&pool, channel.id, chat_id, None, "operator", "instagram:o1").await;

    let reads = chatbridge::db::mark_messages_read(&pool, channel.id, "instagram:o1", "client")
        .await
        .unwrap();
    assert_eq!(reads.len(), 1, "only the operator's message is marked");
    assert_eq!(
        status_of(&pool, theirs).await,
        "new",
        "the reader's own message is not a read receipt"
    );
}

#[tokio::test]
async fn a_read_receipt_leaves_other_chats_on_the_same_channel_alone() {
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "mine").await;
    let other_client = chatbridge::db::create_client(&pool).await.unwrap();
    let other_chat = chatbridge::db::find_or_create_chat(&pool, other_client, channel.id)
        .await
        .unwrap();

    seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "instagram:mine",
    )
    .await;
    let elsewhere = seed_message(
        &pool,
        channel.id,
        other_chat,
        None,
        "operator",
        "instagram:theirs",
    )
    .await;

    chatbridge::db::mark_messages_read(&pool, channel.id, "instagram:mine", "client")
        .await
        .unwrap();

    assert_eq!(status_of(&pool, elsewhere).await, "new");
}

#[tokio::test]
async fn mark_chat_read_marks_everything_unread_from_the_other_side() {
    let pool = setup_pool().await;
    let (channel, client_id, chat_id) = seed_chat(&pool, "fallback").await;
    seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "operator:local-1",
    )
    .await;
    tick().await;
    seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "operator:local-2",
    )
    .await;
    tick().await;
    let theirs = seed_message(
        &pool,
        channel.id,
        chat_id,
        Some(client_id),
        "client",
        "instagram:in-1",
    )
    .await;

    let reads = chatbridge::db::mark_chat_read(&pool, channel.id, client_id, "client")
        .await
        .unwrap();
    assert_eq!(
        reads.len(),
        2,
        "both operator messages, neither of them anchored"
    );
    assert_eq!(status_of(&pool, theirs).await, "new");
}

#[tokio::test]
async fn mark_chat_read_without_an_active_chat_returns_empty() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("nochat_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };

    let reads = chatbridge::db::mark_chat_read(&pool, channel.id, client_id, "client")
        .await
        .unwrap();
    assert!(reads.is_empty(), "no chat is not an error");
}

/// Deliver a signed Instagram read receipt. `mid` is `None` for the shape Meta may
/// send without one.
async fn post_instagram_read(
    addr: std::net::SocketAddr,
    user_id: &str,
    igsid: &str,
    mid: Option<&str>,
) {
    let read = match mid {
        Some(mid) => serde_json::json!({ "mid": mid }),
        None => serde_json::json!({}),
    };
    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": igsid},
                "recipient": {"id": user_id},
                "read": read
            }]
        }]
    });
    let raw = serde_json::to_vec(&body).unwrap();
    let signature = sign_body(TEST_APP_SECRET, &raw);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/webhook/instagram"))
        .header("content-type", "application/json")
        .header("X-Hub-Signature-256", format!("sha256={signature}"))
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Poll one side's message status until it reaches `want`, then return what it is.
///
/// Not "until it stops being new": an operator reply passes through `delivered` on its
/// way to `read`, so anything less specific would stop at the wrong one.
async fn await_status(
    pool: &sqlx::PgPool,
    channel_id: Uuid,
    sender_type: &str,
    want: &str,
) -> String {
    let mut status = String::new();
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        status = status_of_side(pool, channel_id, sender_type).await;
        if status == want {
            break;
        }
    }
    status
}

/// One side's message status, read once.
async fn status_of_side(pool: &sqlx::PgPool, channel_id: Uuid, sender_type: &str) -> String {
    sqlx::query_scalar("SELECT status FROM messages WHERE channel_id = $1 AND sender_type = $2")
        .bind(channel_id)
        .bind(sender_type)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn an_instagram_read_receipt_marks_the_operators_reply() {
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
    let (api, _requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me/messages": {"message_id": "mid_read", "recipient_id": igsid},
    }))
    .await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, _inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;
    let reply = serde_json::json!({
        "action": "send", "chat_id": chat_id, "mid": Uuid::new_v4().to_string(),
        "text": "operator answers"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();

    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let adopted: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM messages
             WHERE channel_id = $1 AND external_message_id = 'instagram:mid_read'",
        )
        .bind(channel.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if adopted == 1 {
            break;
        }
    }

    post_instagram_read(addr, &user_id, &igsid, Some("mid_read")).await;

    assert_eq!(
        await_status(&pool, channel.id, "operator", "read").await,
        "read",
        "the customer's receipt has to mark the reply it named"
    );
}

#[tokio::test]
async fn a_read_receipt_with_an_unknown_mid_falls_back_to_the_chat() {
    // Two ways to get here: the receipt overtook the id adoption, or the owner
    // answered from the Instagram app so the reply only exists as an echo. Either
    // way the message must not stay unread forever.
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
    let (api, _requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me/messages": {"message_id": "mid_never_echoed", "recipient_id": igsid},
    }))
    .await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, _inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;
    let reply = serde_json::json!({
        "action": "send", "chat_id": chat_id, "mid": Uuid::new_v4().to_string(),
        "text": "operator answers"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    post_instagram_read(addr, &user_id, &igsid, Some("mid_nobody_has_ever_seen")).await;

    assert_eq!(
        await_status(&pool, channel.id, "operator", "read").await,
        "read",
        "an unknown anchor falls back to the whole chat"
    );
    assert_eq!(
        status_of_side(&pool, channel.id, "client").await,
        "new",
        "the customer's own message is not marked by the customer's receipt"
    );
}

#[tokio::test]
async fn reading_an_instagram_message_marks_the_thread_seen_on_instagram() {
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
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({})).await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;

    let read = serde_json::json!({"action": "read", "chat_id": chat_id, "mid": inbound_id});
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&read).unwrap().into(),
        ))
        .await
        .unwrap();

    let mut seen = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if saw_body(&requests, "me/messages", "mark_seen") {
            seen = true;
            break;
        }
    }
    assert!(
        seen,
        "the operator's read never reached Instagram: {:?}",
        requests.lock().unwrap()
    );
}

#[tokio::test]
async fn reading_a_widget_message_sends_nothing_to_any_provider() {
    // The seen marker is provider-specific. A widget customer learns about it over
    // their own socket, and there is no API to call.
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("seen_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    let inbound = seed_message(
        &pool,
        channel.id,
        chat_id,
        Some(client_id),
        "client",
        &format!("widget:{client_id}:{}", Uuid::new_v4()),
    )
    .await;

    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({})).await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let read = serde_json::json!({
        "action": "read", "chat_id": chat_id, "mid": inbound.to_string()
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&read).unwrap().into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(count(&requests, "POST", "me/messages"), 0);
}
