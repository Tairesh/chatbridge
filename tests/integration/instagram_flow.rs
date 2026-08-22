use crate::common::*;
use crate::support::*;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite;
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::routes;

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

#[tokio::test]
async fn an_insert_that_did_not_happen_is_never_delivered() {
    // The invariant: we never send a message we did not store. `persist_and_publish`
    // returns the row it inserted, and delivery is spawned only on `Some`. Here the
    // second send collides with the first on `(channel_id, external_message_id)`, so
    // there is no row for it and nothing goes out.
    //
    // This is not message-level idempotency and does not claim to be: the id it
    // collides on is `operator:<mid>`, which a successful send replaces with the
    // provider's. The send is kept failing so the local id stays put and the
    // collision is the thing under test.
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
        "me/messages": {"error": {"message": "temporarily unavailable", "code": 2}},
    }))
    .await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, _inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;

    let mid = Uuid::new_v4().to_string();
    for _ in 0..2 {
        let reply = serde_json::json!({
            "action": "send", "chat_id": chat_id, "mid": mid, "text": "once"
        });
        op_ws
            .send(tungstenite::Message::Text(
                serde_json::to_string(&reply).unwrap().into(),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    assert_eq!(
        count(&requests, "POST", "me/messages"),
        1,
        "the second send of a mid we already stored must not reach Instagram: {:?}",
        requests.lock().unwrap()
    );

    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM messages WHERE channel_id = $1 AND sender_type = 'operator'",
    )
    .bind(channel.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rows, 1, "and it must not have produced a second row either");
}

#[tokio::test]
async fn an_echo_from_the_instagram_app_lands_in_history() {
    // The owner can answer from the Instagram app. The echo is the only record of
    // that message we will ever get, and a read receipt for it anchors on nothing
    // unless the row exists.
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let customer = format!("igsid_{}", Uuid::new_v4().simple());
    let channel = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({})).await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": user_id},
                "recipient": {"id": customer},
                "message": {
                    "mid": "mid_from_app",
                    "text": "answered from the phone",
                    "is_echo": true
                }
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

    let mut row: Option<(String, Option<Uuid>, Option<Uuid>)> = None;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        row = sqlx::query_as(
            "SELECT sender_type, sender_id, chat_id FROM messages
             WHERE channel_id = $1 AND external_message_id = 'instagram:mid_from_app'",
        )
        .bind(channel.id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        if row.is_some() {
            break;
        }
    }

    let (sender_type, sender_id, chat_id) = row.expect("the echo has to be stored");
    assert_eq!(sender_type, "operator", "it is the account's own message");
    assert!(sender_id.is_none(), "nobody in `operators` typed it");
    assert!(chat_id.is_some(), "it belongs to the customer's chat");
}

#[tokio::test]
async fn an_echo_of_our_own_reply_adds_nothing() {
    // Our reply comes back as an echo carrying the id we already adopted, so the
    // unique index absorbs it. Without the adoption it would appear twice.
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
        "me/messages": {"message_id": "mid_echoed", "recipient_id": igsid},
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

    // Wait for the adoption before echoing, which is the ordering Meta produces in
    // practice; the racing order is covered by `adopting_over_an_echo_...`.
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let adopted: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM messages
             WHERE channel_id = $1 AND external_message_id = 'instagram:mid_echoed'",
        )
        .bind(channel.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if adopted == 1 {
            break;
        }
    }

    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": user_id},
                "recipient": {"id": igsid},
                "message": {"mid": "mid_echoed", "text": "operator answers", "is_echo": true}
            }]
        }]
    });
    let raw = serde_json::to_vec(&body).unwrap();
    let signature = sign_body(TEST_APP_SECRET, &raw);
    reqwest::Client::new()
        .post(format!("http://{addr}/webhook/instagram"))
        .header("content-type", "application/json")
        .header("X-Hub-Signature-256", format!("sha256={signature}"))
        .body(raw)
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let operator_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM messages WHERE channel_id = $1 AND sender_type = 'operator'",
    )
    .bind(channel.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        operator_rows, 1,
        "the echo of our own reply must not duplicate it"
    );
}

#[tokio::test]
async fn a_reply_instagram_refuses_is_marked_undelivered() {
    // History that shows a refused reply as delivered is history that lies.
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
        "me/messages": {"error": {
            "message": "This message is sent outside of allowed window",
            "code": 10,
            "error_subcode": 2534022
        }},
    }))
    .await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, _inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;
    let reply = serde_json::json!({
        "action": "send", "chat_id": chat_id, "mid": Uuid::new_v4().to_string(),
        "text": "too late"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();

    // The operator is told, and told which row.
    let failure = loop {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            wait_for_ws_msg(&mut op_ws),
        )
        .await
        .expect("timed out waiting for the delivery failure");
        if event["action"] == "delivery_failed" {
            break event;
        }
    };
    assert!(
        failure["reason"]
            .as_str()
            .unwrap()
            .contains("24-hour window"),
        "the operator has to learn that waiting will not help: {failure}"
    );

    let (id, status, external): (Uuid, String, String) = sqlx::query_as(
        "SELECT id, status, external_message_id FROM messages
         WHERE channel_id = $1 AND sender_type = 'operator'",
    )
    .bind(channel.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert!(
        external.starts_with("operator:"),
        "there is no provider id to adopt for a message the provider refused"
    );
    assert_eq!(failure["message_id"].as_str().unwrap(), id.to_string());
}

#[tokio::test]
async fn a_delivered_reply_tells_the_operator_and_the_database() {
    // The first tick: the provider took the message. It has to reach the panel live
    // *and* survive a reload, which is why it is a status and not only an event.
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
        "me/messages": {"message_id": "mid_delivered", "recipient_id": igsid},
    }))
    .await;
    let addr = spawn_app(build_state_ig(pool.clone(), api).await).await;
    let (mut op_ws, _operator_id, _op_guard) = operator_ws_connect(addr).await;

    let (chat_id, _inbound_id) =
        inbound_instagram_chat(addr, &user_id, &igsid, &channel, &mut op_ws).await;
    let reply = serde_json::json!({
        "action": "send", "chat_id": chat_id, "mid": Uuid::new_v4().to_string(),
        "text": "on its way"
    });
    op_ws
        .send(tungstenite::Message::Text(
            serde_json::to_string(&reply).unwrap().into(),
        ))
        .await
        .unwrap();

    let delivered = loop {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            wait_for_ws_msg(&mut op_ws),
        )
        .await
        .expect("timed out waiting for the delivery notification");
        if event["action"] == "delivered" {
            break event;
        }
    };

    let (id, status): (Uuid, String) = sqlx::query_as(
        "SELECT id, status FROM messages WHERE channel_id = $1 AND sender_type = 'operator'",
    )
    .bind(channel.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "delivered");
    assert_eq!(delivered["message_id"].as_str().unwrap(), id.to_string());
}
