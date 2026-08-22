//! Driving the WebSocket surface, and waiting on what it publishes.

use futures_util::StreamExt;
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

use super::*;
use crate::common::{TestClient, TestOperator};

/// Wait for a Redis message on the `incoming_messages` channel that matches the given channel_id.
/// Skips messages from other channels (concurrent tests).
pub async fn wait_for_redis_msg(
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

/// Connect to a WS endpoint and consume the initial auth message.
/// Returns the websocket stream, the JWT token, and a cleanup guard for the client row.
pub async fn ws_connect(
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

/// Connect to the operator WebSocket, wait for auth, return stream + operator_id + cleanup guard.
pub async fn operator_ws_connect(
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
pub async fn wait_for_ws_msg(
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

/// Deliver one inbound Instagram message and return `(chat_id, message_id)` — the
/// chat it created and the row of the message itself, which the read tests need as
/// the `mid` of an operator read receipt.
pub async fn inbound_instagram_chat(
    addr: std::net::SocketAddr,
    user_id: &str,
    igsid: &str,
    channel: &crate::common::TestChannel,
    op_ws: &mut (
             impl futures_util::Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin
         ),
) -> (String, String) {
    let body = serde_json::json!({
        "object": "instagram",
        "entry": [{
            "id": user_id,
            "time": 1_787_416_334_529i64,
            "messaging": [{
                "sender": {"id": igsid},
                "recipient": {"id": user_id},
                "message": {
                    "mid": format!("in_{}", Uuid::new_v4().simple()),
                    "text": "customer asks"
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

    let deadline = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        let event = tokio::time::timeout(remaining, wait_for_ws_msg(op_ws))
            .await
            .expect("timed out waiting for the inbound message");
        if event["channel_id"] == channel.id.to_string() && event["type"] == "message" {
            return (
                event["chat_id"].as_str().unwrap().to_string(),
                event["id"].as_str().unwrap().to_string(),
            );
        }
    }
}
