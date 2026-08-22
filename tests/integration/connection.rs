use crate::common::*;
use crate::support::*;

use axum::http::StatusCode;
use uuid::Uuid;

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
