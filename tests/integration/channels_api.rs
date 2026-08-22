use crate::common::*;
use crate::support::*;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use uuid::Uuid;

use chatbridge::model::ProviderKind;
use chatbridge::routes;

#[tokio::test]
async fn get_channels_lists_live_and_deleted_with_endpoints() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let live = insert_test_widget_channel(&pool, &widget_id).await;
    let dead_id = format!("api_dead_{}", Uuid::new_v4());
    let dead = insert_test_widget_channel(&pool, &dead_id).await;
    chatbridge::db::soft_delete_channel(&pool, dead.id)
        .await
        .unwrap();

    let state = build_state(pool.clone()).await;
    let app = routes::build(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    let live_row = list
        .iter()
        .find(|c| c["id"] == live.id.to_string())
        .expect("live channel listed");
    assert_eq!(live_row["provider"], "widget");
    assert_eq!(live_row["external_key"], widget_id);
    assert_eq!(live_row["deleted_at"], serde_json::Value::Null);
    assert_eq!(
        live_row["endpoint"],
        format!("wss://test.example.com/ws/{widget_id}")
    );

    let dead_row = list
        .iter()
        .find(|c| c["id"] == dead.id.to_string())
        .expect("deleted channel is listed too, not hidden");
    assert!(dead_row["deleted_at"].is_string());
}

#[tokio::test]
async fn get_channels_returns_telegram_secrets_in_full() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "api_secret").await;

    let state = build_state(pool.clone()).await;
    let app = routes::build(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    let row = list
        .iter()
        .find(|c| c["id"] == guard.id.to_string())
        .expect("channel listed");
    // Deliberate: the panel shows and edits keys. See docs/tech_debt.md.
    assert_eq!(row["config"]["bot_secret"], "api_secret");
    assert!(row["config"]["bot_token"].as_str().unwrap().contains(':'));
    assert_eq!(
        row["endpoint"],
        format!("https://test.example.com/webhook/telegram/{}", guard.id)
    );
}

#[tokio::test]
async fn post_channel_creates_a_widget_channel() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id, "name": "Acme site"}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    assert_eq!(body["provider"], "widget");
    assert_eq!(body["name"], "Acme site");
    assert_eq!(body["external_key"], widget_id);
    assert_eq!(body["config"], serde_json::json!({}));
    assert_eq!(
        body["endpoint"],
        format!("wss://test.example.com/ws/{widget_id}")
    );

    let stored = chatbridge::db::find_live_channel_by_id(&pool, id)
        .await
        .unwrap();
    assert!(stored.is_some(), "row persisted");
}

#[tokio::test]
async fn post_channel_defaults_widget_name_to_the_widget_id() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let _guard = TestChannel {
        id: body["id"].as_str().unwrap().parse().unwrap(),
    };
    assert_eq!(body["name"], widget_id);
}

#[tokio::test]
async fn post_channel_rejects_a_widget_id_that_breaks_the_route() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": "has spaces/and-slash"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_channel_duplicate_widget_id_conflicts_with_channel_exists() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let existing = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], existing.id.to_string());
    assert_eq!(body["deleted_at"], serde_json::Value::Null);
}

#[tokio::test]
async fn post_channel_on_a_deleted_identity_conflicts_with_channel_deleted() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let existing = insert_test_widget_channel(&pool, &widget_id).await;
    chatbridge::db::soft_delete_channel(&pool, existing.id)
        .await
        .unwrap();
    let state = build_state(pool.clone()).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "widget", "widget_id": widget_id}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_deleted");
    assert_eq!(body["channel_id"], existing.id.to_string());
    assert!(
        body["deleted_at"].is_string(),
        "the panel shows when it was deleted"
    );
}

#[tokio::test]
async fn post_instagram_channel_derives_its_identity_from_the_token() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"success": true},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "instagram", "access_token": "IGQ_token"}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    // The whole point of the manual path is that it produces a working channel rather
    // than a silent one, so the subscribe is the assertion that matters.
    assert!(
        subscribed_all_fields(&requests),
        "the manual path must subscribe too"
    );

    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    // The operator types only the token; /me is the authority on who it belongs to.
    assert_eq!(body["external_key"], user_id);
    assert_eq!(body["name"], "@yourbiz");
    assert_eq!(body["config"]["access_token"], "IGQ_token");
    assert_eq!(body["config"]["username"], "yourbiz");
    assert!(
        body["config"]["token_expires_at"].is_null(),
        "a pasted token has no known expiry; the refresher fills it in"
    );
    assert_eq!(
        body["endpoint"],
        "https://test.example.com/webhook/instagram"
    );
}

#[tokio::test]
async fn post_instagram_channel_rejects_an_empty_token() {
    let pool = setup_pool().await;
    // No mock: validation must fail before any network call.
    let state = build_state(pool.clone()).await;

    let (status, body) = request_text(
        state,
        "POST",
        "/api/channels",
        Some(serde_json::json!({"provider": "instagram", "access_token": "  "})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Asserting the status alone proves nothing: with the guard removed, `fetch_profile`
    // hits the unreachable default base, and that failure is also mapped to a 400.
    assert!(body.contains("must not be empty"), "{body}");
}

#[tokio::test]
async fn post_instagram_channel_rolls_back_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"error": {"message": "no permission", "code": 10}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "instagram", "access_token": "IGQ_token"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "the manual path must not leave a silent channel behind"
    );
}

#[tokio::test]
async fn patch_instagram_channel_drops_the_stale_expiry() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({
            "access_token": "old",
            "token_expires_at": "2026-10-19T09:00:00Z",
            "username": "yourbiz"
        }),
    )
    .await;
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
        "me/subscribed_apps": {"success": true},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "new"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["config"]["access_token"], "new");
    assert!(
        body["config"]["token_expires_at"].is_null(),
        "the old expiry described the token that was just replaced"
    );
    assert_eq!(body["config"]["username"], "yourbiz");
    assert!(
        subscribed_all_fields(&requests),
        "a new token has to be armed, or the whole Rearm branch could be deleted unnoticed"
    );
}

#[tokio::test]
async fn patch_instagram_channel_refuses_a_token_from_another_account() {
    let pool = setup_pool().await;
    let ours = format!("ig_{}", Uuid::new_v4().simple());
    let theirs = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &ours,
        serde_json::json!({"access_token": "ours"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me": {"user_id": theirs, "username": "someone_else"},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "theirs"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "ours");
}

#[tokio::test]
async fn delete_instagram_channel_unsubscribes() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({})).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _, _) = request_raw(state, "DELETE", &format!("/api/channels/{}", guard.id)).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    // Unsubscribing is best effort, so a failure is invisible from the outside — the
    // request log is the only way to tell "it failed" from "it never happened".
    assert!(
        saw(&requests, "DELETE", "me/subscribed_apps"),
        "the account has to be unsubscribed: {:?}",
        requests.lock().unwrap()
    );
}

#[tokio::test]
async fn delete_instagram_channel_survives_a_failing_unsubscribe() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "revoked"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"error": {"message": "Invalid OAuth access token.", "code": 190}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _, _) = request_raw(state, "DELETE", &format!("/api/channels/{}", guard.id)).await;

    // A channel whose token was revoked must stay deletable.
    assert_eq!(status, StatusCode::NO_CONTENT);
    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(deleted_at.is_some());
}

#[tokio::test]
async fn patch_does_not_subscribe_a_channel_that_stays_deleted() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "old"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let (api, requests) = spawn_mock_instagram_recording(serde_json::json!({
        "me": {"user_id": user_id, "username": "yourbiz"},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    // Editing a deleted channel's token is allowed; subscribing it is not. Meta would
    // start delivering events for a channel the app rejects, and report the failures
    // against something the operator believes is gone.
    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "instagram", "access_token": "new"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        !saw(&requests, "POST", "me/subscribed_apps"),
        "a channel that stays deleted must not be armed"
    );
}

#[tokio::test]
async fn restore_instagram_channel_resubscribes() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "tok"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    // Subscribing fails, so a restore that forgot to re-subscribe would pass this
    // test silently; the 502 is what proves the call happened.
    let api = spawn_mock_instagram(serde_json::json!({
        "me/subscribed_apps": {"error": {"message": "nope", "code": 10}},
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({"restore": true})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        deleted_at.is_none(),
        "the row is already committed; only the subscription failed"
    );
}

#[tokio::test]
async fn post_telegram_channel_registers_the_webhook_and_stores_a_secret() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAtoken")}),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let _guard = TestChannel { id };

    // external_key is the bot id from getMe, not a parsed token prefix.
    assert_eq!(body["external_key"], bot_id.to_string());
    // The name defaults to the bot's @username.
    assert_eq!(body["name"], "@acme_bot");
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAtoken"));
    let secret = body["config"]["bot_secret"].as_str().unwrap();
    assert_eq!(secret.len(), 32, "32 hex chars from a v4 UUID");
    assert!(secret.bytes().all(|b| b.is_ascii_alphanumeric()));
    assert_eq!(
        body["endpoint"],
        format!("https://test.example.com/webhook/telegram/{id}")
    );
}

#[tokio::test]
async fn post_telegram_channel_rejects_a_token_telegram_refuses() {
    let pool = setup_pool().await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "Unauthorized"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": "123456789:AAbad"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_telegram_channel_rejects_a_malformed_token_without_calling_telegram() {
    let pool = setup_pool().await;
    // getMe is wired to fail loudly: reaching it would mean validation was skipped.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "should never be called"},
    }))
    .await;
    let state = build_state_with(pool, api).await;

    let (status, _) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": "not-a-token"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_telegram_channel_rolls_the_row_back_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook: HTTPS url must be provided"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = post_json(
        state.clone(),
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAtoken")}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);

    // The rollback is a HARD delete, so the identity is free and a retry is a
    // clean create rather than a 409 on a channel that never worked.
    let leftover = chatbridge::db::find_channel_by_external_key(
        &pool,
        ProviderKind::Telegram,
        &bot_id.to_string(),
    )
    .await
    .unwrap();
    assert!(leftover.is_none(), "no row may survive a failed setWebhook");
}

#[tokio::test]
async fn post_telegram_channel_conflicts_before_touching_the_webhook() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let existing = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "old_secret"}),
    )
    .await;

    // setWebhook is wired to fail: if the handler called it, the test would see 502
    // instead of 409, which is exactly the webhook-hijack ordering bug.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "must not be reached"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = post_json(
        state,
        "/api/channels",
        serde_json::json!({"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], existing.id.to_string());

    // The live channel's stored secret is untouched.
    let stored = chatbridge::db::find_channel_by_id(&pool, existing.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.config["bot_secret"], "old_secret");
}

#[tokio::test]
async fn patch_channel_renames_without_contacting_the_provider() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "patch_secret").await;
    // getMe fails loudly: a plain rename must not call Telegram at all.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": {"ok": false, "description": "must not be reached"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({"name": "Renamed"})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Renamed");
    assert_eq!(
        body["config"]["bot_secret"], "patch_secret",
        "config untouched"
    );
}

#[tokio::test]
async fn patch_channel_unknown_id_returns_404() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", Uuid::new_v4()),
        Some(serde_json::json!({"name": "x"})),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_channel_cannot_change_the_provider() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": "123456789:AA"}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_channel_moves_a_widget_id_and_conflicts_when_taken() {
    let pool = setup_pool().await;
    let first = format!("api_a_{}", Uuid::new_v4());
    let second = format!("api_b_{}", Uuid::new_v4());
    let moving = insert_test_widget_channel(&pool, &first).await;
    let blocker = insert_test_widget_channel(&pool, &second).await;
    let state = build_state(pool.clone()).await;

    // Free key — accepted, and the endpoint follows the new key.
    let free = format!("api_c_{}", Uuid::new_v4());
    let (status, body) = request_json(
        state.clone(),
        "PATCH",
        &format!("/api/channels/{}", moving.id),
        Some(serde_json::json!({"spec": {"provider": "widget", "widget_id": free}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["external_key"], free);
    assert_eq!(
        body["endpoint"],
        format!("wss://test.example.com/ws/{free}")
    );

    // Taken key — 409 naming the blocking channel.
    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", moving.id),
        Some(serde_json::json!({"spec": {"provider": "widget", "widget_id": second}})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "channel_exists");
    assert_eq!(body["channel_id"], blocker.id.to_string());
}

#[tokio::test]
async fn patch_channel_rotates_a_telegram_token_for_the_same_bot() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "kept_secret"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAnew"));
    assert_eq!(
        body["config"]["bot_secret"], "kept_secret",
        "the webhook secret survives a token rotation"
    );
}

#[tokio::test]
async fn patch_channel_refuses_a_token_belonging_to_another_bot() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let other_bot_id = bot_id + 1;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({"getMe": get_me_ok(other_bot_id)})).await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{other_bot_id}:AAother")}
        })),
    )
    .await;

    // Repointing a channel at a different bot is not an edit: chats and messages
    // hang off this channel id.
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_channel_restores_and_rewrites_the_submitted_fields() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAdead"), "bot_secret": "old"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": true, "result": true},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "restore": true,
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAfresh")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted_at"], serde_json::Value::Null);
    assert_eq!(
        body["config"]["bot_token"],
        format!("{bot_id}:AAfresh"),
        "restore writes the freshly entered token, not the dead one"
    );
    assert_eq!(body["id"], guard.id.to_string(), "the original id is kept");
}

#[tokio::test]
async fn patch_channel_returns_502_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn patch_channel_drops_the_cache_even_when_set_webhook_fails() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "bad webhook"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    // Warm the cache with the pre-PATCH config.
    state
        .cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();

    let (status, _) = request_json(
        state.clone(),
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    // The UPDATE committed before setWebhook failed, so the cache must not keep
    // serving the old config — the webhook handler verifies secrets against it.
    let cached = state
        .cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel still live");
    assert_eq!(cached.config["bot_token"], format!("{bot_id}:AAnew"));
}

#[tokio::test]
async fn patch_channel_does_not_arm_the_webhook_of_a_channel_that_stays_deleted() {
    let pool = setup_pool().await;
    let bot_id = (Uuid::new_v4().as_u128() as u32) as i64;
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &bot_id.to_string(),
        serde_json::json!({"bot_token": format!("{bot_id}:AAold"), "bot_secret": "s"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    // setWebhook is wired to fail: reaching it would turn this into a 502.
    let api = spawn_mock_telegram(serde_json::json!({
        "getMe": get_me_ok(bot_id),
        "setWebhook": {"ok": false, "description": "must not be reached"},
        "deleteWebhook": {"ok": true, "result": true},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, body) = request_json(
        state,
        "PATCH",
        &format!("/api/channels/{}", guard.id),
        Some(serde_json::json!({
            "spec": {"provider": "telegram", "bot_token": format!("{bot_id}:AAnew")}
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "editing a deleted channel is allowed"
    );
    assert!(body["deleted_at"].is_string(), "and it stays deleted");
    assert_eq!(body["config"]["bot_token"], format!("{bot_id}:AAnew"));
}

#[tokio::test]
async fn delete_channel_soft_deletes_and_is_idempotent() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;
    let state = build_state(pool.clone()).await;

    let (status, _) = request_json(
        state.clone(),
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let row = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("the row survives — history is kept");
    let first_deleted_at = row.deleted_at.expect("deleted_at set");

    // A repeat delete answers 204 as well and does not move the timestamp.
    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let again = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.deleted_at, Some(first_deleted_at));
}

#[tokio::test]
async fn delete_channel_unknown_id_is_still_204() {
    let pool = setup_pool().await;
    let state = build_state(pool).await;
    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn delete_channel_succeeds_even_when_delete_webhook_fails() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "del_secret").await;
    // A revoked token makes deleteWebhook fail; the channel must still be deletable.
    let api = spawn_mock_telegram(serde_json::json!({
        "deleteWebhook": {"ok": false, "description": "Unauthorized"},
    }))
    .await;
    let state = build_state_with(pool.clone(), api).await;

    let (status, _) = request_json(
        state,
        "DELETE",
        &format!("/api/channels/{}", guard.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    let row = chatbridge::db::find_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.deleted_at.is_some());
}
