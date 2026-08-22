use crate::common::*;
use crate::support::*;

use axum::http::StatusCode;
use uuid::Uuid;

#[tokio::test]
async fn oauth_providers_lists_instagram_with_its_redirect_uri() {
    let pool = setup_pool().await;
    let state = build_state(pool.clone()).await;

    let (status, body) = request_json(state, "GET", "/api/oauth/providers", None).await;

    assert_eq!(status, StatusCode::OK);
    let list = body.as_array().unwrap();
    assert_eq!(list.len(), 1, "only instagram has an OAuth login today");
    assert_eq!(list[0]["provider"], "instagram");
    assert_eq!(list[0]["label"], "Instagram");
    assert_eq!(list[0]["start_path"], "/api/oauth/instagram/start");
    // The panel shows this so it can be pasted into the Meta dashboard.
    assert_eq!(
        list[0]["redirect_uri"],
        format!("{TEST_PUBLIC_BASE_URL}/api/oauth/instagram/callback")
    );
}

#[tokio::test]
async fn oauth_start_redirects_to_the_provider_with_a_signed_state() {
    let pool = setup_pool().await;
    let state = build_state_ig(pool.clone(), "http://mock.test".into()).await;

    let (status, headers, _) = request_raw(state, "GET", "/api/oauth/instagram/start").await;

    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    let location = headers["location"].to_str().unwrap();
    assert!(
        location.starts_with("http://mock.test/oauth/authorize?"),
        "{location}"
    );
    assert!(
        location.contains(&format!("client_id={TEST_APP_ID}")),
        "{location}"
    );

    // The state must verify against the app secret and carry no pinned channel.
    let url = reqwest::Url::parse(location).unwrap();
    let token = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .expect("state parameter");
    let claims = chatbridge::oauth::verify_state(
        TEST_JWT_SECRET.as_bytes(),
        &token,
        chatbridge::model::ProviderKind::Instagram,
    )
    .unwrap();
    assert_eq!(claims.ch, None);
}

#[tokio::test]
async fn oauth_start_pins_the_channel_the_reconnect_button_came_from() {
    let pool = setup_pool().await;
    let state = build_state_ig(pool.clone(), "http://mock.test".into()).await;
    let channel_id = Uuid::new_v4();

    let (status, headers, _) = request_raw(
        state,
        "GET",
        &format!("/api/oauth/instagram/start?channel_id={channel_id}"),
    )
    .await;

    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    let url = reqwest::Url::parse(headers["location"].to_str().unwrap()).unwrap();
    let token = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .unwrap();
    let claims = chatbridge::oauth::verify_state(
        TEST_JWT_SECRET.as_bytes(),
        &token,
        chatbridge::model::ProviderKind::Instagram,
    )
    .unwrap();
    assert_eq!(claims.ch, Some(channel_id));
}

#[tokio::test]
async fn oauth_start_is_404_for_a_provider_without_a_login() {
    let pool = setup_pool().await;
    let state = build_state(pool.clone()).await;

    let (status, _, _) = request_raw(state.clone(), "GET", "/api/oauth/telegram/start").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, _) = request_raw(state, "GET", "/api/oauth/nonsense/start").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn oauth_callback_creates_a_channel_and_subscribes_it() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let (api, requests) =
        spawn_mock_instagram_recording(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    // Taken before the first assertion: this test creates a real row, and a failing
    // assertion below would otherwise leak it into the shared database — which is
    // exactly what happened once while checking the subscribe assertion can fail.
    let _key_guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the callback is a page, never a 4xx"
    );
    assert!(body.contains("\"ok\":true"), "{body}");
    assert!(body.contains("\"created\":true"), "{body}");
    // Without this the test passes with the entire subscribe call deleted, because
    // the mock answers unlisted paths with {"success": true} and the row is written
    // either way.
    assert!(
        subscribed_all_fields(&requests),
        "no subscribe reached the provider: {:?}",
        requests.lock().unwrap()
    );

    let row = sqlx::query_as::<_, (Uuid, String, String, serde_json::Value)>(
        "SELECT id, name, external_key, config FROM channels
         WHERE provider = 'instagram' AND external_key = $1",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let _guard = TestChannel { id: row.0 };

    assert_eq!(
        row.1, "@yourbiz",
        "the name defaults to the account's handle"
    );
    assert_eq!(
        row.3["access_token"], "long_lived",
        "the short-lived token is never stored"
    );
    assert_eq!(row.3["username"], "yourbiz");
    assert!(
        row.3["token_expires_at"].is_string(),
        "the 60-day expiry is recorded so the refresher can find it"
    );
}

#[tokio::test]
async fn oauth_callback_on_a_live_channel_replaces_the_token_and_keeps_the_name() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;
    sqlx::query("UPDATE channels SET name = 'Support' WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"created\":false"), "{body}");

    let (name, config) = sqlx::query_as::<_, (String, serde_json::Value)>(
        "SELECT name, config FROM channels WHERE id = $1",
    )
    .bind(guard.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        name, "Support",
        "a login must not undo an operator's rename"
    );
    assert_eq!(config["access_token"], "long_lived");
}

#[tokio::test]
async fn oauth_callback_restores_a_deleted_channel() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;
    sqlx::query("UPDATE channels SET deleted_at = now() WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    let (api, requests) =
        spawn_mock_instagram_recording(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"restored\":true"), "{body}");
    assert!(
        subscribed_all_fields(&requests),
        "a restored channel has to be subscribed again"
    );

    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM channels WHERE id = $1")
            .bind(guard.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        deleted_at.is_none(),
        "a login is an explicit 'I want this account'"
    );
}

#[tokio::test]
async fn oauth_callback_refuses_a_different_account_when_a_channel_is_pinned() {
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

    let api = spawn_mock_instagram(instagram_login_ok(&theirs, "someone_else")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _stranger = TestChannelKey::instagram(&theirs);

    let (status, body) = oauth_callback(state, Some(guard.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("someone_else"),
        "the page names the account: {body}"
    );

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        config["access_token"], "ours",
        "the pinned channel is untouched"
    );

    let stranger: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&theirs)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        stranger, 0,
        "and no channel is created for the other account"
    );
}

#[tokio::test]
async fn oauth_callback_rolls_back_a_new_channel_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    // Every subscribe attempt fails, including the per-field probes.
    responses["me/subscribed_apps"] = serde_json::json!({
        "error": {"message": "Application does not have permission", "code": 10}
    });
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;

    // If the rollback regresses, the assertion below fires *and* the row survives; the
    // guard is what stops it from poisoning the shared database for every later test.
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "a row that never worked must not occupy this account's identity forever"
    );
}

#[tokio::test]
async fn oauth_callback_keeps_an_existing_channel_when_subscribing_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "stale"}),
    )
    .await;

    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    responses["me/subscribed_apps"] =
        serde_json::json!({"error": {"message": "temporarily unavailable", "code": 2}});
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;

    let (status, body) = oauth_callback(state, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"ok\":true"),
        "the token is still an improvement: {body}"
    );
    assert!(body.contains("\"warning\""), "{body}");

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "long_lived");
}

#[tokio::test]
async fn oauth_callback_writes_nothing_for_a_tampered_state() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, _, body) = request_raw(
        state,
        "GET",
        "/api/oauth/instagram/callback?code=AQB123&state=not.a.jwt",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn oauth_callback_reports_a_cancelled_login_without_writing() {
    let pool = setup_pool().await;
    // No mock at all: a cancelled login must not reach the provider.
    let state = build_state(pool.clone()).await;

    let (status, _, body) = request_raw(
        state,
        "GET",
        "/api/oauth/instagram/callback?error=access_denied&error_description=User+denied",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(body.contains("User denied"), "{body}");
}

#[tokio::test]
async fn oauth_callback_refuses_a_pinned_channel_that_no_longer_exists() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    // Deleted between opening the popup and finishing the login.
    let (status, body) = oauth_callback(state, Some(Uuid::new_v4())).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0, "a pin that cannot be honoured writes nothing");
}

#[tokio::test]
async fn oauth_callback_refuses_a_pinned_channel_of_another_provider() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let telegram = insert_test_telegram_channel(&pool, "s").await;
    let api = spawn_mock_instagram(instagram_login_ok(&user_id, "yourbiz")).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, Some(telegram.id)).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("telegram"),
        "the page says what the channel is: {body}"
    );
}

#[tokio::test]
async fn oauth_callback_writes_nothing_when_the_code_exchange_fails() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let mut responses = instagram_login_ok(&user_id, "yourbiz");
    responses["oauth/access_token"] = serde_json::json!({
        "error": {"message": "This authorization code has been used.", "code": 100}
    });
    let api = spawn_mock_instagram(responses).await;
    let state = build_state_ig(pool.clone(), api).await;
    let _guard = TestChannelKey::instagram(&user_id);

    let (status, body) = oauth_callback(state, None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"ok\":false"), "{body}");
    assert!(
        body.contains("authorization code"),
        "Meta's own message survives: {body}"
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE external_key = $1")
        .bind(&user_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}
