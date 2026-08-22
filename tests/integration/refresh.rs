use crate::common::*;
use crate::support::*;

use uuid::Uuid;

#[tokio::test]
async fn the_refresher_replaces_an_expiring_token() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({
            "access_token": "about_to_die",
            "token_expires_at": (chrono::Utc::now() + chrono::TimeDelta::days(2)).to_rfc3339(),
            "username": "yourbiz"
        }),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "access_token": "fresh", "token_type": "bearer", "expires_in": 5_183_944
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    // `refresh_channel`, not `refresh_due_tokens`: the pass walks every live Instagram
    // channel in the shared database and would rewrite rows that other tests are
    // asserting on, failing them at random depending on scheduling.
    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap();

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(config["access_token"], "fresh");
    assert_eq!(
        config["username"], "yourbiz",
        "the handle survives a refresh"
    );
    let expires_at = config["token_expires_at"].as_str().unwrap();
    let parsed = chrono::DateTime::parse_from_rfc3339(expires_at).unwrap();
    assert!(
        parsed > chrono::Utc::now() + chrono::TimeDelta::days(50),
        "the new expiry should be ~60 days out, got {expires_at}"
    );
}

#[tokio::test]
async fn the_refresher_fills_in_a_missing_expiry() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "pasted_by_hand"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "access_token": "fresh", "token_type": "bearer", "expires_in": 5_183_944
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    assert!(
        chatbridge::refresh::is_due(&config, chrono::Utc::now()),
        "an unknown expiry has to be due, or a hand-pasted token never acquires one"
    );
    chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap();

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        config["token_expires_at"].is_string(),
        "a hand-pasted token acquires a real expiry on the first pass"
    );
}

#[tokio::test]
async fn a_rejected_refresh_leaves_the_stored_token_alone() {
    let pool = setup_pool().await;
    let user_id = format!("ig_{}", Uuid::new_v4().simple());
    let guard = insert_test_channel(
        &pool,
        "instagram",
        &user_id,
        serde_json::json!({"access_token": "already_expired"}),
    )
    .await;
    let api = spawn_mock_instagram(serde_json::json!({
        "refresh_access_token": {
            "error": {"message": "Error validating access token", "code": 190}
        },
    }))
    .await;
    let state = build_state_ig(pool.clone(), api).await;

    let (channel, config) = channel_for_refresh(&pool, guard.id).await;
    let err = chatbridge::refresh::refresh_channel(&state, &channel, &config)
        .await
        .unwrap_err();
    assert!(err.contains("Error validating access token"), "{err}");

    let config: serde_json::Value = sqlx::query_scalar("SELECT config FROM channels WHERE id = $1")
        .bind(guard.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        config["access_token"], "already_expired",
        "a failed refresh must not overwrite the stored token"
    );
}
