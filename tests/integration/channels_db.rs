use crate::common::*;
use crate::support::*;

use axum::http::StatusCode;
use uuid::Uuid;

use chatbridge::model::ProviderKind;

#[tokio::test]
async fn channel_insert_and_find_live() {
    let pool = setup_pool().await;
    let id = Uuid::new_v4();
    let key = format!("chan_{}", Uuid::new_v4());
    let created = chatbridge::db::insert_channel(
        &pool,
        id,
        ProviderKind::Widget,
        "My widget",
        &key,
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    let _guard = TestChannel { id };

    assert_eq!(created.id, id);
    assert_eq!(created.provider, "widget");
    assert_eq!(created.name, "My widget");
    assert_eq!(created.external_key, key);
    assert!(created.deleted_at.is_none());

    let by_id = chatbridge::db::find_live_channel_by_id(&pool, id)
        .await
        .unwrap()
        .expect("live channel by id");
    assert_eq!(by_id.external_key, key);

    let by_key =
        chatbridge::db::find_live_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .expect("live channel by key");
    assert_eq!(by_key.id, id);
}

#[tokio::test]
async fn channel_soft_delete_hides_from_live_queries_only() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    let deleted = chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap()
        .expect("row returned");
    assert!(deleted.deleted_at.is_some());

    assert!(
        chatbridge::db::find_live_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none(),
        "soft-deleted channel must be invisible to the live query"
    );
    assert!(
        chatbridge::db::find_live_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        chatbridge::db::find_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_some(),
        "the any-state query must still see it"
    );
    assert!(
        chatbridge::db::find_channel_by_external_key(&pool, ProviderKind::Widget, &key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn channel_insert_duplicate_external_key_is_unique_violation() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let _guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    let err = chatbridge::db::insert_channel(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Widget,
        "dup",
        &key,
        &serde_json::json!({}),
    )
    .await
    .expect_err("second insert on the same identity must fail");

    match err {
        sqlx::Error::Database(ref e) => assert!(e.is_unique_violation()),
        other => panic!("expected a unique violation, got {other:?}"),
    }
}

#[tokio::test]
async fn channel_insert_duplicate_key_conflicts_even_when_deleted() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let err = chatbridge::db::insert_channel(
        &pool,
        Uuid::new_v4(),
        ProviderKind::Widget,
        "dup",
        &key,
        &serde_json::json!({}),
    )
    .await
    .expect_err("the identity stays taken after a soft delete");

    match err {
        sqlx::Error::Database(ref e) => assert!(e.is_unique_violation()),
        other => panic!("expected a unique violation, got {other:?}"),
    }
}

#[tokio::test]
async fn channel_update_renames_restores_and_rewrites_config() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(
        &pool,
        "telegram",
        &key,
        serde_json::json!({"bot_token": "old"}),
    )
    .await;
    chatbridge::db::soft_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    let updated = chatbridge::db::update_channel(
        &pool,
        guard.id,
        Some("Renamed"),
        None,
        Some(&serde_json::json!({"bot_token": "new"})),
        true,
    )
    .await
    .unwrap()
    .expect("row returned");

    assert_eq!(updated.name, "Renamed");
    assert!(updated.deleted_at.is_none(), "restore clears deleted_at");
    assert_eq!(updated.config["bot_token"], "new");
}

#[tokio::test]
async fn channel_update_leaves_untouched_fields_alone() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({"keep": true})).await;

    let updated =
        chatbridge::db::update_channel(&pool, guard.id, Some("Only a rename"), None, None, false)
            .await
            .unwrap()
            .expect("row returned");

    assert_eq!(updated.name, "Only a rename");
    assert_eq!(updated.external_key, key, "external_key untouched");
    assert_eq!(updated.config["keep"], true, "config untouched");
}

#[tokio::test]
async fn channel_list_puts_live_channels_first() {
    let pool = setup_pool().await;
    let live_key = format!("chan_live_{}", Uuid::new_v4());
    let dead_key = format!("chan_dead_{}", Uuid::new_v4());
    let live = insert_test_channel(&pool, "widget", &live_key, serde_json::json!({})).await;
    let dead = insert_test_channel(&pool, "widget", &dead_key, serde_json::json!({})).await;
    chatbridge::db::soft_delete_channel(&pool, dead.id)
        .await
        .unwrap();

    let all = chatbridge::db::list_channels(&pool).await.unwrap();
    let live_pos = all
        .iter()
        .position(|c| c.id == live.id)
        .expect("live listed");
    let dead_pos = all
        .iter()
        .position(|c| c.id == dead.id)
        .expect("deleted listed");
    assert!(
        live_pos < dead_pos,
        "live channels must sort before deleted ones"
    );
}

#[tokio::test]
async fn channel_hard_delete_removes_the_row() {
    let pool = setup_pool().await;
    let key = format!("chan_{}", Uuid::new_v4());
    let guard = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;

    chatbridge::db::hard_delete_channel(&pool, guard.id)
        .await
        .unwrap();

    assert!(
        chatbridge::db::find_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none()
    );
    // The identity is free again, which is the whole point of the create rollback.
    let reused = insert_test_channel(&pool, "widget", &key, serde_json::json!({})).await;
    assert_ne!(reused.id, guard.id);
}

#[tokio::test]
async fn deleted_channel_is_invisible_to_the_hot_path() {
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

    // The handler invalidated the cache, so the next read-through misses the DB filter.
    assert!(
        state
            .cache
            .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
            .await
            .unwrap()
            .is_none()
    );

    // And the widget cannot connect any more.
    let addr = spawn_app(state).await;
    let url = format!("ws://{addr}/ws/{widget_id}");
    assert!(
        tokio_tungstenite::connect_async(&url).await.is_err(),
        "a deleted widget channel must refuse the upgrade"
    );
}
