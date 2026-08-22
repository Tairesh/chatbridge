use crate::common::*;
use crate::support::*;

use std::sync::Arc;

use uuid::Uuid;

use chatbridge::cache::{ChannelCache, ChatCache, ClientCache, OperatorCache};
use chatbridge::model::ProviderKind;

#[tokio::test]
async fn cache_lookup_by_external_key_and_invalidation() {
    let pool = setup_pool().await;
    let widget_id = format!("cache_test_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    let cache = Arc::new(ChannelCache::new());

    // First lookup — cache miss, loads from DB
    let ch = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);

    // Delete from DB — a cached entry must still be served
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    cache.invalidate(guard.id);

    let after = cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap();
    assert!(after.is_none(), "None after invalidation + DB delete");
}

#[tokio::test]
async fn cache_lookup_by_id_and_invalidation() {
    let pool = setup_pool().await;
    let guard = insert_test_telegram_channel(&pool, "cache_secret").await;

    let cache = Arc::new(ChannelCache::new());

    let ch = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.id, guard.id);
    assert_eq!(ch.config["bot_secret"], "cache_secret");

    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    let cached = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("should be served from cache");
    assert_eq!(cached.id, guard.id);

    cache.invalidate(guard.id);
    assert!(
        cache
            .get_channel_by_id(&pool, guard.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cache_invalidate_by_id_clears_the_external_key_index() {
    let pool = setup_pool().await;
    let widget_id = format!("cache_test_{}", Uuid::new_v4());
    let guard = insert_test_widget_channel(&pool, &widget_id).await;

    let cache = Arc::new(ChannelCache::new());
    // Warm both maps through the key lookup, then evict by id only.
    cache
        .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
        .await
        .unwrap()
        .expect("channel should exist");

    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();
    cache.invalidate(guard.id);

    assert!(
        cache
            .get_channel_by_external_key(&pool, ProviderKind::Widget, &widget_id)
            .await
            .unwrap()
            .is_none(),
        "invalidating by id must also drop the secondary index entry"
    );
}

#[tokio::test]
async fn cache_invalidation_via_redis_pubsub() {
    let pool = setup_pool().await;
    let bot_secret = "redis_inv_secret";
    let guard = insert_test_telegram_channel(&pool, bot_secret).await;

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let cache = Arc::new(ChannelCache::new());

    // Populate cache
    let ch = cache
        .get_channel_by_id(&pool, guard.id)
        .await
        .unwrap()
        .expect("channel should exist");
    assert_eq!(ch.config["bot_secret"], bot_secret);

    // Start invalidation listener
    chatbridge::cache::spawn_invalidation_listener(
        &redis_url,
        cache.clone(),
        Arc::new(ClientCache::new()),
        Arc::new(OperatorCache::new()),
        Arc::new(ChatCache::new()),
    )
    .await;

    // Delete from DB so we can detect cache eviction
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard.id)
        .execute(&pool)
        .await
        .unwrap();

    // Publish invalidation via Redis
    let mut redis = setup_redis().await;
    redis::AsyncCommands::publish::<_, _, ()>(
        &mut redis,
        chatbridge::cache::INVALIDATION_CHANNEL,
        format!("channel:{}", guard.id),
    )
    .await
    .unwrap();

    // Give the listener a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cache should be evicted, DB is empty → None
    let after = cache.get_channel_by_id(&pool, guard.id).await.unwrap();
    assert!(
        after.is_none(),
        "should be None after Redis pubsub invalidation"
    );
}

#[tokio::test]
async fn cache_invalidation_does_not_affect_other_channels() {
    let pool = setup_pool().await;
    let guard_a = insert_test_telegram_channel(&pool, "secret_a").await;
    let guard_b = insert_test_telegram_channel(&pool, "secret_b").await;

    let cache = Arc::new(ChannelCache::new());

    // Populate both
    cache
        .get_channel_by_id(&pool, guard_a.id)
        .await
        .unwrap()
        .unwrap();
    cache
        .get_channel_by_id(&pool, guard_b.id)
        .await
        .unwrap()
        .unwrap();

    // Invalidate only A
    cache.invalidate(guard_a.id);

    // B should still be cached even if we delete it from DB
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(guard_b.id)
        .execute(&pool)
        .await
        .unwrap();
    let b = cache
        .get_channel_by_id(&pool, guard_b.id)
        .await
        .unwrap()
        .expect("channel B should still be cached");
    assert_eq!(b.config["bot_secret"], "secret_b");
}

#[tokio::test]
async fn client_cache_invalidated_via_redis_pubsub() {
    let pool = setup_pool().await;
    let mut redis = setup_redis().await;
    let client_cache = Arc::new(chatbridge::cache::ClientCache::new());

    // Insert a client
    let client_id = Uuid::new_v4();
    chatbridge::db::upsert_client(
        &pool,
        client_id,
        chatbridge::model::ProviderKind::Telegram,
        "invalidation_test_user",
        Some("Old Name"),
        None,
    )
    .await
    .unwrap();
    let _client_guard = TestClient { id: client_id };

    // Populate cache via read-through
    let cached = client_cache
        .get_client(
            &pool,
            chatbridge::model::ProviderKind::Telegram,
            "invalidation_test_user",
        )
        .await
        .unwrap();
    assert_eq!(cached.unwrap().name.as_deref(), Some("Old Name"));

    // Start invalidation listener
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let channel_cache = Arc::new(chatbridge::cache::ChannelCache::new());
    chatbridge::cache::spawn_invalidation_listener(
        &redis_url,
        channel_cache,
        client_cache.clone(),
        Arc::new(OperatorCache::new()),
        Arc::new(ChatCache::new()),
    )
    .await;

    // Give listener time to subscribe
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish invalidation
    chatbridge::cache::publish_invalidation(&mut redis, "client", client_id).await;

    // Wait for invalidation to propagate
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Update DB with new name
    chatbridge::db::upsert_client(
        &pool,
        client_id,
        chatbridge::model::ProviderKind::Telegram,
        "invalidation_test_user",
        Some("New Name"),
        None,
    )
    .await
    .unwrap();

    // Cache should have been cleared — next read-through returns fresh data
    let refreshed = client_cache
        .get_client(
            &pool,
            chatbridge::model::ProviderKind::Telegram,
            "invalidation_test_user",
        )
        .await
        .unwrap();
    assert_eq!(
        refreshed.unwrap().name.as_deref(),
        Some("New Name"),
        "cache should return fresh DB data after invalidation"
    );
}
