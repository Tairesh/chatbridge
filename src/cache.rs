use std::collections::HashMap;
use std::sync::RwLock;

use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{self, InstagramChannel, TelegramChannel, WidgetChannel};
use crate::error::WebhookError;

/// In-memory channel cache with read-through to Postgres.
/// Invalidated via Redis Pub/Sub on the `channel_invalidation` topic.
pub struct ChannelCache {
    instagram: RwLock<HashMap<String, Option<InstagramChannel>>>,
    telegram: RwLock<HashMap<Uuid, Option<TelegramChannel>>>,
    widget: RwLock<HashMap<String, Option<WidgetChannel>>>,
}

impl Default for ChannelCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelCache {
    pub fn new() -> Self {
        Self {
            instagram: RwLock::new(HashMap::new()),
            telegram: RwLock::new(HashMap::new()),
            widget: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_instagram_channel(
        &self,
        pool: &PgPool,
        user_id: &str,
    ) -> Result<Option<InstagramChannel>, WebhookError> {
        if let Some(cached) = self.instagram.read().unwrap().get(user_id) {
            return Ok(cached.clone());
        }

        let channel = db::find_instagram_channel_by_user_id(pool, user_id).await?;
        self.instagram
            .write()
            .unwrap()
            .insert(user_id.to_owned(), channel.clone());
        Ok(channel)
    }

    pub async fn get_telegram_channel(
        &self,
        pool: &PgPool,
        channel_id: Uuid,
    ) -> Result<Option<TelegramChannel>, WebhookError> {
        if let Some(cached) = self.telegram.read().unwrap().get(&channel_id) {
            return Ok(cached.clone());
        }

        let channel = db::find_telegram_channel_by_id(pool, channel_id).await?;
        self.telegram
            .write()
            .unwrap()
            .insert(channel_id, channel.clone());
        Ok(channel)
    }

    pub async fn get_widget_channel(
        &self,
        pool: &PgPool,
        widget_id: &str,
    ) -> Result<Option<WidgetChannel>, WebhookError> {
        if let Some(cached) = self.widget.read().unwrap().get(widget_id) {
            return Ok(cached.clone());
        }

        let channel = db::find_widget_channel_by_widget_id(pool, widget_id).await?;
        self.widget
            .write()
            .unwrap()
            .insert(widget_id.to_owned(), channel.clone());
        Ok(channel)
    }

    /// Evict a single channel from the cache.
    ///
    /// `provider` is `"instagram"`, `"telegram"`, or `"widget"`.
    /// `channel_id` is the channel UUID to remove.
    pub fn invalidate(&self, provider: &str, channel_id: Uuid) {
        match provider {
            "instagram" => {
                self.instagram
                    .write()
                    .unwrap()
                    .retain(|_, v| v.as_ref().is_none_or(|ch| ch.id != channel_id));
            }
            "telegram" => {
                self.telegram.write().unwrap().remove(&channel_id);
            }
            "widget" => {
                self.widget
                    .write()
                    .unwrap()
                    .retain(|_, v| v.as_ref().is_none_or(|ch| ch.id != channel_id));
            }
            other => {
                tracing::warn!(provider = other, "unknown provider in cache invalidation");
            }
        }
    }
}

/// Redis Pub/Sub invalidation topic.
pub const INVALIDATION_CHANNEL: &str = "channel_invalidation";

/// Spawn a background task that subscribes to Redis `channel_invalidation`
/// and evicts individual channels from the local cache.
///
/// Expected message format: `"provider:channel_uuid"`
/// e.g. `"instagram:550e8400-e29b-41d4-a716-446655440000"`
pub async fn spawn_invalidation_listener(redis_url: &str, cache: std::sync::Arc<ChannelCache>) {
    let client = redis::Client::open(redis_url).expect("invalid REDIS_URL for cache listener");
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .expect("failed to create Redis pubsub for cache invalidation");

    pubsub
        .subscribe(INVALIDATION_CHANNEL)
        .await
        .expect("failed to subscribe to channel_invalidation");

    tokio::spawn(async move {
        use futures_util::StreamExt;

        tracing::info!("cache invalidation listener started");
        loop {
            let msg: redis::Msg = match pubsub.on_message().next().await {
                Some(msg) => msg,
                None => {
                    tracing::warn!("cache invalidation subscription ended");
                    break;
                }
            };

            let payload: String = match msg.get_payload() {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("bad invalidation payload: {e}");
                    continue;
                }
            };

            let Some((provider, uuid_str)) = payload.split_once(':') else {
                tracing::warn!(payload = %payload, "invalid invalidation format, expected provider:uuid");
                continue;
            };

            let Ok(channel_id) = uuid_str.parse::<Uuid>() else {
                tracing::warn!(payload = %payload, "invalid uuid in invalidation message");
                continue;
            };

            tracing::info!(provider = provider, %channel_id, "invalidating cached channel");
            cache.invalidate(provider, channel_id);
        }
    });
}
