use std::collections::HashMap;
use std::sync::RwLock;

use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{self, Channel, ChatInfo, Client};
use crate::error::AppError;
use crate::model::ProviderKind;

/// In-memory channel cache with read-through to Postgres.
/// Invalidated via Redis Pub/Sub on the `cache_invalidation` topic and directly
/// by every channel CRUD mutation.
/// Only caches positive lookups of live channels — misses always hit the database.
///
/// Dual-keyed like `ClientCache`: a primary store by UUID plus a secondary index
/// from the provider's identity to that UUID.
pub struct ChannelCache {
    by_id: RwLock<HashMap<Uuid, Channel>>,
    ext_index: RwLock<HashMap<(ProviderKind, String), Uuid>>,
}

impl Default for ChannelCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelCache {
    pub fn new() -> Self {
        Self {
            by_id: RwLock::new(HashMap::new()),
            ext_index: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a channel into both the primary store and the secondary index.
    fn insert(&self, channel: &Channel) {
        self.by_id
            .write()
            .unwrap()
            .insert(channel.id, channel.clone());
        if let Ok(provider) = channel.provider.parse::<ProviderKind>() {
            self.ext_index
                .write()
                .unwrap()
                .insert((provider, channel.external_key.clone()), channel.id);
        }
    }

    /// Read-through lookup by channel UUID.
    pub async fn get_channel_by_id(
        &self,
        pool: &PgPool,
        channel_id: Uuid,
    ) -> Result<Option<Channel>, AppError> {
        if let Some(cached) = self.by_id.read().unwrap().get(&channel_id) {
            return Ok(Some(cached.clone()));
        }

        let channel = db::find_live_channel_by_id(pool, channel_id).await?;
        if let Some(ref ch) = channel {
            self.insert(ch);
        }
        Ok(channel)
    }

    /// Read-through lookup by the provider's identity for the channel.
    /// Resolves the UUID via the secondary index, then reads the primary store.
    pub async fn get_channel_by_external_key(
        &self,
        pool: &PgPool,
        provider: ProviderKind,
        external_key: &str,
    ) -> Result<Option<Channel>, AppError> {
        let key = (provider, external_key.to_owned());
        if let Some(&uuid) = self.ext_index.read().unwrap().get(&key)
            && let Some(cached) = self.by_id.read().unwrap().get(&uuid)
        {
            return Ok(Some(cached.clone()));
        }

        let channel = db::find_live_channel_by_external_key(pool, provider, external_key).await?;
        if let Some(ref ch) = channel {
            self.insert(ch);
        }
        Ok(channel)
    }

    /// Evict a channel by UUID from both maps.
    pub fn invalidate(&self, channel_id: Uuid) {
        self.by_id.write().unwrap().remove(&channel_id);
        self.ext_index
            .write()
            .unwrap()
            .retain(|_, v| *v != channel_id);
    }
}

/// In-memory client cache with read-through to Postgres.
/// Primary store keyed by UUID; secondary index `(provider, external_id) → UUID`.
/// Invalidated by client UUID — evicts from both maps.
pub struct ClientCache {
    by_uuid: RwLock<HashMap<Uuid, Client>>,
    ext_index: RwLock<HashMap<(ProviderKind, String), Uuid>>,
}

impl Default for ClientCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientCache {
    pub fn new() -> Self {
        Self {
            by_uuid: RwLock::new(HashMap::new()),
            ext_index: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a client into both the primary store and the secondary index.
    fn insert(&self, client: &Client) {
        self.by_uuid
            .write()
            .unwrap()
            .insert(client.id, client.clone());
        if let Some(ref ext_id) = client.external_id
            && let Ok(provider) = client.provider.parse::<ProviderKind>()
        {
            self.ext_index
                .write()
                .unwrap()
                .insert((provider, ext_id.clone()), client.id);
        }
    }

    /// Read-through lookup by provider + external_id.
    /// Resolves UUID via the secondary index, then fetches from the primary store.
    pub async fn get_client(
        &self,
        pool: &PgPool,
        provider: ProviderKind,
        external_id: &str,
    ) -> Result<Option<Client>, sqlx::Error> {
        let key = (provider, external_id.to_owned());
        if let Some(&uuid) = self.ext_index.read().unwrap().get(&key)
            && let Some(cached) = self.by_uuid.read().unwrap().get(&uuid)
        {
            return Ok(Some(cached.clone()));
        }

        let client = crate::db::find_client_by_external_id(pool, provider, external_id).await?;
        if let Some(ref c) = client {
            self.insert(c);
        }
        Ok(client)
    }

    /// Read-through lookup by UUID.
    pub async fn get_client_by_uuid(
        &self,
        pool: &PgPool,
        client_id: Uuid,
    ) -> Result<Option<Client>, sqlx::Error> {
        if let Some(cached) = self.by_uuid.read().unwrap().get(&client_id) {
            return Ok(Some(cached.clone()));
        }

        let client = crate::db::find_client_by_uuid(pool, client_id).await?;
        if let Some(ref c) = client {
            self.insert(c);
        }
        Ok(client)
    }

    /// Evict a client by UUID from both maps.
    pub fn invalidate(&self, client_id: Uuid) {
        self.by_uuid.write().unwrap().remove(&client_id);
        self.ext_index
            .write()
            .unwrap()
            .retain(|_, v| *v != client_id);
    }
}

/// Cached operator info for event enrichment.
#[derive(Debug, Clone)]
pub struct CachedOperator {
    pub id: Uuid,
    pub name: String,
}

/// In-memory operator cache with read-through to Postgres.
pub struct OperatorCache {
    pub(crate) operators: RwLock<HashMap<Uuid, CachedOperator>>,
}

impl Default for OperatorCache {
    fn default() -> Self {
        Self::new()
    }
}

impl OperatorCache {
    pub fn new() -> Self {
        Self {
            operators: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_operator(
        &self,
        pool: &PgPool,
        operator_id: Uuid,
    ) -> Result<Option<CachedOperator>, sqlx::Error> {
        if let Some(cached) = self.operators.read().unwrap().get(&operator_id) {
            return Ok(Some(cached.clone()));
        }

        let operator = db::find_operator_by_id(pool, operator_id).await?;
        if let Some(ref op) = operator {
            let cached = CachedOperator {
                id: op.id,
                name: op.name.clone(),
            };
            self.operators
                .write()
                .unwrap()
                .insert(operator_id, cached.clone());
            return Ok(Some(cached));
        }
        Ok(None)
    }

    pub fn invalidate(&self, operator_id: Uuid) {
        self.operators.write().unwrap().remove(&operator_id);
    }
}

/// In-memory chat cache: chat_id -> ChatInfo { client_id, channel_id }.
pub struct ChatCache {
    pub(crate) chats: RwLock<HashMap<Uuid, ChatInfo>>,
}

impl Default for ChatCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatCache {
    pub fn new() -> Self {
        Self {
            chats: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_chat_info(
        &self,
        pool: &PgPool,
        chat_id: Uuid,
    ) -> Result<Option<ChatInfo>, sqlx::Error> {
        if let Some(cached) = self.chats.read().unwrap().get(&chat_id) {
            return Ok(Some(cached.clone()));
        }

        let info = db::find_chat_info(pool, chat_id).await?;
        if let Some(ref ci) = info {
            self.chats.write().unwrap().insert(chat_id, ci.clone());
        }
        Ok(info)
    }

    pub fn invalidate(&self, chat_id: Uuid) {
        self.chats.write().unwrap().remove(&chat_id);
    }
}

/// Redis Pub/Sub invalidation topic.
pub const INVALIDATION_CHANNEL: &str = "cache_invalidation";

/// Spawn a background task that subscribes to Redis `cache_invalidation`
/// and evicts individual entries from the local caches.
///
/// Expected message format: `"entity_type:uuid"`
/// e.g. `"channel:550e8400-e29b-41d4-a716-446655440000"`
/// or  `"client:550e8400-e29b-41d4-a716-446655440000"`
pub async fn spawn_invalidation_listener(
    redis_url: &str,
    channel_cache: std::sync::Arc<ChannelCache>,
    client_cache: std::sync::Arc<ClientCache>,
    operator_cache: std::sync::Arc<OperatorCache>,
    chat_cache: std::sync::Arc<ChatCache>,
) {
    let client = redis::Client::open(redis_url).expect("invalid REDIS_URL for cache listener");
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .expect("failed to create Redis pubsub for cache invalidation");

    pubsub
        .subscribe(INVALIDATION_CHANNEL)
        .await
        .expect("failed to subscribe to cache_invalidation");

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

            let Some((entity_type, uuid_str)) = payload.split_once(':') else {
                tracing::warn!(payload = %payload, "invalid invalidation format, expected entity_type:uuid");
                continue;
            };

            let Ok(id) = uuid_str.parse::<Uuid>() else {
                tracing::warn!(payload = %payload, "invalid uuid in invalidation message");
                continue;
            };

            match entity_type {
                "channel" => {
                    tracing::info!(%id, "invalidating cached channel");
                    channel_cache.invalidate(id);
                }
                "client" => {
                    tracing::info!(%id, "invalidating cached client");
                    client_cache.invalidate(id);
                }
                "operator" => {
                    tracing::info!(%id, "invalidating cached operator");
                    operator_cache.invalidate(id);
                }
                "chat" => {
                    tracing::info!(%id, "invalidating cached chat");
                    chat_cache.invalidate(id);
                }
                other => {
                    tracing::warn!(
                        entity_type = other,
                        "unknown entity type in cache invalidation"
                    );
                }
            }
        }
    });
}

/// Publish a cache invalidation event to Redis.
pub async fn publish_invalidation(
    redis: &mut redis::aio::ConnectionManager,
    entity_type: &str,
    id: Uuid,
) {
    use redis::AsyncCommands;
    let msg = format!("{entity_type}:{id}");
    if let Err(e) = redis.publish::<_, _, ()>(INVALIDATION_CHANNEL, &msg).await {
        tracing::error!("failed to publish cache invalidation: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_cache_invalidate_removes_matching_entry() {
        let cache = ClientCache::new();
        let client_id = Uuid::new_v4();
        let client = crate::db::Client {
            id: client_id,
            provider: "telegram".into(),
            external_id: Some("12345".into()),
            name: Some("Test".into()),
            username: None,
            updated_at: chrono::Utc::now(),
        };
        cache.insert(&client);

        assert_eq!(cache.by_uuid.read().unwrap().len(), 1);
        assert_eq!(cache.ext_index.read().unwrap().len(), 1);
        cache.invalidate(client_id);
        assert!(cache.by_uuid.read().unwrap().is_empty());
        assert!(cache.ext_index.read().unwrap().is_empty());
    }

    #[test]
    fn operator_cache_invalidate_removes_entry() {
        let cache = OperatorCache::new();
        let id = Uuid::new_v4();
        cache.operators.write().unwrap().insert(
            id,
            CachedOperator {
                id,
                name: "Alice".into(),
            },
        );
        assert_eq!(cache.operators.read().unwrap().len(), 1);
        cache.invalidate(id);
        assert!(cache.operators.read().unwrap().is_empty());
    }

    #[test]
    fn chat_cache_invalidate_removes_entry() {
        let cache = ChatCache::new();
        let chat_id = Uuid::new_v4();
        cache.chats.write().unwrap().insert(
            chat_id,
            crate::db::ChatInfo {
                client_id: Uuid::new_v4(),
                channel_id: Uuid::new_v4(),
            },
        );
        assert_eq!(cache.chats.read().unwrap().len(), 1);
        cache.invalidate(chat_id);
        assert!(cache.chats.read().unwrap().is_empty());
    }

    #[test]
    fn client_cache_invalidate_keeps_non_matching_entries() {
        let cache = ClientCache::new();
        let client_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        let client = crate::db::Client {
            id: client_id,
            provider: "instagram".into(),
            external_id: Some("abc".into()),
            name: None,
            username: None,
            updated_at: chrono::Utc::now(),
        };
        cache.insert(&client);

        cache.invalidate(other_id);
        assert_eq!(cache.by_uuid.read().unwrap().len(), 1);
        assert_eq!(cache.ext_index.read().unwrap().len(), 1);
    }
}
