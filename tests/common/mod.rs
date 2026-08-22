//! Shared test helpers for integration tests.

#![allow(dead_code)]

use sqlx::PgPool;
use uuid::Uuid;

/// Run a DELETE query in a fresh runtime (safe to call from Drop).
fn drop_delete(table: &str, id: Uuid) {
    drop_delete_by_column(table, "id", id);
}

/// Run a DELETE query matching a specific column value.
fn drop_delete_by_column(table: &str, column: &str, id: Uuid) {
    drop_query(&format!("DELETE FROM {} WHERE {} = $1", table, column), id);
}

/// Run an arbitrary query with a single UUID bind parameter.
fn drop_query(query: &str, id: Uuid) {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let query = query.to_owned();
    // Fresh pool on a fresh runtime — the original pool's connections are
    // pinned to the test runtime's I/O driver and can't be reused here.
    std::thread::scope(|s| {
        s.spawn(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let pool = PgPool::connect(&db_url).await.unwrap();
                let _ = sqlx::query(&query).bind(id).execute(&pool).await;
            });
        });
    });
}

/// Deletes a channel by provider identity rather than by id.
///
/// For tests that assert a row was *not* created. When such a test fails it fails
/// because the row exists — and without this guard that orphan stays in the shared
/// database forever, where the token refresher will later pick it up and rewrite it.
pub struct TestChannelKey {
    pub provider: &'static str,
    pub external_key: String,
}

impl TestChannelKey {
    pub fn instagram(external_key: &str) -> Self {
        Self {
            provider: "instagram",
            external_key: external_key.to_owned(),
        }
    }
}

impl Drop for TestChannelKey {
    fn drop(&mut self) {
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let provider = self.provider;
        let key = self.external_key.clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let pool = PgPool::connect(&db_url).await.unwrap();
                    let _ = sqlx::query(
                        "DELETE FROM channels WHERE provider = $1 AND external_key = $2",
                    )
                    .bind(provider)
                    .bind(&key)
                    .execute(&pool)
                    .await;
                });
            });
        });
    }
}

/// RAII guard that deletes a test row on drop, even if the test panics.
pub struct TestChannel {
    pub id: Uuid,
}

impl Drop for TestChannel {
    fn drop(&mut self) {
        let id = self.id;
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        std::thread::scope(|s| {
            s.spawn(|| {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let pool = PgPool::connect(&db_url).await.unwrap();
                    // Collect client IDs before deleting referencing rows
                    let client_ids: Vec<(Uuid,)> = sqlx::query_as(
                        "SELECT sender_id AS id FROM messages WHERE channel_id = $1 AND sender_id IS NOT NULL
                         UNION
                         SELECT client_id FROM chats WHERE channel_id = $1",
                    )
                    .bind(id)
                    .fetch_all(&pool)
                    .await
                    .unwrap_or_default();
                    // Delete in FK order
                    let _ = sqlx::query("DELETE FROM messages WHERE channel_id = $1").bind(id).execute(&pool).await;
                    let _ = sqlx::query("DELETE FROM chats WHERE channel_id = $1").bind(id).execute(&pool).await;
                    for (client_id,) in &client_ids {
                        let _ = sqlx::query("DELETE FROM clients WHERE id = $1").bind(client_id).execute(&pool).await;
                    }
                    let _ = sqlx::query("DELETE FROM channels WHERE id = $1").bind(id).execute(&pool).await;
                });
            });
        });
    }
}

/// RAII guard that deletes a client row on drop.
pub struct TestClient {
    pub id: Uuid,
}

impl Drop for TestClient {
    fn drop(&mut self) {
        drop_delete("clients", self.id);
    }
}

/// RAII guard that deletes a chat row on drop.
pub struct TestChat {
    pub id: Uuid,
}

impl Drop for TestChat {
    fn drop(&mut self) {
        drop_delete("chats", self.id);
    }
}

/// RAII guard that deletes a message row on drop.
pub struct TestMessage {
    pub id: Uuid,
}

impl Drop for TestMessage {
    fn drop(&mut self) {
        drop_delete("messages", self.id);
    }
}

/// RAII guard that deletes an operator row on drop.
pub struct TestOperator {
    pub id: Uuid,
}

impl Drop for TestOperator {
    fn drop(&mut self) {
        drop_delete("operators", self.id);
    }
}

pub async fn setup_pool() -> PgPool {
    let url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let pool = PgPool::connect(&url)
        .await
        .expect("failed to connect to test DB");
    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("failed to run migrations");
    pool
}
