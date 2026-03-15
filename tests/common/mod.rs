//! Shared test helpers for integration tests.

#![allow(dead_code)]

use sqlx::PgPool;
use uuid::Uuid;

/// Run a DELETE query in a fresh runtime (safe to call from Drop).
fn drop_delete(table: &str, id: Uuid) {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let query = format!("DELETE FROM {} WHERE id = $1", table);
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

/// RAII guard that deletes a test row on drop, even if the test panics.
pub struct TestChannel {
    pub table: &'static str,
    pub id: Uuid,
}

impl Drop for TestChannel {
    fn drop(&mut self) {
        drop_delete(self.table, self.id);
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
