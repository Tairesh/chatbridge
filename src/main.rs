use std::sync::Arc;

use chatbridge::cache::{self, ChannelCache};
use chatbridge::config::{AppConfig, AppState};
use chatbridge::{db, routes};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env();
    let port = config.port;

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = db::init_pool(&database_url).await;
    db::run_migrations(&db).await;

    let redis_client = redis::Client::open(config.redis_url.as_str()).expect("invalid REDIS_URL");
    let redis = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("failed to connect to Redis");
    tracing::info!("connected to Redis");

    let cache = Arc::new(ChannelCache::new());
    cache::spawn_invalidation_listener(&config.redis_url, cache.clone()).await;

    let state = Arc::new(AppState {
        config,
        db,
        redis,
        cache,
    });
    let app = routes::build(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
    tracing::info!("shutdown complete");
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
            _ = sigterm.recv() => tracing::info!("received SIGTERM, shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
        tracing::info!("received SIGINT, shutting down");
    }
}
