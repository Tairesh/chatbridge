use std::sync::Arc;

use webhook::config::{AppConfig, AppState};
use webhook::{db, routes};

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

    let state = Arc::new(AppState { config, db, redis });
    let app = routes::build(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
