# Chatbridge

[![CI](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml/badge.svg)](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml)

Multi-provider chat bridge for **Instagram**, **Telegram**, and **WebSocket chat widgets**, built with Rust, Axum, PostgreSQL, and Redis.

## Event Flow

```
                         ┌─────────────────────────────────────┐
                         │          Chatbridge Server           │
                         │              :3800                  │
                         │                                     │
  Instagram/Meta ──POST──▶ /webhook/instagram                  │
                         │   │                                 │
                         │   ├─ Verify HMAC-SHA256 signature   │
                         │   │  (X-Hub-Signature-256 header)   │
                         │   │                                 │
                         │   ├─ Return 200 OK ◀── immediate    │
                         │   │                                 │
                         │   └─ Background: parse payload      │
                         │      ├─ Match sender/recipient ID   │
                         │      │  (in-memory cache → DB)      │
                         │      ├─ Emit InternalMessage ──▶ stdout
                         │      └─ Publish ──▶ Redis instagram:{id}
                         │                                     │
    Telegram ────POST────▶ /webhook/telegram/{channel_id}      │
                         │   │                                 │
                         │   ├─ Lookup bot_secret by UUID      │
                         │   │  (in-memory cache → DB fallback)│
                         │   │                                 │
                         │   ├─ Verify secret token header     │
                         │   │  (X-Telegram-Bot-Api-Secret-Token)
                         │   │                                 │
                         │   ├─ Return 200 OK ◀── immediate    │
                         │   │                                 │
                         │   └─ Background: parse Update       │
                         │      ├─ Emit InternalMessage ──▶ stdout
                         │      └─ Publish ──▶ Redis telegram:{id}
                         │                                     │
  Widget Client ───WS────▶ /ws/{widget_id}                     │
                         │   │                                 │
                         │   ├─ Lookup widget_id               │
                         │   │  (in-memory cache → DB)         │
                         │   ├─ Upgrade to WebSocket           │
                         │   │                                 │
                         │   ├─ Track connection (AtomicUsize) │
                         │   │                                 │
                         │   └─ Message loop (select!):        │
                         │      ├─ 5min idle → ping/pong       │
                         │      ├─ Shutdown → close frame      │
                         │      ├─ Parse JSON action message    │
                         │      │  (send / edit)                │
                         │      ├─ Emit InternalMessage ──▶ stdout
                         │      ├─ Publish ──▶ Redis widget:{id}
                         │      └─ Send ACK ──▶ client (5s timeout)
                         │                                     │
  Instagram/Meta ──GET───▶ /webhook/instagram                  │
                         │   └─ Subscription verification      │
                         │      (hub.challenge handshake)      │
                         └───────┬─────────────────┬───────────┘
                                 │                 │
                            ┌────▼────┐      ┌─────▼─────┐
                            │ Postgres │      │   Redis   │
                            │  :5432   │      │   :6379   │
                            └─────────┘      └───────────┘
                         instagram_channels   pub/sub channels:
                         telegram_channels    instagram:{uuid}
                         widget_channels      telegram:{uuid}
                                              widget:{uuid}
                                              channel_invalidation
```

### InternalMessage

Every successfully parsed webhook event becomes an `InternalMessage`:

```
┌──────────────────────────────────────────────┐
│ InternalMessage                              │
├──────────────────────────────────────────────┤
│ message_id   "instagram:aWdf..." / "telegram:42" / "widget:uuid" │
│ channel_id   UUID (from DB)                  │
│ provider     Instagram | Telegram | Widget   │
│ event        Message | Edit | Read | Reaction | Unknown │
│ timestamp    Unix ms                         │
│ raw          Full original JSON              │
└──────────────────────────────────────────────┘
```

### Channel Cache

Channel configuration (tokens, secrets, IDs) is cached in-memory to avoid a Postgres round-trip on every incoming webhook. The cache uses a read-through strategy: on a miss it queries the database and stores the result locally.

When a channel is updated or deleted, publish an invalidation event to Redis so all replicas evict the stale entry:

```bash
# Invalidate a specific channel
redis-cli PUBLISH channel_invalidation "instagram:<channel_uuid>"
redis-cli PUBLISH channel_invalidation "telegram:<channel_uuid>"
redis-cli PUBLISH channel_invalidation "widget:<channel_uuid>"
```

Each replica runs a background listener on the `channel_invalidation` topic that parses the `"provider:uuid"` message and evicts only the matching cache entry.

## Project Structure

```
src/
├── main.rs              # Entrypoint: load config, connect DB, start server, graceful shutdown with WS drain
├── lib.rs               # Public module re-exports
├── cache.rs             # In-memory channel cache with Redis Pub/Sub invalidation
├── config.rs            # AppConfig (env vars) + AppState (config + DB pool + Redis + cache + connection counter + shutdown token)
├── db.rs                # Postgres pool, migrations, channel queries
├── error.rs             # WebhookError → HTTP status mapping
├── model.rs             # InternalMessage, ProviderKind, EventKind, WsInbound/WsAck
├── handler.rs           # Axum request handlers + WebSocket handler
├── routes.rs            # Router assembly
└── provider/
    ├── mod.rs           # WebhookProvider trait (verify + parse)
    ├── instagram.rs     # Constant-time HMAC-SHA256 verification, Meta payload parsing
    └── telegram.rs      # Secret token verification, Telegram Update parsing

docker/
├── Dockerfile           # Multi-stage build
└── nginx.conf           # Nginx reverse proxy config

widget/
└── index.html           # Chat widget test page (WebSocket client)

migrations/              # SQL migrations (auto-run on startup)
tests/integration.rs     # Integration tests (require Postgres + Redis)
compose.yaml             # nginx + chatbridge + postgres + redis services
```

## Quick Start

### With Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your tokens

docker compose up --build
```

Nginx listens on `http://localhost:80` and proxies to the chatbridge server (including WebSocket upgrade for `/ws/`), with Postgres and Redis provisioned automatically.

### Without Docker

Requires Rust 1.88+, a running PostgreSQL instance, and Redis.

```bash
# Set environment variables
export META_VERIFY_TOKEN=your_token
export INSTAGRAM_APP_SECRET=your_secret
export DATABASE_URL=postgres://chatbridge:chatbridge@localhost:5432/chatbridge
export REDIS_URL=redis://localhost:6379

# Build and run
cargo build
cargo run
```

Migrations run automatically on startup.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `META_VERIFY_TOKEN` | yes | — | Token for Instagram webhook subscription handshake |
| `INSTAGRAM_APP_SECRET` | yes | — | HMAC-SHA256 secret for Instagram signature validation |
| `DATABASE_URL` | yes | — | Postgres connection string |
| `REDIS_URL` | yes | — | Redis connection string |

## Testing

### Unit tests (no database needed)

```bash
cargo test --lib
```

Covers HMAC verification, secret token validation, event classification, payload deserialization, WebSocket message types, and UUID mid validation.

### All tests (unit + integration)

Requires running Postgres and Redis (e.g. via `docker compose up -d postgres redis`):

```bash
DATABASE_URL=postgres://chatbridge:chatbridge@localhost:5432/chatbridge REDIS_URL=redis://localhost:6379 cargo test
```

Integration tests cover:
- Meta webhook subscription verification (valid/invalid token, wrong mode)
- Instagram POST ingestion (valid/invalid/missing signature, channel lookup, non-instagram object rejection)
- Telegram POST ingestion (valid/invalid secret, unknown channel → 404)
- WebSocket widget (connect, ACK, multiple messages, error recovery, unknown widget, invalid mid rejection)
- WebSocket edit action (edit ACK, Redis edit event, unknown action error)
- Redis pub/sub verification for all three providers
- Channel cache (read-through, per-channel invalidation, Redis Pub/Sub eviction, cross-channel isolation)

Test data cleanup uses RAII drop guards (`TestChannel`) to ensure rows are deleted even if a test panics.

### Linting

```bash
cargo clippy
cargo fmt --check
```
