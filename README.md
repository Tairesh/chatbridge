# Webhook Microservice

[![CI](https://github.com/Tairesh/webhook/actions/workflows/ci.yml/badge.svg)](https://github.com/Tairesh/webhook/actions/workflows/ci.yml)

Multi-provider webhook receiver for **Instagram**, **Telegram**, and **WebSocket chat widgets**, built with Rust, Axum, PostgreSQL, and Redis.

## Event Flow

```
                         ┌─────────────────────────────────────┐
                         │           Webhook Server            │
                         │              :3000                  │
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
                         │      │  against instagram_channels  │
                         │      ├─ Emit InternalMessage ──▶ stdout
                         │      └─ Publish ──▶ Redis instagram:{id}
                         │                                     │
    Telegram ────POST────▶ /webhook/telegram/{channel_id}      │
                         │   │                                 │
                         │   ├─ Lookup bot_secret by UUID      │
                         │   │  from telegram_channels         │
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
                         │   ├─ Lookup widget_id in DB         │
                         │   ├─ Upgrade to WebSocket           │
                         │   │                                 │
                         │   └─ Message loop:                  │
                         │      ├─ Parse JSON action message    │
                         │      │  (send / edit)                │
                         │      ├─ Emit InternalMessage ──▶ stdout
                         │      ├─ Publish ──▶ Redis widget:{id}
                         │      └─ Send ACK ──▶ client         │
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
│ event        Message | Edit | Read | Reaction│
│ timestamp    Unix ms                         │
│ raw          Full original JSON              │
└──────────────────────────────────────────────┘
```

## Project Structure

```
src/
├── main.rs              # Entrypoint: load config, connect DB, start server
├── lib.rs               # Public module re-exports
├── config.rs            # AppConfig (env vars) + AppState (config + DB pool + Redis)
├── db.rs                # Postgres pool, migrations, channel queries
├── error.rs             # WebhookError → HTTP status mapping
├── model.rs             # InternalMessage, ProviderKind, EventKind, WsInbound/WsAck
├── handler.rs           # Axum request handlers + WebSocket handler
├── routes.rs            # Router assembly
└── provider/
    ├── mod.rs           # WebhookProvider trait (verify + parse)
    ├── instagram.rs     # HMAC-SHA256 verification, Meta payload parsing
    └── telegram.rs      # Secret token verification, Telegram Update parsing

docker/
├── Dockerfile           # Multi-stage build
└── nginx.conf           # Nginx reverse proxy config

widget/
└── index.html           # Chat widget test page (WebSocket client)

migrations/              # SQL migrations (auto-run on startup)
tests/integration.rs     # Integration tests (require Postgres + Redis)
compose.yaml             # nginx + webhook + postgres + redis services
```

## Quick Start

### With Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your tokens

docker compose up --build
```

Nginx listens on `http://localhost:80` and proxies to the webhook server (including WebSocket upgrade for `/ws/`), with Postgres and Redis provisioned automatically.

### Without Docker

Requires Rust 1.88+, a running PostgreSQL instance, and Redis.

```bash
# Set environment variables
export META_VERIFY_TOKEN=your_token
export INSTAGRAM_APP_SECRET=your_secret
export DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook
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
| `PORT` | no | `3000` | Server listen port |

## Testing

### Unit tests (no database needed)

```bash
cargo test --lib
```

Covers HMAC verification, secret token validation, event classification, payload deserialization, and WebSocket message types.

### All tests (unit + integration)

Requires running Postgres and Redis (e.g. via `docker compose up -d postgres redis`):

```bash
DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook REDIS_URL=redis://localhost:6379 cargo test
```

Integration tests cover:
- Meta webhook subscription verification (valid/invalid token, wrong mode)
- Instagram POST ingestion (valid/invalid/missing signature, channel lookup)
- Telegram POST ingestion (valid/invalid secret, unknown channel → 404)
- WebSocket widget (connect, ACK, multiple messages, error recovery, unknown widget)
- WebSocket edit action (edit ACK, Redis edit event, unknown action error)
- Redis pub/sub verification for all three providers

### Linting

```bash
cargo clippy
cargo fmt --check
```
