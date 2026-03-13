# Webhook Microservice

Multi-provider webhook receiver for **Instagram** and **Telegram**, built with Rust, Axum, and PostgreSQL.

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
                         │      └─ Emit InternalMessage ──▶ stdout
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
                         │      └─ Emit InternalMessage ──▶ stdout
                         │                                     │
  Instagram/Meta ──GET───▶ /webhook/instagram                  │
                         │   └─ Subscription verification      │
                         │      (hub.challenge handshake)      │
                         └──────────────┬──────────────────────┘
                                        │
                                   ┌────▼────┐
                                   │ Postgres │
                                   │  :5432   │
                                   └─────────┘
                              instagram_channels
                              telegram_channels
```

### InternalMessage

Every successfully parsed webhook event becomes an `InternalMessage`:

```
┌──────────────────────────────────────────────┐
│ InternalMessage                              │
├──────────────────────────────────────────────┤
│ message_id   "instagram:aWdf..." / "telegram:42" │
│ channel_id   UUID (from DB)                  │
│ provider     Instagram | Telegram            │
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
├── config.rs            # AppConfig (env vars) + AppState (config + DB pool)
├── db.rs                # Postgres pool, migrations, channel queries
├── error.rs             # WebhookError → HTTP status mapping
├── model.rs             # InternalMessage, ProviderKind, EventKind
├── handler.rs           # Axum request handlers
├── routes.rs            # Router assembly
└── provider/
    ├── mod.rs           # WebhookProvider trait (verify + parse)
    ├── instagram.rs     # HMAC-SHA256 verification, Meta payload parsing
    └── telegram.rs      # Secret token verification, Telegram Update parsing

docker/
├── Dockerfile           # Multi-stage build
└── nginx.conf           # Nginx reverse proxy config

migrations/              # SQL migrations (auto-run on startup)
tests/integration.rs     # Integration tests (require Postgres)
compose.yaml             # nginx + webhook + postgres services
```

## Quick Start

### With Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your tokens

docker compose up --build
```

Nginx listens on `http://localhost:80` and proxies to the webhook server, with Postgres provisioned automatically.

### Without Docker

Requires Rust 1.88+ and a running PostgreSQL instance.

```bash
# Set environment variables
export META_VERIFY_TOKEN=your_token
export INSTAGRAM_APP_SECRET=your_secret
export DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook

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
| `PORT` | no | `3000` | Server listen port |

## Testing

### Unit tests (no database needed)

```bash
cargo test --lib
```

Covers HMAC verification, secret token validation, event classification, and payload deserialization.

### All tests (unit + integration)

Requires a running Postgres instance (e.g. via `docker compose up -d postgres`):

```bash
DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook cargo test
```

Integration tests cover:
- Meta webhook subscription verification (valid/invalid token, wrong mode)
- Instagram POST ingestion (valid/invalid/missing signature, channel lookup)
- Telegram POST ingestion (valid/invalid secret, unknown channel → 404)

### Linting

```bash
cargo clippy
cargo fmt --check
```
