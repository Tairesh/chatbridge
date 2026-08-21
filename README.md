# Chatbridge

[![CI](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml/badge.svg)](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml)

Multi-provider chat bridge for **Instagram**, **Telegram**, and **WebSocket chat widgets** with **operator dashboard**, built with Rust, Axum, PostgreSQL, and Redis. Supports bidirectional messaging between operators and clients across all providers.

## Event Flow

```
                         ┌──────────────────────────────────────────┐
                         │           Chatbridge Server               │
                         │               :3800                      │
                         │                                          │
  Instagram/Meta ──POST──▶ /webhook/instagram                       │
                         │   │                                      │
                         │   ├─ Verify HMAC-SHA256 signature        │
                         │   │  (X-Hub-Signature-256 header)        │
                         │   │                                      │
                         │   ├─ Return 200 OK ◀── immediate         │
                         │   │                                      │
                         │   └─ Background: parse payload           │
                         │      ├─ Match sender/recipient ID        │
                         │      │  (in-memory cache → DB)           │
                         │      ├─ Resolve client identity          │
                         │      │  (DB lookup + Graph API bg)       │
                         │      ├─ Persist → Postgres messages      │
                         │      └─ Publish ──▶ Redis incoming_messages
                         │                                          │
    Telegram ────POST────▶ /webhook/telegram/{channel_id}           │
                         │   │                                      │
                         │   ├─ Lookup bot_secret by UUID           │
                         │   │  (in-memory cache → DB fallback)     │
                         │   │                                      │
                         │   ├─ Verify secret token header          │
                         │   │  (X-Telegram-Bot-Api-Secret-Token)   │
                         │   │                                      │
                         │   ├─ Return 200 OK ◀── immediate         │
                         │   │                                      │
                         │   └─ Background: parse Update            │
                         │      ├─ Resolve client from `from`       │
                         │      │  (cache → DB, 24h staleness)      │
                         │      ├─ Persist → Postgres messages      │
                         │      └─ Publish ──▶ Redis incoming_messages
                         │                                          │
  Widget Client ───WS────▶ /ws/{widget_id}                          │
                         │   │                                      │
                         │   ├─ Lookup widget_id                    │
                         │   │  (in-memory cache → DB)              │
                         │   ├─ Upgrade to WebSocket                │
                         │   ├─ JWT auth (issue or verify)          │
                         │   ├─ Send chat event if a chat exists    │
                         │   │  {action, chat_id, status}           │
                         │   ├─ Register in ClientRegistry (mpsc)   │
                         │   │                                      │
                         │   └─ Message loop (select!):             │
                         │      ├─ Receive from registry → forward  │
                         │      ├─ 5min idle → ping/pong            │
                         │      ├─ Shutdown → close frame           │
                         │      ├─ Parse JSON action                │
                         │      │  (send / edit / read)             │
                         │      ├─ Persist → Postgres messages      │
                         │      ├─ Publish ──▶ Redis incoming_messages
                         │      └─ Send ACK ──▶ client (5s timeout) │
                         │                                          │
    Operator ──────WS────▶ /ws/operator                             │
                         │   │                                      │
                         │   ├─ JWT auth (issue or verify)          │
                         │   ├─ Register in ClientRegistry (mpsc)   │
                         │   │                                      │
                         │   └─ Message loop (select!):             │
                         │      ├─ Receive from registry → forward  │
                         │      ├─ 5min idle → ping/pong            │
                         │      ├─ Shutdown → close frame           │
                         │      ├─ Parse JSON action                │
                         │      │  {action, chat_id, mid, text}     │
                         │      ├─ Persist → Postgres messages      │
                         │      ├─ Publish ──▶ Redis incoming_messages
                         │      ├─ Send ACK ──▶ operator            │
                         │      └─ Telegram? → spawn delivery       │
                         │         └─ POST Bot API /sendMessage     │
                         │                                          │
    Operator ──────GET───▶ /api/chats                               │
                         │   └─ List active chats with summaries    │
                         │                                          │
    Operator ──────GET───▶ /api/chats/{chat_id}                     │
                         │   └─ Chat message history (also used by  │
                         │      the widget to load its own history) │
                         │                                          │
  Instagram/Meta ──GET───▶ /webhook/instagram                       │
                         │   └─ Subscription verification           │
                         │      (hub.challenge handshake)           │
                         │                                          │
                         │  ┌─ Shared Redis Listener ─────────────┐ │
                         │  │ Subscribe: incoming_messages        │ │
                         │  │ On event:                           │ │
                         │  │  ├─ Resolve chat → client_id        │ │
                         │  │  ├─ Send to client (if not sender)  │ │
                         │  │  └─ Send to operators (skip sender) │ │
                         │  └─────────────────────────────────────┘ │
                         └───────┬─────────────────┬────────────────┘
                                 │                 │
                            ┌────▼────┐      ┌─────▼─────┐
                            │ Postgres │      │   Redis   │
                            │  :5432   │      │   :6379   │
                            └─────────┘      └───────────┘
                         channels             pub/sub channels:
                         instagram_channels   incoming_messages
                         telegram_channels    cache_invalidation
                         widget_channels
                         clients
                         operators
                         chats
                         messages
```

### Message Lifecycle

Handlers build a `NewMessage`, then branch on `EventKind`:

```
EventKind::Message  → INSERT (dedup) → publish IncomingEvent::Message
EventKind::Edit     → UPDATE text    → publish IncomingEvent::Edit
EventKind::Read     → UPDATE status  → publish IncomingEvent::Read (watermark)
EventKind::Reaction → log only
EventKind::Unknown  → log only
```

Only `Message` events create rows. `Edit` and `Read` mutate existing rows found by `(channel_id, external_message_id)`. Read receipts use watermark semantics: all messages in the same chat up to the referenced message are marked as read.

Published to Redis as `IncomingEvent` with a `"type"` discriminator:
```json
{"type": "message", "id": "...", "text": "...", "status": "new", ...}
{"type": "edit", "id": "...", "text": "...", "edited_at": "...", ...}
{"type": "read", "id": "...", ...}
```

Chat resolution: if the sender exists in the `clients` table, `find_or_create_chat` finds or creates an active chat for `(client_id, channel_id)`. At most one active chat per pair (enforced by partial unique index).

### Caching

Channel, client, operator, and chat data are cached in-memory (`ChannelCache`, `ClientCache`, `OperatorCache`, `ChatCache`) to avoid a Postgres round-trip on every incoming webhook/message. All caches use a read-through strategy: on a miss, query the database and store the result locally.

When data is updated, publish an invalidation event to Redis so all replicas evict the stale entry:

```bash
# Invalidate a specific entity
redis-cli PUBLISH cache_invalidation "channel:<uuid>"
redis-cli PUBLISH cache_invalidation "client:<uuid>"
redis-cli PUBLISH cache_invalidation "operator:<uuid>"
redis-cli PUBLISH cache_invalidation "chat:<uuid>"
```

Each replica runs a background listener on the `cache_invalidation` topic that dispatches by entity type. Providers automatically publish client invalidation after every `upsert_client`.

## Project Structure

```
src/
├── main.rs              # Entrypoint: load config, connect DB, start server, spawn shared listener, graceful shutdown
├── lib.rs               # Public module re-exports
├── cache.rs             # In-memory caches (channel, client, operator, chat) with Redis Pub/Sub invalidation; ClientCache dual-keyed by UUID + (provider, external_id)
├── config.rs            # AppConfig (env vars) + AppState (config + DB pool + Redis + caches + registry + shutdown token)
├── db.rs                # Postgres pool, migrations, channel/client/operator/chat queries, message persistence
├── error.rs             # WebhookError → HTTP status mapping
├── jwt.rs               # HS256 JWT sign/verify for WebSocket identity (widget clients + operators)
├── registry.rs          # ClientRegistry (tracks active WS connections per client/operator UUID via mpsc channels)
├── model.rs             # Sender, NewMessage, IncomingMessage/Edit/Read, ProviderKind, EventKind, WsInbound, OperatorInbound, WsOutbound
├── pipeline.rs          # Shared message processing pipeline (persist_and_publish, resolve_sender, publish_event)
├── listener.rs          # Shared Redis listener (spawn_message_listener) — dispatches events to WS connections
├── handler/
│   ├── mod.rs           # Shared handler utilities (ConnectionGuard, send_outbound, resolve_client/operator, WS constants)
│   ├── webhook.rs       # HTTP webhook handlers (meta_verify, instagram_ingest, telegram_ingest)
│   ├── widget_ws.rs     # Widget WebSocket handler
│   ├── operator_ws.rs   # Operator WebSocket handler (async Telegram delivery via Bot API)
│   └── api.rs           # REST API handlers (get_chats, get_chat_messages)
├── routes.rs            # Router assembly
└── provider/
    ├── mod.rs           # WebhookProvider trait (verify + parse)
    ├── instagram.rs     # HMAC-SHA256 verification, Meta payload parsing, client identity via Graph API
    └── telegram.rs      # Secret token verification, Telegram Update parsing, client resolution, outbound sendMessage via Bot API

docker/
├── Dockerfile           # Multi-stage build
└── nginx.conf           # Nginx reverse proxy config

frontend/
├── widget.html          # Chat widget test page (WebSocket client)
└── operator.html        # Operator dashboard (WebSocket + REST API)

migrations/              # SQL migrations (auto-run on startup)
tests/
├── common/mod.rs        # Shared test helpers (RAII cleanup guards, pool setup)
├── integration.rs       # Integration tests (require Postgres + Redis)
└── client_identity.rs   # Client identity upsert tests
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
| `WIDGET_JWT_SECRET` | yes | — | HMAC-SHA256 secret for WebSocket JWTs (widget clients + operators) |

## Testing

### Unit tests (no database needed)

```bash
just test-unit
```

Covers HMAC verification, secret token validation, event classification, payload deserialization, WebSocket message types, UUID mid validation, TelegramUser/display name building, and the chat-history WebSocket event.

### All tests (unit + integration)

Requires Docker; `just test` starts Postgres and Redis and injects `DATABASE_URL` / `REDIS_URL`:

```bash
just test
```

Integration tests cover:
- Meta webhook subscription verification (valid/invalid token, wrong mode)
- Instagram POST ingestion (valid/invalid/missing signature, channel lookup, non-instagram object rejection)
- Telegram POST ingestion (valid/invalid secret, unknown channel → 404)
- WebSocket widget (connect, ACK, multiple messages, error recovery, unknown widget, invalid mid rejection)
- WebSocket edit action (edit ACK, Redis edit event, unknown action error)
- Operator WebSocket (connect, send message to widget client, edit reaches widget client)
- Read receipt forwarding (widget read → operator, operator read → widget)
- Operator REST API (list chats, chat messages, unknown chat → 404)
- Widget chat history (`find_last_chat` returns a closed chat, the newest of several, or None; `sender_name` resolution; chat event on reconnect and its absence for new clients)
- Redis pub/sub verification for all three providers
- Client identity upsert (create, update, conflict handling)
- Telegram client reuse (same client_id across messages from same user)
- Channel cache (read-through, per-channel invalidation, Redis Pub/Sub eviction, cross-channel isolation)
- Client cache invalidation via Redis Pub/Sub

Test data cleanup uses RAII drop guards (`TestChannel`, `TestClient`, `TestChat`, `TestMessage`, `TestOperator`) in `tests/common/` to ensure rows are deleted even if a test panics.

### Linting

```bash
just lint
just fmt-check
```

`just check` runs fmt, clippy, and the full suite in one go.
