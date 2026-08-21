# Tech Debt

Known shortcuts and gaps, recorded when they are found rather than when they are fixed.
Each entry: what is wrong, why it matters, what a fix looks like.

## REST API has no authorization

**Status:** open (found 2026-08-21)

`GET /api/chats` and `GET /api/chats/{chat_id}` (`src/handler/api.rs`, wired in
`src/routes.rs`) accept any request. There is no token check and no ownership check,
so anyone who knows a chat UUID can read the whole conversation, and anyone at all can
list every active chat together with client names and last-message text.

Both sides of the app already have a JWT: `resolve_client` and `resolve_operator`
(`src/handler/mod.rs`) sign one with `WIDGET_JWT_SECRET`, and the frontends keep it in
localStorage (`chatbridge_token` / `operator_token`). Today it is used only for the
WebSocket handshake — the REST calls in `frontend/operator.html` send nothing.

This gets worse once the widget loads chat history over REST: the chat UUID starts
living in public client-side JS, so it stops being even an accidental secret.

**Fix:** require the JWT on the chat REST endpoints.
- Operator token → may list chats and read any chat.
- Client token → may read only chats where `chats.client_id` matches the token subject;
  anything else returns 404 (not 403, to avoid confirming that the chat exists).
- Distinguishing the two requires knowing whether a JWT subject is an operator or a
  client — either a role claim in the JWT or a lookup against `operators` / `clients`.

## `chats.status` is unenforced and never written

**Status:** open (found 2026-08-21)

`status TEXT NOT NULL DEFAULT 'new'` (`migrations/20260317000002_create_chats.sql`) has no
CHECK constraint and no Rust enum, and no code path ever writes it — every chat stays
`'new'` forever. Two behaviours already depend on the column: the partial unique index
`idx_chats_active (client_id, channel_id) WHERE status = 'new'` (one active chat per
client per channel) and the `WHERE c.status = 'new'` filter in `list_active_chats`.

So chat closing is designed for but not implemented, and nothing prevents a typo'd
status value from silently dropping a chat out of both the index and the operator inbox.

**Fix:** add chat closing (an endpoint or operator WS action that sets `'closed'`), a
`CHECK (status IN ('new', 'closed'))` constraint, and a `ChatStatus` enum in `model.rs`
mirroring `ProviderKind`.

## `get_chat_messages` returns the oldest 100 messages and has no pagination

**Status:** open (found 2026-08-21)

`get_chat_messages` (`src/db.rs:337`) is `ORDER BY created_at ASC LIMIT 100` with no offset,
no cursor, and no total count. Past 100 messages in a chat, the query keeps returning the
same oldest 100 and silently drops everything newer — the exact opposite of what a chat UI
needs. There is no way to ask for the rest.

`frontend/operator.html` already hits this: open a long-running chat and the operator sees
the start of the conversation with no indication that recent messages exist. Widget history
loading (`docs/superpowers/specs/2026-08-21-widget-chat-history-design.md`) inherits the
same behaviour, and there it is worse — the client's own recent messages would be missing
from their own transcript.

**Fix:** invert the query and paginate.
- Fetch the *newest* N (`ORDER BY created_at DESC LIMIT n`), reverse for display.
- Keyset pagination rather than OFFSET: `AND created_at < $cursor` (with `id` as tiebreaker,
  since `created_at` is not unique), returning the next cursor to the client.
- Both frontends then load older messages on scroll-up instead of assuming one request is
  the whole chat.

## Settings panel and channel API are unauthenticated

**Status:** open (found 2026-08-22)

`/api/channels` (`src/handler/channels.rs`, wired in `src/routes.rs`) has no authorization at
all, and `frontend/settings.html` requires no login. Anyone who can reach the server can create
channels, read every Telegram bot token and Instagram access token **in plain text** from
`GET /api/channels`, repoint a bot's webhook at their own server via
`POST /api/channels/{id}/webhook`, and soft-delete every channel.

This is strictly worse than "REST API has no authorization" above, which it extends: that entry
leaks conversations, this one leaks the credentials that own them.

Accepted deliberately — there is no production deployment yet, and returning keys in full was a
requirement for the panel, since editing a token means seeing it. It must be closed before the
first deployment that is reachable from the internet.

**Fix:** whatever authorization scheme closes the entry above, applied to `/api/channels` first,
plus a decision about whether the panel should return secrets at all once more than one person
can reach it.

## `escapeHtml` and the date formatters exist in three copies

**Status:** open (found 2026-08-22)

`frontend/operator.html`, `frontend/widget.html` and `frontend/settings.html` each carry their own
copy of `escapeHtml`, plus a near-identical date formatter (`formatTime` / `formatDate`).

Deliberately not fixed while adding the settings page: extracting `frontend/common.js` means
editing two already-working pages for six lines of gain, which is a worse diff than the
duplication. The shared stylesheet (`frontend/style.css`) was extracted because the CSS
duplication was much larger.

Note that `settings.html` additionally needed `escapeAttr`, because `escapeHtml` (built on
`textContent` → `innerHTML`) does not escape quotes and the settings page writes operator-supplied
values into HTML attributes. Any future shared module must keep both.

**Fix:** a `frontend/common.js` holding `escapeHtml`, `escapeAttr` and one date formatter, loaded
by `operator.html` and `settings.html`. Leave `widget.html` self-contained — it is embedded into
third-party sites and should not depend on files it does not control.

## A crash between a channel's INSERT and its `setWebhook` strands the bot identity

**Status:** open (found 2026-08-22)

`create_telegram` (`src/handler/channels.rs`) commits the row, then calls `setWebhook`, which can
take up to the 10-second reqwest timeout. Two things hold inside that window: `GET /api/channels`
lists a channel that has no webhook yet, and a process crash leaves the row committed with nothing
to roll it back. Because the unique index on `(provider, external_key)` has no `deleted_at` filter,
that bot's identity is then occupied permanently by a channel that never worked, and the only
recovery is "Re-register" from the panel.

The ordering itself is deliberate and must not be reversed: INSERT precedes `setWebhook` so that
the unique index, rather than a racy pre-check, arbitrates duplicates and a live channel's webhook
cannot be hijacked. An in-process `setWebhook` failure is already handled by a physical rollback.
Only the crash case is uncovered.

**Fix:** a `webhook_registered_at` column marking a channel provisional until `setWebhook` returns,
with the panel offering to finish or discard provisional channels; or a reconciliation pass on
startup.

## `WebhookError` is now the error type for all REST endpoints

**Status:** open (found 2026-08-22)

`WebhookError` (`src/error.rs`) started as the webhook ingestion error type and is now returned by
the chat REST API and the whole channel CRUD surface, including variants (`Conflict`, `BadGateway`)
that no webhook path produces. The name misleads a reader into thinking `handler/channels.rs`
handles webhooks.

**Fix:** rename to `AppError`. Purely mechanical but touches every handler and both test files, so
it was left out of the settings-panel change rather than inflating that diff.
