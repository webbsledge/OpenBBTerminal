# `openbb_agent_server.app.router`

FastAPI router that exposes the OpenBB Workspace custom-agent surface — `/agents.json` for discovery, `/v1/query` for chat, plus per-user resources for memory, traces, usage, and conversation history.

**Source:** [`openbb_agent_server/app/router.py`](../../../openbb_agent_server/app/router.py)

## Factory

### `def build_router(*, settings, auth, history, memory, translator, widget_store) -> APIRouter`

Assemble the API router with collaborators baked in. Called by `app.py` during app construction; the resulting router is mounted on the FastAPI app.

## Endpoints

All `/v1/*` endpoints require an authenticated principal and partition every read by `user_id`. The auth dependency is the registered `AuthBackend` plugin (see [`operating/auth.md`](../../operating/auth.md)).

### Discovery

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| GET | `/agents.json` | none | Workspace pulls the per-profile metadata (`{name, description, image, features, query_path}`) once on add. |

### Chat (streaming)

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| POST | `/v1/query` | `agent:query` | Default profile. Body: `QueryRequest`. Returns `text/event-stream` of `MessageChunkSSE` / `StatusUpdateSSE` / `FunctionCallSSE` / `MessageArtifactSSE` / `CitationCollectionSSE`. |
| POST | `/agents/{agent_name}/v1/query` | `agent:query` | Same shape, but answers from the named profile. |

### Conversations

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| GET | `/v1/conversations` | `agent:query` | Per-user list of recent conversations. |
| GET | `/v1/conversations/{conversation_id}/messages` | `agent:query` | Full message log for one thread. |
| POST | `/v1/conversations/{conversation_id}/cancel` | `agent:query` | Signal an `asyncio.Event` keyed on `(user_id, run_id)` so an in-flight run aborts gracefully. **In-process only** — there is no cross-worker broadcast today. Status 202. |

### Identity

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| GET | `/v1/me` | any | Return the authenticated principal (`user_id`, `display_name`, `email`, `scopes`). |
| DELETE | `/v1/me` | `agent:query` | Permanently delete the user and every cascading row (conversations, traces, memory, …). Status 204. |

### Memory

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| GET | `/v1/memory` | `memory:read` | List durable memories with optional `?q=` substring filter. |
| PATCH | `/v1/memory/{memory_id}` | `memory:write` | Update content / tags / scope on one memory row. |
| DELETE | `/v1/memory/{memory_id}` | `memory:write` | Remove one memory row. Status 204. |

### Observability

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| GET | `/v1/traces/{trace_id}` | `agent:query` | Trace bundle: messages + tool_calls + usage + artifacts + citations for one run. |
| GET | `/v1/usage` | `agent:query` | Aggregated token usage; filter by `?trace_id=` / `?conversation_id=` / `?from=` / `?to=`. |

## Cross-cutting behaviour

- **Cancellation.** Every active run records its `asyncio.Event` in `_cancellations[(user_id, run_id)]`. A client disconnect triggers a `CancelledError` on the FastAPI handler; `/v1/conversations/{id}/cancel` sets the event from outside the run.
- **Trace headers.** Every response includes `X-Server-Trace-ID` and echoes `X-Trace-ID` (server-generated UUIDv7 if the client didn't send one).
- **Widget pre-fetch.** When the last user turn is a human message and `widgets.primary` is non-empty, the router emits a `StatusUpdateSSE` ("Fetching {names} data from Workspace…") and a `FunctionCallSSE(get_widget_data)` before invoking the agent — see [`guides/widgets-and-data.md`](../../guides/widgets-and-data.md).
- **PDF promotion.** The router walks `widgets.primary` / `widgets.secondary` and every tool message looking for PDF byte references, stamps them with `source_widget_uuid` / `source_widget_id`, and promotes them into `uploaded_files` so `pdf_extract` can read them by name.
- **Memory write-back.** Post-turn, the optional `memory_writer` middleware extracts durable facts from the assistant's reply and writes them to the `memories` table (gated by the `memory:write` scope).
