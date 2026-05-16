# `openbb_agent_server.persistence.store`

`HistoryStore` ABC plus the immutable record types every plugin and endpoint consumes off the store. All queries are principal-scoped — there is no cross-user read path.

**Source:** [`openbb_agent_server/persistence/store.py`](../../../openbb_agent_server/persistence/store.py)

## Record types

Every record is a frozen Pydantic model so plugins can pass them around without worrying about mutation.

### `class TraceRecord`

| Field | Type | Purpose |
| --- | --- | --- |
| `trace_id` | `str` | Stable join key across logs / usage / tool_calls / artifacts / citations. |
| `user_id` | `str` | Partition key. |
| `conversation_id` | `str \| None` | Conversation the trace belongs to. |
| `run_id` | `str \| None` | One agent invocation. |
| `started_at` / `ended_at` | `datetime` / `datetime \| None` | Wall-clock bounds. |
| `status` | `str` | `running` / `complete` / `error` / `cancelled`. |

### `class MessageRecord`

| Field | Type | Purpose |
| --- | --- | --- |
| `conversation_id` | `str` | Thread id. |
| `seq` | `int` | Position within the conversation (1-based monotonic). |
| `role` | `str` | `human` / `ai` / `tool`. |
| `content` | `str` | Message body. |
| `user_id` | `str` | Owner. |
| `trace_id` | `str \| None` | Trace that produced this message. |
| `ts` | `datetime` | When it was persisted. |

### `class ToolCallRecord`

| Field | Type | Purpose |
| --- | --- | --- |
| `trace_id` | `str` | Owning trace. |
| `seq` | `int` | Position within the trace. |
| `user_id` | `str` | Partition. |
| `tool_name` | `str` | Bound LangChain tool name. |
| `args` | `dict` | Resolved arguments. |
| `result` | `dict \| None` | Returned payload (when success). |
| `error` | `str \| None` | Exception message (when failure). |
| `latency_ms` | `int \| None` | Wall-clock duration. |
| `side` | `str` | `server` (ran in-process) / `client` (dispatched to Workspace). |
| `state` | `str` | `pending` / `complete` / `error`. |

### `class UsageRecord`

| Field | Type | Purpose |
| --- | --- | --- |
| `trace_id` | `str` | Owning trace. |
| `user_id` | `str` | Partition. |
| `model` | `str` | Model identifier reported in `usage_metadata`. |
| `input_tokens` / `output_tokens` | `int` | Token counts. |
| `cache_read` / `cache_creation` | `int` | Prompt-caching deltas (Anthropic / OpenAI). |
| `cost_usd` | `float` | Server-computed USD cost. |

## `class HistoryStore(ABC)`

Persistence ABC. All methods take a `UserPrincipal`; the implementation MUST filter every query by `principal.user_id`.

| Method | Purpose |
| --- | --- |
| `upsert_user(principal)` | Insert / refresh the `users` row at first sight. |
| `begin_trace(principal, trace_id, conversation_id, run_id)` | Create the `traces` row at run start. |
| `end_trace(principal, trace_id, status)` | Set `ended_at` + `status` at run end. |
| `append_message(principal, conversation_id, role, content, trace_id) -> int` | Append one message and return its new `seq`. |
| `list_conversations(principal, limit=50) -> list[dict]` | Per-user list of recent conversations. |
| `get_messages(principal, conversation_id, limit=200) -> list[MessageRecord]` | Full message log for one thread. |
| `record_tool_call(principal, trace_id, tool_name, args, result, error, latency_ms, side, state)` | Record one tool call. |
| `record_usage(principal, trace_id, usage: UsageRecord)` | Record one usage row. |
| `delete_user(principal)` | Right-to-erasure — cascade-delete every row owned by this user. |
| `get_trace_bundle(principal, trace_id) -> dict \| None` | Full audit-join (messages + tool_calls + usage + artifacts + citations) for one trace. Powers `GET /v1/traces/{id}`. |
| `usage_summary(principal, trace_id=None, conversation_id=None) -> dict` | Aggregated usage rows. Powers `GET /v1/usage`. |

## Implementations

- [`SqliteHistoryStore`](sqlite_store.md) — SQLite default + Postgres mode under the same SQLAlchemy ORM.
