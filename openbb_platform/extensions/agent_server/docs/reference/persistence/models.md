# `openbb_agent_server.persistence.models`

SQLAlchemy ORM definitions for every persistent row the agent server writes. The `SqliteHistoryStore` and the `WidgetDataStore` both target this schema. Tables are created on demand by `init_schema()`; there are no migrations — schema changes must be additive.

**Source:** [`openbb_agent_server/persistence/models.py`](../../../openbb_agent_server/persistence/models.py)

## `class Base(DeclarativeBase)`

Common declarative base. Two `type_annotation_map` entries make `dict[str, Any]` / `list[Any]` mapped types resolve to the `JSON` column type — used liberally for payload blobs across the schema.

## Tables

### `users`

The root identity table. Cascade-targeted by every per-user row through `ForeignKey("users.user_id", ondelete="CASCADE")`.

| Field | Type | Purpose |
| --- | --- | --- |
| `user_id` | `String` (PK) | Stable identifier from the auth backend. |
| `display_name` | `String?` | Display name from the principal. |
| `email` | `String?` | Email (may be redacted depending on auth backend). |
| `created_at` | `DateTime(timezone=True)` | First-sight timestamp. |
| `last_seen_at` | `DateTime(timezone=True)` | Bumped on every `upsert_user` call. |
| `quota_json` | `JSON` | Per-user quota / rate-limit config; opaque to the store. |
| `memory_opt_in` | `Boolean` | Whether the user has opted into durable memory. |

### `api_keys`

Auth credentials for the `api_key_table` backend.

| Field | Type | Purpose |
| --- | --- | --- |
| `key_id` | `String` (PK) | Short prefix shown to the user. |
| `user_id` | FK → `users.user_id` | Owner. |
| `hashed_secret` | `String` | Argon2id hash of the bearer secret. |
| `label` | `String?` | Operator-facing tag. |
| `scopes` | `JSON` | Granted scope list. |
| `created_at` | `DateTime(timezone=True)` | Issue time. |
| `revoked_at` | `DateTime(timezone=True)?` | Set by `keys revoke`. |

Index: `ix_api_keys_user (user_id)`.

### `conversations`

Per-thread metadata.

| Field | Type | Purpose |
| --- | --- | --- |
| `conversation_id` | `String` (PK) | Client-supplied or generated. |
| `user_id` | FK → `users.user_id` | Owner. |
| `title` | `String?` | Optional human label. |
| `summary_blob_ref` | `Text?` | Reserved for long-conversation summarisation. |
| `created_at` / `updated_at` | `DateTime(timezone=True)` | Lifecycle. |

Index: `ix_conversations_user (user_id, updated_at)`. Cascades `messages` (delete-orphan).

### `traces`

One row per HTTP exchange. Join key across the audit tables.

| Field | Type | Purpose |
| --- | --- | --- |
| `trace_id` | `String` (PK) | UUIDv7 from the request header or generated. |
| `user_id` | FK → `users.user_id` | Partition. |
| `conversation_id` | `String?` | Thread; `None` for single-shot calls. |
| `run_id` | `String?` | One agent invocation; multiple per trace if the model loops. |
| `started_at` / `ended_at` | `DateTime(timezone=True)?` | Lifecycle. |
| `status` | `String` | `"running"` / `"complete"` / `"error"` / `"cancelled"`. |

Index: `ix_traces_user (user_id, started_at)`.

### `messages`

Conversation transcript.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `conversation_id` | FK → `conversations.conversation_id` | Thread. |
| `user_id` | `String` | Partition (denormalised for indexed reads). |
| `seq` | `Integer` | Monotonic position within the conversation. |
| `role` | `String` | `human` / `ai` / `tool`. |
| `content` | `Text` | Message body. |
| `widget_refs` | `JSON` | Selected widgets at the time of the turn (immutable record). |
| `file_refs` | `JSON` | Uploaded-file refs. |
| `trace_id` | `String?` | Trace that produced this message. |
| `ts` | `DateTime(timezone=True)` | Persist time. |

Index: `ix_messages_user_conv_seq (user_id, conversation_id, seq)`.

### `runs`

Per-invocation metadata distinct from `traces`. Each trace may produce multiple runs (model loop, retry).

| Field | Type | Purpose |
| --- | --- | --- |
| `run_id` | `String` (PK) | LangGraph run id. |
| `user_id` | `String` | Partition. |
| `conversation_id` | `String?` | Thread. |
| `trace_id` | `String?` | Trace this run belongs to. |
| `model` | `String?` | Model id used. |
| `system_prompt_hash` | `String?` | Hash of the system prompt (for caching analysis). |
| `status` | `String` | `running` / `complete` / `error`. |
| `started_at` / `ended_at` | `DateTime(timezone=True)?` | Lifecycle. |

Index: `ix_runs_user (user_id, started_at)`.

### `tool_calls`

One row per tool invocation.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `trace_id` | `String` | Owner trace. |
| `user_id` | `String` | Partition. |
| `seq` | `Integer` | Position within the trace. |
| `tool_name` | `String` | Bound LangChain tool name. |
| `args_json` | `JSON` | Resolved arguments. |
| `result_json` | `JSON?` | Result on success. |
| `error` | `Text?` | Exception message on failure. |
| `latency_ms` | `Integer?` | Wall-clock duration. |
| `side` | `String` | `"server"` (ran in-process) / `"client"` (dispatched to Workspace). |
| `state` | `String` | `"pending"` / `"complete"` / `"error"`. |
| `ts` | `DateTime(timezone=True)` | Record time. |

Index: `ix_tool_calls_user_trace (user_id, trace_id, seq)`.

### `usage`

Per-model token usage.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `trace_id` | `String` | Owner trace. |
| `user_id` | `String` | Partition. |
| `seq` | `Integer` | Position within the trace. |
| `model` | `String` | Model identifier. |
| `input_tokens` / `output_tokens` | `Integer` | Token counts. |
| `cache_read` / `cache_creation` | `Integer` | Prompt-caching deltas. |
| `cost_usd` | `Float` | Server-computed USD cost. |
| `ts` | `DateTime(timezone=True)` | Record time. |

Index: `ix_usage_user_trace (user_id, trace_id, seq)`.

### `artifacts`

One row per `emit_*_artifact` call.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `trace_id` | `String` | Owner. |
| `user_id` | `String` | Partition. |
| `seq` | `Integer` | Position within the trace. |
| `kind` | `String` | `text` / `table` / `chart` / `html` / `snowflake_query` / `snowflake_python`. |
| `payload_blob_ref` | `Text?` | Pointer for out-of-line storage (currently unused). |
| `payload_json` | `JSON?` | Inline payload. |
| `mime` | `String?` | MIME if applicable. |
| `ts` | `DateTime(timezone=True)` | Record time. |

Index: `ix_artifacts_user_trace (user_id, trace_id, seq)`.

### `citations`

One row per `cite()` call.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `trace_id` | `String` | Owner. |
| `user_id` | `String` | Partition. |
| `seq` | `Integer` | Position within the trace. |
| `source` | `String?` | Source label (chip title). |
| `source_url` | `String?` | URL or widget origin. |
| `page` | `Integer?` | PDF page (when applicable). |
| `bbox_json` | `JSON?` | Quote bounding boxes. |
| `text_snippet` | `Text?` | Quoted text. |

Index: `ix_citations_user_trace (user_id, trace_id, seq)`.

### `widget_data`

One row per ingested `get_widget_data` response.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `user_id` | FK → `users.user_id` | Owner. |
| `conversation_id` | `String` | Thread. |
| `widget_uuid` | `String` | Per-instance Workspace UUID. |
| `widget_name` | `String?` | Human label. |
| `origin` | `String?` | Vendor / integration. |
| `input_args` | `JSON` | The widget's params at fetch time. |
| `columns` | `JSON?` | Column list. |
| `rows` | `JSON` | Row payload. |
| `ingested_at` | `DateTime(timezone=True)` | Fetch time. |

Index: `ix_widget_data_lookup (user_id, conversation_id, widget_uuid, ingested_at)`.

### `pdf_documents`

One row per ingested PDF.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id; parent of `pdf_pages`. |
| `user_id` | `String` | Partition. |
| `file_key` | `String` | Stable hash of the PDF bytes (dedup key). |
| `name` | `String` | Filename. |
| `url` | `String?` | Source URL if any. |
| `mime` | `String?` | MIME if known. |
| `total_pages` | `Integer` | Page count. |
| `metadata_json` | `JSON` | Raw PDF metadata. |
| `toc_json` | `JSON` | Table of contents. |
| `status` | `String` | `pending` / `ready` / `error`. |
| `error` | `Text?` | Parse error on `status="error"`. |
| `ingested_at` | `DateTime(timezone=True)` | Ingest time. |

Index: `ix_pdf_documents_lookup (user_id, file_key)`.

### `pdf_pages`

One row per parsed page.

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `Integer` (PK, auto) | Row id. |
| `pdf_id` | FK → `pdf_documents.id` | Parent. |
| `page` | `Integer` | 1-based page number. |
| `text` | `Text` | Extracted text. |
| `words_json` | `JSON` | Per-word bounding boxes (for citation highlights). |

Index: `ix_pdf_pages_doc_page (pdf_id, page)`.

### `pending_runs`

State blob for a run that yielded on a client-side tool call. Resumed by the next `POST /v1/query` carrying the matching `run_id`.

| Field | Type | Purpose |
| --- | --- | --- |
| `run_id` | `String` (PK) | LangGraph run id. |
| `user_id` | `String` | Partition. |
| `state_blob` | `JSON` | Opaque LangGraph checkpoint payload. |
| `created_at` | `DateTime(timezone=True)` | Creation time. |

Index: `ix_pending_runs_user (user_id)`.

## See also

- [`persistence/store.md`](store.md) — ABC and record types.
- [`persistence/sqlite_store.md`](sqlite_store.md) — the SQLAlchemy implementation that targets this schema.
- [`operating/persistence.md`](../../operating/persistence.md) — operational guide.
