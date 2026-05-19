# `openbb_agent_server.runtime.context`

Per-request `RunContext` plus the contextvars that propagate it through the agent loop. Every tool source, middleware, and sub-agent reaches the active context via `context.current()`.

**Source:** [`openbb_agent_server/runtime/context.py`](../../../openbb_agent_server/runtime/context.py)

## `class WidgetRef(BaseModel)`

One Workspace-supplied widget the user has selected as context. Pydantic `extra="allow"` — additional fields Workspace sends (`name`, `description`, etc.) are accessible via `model_extra` without being part of the public schema.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `uuid` | `str` | required | Per-instance dashboard widget UUID. Stable lookup key. |
| `widget_id` | `str` | `""` | Internal slug (e.g. `blk_drill_fund_documents`). Multiple instances share this. |
| `origin` | `str` | `""` | Workspace's `origin` field. |
| `params` | `dict[str, Any]` | `{}` | Widget params (ticker, doc_type, etc.) flattened to a key→value dict. |
| `data` | `Any` | `None` | Pre-fetched rows. Populated only after a `get_widget_data` round-trip. |

## `class FileRef(BaseModel)`

One uploaded file (PDF / image / spreadsheet / raw). Pydantic `extra="allow"`.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `name` | `str` | required | Filename (used to disambiguate matches in `pdf_extract`). |
| `mime` | `str \| None` | `None` | MIME type if known. |
| `data_base64` | `str \| None` | `None` | Inline base64-encoded bytes. |
| `url` | `str \| None` | `None` | Workspace-served URL. The PDF tools accept either. |

## `class RunContext(BaseModel)`

Bundles identity + request payload for one `/v1/query` exchange. Bound on a contextvar for the lifetime of the run by `bind()`.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `principal` | `UserPrincipal` | required | Resolved user identity (see [principal.md](principal.md)). |
| `trace_id` | `str` | required | One trace per HTTP exchange — join key across logs / usage / tool_calls. |
| `run_id` | `str` | required | One agent invocation; resets between turns of the same conversation. |
| `conversation_id` | `str` | required | Multi-turn thread id; persistence partition. |
| `agent_name` | `str` | `"default"` | Profile name picked by the router. |
| `timezone` | `str \| None` | `None` | Forwarded from `QueryRequest.timezone`. |
| `widgets` | `tuple[WidgetRef, ...]` | `()` | Flattened primary + secondary + extra widgets from `WidgetsBag`. |
| `uploaded_files` | `tuple[FileRef, ...]` | `()` | Files attached this turn (after the router's promotion pass). |
| `api_keys` | `dict[str, str]` | `{}` | Per-request keys forwarded by Workspace; takes precedence over env vars. |
| `api_urls` | `dict[str, str]` | `{}` | Per-request base URLs. |
| `tools` | `tuple[dict, ...]` | `()` | The user's enabled MCP tool list (when `mcp-tools` feature is on). |
| `workspace_options` | `dict[str, Any]` | `{}` | Custom-feature option values keyed by id (`{"search-web": True, "model": "gpt-4o"}`). Read a value directly, or test a toggle with `ctx.has_workspace_option("X")`. |

`has_workspace_option(slug: str) -> bool` returns `True` only when the option's value is truthy — a toggle the user left off arrives as `False`.

## Contextvars

### `current() -> RunContext`

Return the current `RunContext` from the contextvar. Raises `LookupError` if no run is bound on this task.

### `runtime_state() -> dict[str, Any]`

Return the per-run mutable scratch dict. Lifetime = one `bind()` block. Used by `jobs.JobRegistry` and middleware that need cheap per-run scratch state.

### `bind(ctx: RunContext) -> Iterator[RunContext]`

Context manager. Binds `ctx` and a fresh runtime-state dict for the lifetime of the `with` block. The router wraps every `astream` call in this.
