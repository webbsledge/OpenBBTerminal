# `openbb_agent_server.app.settings`

Top-level Pydantic settings model. Reads from `OPENBB_AGENT_*` env vars (with `__` as the nested delimiter) AND from the `[agent]` TOML section via `AgentServerSettings.from_toml(...)`. Env wins over TOML; both are flattened into the same field set.

**Source:** [`openbb_agent_server/app/settings.py`](../../../openbb_agent_server/app/settings.py)

## `class FeatureSpec(BaseSettings)`

One row in the `agents.json` `features` map. `extra="allow"` so Workspace-specific fields pass through untouched.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `label` | `str` | required | Human label shown in Workspace's chat-input settings menu. |
| `description` | `str` | required | One-paragraph description shown on hover. |
| `default` | `bool` | `False` | Whether the toggle starts on. |

## `class AgentMetadata(BaseSettings)`

Static fields rendered in `GET /agents.json`. Env prefix `OPENBB_AGENT_META_`. Frozen.

| Field | Type | Default | Env var | Purpose |
| --- | --- | --- | --- | --- |
| `name` | `str` | `"OpenBB · NVIDIA Stack"` | `OPENBB_AGENT_META_NAME` | Display name on the Workspace agent picker. |
| `description` | `str` | (long string — see source) | `OPENBB_AGENT_META_DESCRIPTION` | Default profile description. |
| `image_url` | `str \| None` | `None` | `OPENBB_AGENT_META_IMAGE_URL` | Optional avatar URL. |

## `class AgentProfile(BaseSettings)`

Resolved per-profile config returned by `AgentServerSettings.resolve_profile(name)`. Frozen; consumers don't mutate.

| Field | Type | Purpose |
| --- | --- | --- |
| `name` | `str` | Profile slug. |
| `metadata` | `AgentMetadata` | Resolved metadata (overlay-merged with the defaults). |
| `model_provider` | `str` | Plugin name from the `openbb_agent_server.models` entry-point group. |
| `model_name` | `str` | Concrete model id passed to the provider. |
| `model_config_` | `dict[str, Any]` | Per-profile model config; aliased on `model_config` in TOML. |
| `tool_sources` | `tuple[str, ...]` | Ordered tool-source plugin names. |
| `tool_source_config` | `dict[str, dict[str, Any]]` | Per-tool-source kwargs. |
| `subagents` | `tuple[str, ...]` | Sub-agent plugin names. |
| `middleware` | `tuple[str, ...]` | Middleware plugin names (in dispatch order). |
| `skills` | `tuple[str, ...]` | Filesystem paths the deepagents `SkillsMiddleware` scans. |
| `features` | `dict[str, Any]` | Feature map rendered in `agents.json`. |
| `system_prompt_file` | `str \| None` | Override path for the system prompt. Inline `system_prompt` strings are NOT supported and raise on parse. |

## `class AgentServerSettings(BaseSettings)`

The top-level model. Env prefix `OPENBB_AGENT_`, nested delimiter `__`, `extra="ignore"`, frozen.

### Networking

| Field | Type | Default | Env | Purpose |
| --- | --- | --- | --- | --- |
| `host` | `str` | `"127.0.0.1"` | `OPENBB_AGENT_HOST` | Bind host. |
| `port` | `int` | `6900` | `OPENBB_AGENT_PORT` | Bind port. |

### Auth

| Field | Type | Default | Env | Purpose |
| --- | --- | --- | --- | --- |
| `auth_backend` | `str` | `"none"` | `OPENBB_AGENT_AUTH_BACKEND` | Entry-point name in `openbb_agent_server.auth`. |
| `auth_config` | `dict[str, Any]` | `{}` | `OPENBB_AGENT_AUTH_CONFIG__*` | Backend kwargs. |

### Model

| Field | Type | Default | Env | Purpose |
| --- | --- | --- | --- | --- |
| `model_provider` | `str` | `"nvidia"` | `OPENBB_AGENT_MODEL_PROVIDER` | Entry-point name in `openbb_agent_server.models`. |
| `model_name` | `str` | `"nvidia/nemotron-3-super-120b-a12b"` | `OPENBB_AGENT_MODEL_NAME` | Model id. |
| `model_config_` (alias `model_config`) | `dict[str, Any]` | `{temperature: 0.4, max_completion_tokens: 8192, top_p: 0.95}` | `OPENBB_AGENT_MODEL_CONFIG__*` | Provider kwargs. |

### Tool sources (default)

```
artifacts, web_search, widget_data, inspect_widget_data, pdf_extract,
dashboard, recall_user_memory, translate, rerank, vision_qa, workspace_mcp
```

| Field | Type | Env | Purpose |
| --- | --- | --- | --- |
| `tool_sources` | `tuple[str, ...]` | `OPENBB_AGENT_TOOL_SOURCES` (comma) | Ordered plugin list. |
| `tool_source_config` | `dict[str, dict[str, Any]]` | `OPENBB_AGENT_TOOL_SOURCE_CONFIG__*` | Per-source kwargs. |

### Subagents & middleware

| Field | Default | Purpose |
| --- | --- | --- |
| `subagents` | `("researcher", "charter", "analyst", "pdf_reader")` | Sub-agent plugin names. |
| `middleware` | `("tool_message_normaliser", "tool_filter", "tool_call_announcer", "usage_recorder", "tool_call_ledger", "loop_guard", "call_limit", "tool_call_limit")` | Middleware in dispatch order. |
| `skills` | `()` | Filesystem paths the deepagents `SkillsMiddleware` scans at run start. |

The default middleware stack is what every profile inherits unless it overrides `middleware` in its TOML. See [`plugins/middleware/index.md`](../plugins/middleware/index.md) for what each one does.

### Prompts

| Field | Default | Purpose |
| --- | --- | --- |
| `system_prompt_file` | `None` | Override path. When `None`, the bundled `prompts/default_system_prompt.md` is used. Inline `system_prompt = "..."` strings are rejected. |

### Checkpointer

| Field | Default | Purpose |
| --- | --- | --- |
| `checkpointer_provider` | `"sqlite"` | LangGraph checkpointer; one of `inmemory` / `sqlite` / `postgres`. |
| `checkpointer_config` | `{}` | Per-provider kwargs. |

### Embeddings / reranker / translator

| Field | Default | Purpose |
| --- | --- | --- |
| `embeddings_provider` | `"nvidia"` | Prose embedder. |
| `embeddings_model` | `"nvidia/nv-embed-v1"` | Model id. |
| `embeddings_config` | `{}` | Per-provider kwargs. |
| `embeddings_code_provider` | `"nvidia-code"` | Code embedder (optional). |
| `embeddings_code_model` | `"nvidia/nv-embedcode-7b-v1"` | Code model id. |
| `embeddings_code_config` | `{}` | Code kwargs. |
| `reranker_provider` | `"nvidia"` | Optional rerank stage; `None` disables. |
| `reranker_model` | `"nv-rerank-qa-mistral-4b:1"` | Model. |
| `reranker_config` | `{}` | Per-provider kwargs. |
| `rerank_fanout` | `32` | ANN over-fetch before rerank. |
| `translation_provider` | `"nvidia"` | Optional translator; `None` disables. |
| `translation_model` | `"nvidia/riva-translate-4b-instruct-v1_1"` | Model. |
| `translation_config` | `{}` | Per-provider kwargs. |
| `translate_for_ingestion` | `True` | Translate non-English chunks during ingestion. |
| `ingest_target_language` | `"English"` | Target language for ingestion translation. |
| `ingest_char_threshold` | `2000` | Sources shorter than this skip ingestion. |
| `ingest_chunk_chars` | `1500` | Chunk size. |
| `ingest_chunk_overlap` | `200` | Adjacent-chunk overlap. |

### Persistence

| Field | Default | Purpose |
| --- | --- | --- |
| `db_url` | `None` | SQLAlchemy URL override (Postgres mode). |
| `data_dir` | `~/.openbb_platform/agent` | Default location for `history.db`, `memory.db`. |

`resolved_db_url()` returns `db_url` if set, else `sqlite+aiosqlite:///{data_dir}/history.db`.

### Features map

`features` defaults to a copy of `DEFAULT_FEATURES`. Operators override per profile or globally.

### Profiles

`profiles: dict[str, dict[str, Any]]` — alternate profiles overlaid on the base settings. The bundled defaults are `mistral-large-3`, `transcribe`, `qwen3-coder`, `seed-oss`, `step-flash`, `minimax-m2`. `default_profile = "default"` picks which one answers `POST /v1/query` (vs. the `POST /agents/{name}/v1/query` per-profile path).

### Methods

| Method | Purpose |
| --- | --- |
| `resolved_db_url() -> str` | Resolve `db_url` to a concrete SQLAlchemy URL, falling back to the SQLite default. |
| `all_profile_names() -> tuple[str, ...]` | Every profile the server hosts (including `default_profile` if absent from `profiles`). |
| `resolve_profile(name=None) -> AgentProfile` | Overlay one profile on the base settings and return the resolved `AgentProfile`. Raises `KeyError` on unknown profile. |
| `from_toml(agent_section) -> AgentServerSettings` (classmethod) | Build settings from the `[agent]` TOML dict. Env vars win — any key already set in `OPENBB_AGENT_<KEY>` is dropped from the TOML overlay before construction. |

## `DEFAULT_FEATURES`

```python
{
    "streaming":               True,
    "widget-dashboard-select": True,
    "widget-dashboard-search": True,
    "widget-global-search":    True,
    "mcp-tools":               True,
    "file-upload":             True,
    "generative-ui":           True,
    "search-web": {
        "label":       "Search Web",
        "description": "Allow the agent to search the public web …",
        "default":     False,
    },
}
```

The `search-web` toggle is a `FeatureSpec`-shaped dict so Workspace renders a per-user opt-in toggle in the chat-input settings menu. `SEARCH_WEB_FEATURE = "search-web"` is exported separately for cross-reference by the `web_search` tool source (which only binds when the toggle is on).

## See also

- [`app/config.md`](config.md) — how the TOML / env / user-settings cascade feeds this.
- [`operating/profiles.md`](../../operating/profiles.md) — operator's guide to profiles.
- [`operating/configuration.md`](../../operating/configuration.md) — env-var reference.
