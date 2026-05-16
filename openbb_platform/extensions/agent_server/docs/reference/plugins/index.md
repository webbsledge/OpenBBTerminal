# `openbb_agent_server.plugins`

Built-in plugin implementations, one subpackage per entry-point group. The runtime resolves names through `runtime/registry.py::load(group, name, config)`; each `__init__.py` here registers the package's plugin classes against the matching `openbb_agent_server.<group>` entry-point group declared in `pyproject.toml`.

**Source:** [`openbb_agent_server/plugins/__init__.py`](../../../openbb_agent_server/plugins/__init__.py)

## Subpackages

| Subpackage | Entry-point group | Contents |
| --- | --- | --- |
| [`plugins.auth`](auth/index.md) | `openbb_agent_server.auth` | `none`, `bearer_static`, `api_key_table`, `oidc_jwt`, `openbb_workspace`. See [auth](../../operating/auth.md). |
| [`plugins.models`](models/index.md) | `openbb_agent_server.models` | `anthropic`, `openai`, `openai_compat`, `nvidia`, `bedrock`, `vertex`, `google_genai`, `groq`, `snowflake`, `fake`. |
| [`plugins.tools`](tools/index.md) | `openbb_agent_server.tools` | Every shipped tool source — artifacts, web_search, widget_data, pdf_extract, mcp_local/http, vision_qa, paligemma_vision, gemma_audio, gemini_image, gemini_embeddings, groq_audio, dashboard, recall_user_memory, translate, rerank, workspace_mcp, client_side, python_module, background_jobs, snowflake. |
| [`plugins.middleware`](middleware/index.md) | `openbb_agent_server.middleware` | `call_limit`, `tool_call_limit`, `tool_call_announcer`, `tool_call_ledger`, `tool_filter`, `tool_message_normaliser`, `loop_guard`, `usage_recorder`. |
| [`plugins.subagents`](subagents/index.md) | `openbb_agent_server.subagents` | `researcher`, `analyst`, `charter`, `pdf_reader`. |
| [`plugins.checkpointers`](checkpointers/index.md) | `openbb_agent_server.checkpointers` | `inmemory`, `sqlite`, `postgres`. |

## How to extend

Every subpackage's contract is documented at:

- [`plugin-system.md`](../../developing/plugin-system.md) — six-group overview, discovery, and the `pyproject.toml` entry-point shape.
- [Writing an auth backend](../../developing/writing-an-auth-backend.md), [a model provider](../../developing/writing-a-model-provider.md), [a tool source](../../developing/writing-a-tool-source.md), [a middleware](../../developing/writing-a-middleware.md), [a sub-agent](../../developing/writing-a-subagent.md) — per-group authoring walkthroughs.

Third-party packages register against the same entry-point groups; no fork or in-tree edit is required.

## See also

- [`runtime/plugins.md`](../runtime/plugins.md) — the six ABCs / protocols every plugin satisfies.
- [`runtime/registry.md`](../runtime/registry.md) — entry-point discovery internals.
