# `openbb_agent_server.plugins.middleware.tool_message_normaliser`

Rewrites the message list into a stable, provider-tolerant shape before every `model_call`. Without this middleware, mixed tool-call / human / assistant histories produce 400s on the stricter providers (Mistral Large 3, Nemotron via NIM) and lose structured `tool_calls` on others. The transform runs on both the sync and async paths and is idempotent — running it twice produces the same output.

**Source:** [`openbb_agent_server/plugins/middleware/tool_message_normaliser.py`](../../../../openbb_agent_server/plugins/middleware/tool_message_normaliser.py)

## Classes

### `ToolMessageNormaliserMiddlewareFactory`

Plugin entry-point name: `tool_message_normaliser`. Enabled by default (first in `AgentServerSettings.middleware` so every other middleware downstream sees the normalised history).

`build(ctx, config) -> AgentMiddleware` returns a per-run `_ToolMessageNormaliserMiddleware`. No config keys.

## Behaviour

The middleware hooks `wrap_model_call` / `awrap_model_call`. For each call it:

1. Calls `_strict_human_assistant(request.messages, preserve_tool_role=_wants_tool_role(request))`.
2. Logs every outgoing message via `_dump_messages` (diagnostic INFO log; turn down the logger to silence).
3. Hands the rewritten messages to the handler via `request.override(messages=new_messages)`.
4. Runs `_dedupe_tool_calls` on the response to collapse duplicate `(name, args)` tool-calls inside a single `AIMessage`.

### Multi-pass transform — `_strict_human_assistant`

**Pass 1** — walk every message:

- `SystemMessage` passes through untouched.
- `HumanMessage` passes through; if there are pending tool payloads (default path), they are prepended as `[tool: <name> result]\n<content>` blocks then cleared.
- `AIMessage` flushes any pending tool payloads as a synthetic `HumanMessage` turn before being emitted. The structured `tool_calls` field is preserved, and the calls are mirrored into `additional_kwargs["tool_calls"]` in OpenAI wire shape so providers that ignore the top-level field still see them (see Pass-2 details below). AIMessages empty on both text and tool-calls are dropped.
- `ToolMessage` branches on the `preserve_tool_role` flag:
  - **Default path** — buffer `(name, content)` for the next flush. The next `AIMessage` or `HumanMessage` pulls these into a `HumanMessage` body of `[tool: <name> result]\n<content>` blocks. This is what every NIM model in the stack was tested against.
  - **Mistral path** — emit the `ToolMessage` verbatim so the serializer renders `{role: "tool", tool_call_id: ..., content: ...}` and the strict function-call/response pairing invariant holds. Triggered by `_wants_tool_role(request)`, which returns `True` when `"mistral"` is a substring of the request's `model.model` / `model.model_name`.
- Unknown message types fall back to a `HumanMessage` carrying the stringified content.

**Pass 2** — merge consecutive same-role turns so the chat template sees strict alternation. `ToolMessage`s never merge (each carries a unique `tool_call_id` paired to a specific assistant call; merging would lose the pairing and the constructor would `KeyError` without a `tool_call_id`). Merged `AIMessage`s take the **union** of their `tool_calls`, and the union is re-mirrored into `additional_kwargs`.

**Pass 3** — drop any `AIMessage` empty on **both** axes (no text content, no tool_calls). These show up when a prior turn was short-circuited (`call_limit` / `loop_guard`) or returned `finish_reason=stop` with no body. Mistral Large 3 rejects them with `Invalid assistant message: content='' tool_calls=None`; other providers tolerate them silently, but the drop makes histories consistent across providers.

### The OpenAI tool-call mirror — `_to_openai_tool_calls`

`langchain_nvidia_ai_endpoints/_utils.py:convert_message_to_dict` forwards **only** `additional_kwargs["tool_calls"]` to the NIM wire and ignores the modern top-level `AIMessage.tool_calls`. We mirror calls into `additional_kwargs` in the OpenAI wire shape — `{id, type: "function", function: {name, arguments: json_str}}` — so the lib's serializer picks them up. Without this mirror, `tool_calls` vanish between the agent loop and the NIM endpoint and Mistral 400s with `content='' tool_calls=None`.

### Streaming-stub filter — `_valid_tool_calls`

The langchain-nvidia chunk parser occasionally emits a stub `tool_call` with `name=""` before the real name token arrives, then never backfills it. If forwarded to Mistral Large 3, the server-side parser collapses the entire `tool_calls` list to `None` and the assistant message is rejected. `_valid_tool_calls` drops any tool_call with empty / missing name from both the structured field and the OpenAI mirror.

### Response-side dedupe — `_dedupe_tool_calls`

Some models occasionally emit the same `(name, args)` tool_call twice within a single `AIMessage`. The dedupe collapses by `(name, json.dumps(args, sort_keys=True))`. If the response is an immutable Pydantic model, a fresh `AIMessage` is constructed with the same `id` and `additional_kwargs`.

## TOML config example

```toml
[agent.middleware]
# Keep this FIRST so downstream middlewares see the normalised history.
middleware = ["tool_message_normaliser", "tool_filter", "tool_call_announcer", ...]

[agent.middleware_config.tool_message_normaliser]
# (no knobs — provider detection is automatic via _wants_tool_role)
```

## See also

- [`loop_guard`](loop_guard.md), [`call_limit`](call_limit.md) — short-circuits that produce the empty-AIMessage shapes Pass 3 cleans up.
- [`../models/index.md`](../models/index.md) — provider plugins; the Mistral path is triggered by name substring on the bound model.
- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `Middleware` plugin protocol.
