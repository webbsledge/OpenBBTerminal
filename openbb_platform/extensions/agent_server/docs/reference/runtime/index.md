# `openbb_agent_server.runtime`

The runtime sub-package wires per-request identity, the plugin ABCs and registry, the agent builder, and the per-run scratch state (background jobs, widget store, identity helpers, services).

**Source:** [`openbb_agent_server/runtime/__init__.py`](../../../openbb_agent_server/runtime/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`principal.md`](principal.md) | `UserPrincipal` — the resolved request identity. Fields, scopes, `has_scope()`. |
| [`context.md`](context.md) | `RunContext`, `WidgetRef`, `FileRef` + the contextvars (`current`, `runtime_state`, `bind`). |
| [`identity.md`](identity.md) | `hash_user_id` / email redaction helpers used by auth backends and the logging filter. |
| [`plugins.md`](plugins.md) | The plugin ABCs / Protocols — `AuthBackend`, `ModelProvider`, `ToolSource`, `SubAgentSpec`, `Middleware`, `CheckpointerProvider`. |
| [`registry.md`](registry.md) | `load(group, name, config)` + `available(group)`. Entry-point lookup; no caching; unknown-key drop with WARNING. |
| [`builder.md`](builder.md) | `build_agent_for_run` — constructs the LangGraph agent for one run from the resolved profile. |
| [`emit.md`](emit.md) | Plugin-side SSE helpers (`reasoning_step`, `*_artifact`, `cite`, `function_call`). |
| [`jobs.md`](jobs.md) | `JobRegistry` — per-run background tasks. |
| [`widget_store.md`](widget_store.md) | `WidgetDataStore` — persistent widget rows + ANN row search. |
| [`pdf_store.md`](pdf_store.md) | Per-PDF ingestion / page cache. |
| [`services.md`](services.md) | `RuntimeServices` — the bundle of collaborators threaded through the router. |

## Per-run lifecycle

1. **Authenticate.** `auth.authenticate(request) -> UserPrincipal`.
2. **Build context.** Router constructs `RunContext(principal, trace_id, run_id, conversation_id, widgets, uploaded_files, …)`.
3. **Bind.** `with context.bind(ctx):` exposes the context + a fresh `runtime_state()` scratch dict for the rest of the call.
4. **Build agent.** `builder.build_agent_for_run(ctx, profile, services)` resolves model, tool sources, sub-agents, middleware, and a system prompt.
5. **Stream.** The LangGraph stream is translated by `DeepAgentEventAdapter` and emitted as SSE.
6. **Cleanup.** `context.bind`'s `finally` calls `jobs.cleanup_state(state)` to cancel any orphaned background tasks.

## Entry-point groups

| Group | ABC / Protocol |
| --- | --- |
| `openbb_agent_server.auth` | `AuthBackend` |
| `openbb_agent_server.models` | `ModelProvider` |
| `openbb_agent_server.tools` | `ToolSource` |
| `openbb_agent_server.subagents` | `SubAgentSpec` (Protocol) |
| `openbb_agent_server.middleware` | `Middleware` |
| `openbb_agent_server.checkpointers` | `CheckpointerProvider` |

See [`plugins.md`](plugins.md) for the contracts and [`registry.md`](registry.md) for the loader.

## See also

- [`developing/plugin-system.md`](../../developing/plugin-system.md) — how to register a plugin against any of the runtime ABCs.
- [`developing/conventions.md`](../../developing/conventions.md) — coding conventions for runtime code.
- [`guides/architecture.md`](../../guides/architecture.md) — end-to-end picture.
