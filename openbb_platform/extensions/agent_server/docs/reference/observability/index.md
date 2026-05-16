# `openbb_agent_server.observability`

Server-side observability: per-run trace context, structured JSON logging, PII redaction. Token usage and tool-call audit live in [`persistence/`](../persistence/index.md) (under the `usage` and `tool_calls` tables) rather than here — this package is only about the on-host logging story.

**Source:** [`openbb_agent_server/observability/__init__.py`](../../../openbb_agent_server/observability/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`logging.md`](logging.md) | `install_trace_logging` — attaches trace-context + PII-redaction filters and a JSON formatter to the root logger. Defines the `TRACE` level (5) and the `redact_pii` helper. |

## Observability outside this package

| Concern | Lives in |
| --- | --- |
| Per-run trace bundle (messages + tool_calls + usage + artifacts + citations) | [`persistence/store.md`](../persistence/store.md) — `HistoryStore.get_trace_bundle`, exposed via `GET /v1/traces/{trace_id}`. |
| Aggregated token usage | `HistoryStore.usage_summary` — exposed via `GET /v1/usage`. |
| Per-record `trace_id` / `run_id` / `user_id` injection | This package's `TraceContextFilter`. |
| The `X-Server-Trace-ID` / `X-Trace-ID` HTTP response headers | [`app/router.md`](../app/router.md). |
| Per-event SSE adapter audit | [`protocol/adapter.md`](../protocol/adapter.md) — buffers reasoning + artifacts + citations for end-of-run flush. |
| Per-run background-task accounting | [`runtime/jobs.md`](../runtime/jobs.md) — `JobRegistry.list_all()` / `status()` snapshots. |

## Log levels

| Level | Numeric | Use for |
| --- | --- | --- |
| `TRACE` | 5 | Per-event adapter decisions, per-row widget-store writes, model-provider request bodies. Off by default. |
| `DEBUG` | 10 | Per-run lifecycle events, retry decisions, plugin construction. |
| `INFO` | 20 | One-line summary of every request / run / artifact / cancellation. The default operational level. |
| `WARNING` | 30 | Recoverable failures (translation fell back, ANN search degraded to substring). |
| `ERROR` | 40 | Failed runs, unhandled tool exceptions. |

## Redaction summary

| Concern | Replacement |
| --- | --- |
| Email addresses | `hash_user_id(email)` — the same sha256 hash that anonymises the principal. |
| `Bearer <token>` | `Bearer <redacted>`. |
| `Authorization:` / `authorization=` headers | `<name>: <redacted>`. |
| `api_key=`, `x-api-key=`, `sk-…`, `nvapi-…`, `grok-…`, `gsk_…`, `tvly-…` | `<redacted-key>`. |

Redaction runs both as a logging filter (catches every record) and as the standalone `redact_pii(text)` helper (callable from any code that emits to non-logger output).

## See also

- [`operating/observability.md`](../../operating/observability.md) — operator's guide.
- [`developing/conventions.md`](../../developing/conventions.md) — when to use `trace()` vs. `debug()` vs. `info()`.
- [`runtime/identity.md`](../runtime/identity.md) — `hash_user_id` and email redaction.
