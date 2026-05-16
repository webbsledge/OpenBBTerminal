# `openbb_agent_server.observability.logging`

Trace-aware structured logging. Installs a JSON formatter on the root logger that surfaces the active run's `trace_id` / `run_id` / `conversation_id` / `user_id` on every record, redacts bearer tokens / API-key-shaped strings / emails before emit, and adds a `TRACE` log level for high-volume diagnostic events.

**Source:** [`openbb_agent_server/observability/logging.py`](../../../openbb_agent_server/observability/logging.py)

## Setup

### `def install_trace_logging(level=logging.INFO) -> None`

Attach `TraceContextFilter` + `PIIRedactionFilter` to the root logger and a `StreamHandler` formatted with `JsonTraceFormatter`. Idempotent — repeated calls are a no-op (the filter check on root short-circuits). Called once by `app/app.py` during server construction.

| Arg | Type | Default | Purpose |
| --- | --- | --- | --- |
| `level` | `int \| str` | `logging.INFO` | Root level. Pass `TRACE` (`5`) to surface the very-verbose plugin logs. |

## TRACE level

```python
TRACE = 5
trace(logger, message, *args, **kwargs)
```

`TRACE` (value `5`) is registered under the name `"TRACE"` on first import. `trace(logger, msg, …)` calls `logger.log(TRACE, msg, …)` after a level check; cheap when disabled.

Use TRACE for entries that would drown out INFO but matter when chasing a regression — per-event SSE adapter decisions, per-row widget-store writes, model-provider request bodies.

## Filters

### `class TraceContextFilter(logging.Filter)`

For every record, reads `runtime.context.current()`; on success, populates `record.trace_id` / `record.run_id` / `record.conversation_id` / `record.user_id` plus a combined `record.trace` dict. When no context is bound (server lifecycle messages, off-loop tasks), the fields are set to `""` / `{}` so the JSON formatter never blows up on a missing attribute.

### `class PIIRedactionFilter(logging.Filter)`

Redacts the record body AND every formatted argument (recursing into dicts / lists / tuples) before emit:

| Pattern | Replacement |
| --- | --- |
| Email addresses (via `runtime.identity.redact_email_in_text`) | The `hash_user_id` of the email — i.e. the same `sha256` hash that anonymises the principal. |
| `Bearer <token>` | `Bearer <redacted>` |
| `Authorization: <value>` / `authorization = <value>` (header style) | `<name>: <redacted>` |
| `api_key=…`, `x-api-key=…`, `sk-…`, `nvapi-…`, `grok-…`, `gsk_…`, `tvly-…` | `<redacted-key>` |

The redaction happens BEFORE format-string interpolation, so `logger.info("token=%s", secret)` is safe — the `secret` argument is redacted independently.

## Formatter

### `class JsonTraceFormatter(logging.Formatter)`

Emits one JSON line per record:

```json
{
  "level": "INFO",
  "logger": "openbb_agent_server.app.router",
  "message": "<redacted>",
  "trace": {
    "trace_id": "01HA…",
    "run_id":   "01HA…",
    "conversation_id": "conv_…",
    "user_id":  "<hash>"
  }
}
```

`exc_info` is rendered through the redaction pass before being added as `payload["exc_info"]` — so stack traces with API-key-shaped strings in them stay safe.

## `def redact_pii(text) -> str`

Standalone PII redactor — same pattern set as `PIIRedactionFilter`, callable from any code that needs to redact before non-logger output (audit dumps, error responses, etc.). Returns `text` unchanged for non-strings or empties.

## See also

- [`runtime/identity.md`](../runtime/identity.md) — the `hash_user_id` / email-redaction helpers.
- [`runtime/context.md`](../runtime/context.md) — the contextvar that feeds the filter.
- [`operating/observability.md`](../../operating/observability.md) — how to ship logs to an external collector.
