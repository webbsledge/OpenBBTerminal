# `openbb_agent_server.runtime.principal`

`UserPrincipal` — the resolved identity attached to every authenticated request. Produced by the active `AuthBackend` plugin and carried on `RunContext.principal` for the lifetime of the run.

**Source:** [`openbb_agent_server/runtime/principal.py`](../../../openbb_agent_server/runtime/principal.py)

## `class UserPrincipal(BaseModel)`

Frozen Pydantic model, `extra="forbid"`. Plugins receive principals by reference and must not mutate them — copy + `model_copy(update=...)` if you need to override a field.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `user_id` | `str` | required (min length 1) | Stable partition key. Every persistent row in the system is keyed on this. For OIDC backends it's the `sub` claim; for `api_key_table` it's the user-id stamped on the issued key; for `none` it's a hash of the calling host. |
| `display_name` | `str \| None` | `None` | Human-readable name (forwarded to `users.display_name` by `upsert_user`). |
| `email` | `str \| None` | `None` | Email address if the backend has one. Treated as PII — redacted by the logging filter (replaced with `hash_user_id`). |
| `scopes` | `tuple[str, ...]` | `()` | Granted scope slugs. Used to gate the `/v1/memory/*` endpoints, the post-turn memory writer, and any per-route gates added by plugins. |
| `raw_claims` | `dict[str, Any]` | `{}` | Backend-defined extras (OIDC claim dict, decoded JWT payload, etc.). Plugins read this for backend-specific extensions; the core server doesn't touch it. |

## Methods

| Method | Purpose |
| --- | --- |
| `has_scope(scope: str) -> bool` | Exact-match scope check. Returns `True` iff `scope` is in `self.scopes`. No wildcard support — scopes are flat slugs. |

## Standard scopes

| Scope | Granted by | Used at |
| --- | --- | --- |
| `agent:query` | Required for `/v1/query`, `/v1/conversations/*`, `/v1/traces/*`, `/v1/usage`. | [`app/router.md`](../app/router.md). |
| `memory:read` | `/v1/memory` GET. | [`app/router.md`](../app/router.md). |
| `memory:write` | `MemoryStore.write`, the post-turn `memory_writer` middleware, `ingest_request_context`. | [`memory/store.md`](../memory/store.md), [`memory/writer.md`](../memory/writer.md). |

Plugins can introduce new scopes — there is no central registry. Issue them via `keys issue --scope <slug>` (for the `api_key_table` backend) or have your custom `AuthBackend` populate `principal.scopes` directly.

## See also

- [`runtime/context.md`](context.md) — `RunContext` that carries the principal through the run.
- [`runtime/identity.md`](identity.md) — `hash_user_id` and email-redaction helpers.
- [`plugins/auth/index.md`](../plugins/auth/index.md) — auth backend implementations.
- [`main.md`](../main.md) — `keys issue --scope` for API-key backends.
