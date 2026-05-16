# `openbb_agent_server.plugins.auth.none`

No-auth single-user backend. Every request resolves to the same shared `anonymous` principal. Logs a `WARNING` on construction so a production deployment running on the default `auth_backend = "none"` is loud about it. This backend is the default in `AgentServerSettings`.

**Source:** [`openbb_agent_server/plugins/auth/none.py`](../../../../openbb_agent_server/plugins/auth/none.py)

## Classes

### `NoneAuthBackend`

Plugin entry-point name: `none`. **Default** value of `AgentServerSettings.auth_backend` — selected automatically when no auth backend is configured.

`__init__(**_config)` accepts and discards arbitrary kwargs so a stray `[auth_config]` block in TOML doesn't break startup. Emits a `WARNING` log line on every construction:

> `NoneAuthBackend is enabled — every request resolves to the single shared user 'anonymous'. Do not use in production.`

`async authenticate(request) -> UserPrincipal` — returns a fixed principal regardless of headers:

| Field | Value |
| --- | --- |
| `user_id` | `"anonymous"` (the module constant `ANONYMOUS_USER_ID`) |
| `display_name` | `"Anonymous"` |
| `scopes` | `("agent:query", "memory:read")` |
| `email` | unset |

The scopes are deliberately read-only on memory: an anonymous principal in shared-process mode can issue queries and read recall hits but can't write new memories — that requires `memory:write`, which only the bearer / table / OIDC backends grant.

## TOML config example

```toml
[auth]
backend = "none"
# No config block needed; any [auth_config] keys are ignored.
```

## See also

- [`../../../operating/auth.md`](../../../operating/auth.md) — production auth choices and migration off the `none` backend.
- [`../../../developing/writing-an-auth-backend.md`](../../../developing/writing-an-auth-backend.md) — the `AuthBackend` plugin protocol.
- [`../../runtime/principal.md`](../../runtime/principal.md) — the `UserPrincipal` model returned by every backend.
- [`bearer_static`](bearer_static.md) — the next step up: still single-user, but at least gated by a shared secret.
