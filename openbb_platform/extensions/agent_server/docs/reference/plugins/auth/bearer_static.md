# `openbb_agent_server.plugins.auth.bearer_static`

Single shared bearer token, single-user. Compares the `Authorization: Bearer <token>` header against a configured secret using `hmac.compare_digest` (constant-time), and resolves the request to one fixed principal. Dev / test only — every authenticated caller becomes the same user.

**Source:** [`openbb_agent_server/plugins/auth/bearer_static.py`](../../../../openbb_agent_server/plugins/auth/bearer_static.py)

## Classes

### `BearerStaticAuthBackend`

Plugin entry-point name: `bearer_static`. Not selected by default — set `auth.backend = "bearer_static"` to use.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `token` | `str \| None` | unset | The shared secret. If not passed, falls back to the `OPENBB_AGENT_AUTH_BEARER` environment variable. If both are empty, `__init__` raises `RuntimeError` — the backend refuses to start without a token. |
| `user_id` | `str` | `"static-user"` | The `user_id` field of the issued `UserPrincipal`. |
| `display_name` | `str \| None` | unset | Forwarded to `UserPrincipal.display_name`. |
| `scopes` | `tuple[str, ...]` | `("agent:query", "memory:read", "memory:write")` | Scopes attached to the issued principal. Note this backend grants `memory:write` by default (unlike `none`). |

Logs a `WARNING` on construction:

> `BearerStaticAuthBackend resolves every authenticated request to user '<user_id>'. Use the api_key_table or oidc_jwt backend in production.`

#### `async authenticate(request) -> UserPrincipal`

- Reads `Authorization` (case-insensitive prefix `"bearer "`). Missing or non-bearer → `HTTPException(401, "missing bearer token")`.
- Strips the prefix and runs `hmac.compare_digest(supplied, self._token)`. Mismatch → `HTTPException(403, "invalid bearer token")`.
- On match, returns a `UserPrincipal` populated from the constructor kwargs above.

## TOML config example

```toml
[auth]
backend = "bearer_static"

[auth_config]
token = "${OPENBB_AGENT_AUTH_BEARER}"  # or set via env directly
user_id = "dev"
display_name = "Local Dev"
scopes = ["agent:query", "memory:read", "memory:write"]
```

## See also

- [`../../../operating/auth.md`](../../../operating/auth.md) — when to use this vs. `api_key_table` / `oidc_jwt`.
- [`../../../developing/writing-an-auth-backend.md`](../../../developing/writing-an-auth-backend.md) — the `AuthBackend` plugin protocol.
- [`api_key_table`](api_key_table.md) — the next step up for multi-user deployments.
- [`none`](none.md) — even-less-gated dev backend.
