# `openbb_agent_server.plugins.auth.openbb_workspace`

Gateway-trust backend for deployments fronted by the OpenBB Workspace. Authentication has already happened at the upstream proxy; the agent server simply trusts an `X-OpenBB-User` header containing the caller's email and hashes it through `hash_user_id` into a stable opaque `user_id`. The principal never carries the raw email as the `user_id` — only as `email` — so downstream persistence stays pseudonymous.

**Source:** [`openbb_agent_server/plugins/auth/openbb_workspace.py`](../../../../openbb_agent_server/plugins/auth/openbb_workspace.py)

## Classes

### `OpenBBWorkspaceAuthBackend`

Plugin entry-point name: `openbb_workspace`. Not selected by default.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `header` | `str` | `"X-OpenBB-User"` (module constant `DEFAULT_HEADER`) | Header to read the user identity from. Override only if you're fronting the server with a non-default gateway. |
| `scopes` | `tuple[str, ...]` | `("agent:query", "memory:read", "memory:write")` (`DEFAULT_SCOPES`) | Scopes attached to every authenticated principal. The Workspace deployment is the trusted boundary — finer-grained authz lives on the gateway. |
| `require_email` | `bool` | `True` | When `True`, the header value must satisfy `is_email(...)` (RFC-5321-ish check from `openbb_agent_server.runtime.identity`). When `False`, any non-empty string is accepted as an identifier and `email` on the principal is `None`. |

#### `async authenticate(request) -> UserPrincipal`

1. Reads `request.headers[self._header]`. Missing → `HTTPException(401, "missing required header X-OpenBB-User")`.
2. `cleaned = raw.strip().lower()`. Whitespace-only → `HTTPException(401, "empty X-OpenBB-User header")`.
3. If `require_email` and `not is_email(cleaned)` → `HTTPException(403, "X-OpenBB-User must be an email address; got a value that doesn't match RFC-5321")`.
4. `email = cleaned if is_email(cleaned) else None`.
5. `user_id = hash_user_id(cleaned)` — HMAC-SHA256 of the normalised value with a server-side pepper, hex-encoded with the runtime-identity prefix. This is the **only** identifier persisted; it is stable across requests but reveals nothing about the underlying email.
6. Returns `UserPrincipal(user_id=user_id, email=email, scopes=self._scopes)`. `display_name` is intentionally left unset — the gateway is expected to render that.

## TOML config example

```toml
[auth]
backend = "openbb_workspace"

[auth_config]
# Defaults are sensible — only set these if you've customised the gateway.
header = "X-OpenBB-User"
scopes = ["agent:query", "memory:read", "memory:write"]
require_email = true
```

## Security notes

- **Never expose the server directly to the internet with this backend.** Anyone who can set `X-OpenBB-User: foo@bar.com` becomes that user. Bind to `127.0.0.1` (or a private subnet) and front it with the Workspace gateway / a reverse proxy that strips client-supplied copies of the header and reinjects the verified one.
- The pepper used by `hash_user_id` is read from the environment by `openbb_agent_server.runtime.identity._pepper()`. If unset, a `WARNING` is logged and an empty pepper is used — rotating the pepper later orphans every persisted user.

## See also

- [`../../runtime/identity.md`](../../runtime/identity.md) — `hash_user_id` and `is_email`.
- [`../../runtime/principal.md`](../../runtime/principal.md) — the `UserPrincipal` model.
- [`../../../guides/workspace-integration.md`](../../../guides/workspace-integration.md) — wiring the agent server behind the Workspace gateway.
- [`../../../operating/auth.md`](../../../operating/auth.md) — choosing between gateway-trust and JWT verification.
- [`oidc_jwt`](oidc_jwt.md) — verify-at-the-server alternative.
