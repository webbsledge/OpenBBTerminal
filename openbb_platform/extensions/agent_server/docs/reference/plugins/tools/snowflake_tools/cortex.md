# `openbb_agent_server.plugins.tools.snowflake_tools.cortex`

Snowflake Cortex helpers. Two transport layers in one module:

- **SQL functions** — `cortex_complete`, `cortex_summarize`, `cortex_sentiment`, `cortex_translate`, `cortex_classify_text`, `cortex_extract_answer`, `cortex_embed`. Each runs a single `SELECT SNOWFLAKE.CORTEX.<FN>(...)` through the supplied `SnowflakeClient`.
- **REST endpoints** — `cortex_search` and `cortex_analyst`. These need OAuth, programmatic-access-token, or KeyPair JWT auth; the helper resolves headers via `auth_headers(creds)`.

Not a tool source — the wrappers are called by [`snowflake_tools.source`](source.md) which registers them as LangChain tools.

**Source:** [`openbb_agent_server/plugins/tools/snowflake_tools/cortex.py`](../../../../../openbb_agent_server/plugins/tools/snowflake_tools/cortex.py)

## SQL helpers

| Function | SQL | Returns |
| --- | --- | --- |
| `cortex_complete(client, *, prompt, model="claude-3-5-sonnet", options=None)` | `SELECT SNOWFLAKE.CORTEX.COMPLETE(%(model)s, %(prompt)s [, PARSE_JSON(%(options)s)])` | `str` — the model's chat completion. |
| `cortex_summarize(client, *, text)` | `SELECT SNOWFLAKE.CORTEX.SUMMARIZE(%(text)s)` | `str` — the summary. |
| `cortex_sentiment(client, *, text)` | `SELECT SNOWFLAKE.CORTEX.SENTIMENT(%(text)s)` | `float` (cast from the scalar — -1..+1). |
| `cortex_translate(client, *, text, target_language, source_language="")` | `SELECT SNOWFLAKE.CORTEX.TRANSLATE(%(t)s, %(src)s, %(dst)s)` | `str` — translated text. |
| `cortex_classify_text(client, *, text, categories)` | `SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(%(t)s, %(c)s)` | `dict` — `_parse_json` of the scalar. |
| `cortex_extract_answer(client, *, question, context)` | `SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(%(q)s, %(c)s)` | `dict` — `_parse_json` of the scalar. |
| `cortex_embed(client, *, text, model="snowflake-arctic-embed-l-v2.0", dim=1024)` | `SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_<dim>(%(model)s, %(text)s)` | `list[float]` vector. `dim == 1024` picks `EMBED_TEXT_1024`; everything else picks `EMBED_TEXT_768`. |

All helpers go through `_scalar(result)` which extracts `result.rows[0][0]`; `RuntimeError("cortex call returned no rows")` if empty.

`_parse_json` tolerates: pre-parsed dict / list (passthrough), JSON string (json.loads), or raw string (returned unchanged on decode failure).

## REST helpers

### `cortex_search(creds, *, database, schema, service, query, columns=None, limit=10, filter_=None, client=None) -> dict[str, Any]`

POSTs `{query, limit, [columns], [filter]}` to:

```
<account_host>/api/v2/databases/<database>/schemas/<schema>/cortex-search-services/<service>:query
```

with `auth_headers(creds)` plus `Content-Type: application/json`. Returns the parsed JSON response. The `client` kwarg lets tests inject a mocked `httpx.Client`; left unset, a fresh `httpx.Client(timeout=60.0)` is opened and closed inside the function.

### `cortex_analyst(creds, *, messages, semantic_model=None, semantic_view=None, stream=False, client=None) -> dict[str, Any]`

POSTs `{messages, stream, [semantic_model_file], [semantic_view]}` to:

```
<account_host>/api/v2/cortex/analyst/message
```

Requires one of `semantic_model` (stage path to a YAML model file) or `semantic_view` (fully-qualified view name); otherwise raises `RuntimeError("Cortex Analyst requires semantic_model or semantic_view")`.

### Auth + account host

`_account_host(creds)` returns either `creds.host.rstrip("/")` or `https://<account>[.<region>].snowflakecomputing.com`. Raises if neither is set.

`auth_headers(creds)` returns:

| Auth mode | Headers |
| --- | --- |
| OAuth (`authenticator == "oauth"`) | `Authorization: Bearer <creds.token>`, `X-Snowflake-Authorization-Token-Type: OAUTH`. |
| Programmatic Access Token (`authenticator == "programmatic_access_token"`) | `Authorization: Bearer <creds.token>`, `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`. |
| KeyPair (`private_key` present) | `Authorization: Bearer <keypair_jwt(creds)>`, `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`. |
| anything else | `RuntimeError` — REST needs OAuth, PAT, or KeyPair JWT. |

### `keypair_jwt(creds, *, lifetime_seconds=3600) -> str`

Mint a Snowflake-compatible KeyPair JWT. Requires `account`, `user`, and `private_key`. Loads the PEM via `cryptography.hazmat.primitives.serialization`, derives the SHA-256 fingerprint of the public key in DER `SubjectPublicKeyInfo` format (formatted as `SHA256:<base64>`), and signs `{iss: "<ACCOUNT>.<USER>.<fingerprint>", sub: "<ACCOUNT>.<USER>", iat, exp}` with `RS256` via `pyjwt`.

## Related

- [`snowflake_tools.source`](source.md) — registers all of the above as `StructuredTool`s.
- [`snowflake_tools.client`](client.md) — the `SnowflakeClient` / `SnowflakeCredentials` types.
- [`snowflake_tools.safety`](safety.md) — read-only enforcement.
