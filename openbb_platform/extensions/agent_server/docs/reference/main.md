# `openbb_agent_server.main`

The `openbb-agent-server` CLI entry point. Top-level dispatch sniffs `--config-file` / `--generate-config` out of argv BEFORE argparse runs so the layered TOML bootstrap (see [`app/config.md`](app/config.md)) can populate `os.environ` ahead of any heavy import.

**Source:** [`openbb_agent_server/main.py`](../../openbb_agent_server/main.py)

## Top-level usage

```
openbb-agent-server [--config-file PATH] [--generate-config [PATH]] [--preset NAME] [serve [...] | keys [...]]
```

| Flag | Purpose |
| --- | --- |
| `--config-file <path>` | Explicit `openbb.toml` path. Sniffed out of argv before argparse runs. Env fallbacks (in priority): `OPENBB_AGENT_CONFIG`, `OPENBB_API_CONFIG`, `OPENBB_CONFIG`. |
| `--generate-config [PATH]` | Write the bundled template to `PATH` and exit. With no `PATH` (or `-`), prints to stdout. Refuses to overwrite existing files. |
| `--preset <name>` | Which template `--generate-config` writes. Default `default` (the end-to-end NVIDIA NIM stack). Choices come from `_PRESETS`. |

The default subcommand is `serve` — bare `openbb-agent-server` runs the server with `[agent]` from the resolved config.

## `serve` subcommand

```
openbb-agent-server serve
  [--host HOST] [--port PORT] [--auth BACKEND]
  [--model-provider PROVIDER] [--model-name NAME]
  [--reload] [--log-level LEVEL]
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--host` | `settings.host` | Bind host (overrides `[agent.host]`). |
| `--port` | `settings.port` | Bind port. |
| `--auth` | `settings.auth_backend` | Auth backend plugin name. Also stamped into `OPENBB_AGENT_AUTH_BACKEND` so children inherit. |
| `--model-provider` | `settings.model_provider` | Model provider plugin name. Stamped into `OPENBB_AGENT_MODEL_PROVIDER`. |
| `--model-name` | `settings.model_name` | Concrete model id. Stamped into `OPENBB_AGENT_MODEL_NAME`. |
| `--reload` | `False` | Pass `reload=True, factory=True` to uvicorn for dev hot-reload. Stamps `OPENBB_AGENT_BOOTSTRAP_TOML` so workers re-run the bootstrap. |
| `--log-level` | `"info"` | Passed to `logging.basicConfig` and uvicorn. |

CLI flags ALWAYS win over TOML. The merge happens in `merge_launcher_kwargs(cli_kwargs, agent_cfg)`. `AgentServerSettings.from_toml(agent_cfg)` then resolves env-vs-TOML (env wins for any `OPENBB_AGENT_<KEY>` that is set).

## `keys` subcommand

Manage `api_key_table` API keys. Requires `auth_backend = "api_key_table"` (the keys live in the same SQLAlchemy DB as the rest of the persistence layer).

### `keys issue`

```
openbb-agent-server keys issue --user-id USER [--scope SCOPE [--scope SCOPE …]]
                               [--label LABEL] [--display-name NAME]
                               [--email EMAIL] [--json]
```

| Flag | Purpose |
| --- | --- |
| `--user-id` | Required. The `user_id` to bind the new key to. |
| `--scope` | Repeatable. Granted scopes. Defaults to `agent:query`, `memory:read`. |
| `--label` | Operator-facing label (free text). |
| `--display-name` | Display name stamped onto the `users` row. |
| `--email` | Email (PII — redacted by the logging filter). |
| `--json` | Print one-line JSON instead of the human-readable format. |

The plaintext key is printed ONCE. There is no recovery — store it immediately.

### `keys revoke`

```
openbb-agent-server keys revoke --key-id <key_id>
```

Marks the key as revoked (sets `revoked_at = now()`). The key is not deleted, so audit logs that reference it still resolve. Exits 1 if the key doesn't exist.

### `keys list`

```
openbb-agent-server keys list [--user-id USER] [--json]
```

List API keys (NEVER prints the plaintext secret — only `key_id`, owner, state, scopes, label). Optional `--user-id` filter.

## Functions

### `def main(argv=None) -> None`

CLI dispatch. Reads argv, runs `bootstrap_launcher_config` BEFORE argparse to feed the env / user-settings cascade, then dispatches to `_serve` or `_keys` based on the subcommand.

### `def _serve(*, args, agent_cfg, explicit_path) -> None`

Internal. Constructs `AgentServerSettings.from_toml(agent_cfg)`, decides between the reload (factory) and direct uvicorn invocation paths.

### `def _keys(*, args, agent_cfg) -> None`

Internal. Resolves the DB URL from `settings.auth_config["db_url"]` or `settings.resolved_db_url()`, instantiates `ApiKeyTableAuthBackend`, and dispatches the sub-subcommand.

### `def _generate_config(target, preset="default") -> None`

Internal. Reads the bundled preset (via `importlib.resources`) and writes it to `target` (or stdout when `target == "-"`). Refuses to overwrite existing files.

## See also

- [`app/config.md`](app/config.md) — the TOML / env cascade `main` drives.
- [`app/settings.md`](app/settings.md) — the model `from_toml` populates.
- [`operating/configuration.md`](../operating/configuration.md) — operator's guide.
- [`operating/auth.md`](../operating/auth.md) — auth backend overview and `keys` workflow.
