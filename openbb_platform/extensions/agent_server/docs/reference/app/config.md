# `openbb_agent_server.app.config`

Layered TOML config bootstrap for the agent server. Runs BEFORE any heavy import so the cascade can set `os.environ` values that downstream modules (auth, models, NIM clients) read at module-import time.

**Source:** [`openbb_agent_server/app/config.py`](../../../openbb_agent_server/app/config.py)

## Cascade

The merged config is built by `bootstrap_launcher_config(explicit_path, argv)` in this order — later layers win:

1. **Built-in defaults.** `[agent]` defaults from `AgentServerSettings`.
2. **User-global TOML.** `~/.openbb_platform/openbb.toml` (via `openbb_core.app.config.loader`).
3. **Project TOML.** `pyproject.toml` `[tool.openbb]` table OR the project's `openbb.toml`.
4. **Explicit TOML.** The path passed via `--config-file <path>` (sniffed out of argv WITHOUT triggering argparse) or one of `OPENBB_AGENT_CONFIG` / `OPENBB_API_CONFIG` / `OPENBB_CONFIG` env vars. First match wins.
5. **`[env]` table.** Pushed into `os.environ` without clobber (existing vars win).
6. **`user_settings.json` credentials.** `~/.openbb_platform/user_settings.json` → `credentials` dict pushed into `os.environ` (UPPERCASE-keyed, no clobber).
7. **`${VAR}` expansion.** Every string in the merged config is recursively expanded against `os.environ` after the env table has been applied — so cross-references between `[env]` and `[agent.*]` resolve.

## Functions

### `def extract_config_file_from_argv(argv=None) -> str | None`

Sniff `--config-file <path>` or `--config-file=<path>` out of `argv` without importing argparse. Returns `None` if not present.

### `def explicit_config_path(argv=None) -> str | None`

Same as above, but falls back to the `OPENBB_AGENT_CONFIG` / `OPENBB_API_CONFIG` / `OPENBB_CONFIG` env vars in priority order.

### `def expand_env_refs(value, env=None) -> tuple[str, list[str]]`

Substitute `$VAR` / `${VAR}` references against `env` (defaults to `os.environ`). Returns `(expanded_string, list_of_missing_var_names)`. Bare `$` not followed by a valid identifier (`$5`, `$ `, `$@`) is left untouched.

### `def apply_user_settings_credentials(*, settings_path=USER_SETTINGS_PATH, env=None) -> list[str]`

Read the `credentials` dict from `~/.openbb_platform/user_settings.json` and push each entry into `os.environ` as `KEY_UPPERCASE = value` — but only if the env var is not already set. Returns the list of applied keys. Used so users can manage API keys in one canonical location and have them flow into every NIM / OpenAI / Anthropic client automatically.

### `def apply_launcher_env(env_section, *, env=None) -> list[str]`

Push entries from the `[env]` table into `os.environ` with the same no-clobber policy. `${VAR}` references inside values are expanded; entries with unresolved references are SKIPPED and logged at WARNING.

### `def expand_in_dict(value, *, env=None) -> Any`

Recursively walk a nested `dict` / `list` / `tuple` / `str` structure, expanding `$VAR` / `${VAR}` in every string against `env`. Non-string scalars pass through unchanged.

### `def load_launcher_config(explicit_path=None, *, apply_to_services=True, apply_to_env=True) -> dict[str, Any]`

Delegate to `openbb_core.app.config.loader.load_layered_config`. Validates that an explicit path parses as TOML before handing it to the loader (so a malformed config file fails loudly at startup rather than silently dropping settings).

### `def merge_launcher_kwargs(cli_kwargs, launcher_section) -> dict[str, Any]`

Overlay `[agent]` under `cli_kwargs` so CLI flags take precedence over the TOML. Used by `main._serve` to pick `host` / `port` / etc.

### `def bootstrap_launcher_config(explicit_path=None, *, argv=None) -> dict[str, Any]`

Two-phase bootstrap. Phase 1: resolve the explicit path. Phase 2: load + apply user-settings + apply `[env]` + expand. Returns the merged config dict. MUST run before any module that reads env vars at import time.

### `def agent_section(cfg) -> dict[str, Any]`

Pull the `[agent]` sub-table out of the merged config. Returns `{}` if missing.

### `def load_preset(preset) -> dict[str, Any]`

Parse one of the bundled preset TOMLs (`_PRESETS` from `main.py`) into a config dict. Applies the `[env]` table and runs `${VAR}` expansion before returning — same contract as the file loader.

## Constants

| Name | Value | Purpose |
| --- | --- | --- |
| `EXPLICIT_CONFIG_ENVS` | `("OPENBB_AGENT_CONFIG", "OPENBB_API_CONFIG", "OPENBB_CONFIG")` | Env-var fallbacks for the explicit config path, in priority order. |
| `CONFIG_FILE_FLAG` | `"--config-file"` | CLI flag name (sniffed by `main.py` before argparse runs). |
| `USER_SETTINGS_PATH` | `"~/.openbb_platform/user_settings.json"` | Canonical credentials file. |

## See also

- [`app/settings.md`](settings.md) — the Pydantic model populated from the merged config.
- [`main.md`](../main.md) — CLI entry point that drives the bootstrap.
- [`operating/configuration.md`](../../operating/configuration.md) — operator's guide.
