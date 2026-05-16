"""Layered TOML config bootstrap for ``openbb-agent-server``."""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any

logger = logging.getLogger("openbb_agent_server.config")

#: ``$VAR`` / ``${VAR}`` reference pattern. ``$`` not followed by a
#: valid identifier (e.g. ``$5``, ``$ ``, ``$@``) is left untouched.
_ENV_REF_PATTERN = re.compile(
    r"\$\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}"
    r"|\$(?P<bare>[A-Za-z_][A-Za-z0-9_]*)"
)

EXPLICIT_CONFIG_ENVS: tuple[str, ...] = (
    "OPENBB_AGENT_CONFIG",
    "OPENBB_API_CONFIG",
    "OPENBB_CONFIG",
)

#: CLI flag that supplies an explicit config path. ``main.py`` sniffs
#: this out of argv before any heavy import runs.
CONFIG_FILE_FLAG = "--config-file"


def extract_config_file_from_argv(argv: list[str] | None = None) -> str | None:
    """Sniff ``--config-file <path>`` out of argv WITHOUT importing args."""
    args = (argv if argv is not None else sys.argv[1:]).copy()
    for i, arg in enumerate(args):
        if arg == CONFIG_FILE_FLAG and i + 1 < len(args):
            value = args[i + 1]
            if value and not value.startswith("--"):
                return value
        elif arg.startswith(f"{CONFIG_FILE_FLAG}="):
            value = arg.split("=", 1)[1]
            if value:
                return value
    return None


def explicit_config_path(argv: list[str] | None = None) -> str | None:
    """Return the explicit config path from argv or env (in priority order)."""
    cli = extract_config_file_from_argv(argv)
    if cli:
        return cli
    for var in EXPLICIT_CONFIG_ENVS:
        v = os.environ.get(var)
        if v:
            return v
    return None


def _validate_explicit_toml(explicit_path: str) -> None:
    from pathlib import Path

    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover — 3.10 backport path
        import tomli as tomllib  # ty: ignore[unresolved-import]

    p = Path(explicit_path)
    if not p.is_file():
        return
    try:
        with p.open("rb") as fh:
            tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(
            f"Malformed TOML at explicit config path '{explicit_path}': {exc}"
        ) from exc


def expand_env_refs(
    value: str, env: Mapping[str, str] | None = None
) -> tuple[str, list[str]]:
    """Substitute ``$VAR`` / ``${VAR}`` references against ``env``."""
    target_env = env if env is not None else os.environ
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group("braced") or match.group("bare")
        if name and name in target_env:
            return target_env[name]
        if name and name not in missing:
            missing.append(name)
        return match.group(0)

    return _ENV_REF_PATTERN.sub(replace, value), missing


USER_SETTINGS_PATH = "~/.openbb_platform/user_settings.json"


def apply_user_settings_credentials(
    *,
    settings_path: str = USER_SETTINGS_PATH,
    env: MutableMapping[str, str] | None = None,
) -> list[str]:
    """Push ``credentials`` from ``user_settings.json`` into ``os.environ``."""
    import json
    from pathlib import Path

    target: MutableMapping[str, str] = env if env is not None else os.environ
    path = Path(settings_path).expanduser()
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "Could not parse %s — skipping credential injection: %s",
            path,
            exc,
        )
        return []
    creds = data.get("credentials")
    if not isinstance(creds, dict):
        return []

    applied: list[str] = []
    for raw_key, raw_value in creds.items():
        if not isinstance(raw_key, str) or not isinstance(raw_value, str):
            continue
        if not raw_value:
            continue
        name = raw_key.upper()
        if name in target:
            continue
        target[name] = raw_value
        applied.append(name)
    if applied:
        logger.debug(
            "user_settings.json: applied %d env var(s) from credentials: %s",
            len(applied),
            ", ".join(sorted(applied)),
        )
    return applied


def apply_launcher_env(
    env_section: dict[str, Any] | None,
    *,
    env: MutableMapping[str, str] | None = None,
) -> list[str]:
    """Push ``[env]`` table entries into ``os.environ`` (no clobber)."""
    if not env_section:
        return []
    target: MutableMapping[str, str] = env if env is not None else os.environ
    applied: list[str] = []
    for key, value in env_section.items():
        if not isinstance(key, str):
            continue
        if key in target:
            continue
        expanded, missing = expand_env_refs(str(value), target)
        if missing:
            logger.warning(
                "Skipping [env] entry %s: references unresolved variable(s) %s",
                key,
                ", ".join(missing),
            )
            continue
        target[key] = expanded
        applied.append(key)
    return applied


def expand_in_dict(
    value: Any,
    *,
    env: Mapping[str, str] | None = None,
) -> Any:
    """Recursively expand ``$VAR`` / ``${VAR}`` in every string in ``value``."""
    if isinstance(value, str):
        expanded, _ = expand_env_refs(value, env)
        return expanded
    if isinstance(value, dict):
        return {k: expand_in_dict(v, env=env) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_in_dict(v, env=env) for v in value]
    if isinstance(value, tuple):
        return tuple(expand_in_dict(v, env=env) for v in value)
    return value


def load_launcher_config(
    explicit_path: str | None = None,
    *,
    apply_to_services: bool = True,
    apply_to_env: bool = True,
) -> dict[str, Any]:
    """Run the layered TOML cascade and return the merged config dict."""
    from openbb_core.app.config.loader import load_layered_config

    if explicit_path:
        _validate_explicit_toml(explicit_path)

    return load_layered_config(
        explicit_path=explicit_path,
        apply_to_services=apply_to_services,
        apply_to_env=apply_to_env,
    )


def merge_launcher_kwargs(
    cli_kwargs: dict[str, Any],
    launcher_section: dict[str, Any] | None,
) -> dict[str, Any]:
    """Overlay ``[agent]`` section under CLI kwargs (CLI wins)."""
    if not launcher_section:
        return cli_kwargs
    merged = dict(launcher_section)
    merged.update(cli_kwargs)
    return merged


def bootstrap_launcher_config(
    explicit_path: str | None = None,
    *,
    argv: list[str] | None = None,
) -> dict[str, Any]:
    """Two-phase bootstrap entry point. Run BEFORE any heavy import."""
    if explicit_path is None:
        explicit_path = explicit_config_path(argv)
    cfg = load_launcher_config(explicit_path)
    apply_user_settings_credentials()
    apply_launcher_env(cfg.get("env"))
    # Expand ``${VAR}`` references inside non-env tables too — by now
    # the [env] keys are merged into os.environ so references resolve.
    cfg = expand_in_dict(cfg)
    return cfg


def agent_section(cfg: dict[str, Any]) -> dict[str, Any]:
    """Pull the ``[agent]`` sub-table out of the merged config."""
    section = cfg.get("agent")
    if not isinstance(section, dict):
        return {}
    return dict(section)


def load_preset(preset: str) -> dict[str, Any]:
    """Parse one of the bundled preset TOMLs into a config dict."""
    from importlib import resources

    # Local import keeps the symbol public at module scope without
    # circularity vs. ``main`` (which imports from this module).
    from openbb_agent_server.main import _PRESETS  # noqa: PLC0415

    resource = _PRESETS.get(preset)
    if resource is None:
        choices = ", ".join(sorted(_PRESETS.keys()))
        raise ValueError(f"unknown preset {preset!r}; choose from: {choices}")

    try:
        import tomllib
    except ImportError:  # pragma: no cover — py3.10
        import tomli as tomllib  # ty: ignore[unresolved-import]

    body = (
        resources.files("openbb_agent_server")
        .joinpath(resource)
        .read_text(encoding="utf-8")
    )
    cfg = tomllib.loads(body)
    # Same env / expansion contract as the file loader path.
    apply_launcher_env(cfg)
    cfg = expand_in_dict(cfg)
    return cfg
