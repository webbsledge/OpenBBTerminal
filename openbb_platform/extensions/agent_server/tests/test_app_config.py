"""Tests for the layered TOML config bootstrap."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from openbb_agent_server.app import config as cfg_mod
from openbb_agent_server.app.config import (
    EXPLICIT_CONFIG_ENVS,
    agent_section,
    apply_launcher_env,
    bootstrap_launcher_config,
    expand_env_refs,
    expand_in_dict,
    explicit_config_path,
    extract_config_file_from_argv,
    merge_launcher_kwargs,
)
from openbb_agent_server.app.settings import AgentServerSettings


def test_extract_config_file_from_argv_space_separated() -> None:
    out = extract_config_file_from_argv(["--port", "8000", "--config-file", "/x.toml"])
    assert out == "/x.toml"


def test_extract_config_file_from_argv_equals_form() -> None:
    out = extract_config_file_from_argv(["--config-file=/y.toml"])
    assert out == "/y.toml"


def test_extract_config_file_from_argv_missing_returns_none() -> None:
    assert extract_config_file_from_argv(["--port", "9001"]) is None


def test_extract_config_file_from_argv_followed_by_flag_is_ignored() -> None:
    # ``--config-file --reload`` should not consume ``--reload`` as the path.
    out = extract_config_file_from_argv(["--config-file", "--reload"])
    assert out is None


def test_explicit_config_path_prefers_cli_over_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENBB_AGENT_CONFIG", "/from-env.toml")
    out = explicit_config_path(["--config-file", "/from-cli.toml"])
    assert out == "/from-cli.toml"


def test_explicit_config_path_falls_through_env_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for v in EXPLICIT_CONFIG_ENVS:
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("OPENBB_CONFIG", "/c.toml")  # last-priority slot
    assert explicit_config_path([]) == "/c.toml"

    # Higher-priority var wins.
    monkeypatch.setenv("OPENBB_AGENT_CONFIG", "/agent.toml")
    assert explicit_config_path([]) == "/agent.toml"


def test_explicit_config_path_returns_none_when_unset() -> None:
    assert explicit_config_path([]) is None


def test_expand_env_refs_resolves_braced_and_bare() -> None:
    env = {"FOO": "bar", "X": "y"}
    out, missing = expand_env_refs("a/${FOO}/$X", env=env)
    assert out == "a/bar/y"
    assert missing == []


def test_expand_env_refs_preserves_literal_dollar_for_non_idents() -> None:
    out, missing = expand_env_refs("$5 and $@ and $", env={})
    assert out == "$5 and $@ and $"
    assert missing == []


def test_expand_env_refs_reports_missing_once() -> None:
    out, missing = expand_env_refs("${A}-${A}-${B}", env={})
    assert out == "${A}-${A}-${B}"
    assert missing == ["A", "B"]


def test_expand_env_refs_uses_os_environ_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("__TEST_EXPAND__", "ok")
    out, _ = expand_env_refs("${__TEST_EXPAND__}")
    assert out == "ok"


def test_expand_in_dict_walks_lists_and_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("X", "42")
    inp = {"a": "${X}", "b": ["${X}", 1], "c": {"d": "${X}"}, "e": ("${X}",)}
    out = expand_in_dict(inp)
    assert out == {"a": "42", "b": ["42", 1], "c": {"d": "42"}, "e": ("42",)}


def test_expand_in_dict_preserves_non_strings() -> None:
    assert expand_in_dict(42) == 42
    assert expand_in_dict(None) is None
    assert expand_in_dict(True) is True


def test_apply_launcher_env_sets_unset_keys() -> None:
    target: dict[str, str] = {}
    applied = apply_launcher_env(
        {"GITHUB_TOKEN": "ghp_xxxx"},
        env=target,
    )
    assert applied == ["GITHUB_TOKEN"]
    assert target == {"GITHUB_TOKEN": "ghp_xxxx"}


def test_apply_launcher_env_no_clobber_real_env() -> None:
    target = {"GITHUB_TOKEN": "from-shell"}
    applied = apply_launcher_env(
        {"GITHUB_TOKEN": "from-toml"},
        env=target,
    )
    assert applied == []
    assert target["GITHUB_TOKEN"] == "from-shell"


def test_apply_launcher_env_expands_refs() -> None:
    target = {"HOST_TOKEN": "ghp_xxxx"}
    applied = apply_launcher_env(
        {"GITHUB_TOKEN": "${HOST_TOKEN}"},
        env=target,
    )
    assert applied == ["GITHUB_TOKEN"]
    assert target["GITHUB_TOKEN"] == "ghp_xxxx"


def test_apply_launcher_env_skips_missing_refs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    target: dict[str, str] = {}
    with caplog.at_level("WARNING"):
        applied = apply_launcher_env(
            {"GITHUB_TOKEN": "${HOST_TOKEN}"},
            env=target,
        )
    assert applied == []
    assert "GITHUB_TOKEN" not in target
    assert any("HOST_TOKEN" in r.message for r in caplog.records)


def test_apply_user_settings_credentials_seeds_env(tmp_path: Path) -> None:
    """``credentials`` from ``user_settings.json`` map to upper-cased env vars."""
    from openbb_agent_server.app import config as cfg_mod

    settings = tmp_path / "user_settings.json"
    settings.write_text(
        json.dumps(
            {
                "credentials": {
                    "groq_api_key": "gsk-from-file",
                    "nvidia_api_key": "nvapi-from-file",
                    "google_api_key": None,  # null → skipped
                    "anthropic_api_key": "",  # empty → skipped
                    "openai_api_key": 12345,  # non-string → skipped
                },
                "preferences": {},
            }
        )
    )
    env: dict[str, str] = {}
    applied = cfg_mod.apply_user_settings_credentials(
        settings_path=str(settings), env=env
    )
    assert "GROQ_API_KEY" in applied
    assert "NVIDIA_API_KEY" in applied
    assert env["GROQ_API_KEY"] == "gsk-from-file"
    assert env["NVIDIA_API_KEY"] == "nvapi-from-file"
    assert "GOOGLE_API_KEY" not in env
    assert "ANTHROPIC_API_KEY" not in env
    assert "OPENAI_API_KEY" not in env


def test_apply_user_settings_credentials_does_not_clobber_real_env(
    tmp_path: Path,
) -> None:
    from openbb_agent_server.app import config as cfg_mod

    settings = tmp_path / "user_settings.json"
    settings.write_text(json.dumps({"credentials": {"groq_api_key": "FROM_FILE"}}))
    env = {"GROQ_API_KEY": "FROM_SHELL"}
    applied = cfg_mod.apply_user_settings_credentials(
        settings_path=str(settings), env=env
    )
    assert applied == []
    assert env["GROQ_API_KEY"] == "FROM_SHELL"


def test_apply_user_settings_credentials_handles_missing_file(
    tmp_path: Path,
) -> None:
    from openbb_agent_server.app import config as cfg_mod

    env: dict[str, str] = {}
    applied = cfg_mod.apply_user_settings_credentials(
        settings_path=str(tmp_path / "nope.json"), env=env
    )
    assert applied == []
    assert env == {}


def test_apply_user_settings_credentials_handles_malformed_json(
    tmp_path: Path,
) -> None:
    from openbb_agent_server.app import config as cfg_mod

    settings = tmp_path / "user_settings.json"
    settings.write_text("not-json")
    env: dict[str, str] = {}
    applied = cfg_mod.apply_user_settings_credentials(
        settings_path=str(settings), env=env
    )
    assert applied == []
    assert env == {}


def test_apply_user_settings_credentials_skips_when_credentials_missing(
    tmp_path: Path,
) -> None:
    from openbb_agent_server.app import config as cfg_mod

    settings = tmp_path / "user_settings.json"
    settings.write_text(json.dumps({"preferences": {"theme": "dark"}}))
    env: dict[str, str] = {}
    applied = cfg_mod.apply_user_settings_credentials(
        settings_path=str(settings), env=env
    )
    assert applied == []
    assert env == {}


def test_apply_launcher_env_handles_none() -> None:
    assert apply_launcher_env(None) == []
    assert apply_launcher_env({}) == []


def test_apply_launcher_env_skips_non_string_keys() -> None:
    target: dict[str, str] = {}
    applied = apply_launcher_env({1: "x"}, env=target)  # type: ignore[dict-item]
    assert applied == []
    assert target == {}


def test_merge_launcher_kwargs_cli_wins() -> None:
    out = merge_launcher_kwargs(
        {"port": 1111},
        {"port": 9999, "host": "0.0.0.0"},
    )
    assert out == {"port": 1111, "host": "0.0.0.0"}


def test_merge_launcher_kwargs_handles_no_launcher() -> None:
    assert merge_launcher_kwargs({"x": 1}, None) == {"x": 1}


def test_merge_launcher_kwargs_handles_empty_launcher() -> None:
    assert merge_launcher_kwargs({"x": 1}, {}) == {"x": 1}


def _write_toml(path: Path, body: str) -> None:
    path.write_text(textwrap.dedent(body).strip() + "\n")


def test_bootstrap_loads_toml_and_pushes_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "openbb.toml"
    _write_toml(
        cfg_path,
        """
        [agent]
        host = "0.0.0.0"
        port = 6900
        model_provider = "fake"

        [env]
        OPENBB_TEST_BOOTSTRAP = "value-from-toml"
        """,
    )
    # Make sure no shell value clobbers the test.
    monkeypatch.delenv("OPENBB_TEST_BOOTSTRAP", raising=False)
    cfg = bootstrap_launcher_config(explicit_path=str(cfg_path))
    assert cfg.get("agent", {}).get("host") == "0.0.0.0"
    import os

    assert os.environ["OPENBB_TEST_BOOTSTRAP"] == "value-from-toml"


def test_bootstrap_substitutes_env_refs_in_agent_table(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HOST_DB_URL", "postgresql+psycopg://prod-db:5432/x")
    cfg_path = tmp_path / "openbb.toml"
    _write_toml(
        cfg_path,
        """
        [agent]
        db_url = "${HOST_DB_URL}"
        model_provider = "fake"
        """,
    )
    merged = bootstrap_launcher_config(explicit_path=str(cfg_path))
    section = agent_section(merged)
    assert section["db_url"] == "postgresql+psycopg://prod-db:5432/x"


def test_bootstrap_real_shell_env_wins_over_env_table(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OPENBB_TEST_KEY", "from-shell")
    cfg_path = tmp_path / "openbb.toml"
    _write_toml(
        cfg_path,
        """
        [env]
        OPENBB_TEST_KEY = "from-toml"
        """,
    )
    bootstrap_launcher_config(explicit_path=str(cfg_path))
    import os

    assert os.environ["OPENBB_TEST_KEY"] == "from-shell"


def test_bootstrap_rejects_malformed_toml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "openbb.toml"
    cfg_path.write_text("not valid = = toml\n")
    with pytest.raises(ValueError):
        bootstrap_launcher_config(explicit_path=str(cfg_path))


def test_agent_section_returns_empty_dict_when_missing() -> None:
    assert agent_section({}) == {}
    assert agent_section({"agent": "not-a-dict"}) == {}


def test_agent_section_returns_copy_not_reference() -> None:
    cfg = {"agent": {"host": "x"}}
    section = agent_section(cfg)
    section["host"] = "y"
    assert cfg["agent"]["host"] == "x"


def test_from_toml_with_empty_section_returns_defaults() -> None:
    s = AgentServerSettings.from_toml({})
    assert s.host == "127.0.0.1"
    assert s.model_provider == "nvidia"


def test_from_toml_top_level_fields_apply() -> None:
    s = AgentServerSettings.from_toml(
        {
            "host": "0.0.0.0",
            "port": 8080,
            "model_provider": "anthropic",
            "model_name": "claude-opus-4-7",
            "tool_sources": ["artifacts", "web_search"],
            "subagents": ["researcher"],
            "middleware": ["usage_recorder"],
            "skills": ["/skills/finance"],
        }
    )
    assert s.host == "0.0.0.0"
    assert s.port == 8080
    assert s.model_provider == "anthropic"
    assert s.model_name == "claude-opus-4-7"
    assert s.tool_sources == ("artifacts", "web_search")
    assert s.subagents == ("researcher",)
    assert s.middleware == ("usage_recorder",)
    assert s.skills == ("/skills/finance",)


def test_from_toml_auth_subtable_applies() -> None:
    s = AgentServerSettings.from_toml(
        {
            "auth": {
                "backend": "oidc_jwt",
                "config": {
                    "jwks_url": "https://idp.example.com/.well-known/jwks.json",
                    "audience": "agent-server",
                },
            }
        }
    )
    assert s.auth_backend == "oidc_jwt"
    assert s.auth_config["jwks_url"].startswith("https://idp.example.com")
    assert s.auth_config["audience"] == "agent-server"


def test_from_toml_model_subtable_applies() -> None:
    s = AgentServerSettings.from_toml(
        {
            "model": {
                "provider": "nvidia",
                "name": "meta/llama-3.1-405b-instruct",
                "config": {"base_url": "https://integrate.api.nvidia.com/v1"},
            }
        }
    )
    assert s.model_provider == "nvidia"
    assert s.model_name == "meta/llama-3.1-405b-instruct"
    assert s.model_config_["base_url"].startswith("https://integrate")


def test_from_toml_metadata_subtable_applies() -> None:
    s = AgentServerSettings.from_toml(
        {
            "metadata": {
                "name": "My Custom Agent",
                "description": "Tailored for finance.",
                "image_url": "https://example.com/logo.png",
            }
        }
    )
    assert s.metadata.name == "My Custom Agent"
    assert s.metadata.description == "Tailored for finance."
    assert s.metadata.image_url == "https://example.com/logo.png"


def test_from_toml_features_merges_over_defaults() -> None:
    s = AgentServerSettings.from_toml(
        {
            "features": {
                "deep-research": {
                    "label": "Deep Research",
                    "default": True,
                    "description": "Multi-step research subagent.",
                }
            }
        }
    )
    # The operator-supplied feature is present…
    assert s.features["deep-research"]["default"] is True
    # …and the built-in toggles survive the merge rather than being
    # dropped by an operator ``[agent.features]`` block.
    assert "search-web" in s.features
    assert "fetch-url" in s.features
    assert s.features["streaming"] is True


def test_from_toml_data_dir_expanduser(tmp_path: Path) -> None:
    s = AgentServerSettings.from_toml({"data_dir": str(tmp_path / "agent")})
    assert s.data_dir == tmp_path / "agent"


def test_from_toml_env_var_still_wins_over_toml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENBB_AGENT_HOST", "from-env-host")
    s = AgentServerSettings.from_toml({"host": "from-toml-host"})
    # ``BaseSettings`` re-reads env vars at class instantiation, so env wins.
    assert s.host == "from-env-host"


def test_container_style_env_mapping(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """End-to-end: container sets HOST_GITHUB_TOKEN, TOML maps it to GITHUB_TOKEN."""
    import os

    monkeypatch.setenv("HOST_GITHUB_TOKEN", "ghp_secret_value")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    cfg_path = tmp_path / "openbb.toml"
    _write_toml(
        cfg_path,
        """
        [env]
        GITHUB_TOKEN = "${HOST_GITHUB_TOKEN}"

        [agent]
        model_provider = "fake"
        """,
    )

    bootstrap_launcher_config(explicit_path=str(cfg_path))
    assert os.environ["GITHUB_TOKEN"] == "ghp_secret_value"


def test_module_constants_are_immutable() -> None:
    # CI-friendly invariant — protects the public surface from drift.
    assert "OPENBB_AGENT_CONFIG" in cfg_mod.EXPLICIT_CONFIG_ENVS
    assert "OPENBB_API_CONFIG" in cfg_mod.EXPLICIT_CONFIG_ENVS
    assert "OPENBB_CONFIG" in cfg_mod.EXPLICIT_CONFIG_ENVS
    assert cfg_mod.CONFIG_FILE_FLAG == "--config-file"


def test_from_toml_loads_profiles_from_subtables() -> None:
    s = AgentServerSettings.from_toml(
        {
            "model_provider": "fake",
            "profiles": {
                "equity": {
                    "metadata": {"name": "Equity Analyst"},
                    "tool_sources": ["artifacts"],
                    "system_prompt_file": "/etc/openbb/equity-prompt.md",
                },
                "research": {
                    "metadata": {"name": "Researcher"},
                    "subagents": ["researcher"],
                },
            },
        }
    )
    assert set(s.profiles) == {"equity", "research"}
    eq = s.resolve_profile("equity")
    assert eq.metadata.name == "Equity Analyst"
    assert eq.tool_sources == ("artifacts",)
    assert eq.system_prompt_file == "/etc/openbb/equity-prompt.md"


def test_resolve_profile_rejects_inline_system_prompt() -> None:
    """Inline ``system_prompt = "..."`` must raise — system prompts are files."""
    s = AgentServerSettings(profiles={"alt": {"system_prompt": "you are an analyst"}})
    with pytest.raises(ValueError, match="system_prompt_file"):
        s.resolve_profile("alt")


def test_from_toml_rejects_inline_system_prompt_at_top_level() -> None:
    with pytest.raises(ValueError, match="system_prompt_file"):
        AgentServerSettings.from_toml(
            {"model_provider": "fake", "system_prompt": "inline forbidden"}
        )


def test_resolve_profile_falls_back_to_base_settings_for_unset_fields() -> None:
    s = AgentServerSettings(
        model_provider="fake",
        tool_sources=("artifacts", "web_search"),
        profiles={"only-name": {"metadata": {"name": "Just A Name"}}},
    )
    p = s.resolve_profile("only-name")
    # Inherits the global tool_sources because the profile didn't override.
    assert p.tool_sources == ("artifacts", "web_search")
    assert p.metadata.name == "Just A Name"


def test_resolve_profile_default_returns_global_settings() -> None:
    s = AgentServerSettings(
        model_provider="fake",
        model_name="llama3",
        tool_sources=("artifacts",),
    )
    p = s.resolve_profile()  # implicit default
    assert p.name == "default"
    assert p.model_provider == "fake"
    assert p.model_name == "llama3"
    assert p.tool_sources == ("artifacts",)


def test_resolve_profile_unknown_name_raises_keyerror() -> None:
    s = AgentServerSettings(model_provider="fake")
    with pytest.raises(KeyError):
        s.resolve_profile("nope")


def test_all_profile_names_includes_default() -> None:
    s = AgentServerSettings(
        model_provider="fake",
        profiles={"a": {}, "b": {}},
    )
    names = s.all_profile_names()
    assert "default" in names
    assert "a" in names
    assert "b" in names


def test_from_toml_loads_tool_source_config() -> None:
    s = AgentServerSettings.from_toml(
        {
            "model_provider": "fake",
            "tool_source_config": {
                "mcp_local": {"config_file": "/etc/openbb/mcp.toml"},
                "web_search": {"provider": "tavily"},
            },
        }
    )
    assert s.tool_source_config["mcp_local"]["config_file"] == "/etc/openbb/mcp.toml"
    assert s.tool_source_config["web_search"]["provider"] == "tavily"


def test_resolve_profile_inherits_base_tool_source_config() -> None:
    s = AgentServerSettings(
        model_provider="fake",
        tool_source_config={"mcp_local": {"config_file": "/base.toml"}},
        profiles={"equity": {}},
    )
    p = s.resolve_profile("equity")
    assert p.tool_source_config["mcp_local"]["config_file"] == "/base.toml"


def test_resolve_profile_overlay_merges_per_tool_source() -> None:
    s = AgentServerSettings(
        model_provider="fake",
        tool_source_config={
            "mcp_local": {"config_file": "/base.toml", "command": "openbb-mcp"}
        },
        profiles={
            "equity": {
                "tool_source_config": {"mcp_local": {"config_file": "/equity.toml"}}
            }
        },
    )
    p = s.resolve_profile("equity")
    # Profile overlay wins on overlap…
    assert p.tool_source_config["mcp_local"]["config_file"] == "/equity.toml"
    # …but inherits the keys it didn't override.
    assert p.tool_source_config["mcp_local"]["command"] == "openbb-mcp"


def test_default_profile_resolves_with_empty_tool_source_config() -> None:
    s = AgentServerSettings(model_provider="fake")
    p = s.resolve_profile()
    assert p.tool_source_config == {}


def test_load_preset_default_returns_a_dict() -> None:
    """``load_preset('default')`` parses the bundled ``openbb.toml.example``."""
    from openbb_agent_server.app.config import load_preset

    out = load_preset("default")
    assert isinstance(out, dict)
    assert out  # bundled preset is non-empty


def test_load_preset_unknown_name_raises() -> None:
    from openbb_agent_server.app.config import load_preset

    with pytest.raises(ValueError, match="unknown preset"):
        load_preset("totally-fake-preset")
