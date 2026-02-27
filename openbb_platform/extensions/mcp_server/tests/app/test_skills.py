"""Unit tests for default skill loading in the MCP server."""

# pylint: disable=protected-access,unused-argument

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastmcp.prompts.prompt import FunctionPrompt
from openbb_mcp_server.app.app import create_mcp_server
from openbb_mcp_server.models.settings import MCPSettings


def _find_skill_calls(mock_mcp):
    """Return add_prompt calls tagged with 'skill'."""
    return [
        call
        for call in mock_mcp.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]


def _find_system_calls(mock_mcp):
    """Return add_prompt calls tagged with 'system'."""
    return [
        call
        for call in mock_mcp.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "system" in call[0][0].tags
    ]


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_load_skills_from_directory(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that skill files (.md) are loaded from the skills directory."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "my_skill.md").write_text(
        "# My Skill\n\nThis is a test skill with code: `dict = {}`.", encoding="utf-8"
    )

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    # Find the add_prompt call that added a skill (tagged with "skill")
    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 1
    added_skill = skill_calls[0][0][0]
    assert added_skill.name == "my_skill"
    assert added_skill.description == "My Skill"
    assert "dict = {}" in added_skill.content
    assert "skill" in added_skill.tags


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_load_txt_skill(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that .txt skill files are also loaded."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "plain_skill.txt").write_text(
        "Plain Text Skill\n\nDo something useful.", encoding="utf-8"
    )

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 1
    added_skill = skill_calls[0][0][0]
    assert added_skill.name == "plain_skill"
    assert added_skill.description == "Plain Text Skill"


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_skip_non_skill_files(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that non-.md/.txt files in the skills directory are ignored."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "ignored.json").write_text('{"not": "a skill"}', encoding="utf-8")
    (skills_dir / "__init__.py").write_text("", encoding="utf-8")
    (skills_dir / "real_skill.md").write_text("# Real\n\nContent.", encoding="utf-8")

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 1
    assert skill_calls[0][0][0].name == "real_skill"


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_skip_empty_skill_files(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that empty skill files are skipped with a warning."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "empty.md").write_text("", encoding="utf-8")

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 0


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_no_skills_when_dir_is_none(
    mock_from_fastapi, mock_tool_registry, mock_process_routes
):
    """Test that no skills are loaded when default_skills_dir is None."""
    settings = MCPSettings(default_skills_dir=None)  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 0


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_no_skills_when_dir_missing(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that no skills are loaded when the directory doesn't exist."""
    settings = MCPSettings(default_skills_dir=str(tmp_path / "nonexistent"))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 0


@pytest.mark.asyncio
async def test_skill_renders_with_curly_braces():
    """Test that a skill with curly braces in content renders without error.

    This validates the StaticPrompt.render() fix that skips str.format()
    when no arguments are defined.
    """
    from openbb_mcp_server.models.prompts import StaticPrompt

    content = '# Skill\n\n```python\nfetcher_dict = {"Example": ExampleFetcher}\n```'
    prompt = StaticPrompt(
        name="test_curly",
        description="Test skill",
        content=content,
        arguments=None,
        tags={"skill"},
    )
    rendered = await prompt.render()
    assert len(rendered) == 1
    assert rendered[0].content.text == content


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_multiple_skills_loaded_in_order(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """Test that multiple skill files are loaded in sorted order."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "b_second.md").write_text("# Second\n\nContent B.", encoding="utf-8")
    (skills_dir / "a_first.md").write_text("# First\n\nContent A.", encoding="utf-8")

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data

    mock_registry_instance = MagicMock()
    mock_tool_registry.return_value = mock_registry_instance

    mock_mcp_instance = MagicMock()
    mock_from_fastapi.return_value = mock_mcp_instance

    create_mcp_server(settings, fastapi_app)

    skill_calls = [
        call
        for call in mock_mcp_instance.add_prompt.call_args_list
        if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
    ]
    assert len(skill_calls) == 2
    assert skill_calls[0][0][0].name == "a_first"
    assert skill_calls[1][0][0].name == "b_second"


# --- Default system prompt nudge tests ---


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_default_system_prompt_added_when_skills_loaded(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """When skills are loaded and no system_prompt_file is set, a default system prompt is added."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "alpha.md").write_text("# Alpha\n\nContent.", encoding="utf-8")
    (skills_dir / "beta.md").write_text("# Beta\n\nContent.", encoding="utf-8")

    settings = MCPSettings(default_skills_dir=str(skills_dir))  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data
    mock_tool_registry.return_value = MagicMock()
    mock_mcp = MagicMock()
    mock_mcp.instructions = None  # Start with no instructions
    mock_from_fastapi.return_value = mock_mcp

    create_mcp_server(settings, fastapi_app)

    system_calls = _find_system_calls(mock_mcp)
    assert len(system_calls) == 1

    added = system_calls[0][0][0]
    assert isinstance(added, FunctionPrompt)
    assert added.name == "system_prompt"
    assert "system" in added.tags

    # The nudge content should list the skill names
    content = added.fn()
    assert "alpha" in content
    assert "beta" in content
    assert "list_prompts" in content
    assert "execute_prompt" in content

    # instructions should be set on the mcp instance for initialize handshake
    assert mock_mcp.instructions is not None
    assert "alpha" in mock_mcp.instructions
    assert "beta" in mock_mcp.instructions


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_no_default_system_prompt_when_custom_set(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """When a custom system_prompt_file is set, no default system prompt is added for skills."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "gamma.md").write_text("# Gamma\n\nContent.", encoding="utf-8")

    prompt_file = tmp_path / "custom_prompt.txt"
    prompt_file.write_text("Custom system prompt text.", encoding="utf-8")

    settings = MCPSettings(
        default_skills_dir=str(skills_dir),
        system_prompt_file=str(prompt_file),
    )  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data
    mock_tool_registry.return_value = MagicMock()
    mock_mcp = MagicMock()
    mock_mcp.instructions = None  # Start with no instructions
    mock_from_fastapi.return_value = mock_mcp

    create_mcp_server(settings, fastapi_app)

    # The custom system prompt is added (from the system_prompt_file block), but
    # no *additional* default nudge should appear. Count system-tagged calls.
    system_calls = _find_system_calls(mock_mcp)
    # Exactly one system prompt (the custom one), not two.
    assert len(system_calls) == 1
    # The custom one is a FunctionPrompt from system_prompt_file, not our nudge.
    added = system_calls[0][0][0]
    assert added.name == "system_prompt"
    # Its content should be the custom text, not the skill nudge.
    content = added.fn()
    assert content == "Custom system prompt text."

    # instructions should be set to the custom prompt content
    assert mock_mcp.instructions == "Custom system prompt text."


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_no_default_system_prompt_when_no_skills(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """When no skills are loaded, no default system prompt nudge is added."""
    settings = MCPSettings(default_skills_dir=None)  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data
    mock_tool_registry.return_value = MagicMock()
    mock_mcp = MagicMock()
    mock_mcp.instructions = None  # Start with no instructions
    mock_from_fastapi.return_value = mock_mcp

    create_mcp_server(settings, fastapi_app)

    system_calls = _find_system_calls(mock_mcp)
    assert len(system_calls) == 0

    # instructions should remain None
    assert mock_mcp.instructions is None


@patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
@patch("openbb_mcp_server.app.app.ToolRegistry")
@patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
def test_explicit_instructions_not_overridden(
    mock_from_fastapi, mock_tool_registry, mock_process_routes, tmp_path
):
    """When instructions is explicitly set in settings, it is not overwritten by auto-generated content."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    (skills_dir / "delta.md").write_text("# Delta\n\nContent.", encoding="utf-8")

    settings = MCPSettings(
        default_skills_dir=str(skills_dir),
        instructions="My explicit instructions.",
    )  # type: ignore
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = []
    mock_process_routes.return_value = mock_processed_data
    mock_tool_registry.return_value = MagicMock()
    mock_mcp = MagicMock()
    # Simulate that FastMCP received the explicit instructions via get_fastmcp_kwargs
    mock_mcp.instructions = "My explicit instructions."
    mock_from_fastapi.return_value = mock_mcp

    create_mcp_server(settings, fastapi_app)

    # The skill system prompt should still be added
    system_calls = _find_system_calls(mock_mcp)
    assert len(system_calls) == 1

    # But instructions should NOT be overwritten
    assert mock_mcp.instructions == "My explicit instructions."
