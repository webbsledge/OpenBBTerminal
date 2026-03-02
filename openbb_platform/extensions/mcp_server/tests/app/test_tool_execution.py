"""Tests for tool execution, prompt rendering, and bundled skill integration.

These tests exercise the actual closure-defined tools (discovery and prompt)
and verify that the real bundled skill files render correctly through the
StaticPrompt pipeline.
"""

# pylint: disable=protected-access,unused-argument

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastmcp.server.providers.openapi import OpenAPITool
from fastmcp.utilities.openapi import HTTPRoute
from mcp.types import TextContent
from openbb_mcp_server.app.app import create_mcp_server
from openbb_mcp_server.models.prompts import StaticPrompt
from openbb_mcp_server.models.registry import ToolRegistry
from openbb_mcp_server.models.settings import MCPSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SKILLS_DIR = Path(__file__).resolve().parent.parent.parent / (
    "openbb_mcp_server" + os.sep + "skills"
)


def _make_tool(name: str, description: str = "desc"):
    """Create a minimal OpenAPITool mock."""
    tool = OpenAPITool(
        MagicMock(),
        HTTPRoute(path=f"/{name}", method="GET"),
        name=name,
        description=description,
        parameters={},
        director=MagicMock(),
    )
    return tool


def _capture_decorated_tools(mock_mcp_instance):
    """Patch ``mock_mcp_instance.tool`` so the undecorated closures are captured.

    Returns a dict mapping ``func.__name__`` → the original function object.
    """
    decorated: dict[str, object] = {}

    def tool_decorator_factory(*args, **kwargs):
        def decorator(func):
            decorated[func.__name__] = func
            return MagicMock()

        return decorator

    mock_mcp_instance.tool = MagicMock(side_effect=tool_decorator_factory)
    return decorated


def _build_server(
    settings: MCPSettings,
    *,
    registry: ToolRegistry | None = None,
    prompts_json: list | None = None,
):
    """Construct the MCP server with controlled mocks and return helpers.

    Returns ``(mcp_mock, decorated_functions, registry)``.
    """
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = prompts_json or []

    mock_mcp_instance = MagicMock()
    mock_mcp_instance.render_prompt = AsyncMock()

    decorated = _capture_decorated_tools(mock_mcp_instance)

    if registry is None:
        registry = ToolRegistry()

    with (
        patch(
            "openbb_mcp_server.app.app.process_fastapi_routes_for_mcp",
            return_value=mock_processed_data,
        ),
        patch(
            "openbb_mcp_server.app.app.ToolRegistry",
            return_value=registry,
        ),
        patch(
            "openbb_mcp_server.app.app.FastMCP.from_fastapi",
            return_value=mock_mcp_instance,
        ),
    ):
        create_mcp_server(settings, fastapi_app)

    return mock_mcp_instance, decorated, registry


# ===================================================================
# Discovery tool execution tests
# ===================================================================


class TestAvailableCategories:
    """Tests for the ``available_categories`` discovery tool."""

    def test_returns_categories_with_subcategories(self):
        """Return categories with their subcategories and tool counts."""
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical"),
        )
        registry.register_tool(
            category="equity",
            subcategory="fundamental",
            tool_name="equity_fundamental_income",
            tool=_make_tool("equity_fundamental_income"),
        )
        registry.register_tool(
            category="economy",
            subcategory="general",
            tool_name="economy_cpi",
            tool=_make_tool("economy_cpi"),
        )

        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=registry)

        result = decorated["available_categories"]()
        names = {c.name for c in result}
        assert names == {"equity", "economy"}

        equity = next(c for c in result if c.name == "equity")
        assert equity.total_tools == 2
        subcat_names = {s.name for s in equity.subcategories}
        assert subcat_names == {"price", "fundamental"}

    def test_empty_registry(self):
        """Return empty list when the registry has no tools."""
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=ToolRegistry())
        assert decorated["available_categories"]() == []


class TestAvailableTools:
    """Tests for the ``available_tools`` discovery tool."""

    def _setup(self):
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical", "Get historical prices"),
        )
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_quote",
            tool=_make_tool("equity_price_quote", "Get quote"),
            enabled=False,
        )
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=registry)
        return decorated

    def test_list_tools_in_category(self):
        """List all tools in a given category."""
        decorated = self._setup()
        result = decorated["available_tools"](category="equity")
        names = {t.name for t in result}
        assert names == {"equity_price_historical", "equity_price_quote"}

    def test_list_tools_in_subcategory(self):
        """List tools filtered to a specific subcategory."""
        decorated = self._setup()
        result = decorated["available_tools"](category="equity", subcategory="price")
        assert len(result) == 2

    def test_reflects_active_state(self):
        """Report active/inactive state correctly based on registry enabled state."""
        decorated = self._setup()
        result = decorated["available_tools"](category="equity")
        by_name = {t.name: t for t in result}
        assert by_name["equity_price_historical"].active is True
        assert by_name["equity_price_quote"].active is False

    def test_unknown_category_raises(self):
        """Raise ValueError when the requested category does not exist."""
        decorated = self._setup()
        with pytest.raises(ValueError, match="not found"):
            decorated["available_tools"](category="nonexistent")

    def test_unknown_subcategory_raises(self):
        """Raise ValueError when the requested subcategory does not exist."""
        decorated = self._setup()
        with pytest.raises(ValueError, match="not found"):
            decorated["available_tools"](category="equity", subcategory="options")


class TestToggleTools:
    """Tests for ``activate_tools`` and ``deactivate_tools``."""

    def _setup(self):
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical"),
            enabled=False,
        )
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_quote",
            tool=_make_tool("equity_price_quote"),
            enabled=True,
        )
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, reg = _build_server(settings, registry=registry)
        return decorated, reg

    def test_activate_tools(self):
        """Activate tools and confirm enabled state in registry."""
        decorated, registry = self._setup()
        msg = decorated["activate_tools"](tool_names=["equity_price_historical"])
        assert "Activated" in msg
        assert registry.is_enabled("equity_price_historical") is True

    def test_deactivate_tools(self):
        """Deactivate tools and confirm disabled state in registry."""
        decorated, registry = self._setup()
        msg = decorated["deactivate_tools"](tool_names=["equity_price_quote"])
        assert "Deactivated" in msg
        assert registry.is_enabled("equity_price_quote") is False

    def test_activate_unknown_tool(self):
        """Report not-found when an unknown tool name is supplied."""
        decorated, _ = self._setup()
        msg = decorated["activate_tools"](tool_names=["nonexistent"])
        assert "Not found" in msg

    def test_activate_mixed(self):
        """Activate a mix of known and unknown tools, reporting both outcomes."""
        decorated, registry = self._setup()
        msg = decorated["activate_tools"](
            tool_names=["equity_price_historical", "nonexistent"]
        )
        assert "Activated" in msg
        assert "Not found" in msg
        assert registry.is_enabled("equity_price_historical") is True

    def test_discovery_tools_not_registered_when_disabled(self):
        """Omit all discovery tools when tool discovery is disabled in settings."""
        settings = MCPSettings(enable_tool_discovery=False)  # type: ignore
        _, decorated, _ = _build_server(settings)
        assert "available_categories" not in decorated
        assert "available_tools" not in decorated
        assert "activate_tools" not in decorated
        assert "deactivate_tools" not in decorated


# ===================================================================
# Prompt tool execution tests
# ===================================================================


# ===================================================================
# PromptsAsTools transform tests
# ===================================================================


class TestPromptsAsToolsTransform:
    """Tests verifying that PromptsAsTools transform replaces hand-rolled prompt tools."""

    def test_transform_is_added(self):
        """PromptsAsTools transform is registered on the mcp instance."""
        from fastmcp.server.transforms import PromptsAsTools

        settings = MCPSettings()  # type: ignore
        mock_mcp, _, _ = _build_server(settings)

        mock_mcp.add_transform.assert_called_once()
        transform = mock_mcp.add_transform.call_args[0][0]
        assert isinstance(transform, PromptsAsTools)

    def test_no_hand_rolled_prompt_tools(self):
        """The old list_prompts / execute_prompt closures are no longer registered."""
        settings = MCPSettings()  # type: ignore
        _, decorated, _ = _build_server(settings)

        assert "list_prompts" not in decorated
        assert "execute_prompt" not in decorated

    def test_inline_prompt_stores_argument_defaults(self):
        """Inline prompt definitions store defaults on StaticPrompt.argument_defaults."""
        prompt_def = {
            "name": "my_prompt",
            "description": "Test",
            "content": "Hello {name}, focus on {aspect}",
            "arguments": [
                {"name": "name", "type": "str", "description": "Name"},
                {
                    "name": "aspect",
                    "type": "str",
                    "description": "Aspect",
                    "default": "fundamentals",
                },
            ],
        }
        settings = MCPSettings()  # type: ignore
        mock_mcp, _, _ = _build_server(settings, prompts_json=[prompt_def])

        # Find the StaticPrompt among add_prompt calls
        added_prompts = [
            call[0][0]
            for call in mock_mcp.add_prompt.call_args_list
            if isinstance(call[0][0], StaticPrompt)
        ]
        assert len(added_prompts) == 1
        assert added_prompts[0].argument_defaults == {"aspect": "fundamentals"}

    @pytest.mark.asyncio
    async def test_static_prompt_renders_with_defaults(self):
        """StaticPrompt.render() applies argument_defaults when caller omits them."""
        from fastmcp.prompts import PromptArgument

        prompt = StaticPrompt(
            name="greeting",
            content="Hello {name}, focus on {aspect}",
            arguments=[
                PromptArgument(name="name", required=True),
                PromptArgument(name="aspect", required=False),
            ],
            argument_defaults={"aspect": "fundamentals"},
        )

        rendered = await prompt.render(arguments={"name": "AAPL"})
        assert rendered[0].content.text == "Hello AAPL, focus on fundamentals"

    @pytest.mark.asyncio
    async def test_static_prompt_caller_overrides_defaults(self):
        """Caller-supplied values override argument_defaults."""
        from fastmcp.prompts import PromptArgument

        prompt = StaticPrompt(
            name="greeting",
            content="Hello {name}, focus on {aspect}",
            arguments=[
                PromptArgument(name="name", required=True),
                PromptArgument(name="aspect", required=False),
            ],
            argument_defaults={"aspect": "fundamentals"},
        )

        rendered = await prompt.render(
            arguments={"name": "AAPL", "aspect": "technicals"}
        )
        assert rendered[0].content.text == "Hello AAPL, focus on technicals"


# ===================================================================
# Bundled skill rendering tests
# ===================================================================


class TestBundledSkillRendering:
    """Verify each real skill file renders through StaticPrompt without error."""

    EXPECTED_SKILLS = {
        "develop_extension": "Build an OpenBB Platform Extension",
        "build_workspace_app": "Build and Run OpenBB Workspace Applications",
        "configure_mcp_server": "Configure and Build the OpenBB MCP Server",
        "work_with_server": "Working With the OpenBB MCP Server",
    }

    def test_skills_directory_exists(self):
        """Verify the skills directory exists at the expected path."""
        assert SKILLS_DIR.is_dir(), f"Skills directory not found: {SKILLS_DIR}"

    def test_all_expected_skills_present(self):
        """Confirm all expected skill subdirectories with SKILL.md are present."""
        skill_dirs = {
            d.name
            for d in SKILLS_DIR.iterdir()
            if d.is_dir() and (d / "SKILL.md").exists()
        }
        for name in self.EXPECTED_SKILLS:
            assert name in skill_dirs, f"Missing skill subdirectory: {name}/SKILL.md"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "skill_name,expected_heading",
        list(EXPECTED_SKILLS.items()),
        ids=list(EXPECTED_SKILLS.keys()),
    )
    async def test_skill_renders_without_error(self, skill_name, expected_heading):
        """Each skill file renders to a PromptMessage with full content intact."""
        skill_file = SKILLS_DIR / skill_name / "SKILL.md"
        content = skill_file.read_text(encoding="utf-8")
        assert len(content) > 100, f"Skill {skill_name} seems too short"

        prompt = StaticPrompt(
            name=skill_name,
            description=expected_heading,
            content=content,
            arguments=None,
            tags={"skill"},
        )
        rendered = await prompt.render()

        assert len(rendered) == 1
        assert rendered[0].role == "user"
        assert isinstance(rendered[0].content, TextContent)
        assert rendered[0].content.text == content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "skill_name",
        list(EXPECTED_SKILLS.keys()),
        ids=list(EXPECTED_SKILLS.keys()),
    )
    async def test_skill_preserves_curly_braces(self, skill_name):
        """Skills with code blocks containing {} must not raise KeyError."""
        skill_file = SKILLS_DIR / skill_name / "SKILL.md"
        content = skill_file.read_text(encoding="utf-8")

        prompt = StaticPrompt(
            name=skill_name,
            description="test",
            content=content,
            arguments=None,
            tags={"skill"},
        )
        # This would raise KeyError if str.format() is incorrectly applied
        rendered = await prompt.render()
        assert rendered[0].content.text == content

    @pytest.mark.asyncio
    async def test_skill_render_with_empty_arguments(self):
        """Passing empty dict as arguments should still bypass str.format."""
        skill_file = SKILLS_DIR / "develop_extension" / "SKILL.md"
        content = skill_file.read_text(encoding="utf-8")

        prompt = StaticPrompt(
            name="develop_extension",
            description="test",
            content=content,
            arguments=None,
            tags={"skill"},
        )
        # Explicit empty dict should also be safe
        rendered = await prompt.render(arguments={})
        assert rendered[0].content.text == content

    def test_skill_description_from_first_heading(self):
        """Verify that each SKILL.md has the expected markdown heading (after YAML frontmatter)."""
        for skill_name, expected_heading in self.EXPECTED_SKILLS.items():
            skill_file = SKILLS_DIR / skill_name / "SKILL.md"
            content = skill_file.read_text(encoding="utf-8")
            # Skip YAML frontmatter block (--- ... ---)
            lines = content.splitlines()
            in_frontmatter = lines[0].strip() == "---" if lines else False
            heading = ""
            for i, line in enumerate(lines):
                if i == 0 and in_frontmatter:
                    continue
                if in_frontmatter and line.strip() == "---":
                    in_frontmatter = False
                    continue
                if not in_frontmatter and line.startswith("#"):
                    heading = line.lstrip("# ").strip()
                    break
            assert (
                heading == expected_heading
            ), f"Skill '{skill_name}' heading mismatch: got '{heading}', expected '{expected_heading}'"


# ===================================================================
# Integration: skills loaded and accessible via prompt tools
# ===================================================================


class TestSkillsIntegration:
    """Test skills loading end-to-end through create_mcp_server."""

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_bundled_skills_registered_via_provider(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """Bundled skills are registered via mcp.add_provider(SkillsDirectoryProvider)."""
        from fastmcp.server.providers.skills import SkillsDirectoryProvider

        settings = MCPSettings()  # type: ignore  (uses default skills dir)
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

        provider_calls = mock_mcp_instance.add_provider.call_args_list
        assert len(provider_calls) >= 1
        assert isinstance(provider_calls[0][0][0], SkillsDirectoryProvider)

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_skill_md_files_have_content(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """Each bundled SKILL.md file has non-trivial content."""
        for skill_name in [
            "develop_extension",
            "build_workspace_app",
            "configure_mcp_server",
            "work_with_server",
        ]:
            skill_file = SKILLS_DIR / skill_name / "SKILL.md"
            assert skill_file.exists(), f"Missing: {skill_file}"
            content = skill_file.read_text(encoding="utf-8")
            assert (
                len(content) > 500
            ), f"Skill '{skill_name}' SKILL.md is suspiciously short ({len(content)} chars)"

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_no_skill_prompts_registered(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """Skills are no longer registered as prompts with the 'skill' tag."""
        settings = MCPSettings()  # type: ignore
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

        skill_prompt_calls = [
            c
            for c in mock_mcp_instance.add_prompt.call_args_list
            if hasattr(c[0][0], "tags") and "skill" in c[0][0].tags
        ]
        assert (
            skill_prompt_calls == []
        ), "Skills should not be registered as prompts in FastMCP v3 — use add_provider instead"
