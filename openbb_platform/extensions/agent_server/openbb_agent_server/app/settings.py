"""Server-level settings (env-prefixed ``OPENBB_AGENT_…`` + ``openbb.toml``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureSpec(BaseSettings):
    """One row in the ``agents.json`` features map."""

    model_config = SettingsConfigDict(extra="allow")

    label: str
    description: str
    default: bool = False


class AgentMetadata(BaseSettings):
    """Static fields rendered in ``GET /agents.json``."""

    model_config = SettingsConfigDict(
        env_prefix="OPENBB_AGENT_META_",
        extra="ignore",
        frozen=True,
    )

    name: str = "OpenBB · NVIDIA Stack"
    description: str = (
        "DeepAgents harness over the OpenBB Platform with NVIDIA NIM "
        "end-to-end: nemotron-3-super-120b-a12b + nv-embed-v1 + "
        "nv-embedcode-7b-v1 + nv-rerank-qa-mistral-4b + riva-translate "
        "+ nemotron-nano-vl-8b vision + gemma-3n audio + paligemma."
    )
    image_url: str | None = None


class AgentProfile(BaseSettings):
    """Resolved per-profile config used by the runtime."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    name: str
    metadata: AgentMetadata
    model_provider: str
    model_name: str
    model_config_: dict[str, Any] = Field(default_factory=dict, alias="model_config")
    tool_sources: tuple[str, ...] = ()
    tool_source_config: dict[str, dict[str, Any]] = Field(default_factory=dict)
    subagents: tuple[str, ...] = ()
    middleware: tuple[str, ...] = ()
    skills: tuple[str, ...] = ()
    features: dict[str, Any] = Field(default_factory=dict)
    system_prompt_file: str | None = None


def _resolve_system_prompt_file(
    overlay: dict[str, Any], base: str | None
) -> str | None:
    """Pick the profile's ``system_prompt_file`` and reject inline strings."""
    if "system_prompt" in overlay:
        raise ValueError(
            "Inline ``system_prompt`` is not supported. Move the prompt to "
            'a file and reference it via ``system_prompt_file = "<path>"``.'
        )
    return overlay.get("system_prompt_file") or base


SEARCH_WEB_FEATURE: str = "search-web"
FETCH_URL_FEATURE: str = "fetch-url"


DEFAULT_FEATURES: dict[str, Any] = {
    "streaming": True,
    "widget-dashboard-select": True,
    "widget-dashboard-search": True,
    "widget-global-search": True,
    "mcp-tools": True,
    "file-upload": True,
    "generative-ui": True,
    SEARCH_WEB_FEATURE: {
        "label": "Search Web",
        "description": (
            "Allow the agent to search the public web when answering. "
            "Each result attaches a citation card with the source URL. "
            "Off by default — turn on for queries about current events "
            "or anything outside the model's training data."
        ),
        "default": False,
    },
    FETCH_URL_FEATURE: {
        "label": "Fetch URL",
        "description": (
            "Allow the agent to fetch and read the full text of a web page "
            "from a URL. SSRF-guarded: private, loopback, link-local and "
            "cloud-metadata hosts are refused. Off by default — turn on to "
            "let the agent read the article behind a link."
        ),
        "default": False,
    },
}


class AgentServerSettings(BaseSettings):
    """Top-level config."""

    model_config = SettingsConfigDict(
        env_prefix="OPENBB_AGENT_",
        env_nested_delimiter="__",
        extra="ignore",
        frozen=True,
    )

    host: str = "127.0.0.1"
    port: int = 6900

    # Plugin selection.
    auth_backend: str = "none"
    auth_config: dict[str, Any] = Field(default_factory=dict)

    model_provider: str = "nvidia"
    model_config_: dict[str, Any] = Field(
        default_factory=lambda: {
            "temperature": 0.4,
            "max_completion_tokens": 8192,
            "top_p": 0.95,
        },
        alias="model_config",
    )
    model_name: str = "nvidia/nemotron-3-super-120b-a12b"

    tool_sources: tuple[str, ...] = (
        "artifacts",
        "web_search",
        "fetch_url",
        "widget_data",
        "inspect_widget_data",
        "pdf_extract",
        "dashboard",
        "recall_user_memory",
        # NVIDIA NIM stack — translation, cross-encoder rerank, and
        # multimodal (chart / image / spreadsheet understanding).
        "translate",
        "rerank",
        "vision_qa",
        "workspace_mcp",
    )

    tool_source_config: dict[str, dict[str, Any]] = Field(default_factory=dict)

    # Subagents and middleware ship enabled by default.
    subagents: tuple[str, ...] = (
        "researcher",
        "charter",
        "analyst",
        "pdf_reader",
    )
    middleware: tuple[str, ...] = (
        "tool_message_normaliser",
        "tool_filter",
        # Surfaces every tool call as a copilotStatusUpdate so the user
        # sees what the agent is doing as it does it.
        "tool_call_announcer",
        "usage_recorder",
        "tool_call_ledger",
        # Short-circuits identical consecutive tool calls (model in a
        # loop): after ``max_repeats`` it returns a synthetic
        # ToolMessage telling the model to stop and answer.
        "loop_guard",
        # Hard caps so a model that gets stuck in a tool-loop terminates
        # gracefully with a final answer instead of streaming forever.
        "call_limit",
        "tool_call_limit",
    )

    # Dynamic skills — list of filesystem paths the deepagents
    # ``SkillsMiddleware`` will scan at run start.
    skills: tuple[str, ...] = ()

    system_prompt_file: str | None = None

    checkpointer_provider: str = "sqlite"
    checkpointer_config: dict[str, Any] = Field(default_factory=dict)

    embeddings_provider: str = "nvidia"
    embeddings_model: str | None = "nvidia/nv-embed-v1"
    embeddings_config: dict[str, Any] = Field(default_factory=dict)

    embeddings_code_provider: str | None = "nvidia-code"
    embeddings_code_model: str | None = "nvidia/nv-embedcode-7b-v1"
    embeddings_code_config: dict[str, Any] = Field(default_factory=dict)

    ingest_char_threshold: int = 2000
    ingest_chunk_chars: int = 1500
    ingest_chunk_overlap: int = 200

    reranker_provider: str | None = "nvidia"
    reranker_model: str | None = "nv-rerank-qa-mistral-4b:1"
    reranker_config: dict[str, Any] = Field(default_factory=dict)
    rerank_fanout: int = 32

    translation_provider: str | None = "nvidia"
    translation_model: str | None = "nvidia/riva-translate-4b-instruct-v1_1"
    translation_config: dict[str, Any] = Field(default_factory=dict)
    translate_for_ingestion: bool = True
    ingest_target_language: str = "English"

    # Persistence: SQLite at ``$HOME/.openbb_platform/agent/history.db``
    # by default. Override with a Postgres URL for multi-worker prod.
    db_url: str | None = None
    data_dir: Path = Path.home() / ".openbb_platform" / "agent"

    # Feature catalog rendered in /agents.json. Operators can override.
    features: dict[str, Any] = Field(default_factory=lambda: dict(DEFAULT_FEATURES))

    metadata: AgentMetadata = Field(default_factory=AgentMetadata)

    profiles: dict[str, dict[str, Any]] = Field(
        default_factory=lambda: {
            "mistral-large-3": {
                "metadata": {
                    "name": "OpenBB · Mistral Large 3 (675B)",
                    "description": (
                        "Long-context Mistral-Large-3 (675B Instruct) on "
                        "NVIDIA NIM, with native vision over uploaded "
                        "images. Larger reasoning headroom for multi-"
                        "document synthesis, quantitative analysis, and "
                        "chart / table OCR. Best with images cropped to "
                        "a near-1:1 aspect ratio."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "mistralai/mistral-large-3-675b-instruct-2512",
                    "config": {
                        "temperature": 0.05,
                        "max_completion_tokens": 16384,
                        "top_p": 0.9,
                    },
                },
                "tool_source_config": {
                    "vision_qa": {
                        "model": "mistralai/mistral-large-3-675b-instruct-2512",
                    },
                },
            },
            "transcribe": {
                "metadata": {
                    "name": "OpenBB · Transcribe (Gemma-3n)",
                    "description": (
                        "Audio / video transcription specialist on "
                        "``google/gemma-3n-e4b-it``. Accepts text + image + "
                        "audio in a single turn, returns text. Use when "
                        "the user attaches an audio or video clip and "
                        "wants a transcript, summary, or per-speaker "
                        "breakdown. 32K-token context, single-channel "
                        "audio."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "google/gemma-3n-e4b-it",
                    "config": {
                        "temperature": 0.05,
                        "max_completion_tokens": 16384,
                        "top_p": 0.9,
                    },
                },
                "tool_sources": [
                    "artifacts",
                    "pdf_extract",
                    "gemma_audio",
                    "paligemma_vision",
                    "inspect_widget_data",
                ],
                # No subagents — single-purpose endpoint.
                "subagents": [],
            },
            "qwen3-coder": {
                "metadata": {
                    "name": "OpenBB · Qwen3 Coder (480B)",
                    "description": (
                        "Code-generation specialist on "
                        "``qwen/qwen3-coder-480b-a35b-instruct`` (480B "
                        "MoE, 35B active). Tuned for OpenBB Platform "
                        "scripting, SQL drafting, and quantitative "
                        "snippets — pair with the ``snowflake`` or "
                        "``mcp_local`` tool sources for end-to-end "
                        "execution."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "qwen/qwen3-coder-480b-a35b-instruct",
                    "config": {
                        "temperature": 0.2,
                        "max_completion_tokens": 16384,
                        "top_p": 0.95,
                    },
                },
                "tool_sources": [
                    "artifacts",
                    "widget_data",
                    "inspect_widget_data",
                    "pdf_extract",
                    "recall_user_memory",
                    "web_search",
                    "workspace_mcp",
                ],
                "subagents": ["analyst"],
            },
            "seed-oss": {
                "metadata": {
                    "name": "OpenBB · Seed-OSS 36B (Thinking Budget)",
                    "description": (
                        "ByteDance Seed-OSS 36B Instruct on NVIDIA NIM "
                        "with a per-turn ``thinking_budget`` cap (1024 "
                        "tokens by default). Lower the budget for "
                        "latency-bound chat; raise it for multi-hop "
                        "synthesis. Same tool surface as the default "
                        "agent."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "bytedance/seed-oss-36b-instruct",
                    "config": {
                        "temperature": 0.3,
                        "max_completion_tokens": 8192,
                        "top_p": 0.9,
                        "extra_body": {"thinking_budget": 1024},
                    },
                },
            },
            "step-flash": {
                "metadata": {
                    "name": "OpenBB · Step 3.5 Flash (Reasoning)",
                    "description": (
                        "StepFun Step-3.5-Flash on NVIDIA NIM — fast "
                        "reasoning model with a configurable "
                        "``reasoning_effort`` enum. Reasoning tokens "
                        "stream live as step-by-step entries in the UI. "
                        "Default effort ``medium``."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "stepfun-ai/step-3.5-flash",
                    "config": {
                        "temperature": 0.4,
                        "max_completion_tokens": 8192,
                        "top_p": 0.9,
                        "extra_body": {"reasoning_effort": "medium"},
                    },
                },
            },
            "minimax-m2": {
                "metadata": {
                    "name": "OpenBB · MiniMax M2.7",
                    "description": (
                        "MiniMax M2.7 on NVIDIA NIM — long-context "
                        "generalist with strong instruction following. "
                        "A third opinion alongside Nemotron and Mistral "
                        "for multi-document synthesis."
                    ),
                },
                "model": {
                    "provider": "nvidia",
                    "name": "minimaxai/minimax-m2.7",
                    "config": {
                        "temperature": 0.4,
                        "max_completion_tokens": 8192,
                        "top_p": 0.9,
                    },
                },
            },
        }
    )
    default_profile: str = "default"

    def resolved_db_url(self) -> str:
        """Resolve the persistence DB URL, defaulting to ``data_dir/history.db``."""
        if self.db_url:
            return self.db_url
        path = self.data_dir / "history.db"
        return f"sqlite+aiosqlite:///{path}"

    def all_profile_names(self) -> tuple[str, ...]:
        """Every profile this server hosts, including the default."""
        names = list(self.profiles.keys())
        if self.default_profile not in names:
            names.insert(0, self.default_profile)
        return tuple(names)

    def resolve_profile(self, name: str | None = None) -> AgentProfile:
        """Resolve a profile name into a fully-populated :class:`AgentProfile`."""
        target = name or self.default_profile
        if target != self.default_profile and target not in self.profiles:
            raise KeyError(f"agent profile {target!r} not configured")
        overlay = self.profiles.get(target) or {}

        meta_overlay = overlay.get("metadata") or {}
        if isinstance(meta_overlay, dict):
            meta = AgentMetadata(
                **{
                    "name": meta_overlay.get("name", self.metadata.name),
                    "description": meta_overlay.get(
                        "description", self.metadata.description
                    ),
                    "image_url": meta_overlay.get("image_url", self.metadata.image_url),
                }
            )
        else:
            meta = self.metadata

        # Per-tool-source kwargs: profile overlay merges over base.
        merged_tool_cfg: dict[str, dict[str, Any]] = {
            k: dict(v) for k, v in self.tool_source_config.items()
        }
        for k, v in (overlay.get("tool_source_config") or {}).items():
            if isinstance(v, dict):
                merged_tool_cfg[k] = {**merged_tool_cfg.get(k, {}), **v}

        model_overlay = overlay.get("model") or {}
        if not isinstance(model_overlay, dict):
            model_overlay = {}
        provider = (
            overlay.get("model_provider")
            or model_overlay.get("provider")
            or self.model_provider
        )
        model_name = (
            overlay.get("model_name")
            if "model_name" in overlay
            else model_overlay.get("name", self.model_name)
        )
        model_cfg_overlay = overlay.get("model_config")
        if model_cfg_overlay is None:
            model_cfg_overlay = model_overlay.get("config")
        if not isinstance(model_cfg_overlay, dict):
            model_cfg_overlay = self.model_config_

        profile_kwargs: dict[str, Any] = {
            "name": target,
            "metadata": meta,
            "model_provider": str(provider),
            "model_name": str(model_name),
            "model_config": dict(model_cfg_overlay),
            "tool_sources": tuple(overlay.get("tool_sources", self.tool_sources)),
            "tool_source_config": merged_tool_cfg,
            "subagents": tuple(overlay.get("subagents", self.subagents)),
            "middleware": tuple(overlay.get("middleware", self.middleware)),
            "skills": tuple(overlay.get("skills", self.skills)),
            "features": dict(overlay.get("features", self.features)),
            "system_prompt_file": _resolve_system_prompt_file(
                overlay, self.system_prompt_file
            ),
        }
        return AgentProfile(**profile_kwargs)

    @classmethod
    def from_toml(  # noqa: PLR0912 — orchestration: walks every promoted key once.
        cls, agent_section: dict[str, Any]
    ) -> AgentServerSettings:
        """Build settings from an ``[agent]`` TOML dict; env vars win."""
        import os as _os

        if not agent_section:
            return cls()

        if "system_prompt" in agent_section:
            raise ValueError(
                "Inline ``system_prompt`` in [agent] is not supported. "
                "Move the prompt to a file and reference it via "
                '``system_prompt_file = "<path>"``.'
            )

        flat: dict[str, Any] = {}
        for k, v in agent_section.items():
            if k in {"auth", "model", "metadata", "features", "profiles"}:
                continue
            flat[k] = v

        if "profiles" in agent_section and isinstance(agent_section["profiles"], dict):
            flat["profiles"] = {
                name: dict(spec)
                for name, spec in agent_section["profiles"].items()
                if isinstance(spec, dict)
            }

        auth = agent_section.get("auth") or {}
        if isinstance(auth, dict):
            if "backend" in auth:
                flat["auth_backend"] = auth["backend"]
            if "config" in auth and isinstance(auth["config"], dict):
                flat["auth_config"] = auth["config"]

        model = agent_section.get("model") or {}
        if isinstance(model, dict):
            if "provider" in model:
                flat["model_provider"] = model["provider"]
            if "name" in model:
                flat["model_name"] = model["name"]
            if "config" in model and isinstance(model["config"], dict):
                flat["model_config"] = model["config"]

        if "features" in agent_section and isinstance(agent_section["features"], dict):
            flat["features"] = dict(agent_section["features"])

        if "metadata" in agent_section and isinstance(agent_section["metadata"], dict):
            flat["metadata"] = AgentMetadata(**agent_section["metadata"])

        if "data_dir" in flat:
            flat["data_dir"] = Path(flat["data_dir"]).expanduser()

        env_winning = {
            k for k in list(flat) if f"OPENBB_AGENT_{k.upper()}" in _os.environ
        }
        for k in env_winning:
            flat.pop(k, None)

        return cls(**flat)
