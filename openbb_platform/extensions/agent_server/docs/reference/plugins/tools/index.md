# `openbb_agent_server.plugins.tools`

Built-in tool sources. Each module exposes one `ToolSource` subclass that the plugin discovery system instantiates and binds at request time; the source's `tools(ctx, config)` returns the LangChain `BaseTool` instances the model sees that turn.

**Source:** [`openbb_agent_server/plugins/tools/__init__.py`](../../../../openbb_agent_server/plugins/tools/__init__.py)

## Tool sources

### Artifact / citation surface

| Source | Summary |
| --- | --- |
| [`artifacts`](artifacts.md) | `emit_html_artifact` / `emit_markdown_artifact` / `emit_table_artifact` / `emit_chart_artifact` / `emit_reasoning_step` / `cite_source`. Markdown sanitiser strips scratchpad sections (Session Intent / Tool activity / Next steps / Sources / Citations / References) and inline tool-call lists. |

### Data access

| Source | Summary |
| --- | --- |
| [`widget_data`](widget_data.md) | `list_widgets` (inventory with content hashes) and `get_widget_data` (server-side dispatcher that yields a `FunctionCallSSE` so Workspace fetches the bytes). |
| [`inspect_widget_data`](inspect_widget_data.md) | Per-run inspection over already-fetched widgets — `list_widget_data`, `read_widget_data`, `search_widget_data`, `describe_widget_data`, `query_widget_data` (SQL with auto-citation). |
| [`pdf_extract`](pdf_extract.md) | `list_pdfs`, `get_pdf_outline`, `search_pdf`, `pdf_extract` with `pdfplumber`-backed caching and citation auto-emit. |
| [`memory_recall`](memory_recall.md) | `recall_user_memory(query, k=8)` against the configured `MemoryStore`, principal-scoped. |
| [`web_search`](web_search.md) | DuckDuckGo or Tavily backend, feature-gated by the `search-web` Workspace option. Auto-cites every result. Security: every snippet is untrusted DATA. |

### Multimodal

| Source | Summary |
| --- | --- |
| [`vision_qa`](vision_qa.md) | `understand_image` over a vision-capable NIM chat model (default Llama-3.1-Nemotron Nano VL). General-purpose chart / table / OCR Q&A. Background-job variant included. |
| [`paligemma_vision`](paligemma_vision.md) | PaliGemma task-prefix style — `caption_image`, `read_image_text` (OCR), `ask_about_image` (VQA). Short factual answers. Background-job variants for each. |
| [`gemma_audio`](gemma_audio.md) | `transcribe_audio` over NIM Gemma-3n-E4B with auto-segmentation via ffmpeg. Long clips stream partial transcripts back as reasoning steps. |
| [`groq_audio`](groq_audio.md) | `transcribe_audio` / `translate_audio` via Groq Whisper. Returns text plus optional segment / word timestamps. |
| [`gemini_image`](gemini_image.md) | `generate_image` / `edit_image` over Google GenAI (Gemini multimodal + Imagen backends). Emits image artifacts. |
| [`gemini_embeddings`](gemini_embeddings.md) | `embed_text` via Google GenAI as an alternative to NVIDIA NIM embedders. |

### NIM specialists

| Source | Summary |
| --- | --- |
| [`translate`](translate.md) | `translate(text, source_language, target_language)` over NVIDIA Riva. Markdown and code fences preserved. |
| [`rerank`](rerank.md) | `rerank(query, candidates, top_k)` cross-encoder reranker. Fallback returns input order on backend failure. |

### MCP integration

| Source | Summary |
| --- | --- |
| [`mcp_local`](mcp_local.md) | Spawns `openbb-mcp` over stdio and surfaces every OpenBB Platform command as a LangChain tool. |
| [`mcp_http`](mcp_http.md) | Connects to a remote MCP server URL (streamable_http / sse / websocket). |
| [`workspace_mcp`](workspace_mcp.md) | Forwards the user's enabled MCP tools through Workspace — each call pauses the run and resumes with Workspace's MCP response. |

### Client-side dispatch

| Source | Summary |
| --- | --- |
| [`client_side`](client_side.md) | The interrupt-based primitive for tools that run inside Workspace (`client:<name>`). |
| [`dashboard`](dashboard.md) | Built on `client_side` — `open_widget`, `change_dashboard`, `highlight_widget`, `add_widget_to_dashboard`. |

### Orchestration

| Source | Summary |
| --- | --- |
| [`background_jobs`](background_jobs.md) | `list_background_jobs`, `check_job`, `wait_for_job`, `cancel_job`. Required by every `submit_*` background variant. |
| [`python_module`](python_module.md) | Import a Python module by dotted path and register its `BaseTool` instances. |

### Data warehouse

| Source | Summary |
| --- | --- |
| [`snowflake_tools/`](snowflake_tools/index.md) | Snowflake — query, catalog introspection, and the full Cortex AI surface (complete / summarise / sentiment / translate / classify / extract / embed / search / analyst). |

## Internal helpers

| Module | Summary |
| --- | --- |
| [`_media`](_media.md) | Async media helpers (`fetch_url`, `to_data_url`, `probe_audio_duration`, `split_audio_bytes`, `flatten_message_content`). Shared by every multimodal source; not a tool source itself. |

## Related

- [Writing a tool source](../../../developing/writing-a-tool-source.md) — the `ToolSource` contract.
- [Operating: configuration](../../../operating/configuration.md) — `[agent.tool_sources]` binding and per-source config blocks.
- [`runtime/emit.py`](../../runtime/emit.md) — the SSE wire format every tool ultimately writes to.
