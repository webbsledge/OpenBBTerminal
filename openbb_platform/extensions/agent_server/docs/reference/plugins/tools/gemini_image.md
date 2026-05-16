# `openbb_agent_server.plugins.tools.gemini_image`

Text-to-image generation and image editing via Google GenAI. Two backends: the Gemini multimodal model (`gemini-2.5-flash-image`) and Imagen (`imagen-4.0-generate-001`). Both emit one or more image artifacts through [`emit.image_artifact`](../../runtime/emit.md), which Workspace renders inline.

**Source:** [`openbb_agent_server/plugins/tools/gemini_image.py`](../../../../openbb_agent_server/plugins/tools/gemini_image.py)

## Classes

### `GeminiImageToolSource`

Plugin entry-point name: `gemini_image`. Constructor takes `api_key`, `default_generate_model`, `default_edit_model`, `default_imagen_model`, `timeout`, `max_retries`. `tools(ctx, config)` resolves the api key (`ctx.api_keys["GOOGLE_API_KEY"]` → `ctx.api_keys["GEMINI_API_KEY"]` → config → constructor); raises `RuntimeError` at tool-build time if absent.

| Tool | Args | Returns |
| --- | --- | --- |
| `generate_image` | `prompt: str` (text prompt), `model: str \| None` (override the per-backend default), `backend: str = "gemini"` (`"gemini"` multimodal or `"imagen"` dedicated image API), `aspect_ratio: str \| None` (Imagen only — `1:1` / `3:4` / `4:3` / `9:16` / `16:9`), `number_of_images: int = 1` (∈ [1, 4]; Imagen up to 4), `negative_prompt: str \| None` (Imagen only), `seed: int \| None` | `{model, backend, prompt, image_count, artifact_uuids, byte_sizes}`. The images render automatically in Workspace via the emitted artifacts. |
| `edit_image` | `prompt: str` (edit instruction, e.g. `"add a red scarf"`), `base_image_name: str \| None` (uploaded file), `base_image_url: str \| None` (public URL), `model: str \| None`, `seed: int \| None` | `{model, prompt, image_count, artifact_uuids, byte_sizes}`. |

### Backend selection

- `generate_image(backend="gemini")` → `client.models.generate_content(model, contents=prompt)` and pulls inline image parts out of the response.
- `generate_image(backend="imagen")` → `client.models.generate_images(model, prompt, config=GenerateImagesConfig(...))` and reads `generated_images[].image.image_bytes`. `aspect_ratio`, `negative_prompt`, `seed`, and `number_of_images` are Imagen-only.
- `edit_image` always uses Gemini multimodal (`generate_content` with the base image as a `Part.from_bytes` inline part).

Empty responses (safety blocks, text-only outputs) raise `RuntimeError` with the cause spelled out.

### Image resolution for edits

`_resolve_uploaded_image` mirrors the audio resolution path: prefer `base_image_url` if set, else look up `base_image_name` in `RunContext.uploaded_files`; `data_base64` decodes inline, otherwise `ref.url` is fetched once via a sync `httpx.Client(timeout=timeout)`.

### Retry loop

`_retry` runs the synchronous backends in a thread (`asyncio.to_thread`) and retries on:

- HTTP 429 / 5xx (read from `exc.code` or `exc.status_code`).
- `httpx.TimeoutException` / `ConnectError`.
- Exception class names `ServerError` / `ResourceExhausted` / `DeadlineExceeded` (Gemini wraps everything in `ClientError`).

`_retry_delay` honours a `retry_delay` field in `ClientError.details.error.details[].retryDelay` when present, otherwise falls back to exponential backoff (`0.5, 1, 2, 4, 8` s, capped at 30 s).

## Side effects

Every produced image is emitted via `emit.image_artifact(name="gemini_image:<hex>", description=prompt[:240], mime, data_base64)`. The Workspace UI renders the artifact inline. The function returns the artifact uuids and byte sizes so the agent can reference them in the textual reply.

## Validation

- `backend` ∈ `{"gemini", "imagen"}`.
- `aspect_ratio` ∈ `{"1:1", "3:4", "4:3", "9:16", "16:9"}` when set.
- `number_of_images` ∈ [1, 4] (pydantic-enforced).
- `max_retries >= 0` and `timeout > 0` (constructor-enforced).

## Config

`[agent.tool_source_config.gemini_image]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `api_key` | string | `None` | Falls back to `GOOGLE_API_KEY` / `GEMINI_API_KEY` in `ctx.api_keys`. |
| `default_generate_model` | string | `"gemini-2.5-flash-image"` | Used when `backend="gemini"` and no per-call `model` is set. |
| `default_edit_model` | string | `"gemini-2.5-flash-image"` | Used by `edit_image` when no per-call `model` is set. |
| `default_imagen_model` | string | `"imagen-4.0-generate-001"` | Used when `backend="imagen"` and no per-call `model` is set. |
| `timeout` | float | `120.0` | Per-request timeout. |
| `max_retries` | int | `5` | Backoff retries for 429 / 5xx / connection errors. |

Requires the `[google_genai]` extra (`google-genai` package).

## Related

- [`gemini_embeddings` tool source](gemini_embeddings.md) — sibling Gemini surface for text vectors.
- [`runtime/emit.py`](../../runtime/emit.md) — `image_artifact` wire shape.
- [Operating: configuration](../../../operating/configuration.md) — forwarding API keys via `QueryRequest.api_keys`.
