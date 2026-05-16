# `openbb_agent_server.plugins.tools._media`

Async media helpers shared by every multimodal tool source — URL fetch with a hard size cap, base64 data-URL encoding off the event loop, audio probe / split via ffmpeg, and a `flatten_message_content` helper for LangChain streaming. Not a tool source itself; nothing here registers tools.

**Source:** [`openbb_agent_server/plugins/tools/_media.py`](../../../../openbb_agent_server/plugins/tools/_media.py)

## Exceptions

| Class | When raised |
| --- | --- |
| `MediaError` | Base class for media-pipeline failures (RuntimeError subclass). |
| `MediaTooLargeError` | Remote / encoded payload exceeded the caller's `max_bytes` budget. |
| `MediaFetchError` | HTTP fetch failed (httpx error during `stream`). |
| `FFmpegUnavailableError` | `ffmpeg` / `ffprobe` not on `$PATH` but required for the requested operation. |

## Dataclasses

| Class | Fields |
| --- | --- |
| `FetchedMedia` | `data: bytes`, `mime: str`, `size_bytes` (property = `len(data)`). |
| `AudioSegment` | `index: int`, `total: int`, `start_s: float`, `end_s: float`, `data: bytes`, `mime: str`. |

Both are frozen dataclasses.

## Functions

### `async def fetch_url(url, *, max_bytes, timeout_s=60.0, fallback_mime="application/octet-stream") -> FetchedMedia`

Stream `url` to bytes with a hard size cap. Imports `httpx` lazily (raises `MediaError` with an install hint if missing). Streams via `client.stream("GET", url)` so partial payloads can be cut short — `MediaTooLargeError` is raised the moment `len(buf) + len(chunk) > max_bytes`. Detects the mime from `Content-Type`, falling back to `mimetypes.guess_type(url)`, finally to `fallback_mime`. HTTP errors are wrapped as `MediaFetchError`.

### `async def to_data_url(raw, *, mime, max_bytes=None) -> str`

Encode `raw` as `data:<mime>;base64,<…>` off the event loop via `asyncio.to_thread`. Optionally enforces a second size cap (separate from the fetch cap). Raises `MediaTooLargeError` if `len(raw) > max_bytes`.

### `async def probe_audio_duration(raw) -> float`

Write `raw` to a temp file (cleaned in `finally`) and run `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 -i <path>`. Returns the parsed duration in seconds. Raises `MediaError` if `ffprobe` returns non-zero or non-numeric output. Requires `ffprobe` on `$PATH`.

### `async def split_audio_bytes(raw, *, max_seconds, target_codec="libopus", target_container="ogg") -> list[AudioSegment]`

Probe duration, then either:

- Return one `AudioSegment` (covering `0..duration`) when `duration <= max_seconds`.
- Compute `ceil(duration / max_seconds)` even-length cut points and run `ffmpeg` once per segment, re-encoding to mono 24 kHz @ 32 kbit/s Opus / Ogg by default. `_ffmpeg_split` streams the input via stdin and reads the segment bytes from stdout; non-zero exit raises `MediaError`.

### `def ffmpeg_available() -> bool`

Return `True` iff both `ffmpeg` **and** `ffprobe` are on `$PATH`. Useful in feature-detection paths.

### `def flatten_message_content(content) -> str`

Collapse a LangChain message `content` field to a single string. Handles three shapes:

- `list` of blocks → concatenate `text` blocks and string blocks; drop everything else.
- non-list, non-falsy → `str(content)`.
- falsy → `""`.

Used by every audio / vision tool that streams `ChatNVIDIA` chunks: `getattr(chunk, "content", "")` may be a list of typed blocks, and the tool needs a plain string to return.

## Related

- [`vision_qa`](vision_qa.md), [`paligemma_vision`](paligemma_vision.md) — use `fetch_url` + `to_data_url`.
- [`gemma_audio`](gemma_audio.md) — uses `split_audio_bytes` + `probe_audio_duration`.
- [`groq_audio`](groq_audio.md) — does its own (sync) fetching but follows the same `max_bytes` / timeout discipline.
