# `openbb_agent_server.plugins.tools.fetch_url`

SSRF-guarded fetch of a single web page's readable text. `web_search` returns snippets; `fetch_url` lets the agent actually read the article behind a URL.

**Source:** [`openbb_agent_server/plugins/tools/fetch_url.py`](../../../../openbb_agent_server/plugins/tools/fetch_url.py)

## Classes

### `FetchUrlToolSource`

Plugin entry-point name: `fetch_url`. In the default `tool_sources`. `tools(ctx, config)` registers one `StructuredTool`, and only when the user has enabled the `fetch-url` Workspace feature — its own per-user toggle (`FETCH_URL_FEATURE`), independent of `web_search`'s `search-web` gate.

| Tool | Args | Returns |
| --- | --- | --- |
| `fetch_url(url)` | `url: str` — absolute http(s) URL | `{url, final_url, status, content_type, text, truncated}` on success; `{url, final_url, status, content_type, note}` for non-text bodies; `{error, url}` when the fetch is refused or fails. |

The fetched page is auto-emitted as one `cite()` (anchored to `final_url`). Extracted text is capped at 20 000 characters (`truncated` flags when more was dropped).

## SSRF guard

Every fetch — and **every redirect hop** — passes these checks before a request is made:

- **Scheme** — only `http` / `https`. `file:`, `ftp:`, `gopher:`, etc. are refused.
- **Address** — the host is resolved via `getaddrinfo`, and every resolved IP is checked. Refused if the IP is private (RFC-1918), loopback, link-local (which includes the `169.254.169.254` cloud-metadata endpoint), multicast, reserved, unspecified, or otherwise not globally routable. IPv4-mapped IPv6 addresses are unwrapped so a private v4 can't hide inside a v6 literal.
- **Redirects** — followed manually (`follow_redirects=False`), re-validating each hop's URL. A public URL therefore cannot bounce the fetch onto an internal address. Capped at 5 hops.
- **Body size** — streamed with a 2 MiB hard cap; the request aborts past it.
- **Timeout** — 20 s wall-clock per request.

A blocked or failed fetch returns `{error, url}` — it never raises into the agent loop.

## Content handling

`text/html` / `application/xhtml+xml` are run through a stdlib `HTMLParser` text extractor that drops `script` / `style` / `noscript` / `template` / `svg`. `text/*` and `application/json` are returned as-is. Any other content type returns a `note` directing the agent to `pdf_extract` for PDFs — binary bytes are never dumped into the response.

## Config

`[agent.tool_source_config.fetch_url]` is currently empty — the size cap, redirect limit, and timeout are module constants. The tool is feature-gated, not config-gated.
