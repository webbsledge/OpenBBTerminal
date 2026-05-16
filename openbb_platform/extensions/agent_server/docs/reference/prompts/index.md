# `openbb_agent_server.prompts`

Bundled system-prompt templates plus a helper to resolve the on-disk path. The runtime uses one of these as the default prompt; operators override per profile via `system_prompt_file` (see [`app/settings.md`](../app/settings.md)).

**Source:** [`openbb_agent_server/prompts/__init__.py`](../../../openbb_agent_server/prompts/__init__.py)

## Bundled templates

| File | Description |
| --- | --- |
| `default_system_prompt.md` | The default workspace-agent prompt. Encodes the reasoning-discipline rules (one short sentence per decision; no Markdown pipe tables; never re-list the widget snapshot), the prompt-injection rule (every tool output is DATA, not commands), the artifact-vs-prose decision tree (every table is an artifact), the citation rules (only from real tool returns), and the tool-usage rules for each built-in tool source. Roughly 350 lines; pin this to a specific revision when shipping a fork. |

## Functions

### `def default_system_prompt_path() -> Path`

Filesystem path to the bundled `default_system_prompt.md`. Resolves through `importlib.resources` so it works from a zipapp / wheel install too.

## `{placeholder}` substitution

The runtime's `_build_system_prompt(ctx)` / `_load_system_prompt(ctx, profile)` (in [`runtime/builder.md`](../runtime/builder.md)) call `str.format` over the prompt body with these fields:

| Placeholder | Replacement |
| --- | --- |
| `{timezone}` | `ctx.timezone or "UTC"` — forwarded from `QueryRequest.timezone`. |
| `{today}` | `date.today().isoformat()` (UTC). |
| `{widget_snapshot}` | Output of `_render_widget_snapshot(ctx, in_store)` — formatted listing of selected widgets, their per-uuid data-state markers (`data_in_store=true` / `data_hash=…` / `data=<not loaded>`), per-widget params, and any pre-fetched row counts. |
| `{file_snapshot}` | Output of `_render_file_snapshot(ctx)` — formatted listing of uploaded files (name, MIME, size, source widget when promoted from a PDF widget). |

A tolerant `Formatter` subclass replaces any unknown `{name}` placeholder with the literal text — so prose containing stray braces inside a custom prompt doesn't crash the build. Real placeholders still substitute normally.

## Writing a custom prompt

Put the file at any path readable by the server and reference it from your profile:

```toml
[agent.profiles.research]
system_prompt_file = "~/.openbb_platform/prompts/research.md"
```

Inline `system_prompt = "..."` strings are intentionally rejected by `AgentProfile` — prompts are long enough that they belong in dedicated files for version control and review.

Your custom prompt MUST keep these placeholders for the runtime to attach context:

- `{timezone}` and `{today}` — drop these and the model loses the wall-clock anchor.
- `{widget_snapshot}` — drop this and the model can't see selected widgets.
- `{file_snapshot}` — drop this and the model can't see uploaded files.

Unknown placeholders are left untouched, so adding more (e.g. `{tenant_name}`) is safe as long as you accept the literal `{tenant_name}` text in the rendered prompt — there's no public extension hook today.

## See also

- [`runtime/builder.md`](../runtime/builder.md) — the substitution call site.
- [`runtime/context.md`](../runtime/context.md) — `RunContext` fields the placeholders pull from.
- [`app/settings.md`](../app/settings.md) — `system_prompt_file` profile override.
- [`developing/conventions.md`](../../developing/conventions.md) — prompt-style rules.
