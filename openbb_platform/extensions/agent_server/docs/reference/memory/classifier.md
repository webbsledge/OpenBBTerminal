# `openbb_agent_server.memory.classifier`

Cheap heuristic for routing ingested content between the prose and code embedders. Picks a verdict from filename / MIME / token density, in that order.

**Source:** [`openbb_agent_server/memory/classifier.py`](../../../openbb_agent_server/memory/classifier.py)

## `def looks_like_code(text, *, filename=None, mime=None) -> bool`

Return `True` when `text` should be embedded by the code model (`nvidia/nv-embedcode-7b-v1` by default) rather than the prose model. Used by [`memory/ingestion.md`](ingestion.md) to set the `kind` field on each chunk; `SqliteMemoryStore` then routes `_code`-suffixed kinds to the `memories_code` table.

### Decision order

1. **Filename match.** If `filename` ends with any extension in the built-in set OR equals one of `dockerfile` / `makefile` / `rakefile` / `vagrantfile`, return `True` immediately.
2. **MIME match.** If `mime` is in the built-in set, return `True`.
3. **Empty text.** Return `False`.
4. **Token-density signal.** Count occurrences of a code-token regex over the first 4 KiB of `text`. The text is code iff `30 × n_tokens >= len(sample)` — roughly one code token per 30 characters.

### Code extensions covered

- **General-purpose languages.** `.py`, `.pyi`, `.ipynb`, `.js`, `.mjs`, `.cjs`, `.jsx`, `.ts`, `.tsx`, `.go`, `.rs`, `.java`, `.kt`, `.kts`, `.scala`, `.clj` / `.cljs` / `.cljc`, `.c`, `.cc`, `.cpp`, `.cxx`, `.h`, `.hh`, `.hpp`, `.hxx`, `.cs`, `.fs`, `.fsx`, `.vb`, `.rb`, `.php`, `.pl`, `.pm`, `.lua`, `.r`, `.swift`, `.m`, `.mm`, `.dart`, `.ex`, `.exs`, `.erl`, `.hrl`, `.elm`, `.sh`, `.bash`, `.zsh`, `.fish`, `.ps1`, `.sql`, `.graphql`, `.gql`.
- **Structured data / config** (code-like enough that the code embedder beats prose on retrieval). `.json`, `.yaml`, `.yml`, `.toml`, `.ini`, `.cfg`, `.conf`, `.xml`, `.html`, `.htm`, `.css`, `.scss`, `.sass`, `.less`, `.csv`, `.tsv`, `.psv`, `.xlsx`, `.xlsm`, `.xls`, `.xlsb`, `.ods`, `.parquet`, `.feather`, `.arrow`.
- **Build / infra.** `.dockerfile`, `.tf`, `.tfvars`, `.hcl`, `.bzl`, `.gradle`, `.cmake`, `.makefile`, `.mk`.

### MIME types covered

`application/json`, `application/xml`, `application/x-yaml`, `application/yaml`, `application/x-toml`, `application/toml`, `application/javascript`, `application/x-python`, `application/x-typescript`, `application/x-sh`, `application/sql`, `application/graphql`, `text/x-python`, `text/x-c`, `text/x-csrc`, `text/x-c++src`, `text/x-java`, `text/x-rust`, `text/x-go`, `text/x-shellscript`, `text/javascript`, `text/css`, `text/html`, `text/x-sql`, plus the spreadsheet / tabular MIMEs (`text/csv`, `text/tab-separated-values`, `application/vnd.ms-excel`, the `openxmlformats` spreadsheet variants, `application/vnd.oasis.opendocument.spreadsheet`, `application/x-parquet`, `application/vnd.apache.arrow.file` / `.stream`).

### Density regex

```
\b(def|class|function|fn|func|return|if|else|elif|for|while|
import|from|export|const|let|var|public|private|static|
struct|enum|interface|trait|impl|null|true|false|None|True|False)\b
|[{}\[\]<>();=]|::|=>|->|<-|//|/\*|\*/|#include|@\w+
```

A few keyword hits combined with bracket / arrow density is usually enough — natural-language prose rarely crosses the threshold.

### See also

- [`memory/ingestion.md`](ingestion.md) — caller.
- [`memory/sqlite_store.md`](sqlite_store.md) — code vs. text routing.
