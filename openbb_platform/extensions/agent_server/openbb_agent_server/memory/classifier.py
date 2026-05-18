"""Code / prose classifier for ingestion routing."""

from __future__ import annotations

import re

_CODE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".py",
        ".pyi",
        ".ipynb",
        ".js",
        ".mjs",
        ".cjs",
        ".jsx",
        ".ts",
        ".tsx",
        ".go",
        ".rs",
        ".java",
        ".kt",
        ".kts",
        ".scala",
        ".clj",
        ".cljs",
        ".cljc",
        ".c",
        ".cc",
        ".cpp",
        ".cxx",
        ".h",
        ".hh",
        ".hpp",
        ".hxx",
        ".cs",
        ".fs",
        ".fsx",
        ".vb",
        ".rb",
        ".php",
        ".pl",
        ".pm",
        ".lua",
        ".r",
        ".swift",
        ".m",
        ".mm",
        ".dart",
        ".ex",
        ".exs",
        ".erl",
        ".hrl",
        ".elm",
        ".sh",
        ".bash",
        ".zsh",
        ".fish",
        ".ps1",
        ".sql",
        ".graphql",
        ".gql",
        ".json",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
        ".conf",
        ".xml",
        ".html",
        ".htm",
        ".css",
        ".scss",
        ".sass",
        ".less",
        ".csv",
        ".tsv",
        ".psv",
        ".xlsx",
        ".xlsm",
        ".xls",
        ".xlsb",
        ".ods",
        ".parquet",
        ".feather",
        ".arrow",
        ".dockerfile",
        ".tf",
        ".tfvars",
        ".hcl",
        ".bzl",
        ".gradle",
        ".cmake",
        ".makefile",
        ".mk",
    }
)

_CODE_MIMES: frozenset[str] = frozenset(
    {
        "application/json",
        "application/xml",
        "application/x-yaml",
        "application/yaml",
        "application/x-toml",
        "application/toml",
        "application/javascript",
        "application/x-python",
        "application/x-typescript",
        "application/x-sh",
        "application/sql",
        "application/graphql",
        "text/x-python",
        "text/x-c",
        "text/x-csrc",
        "text/x-c++src",
        "text/x-java",
        "text/x-rust",
        "text/x-go",
        "text/x-shellscript",
        "text/javascript",
        "text/css",
        "text/html",
        "text/x-sql",
        "text/csv",
        "text/tab-separated-values",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
        "application/vnd.ms-excel.sheet.macroenabled.12",
        "application/vnd.ms-excel.sheet.binary.macroenabled.12",
        "application/vnd.oasis.opendocument.spreadsheet",
        "application/x-parquet",
        "application/vnd.apache.arrow.file",
        "application/vnd.apache.arrow.stream",
    }
)

_CODE_TOKENS = re.compile(
    r"\b(def|class|function|fn|func|return|if|else|elif|for|while|"
    r"import|from|export|const|let|var|public|private|static|"
    r"struct|enum|interface|trait|impl|null|true|false|None|True|False)\b"
    r"|[{}\[\]<>();=]|::|=>|->|<-|//|/\*|\*/|#include|@\w+"
)


def looks_like_code(
    text: str,
    *,
    filename: str | None = None,
    mime: str | None = None,
) -> bool:
    """Return True when ``text`` should be embedded by the code model."""
    if filename:
        lower = filename.lower()
        for ext in _CODE_EXTENSIONS:
            if lower.endswith(ext):
                return True
        base = lower.rsplit("/", 1)[-1]
        if base in {"dockerfile", "makefile", "rakefile", "vagrantfile"}:
            return True

    if mime and mime.lower() in _CODE_MIMES:
        return True

    if not text:
        return False

    sample = text[:4000]
    if not sample.strip():
        return False
    n_tokens = sum(1 for _ in _CODE_TOKENS.finditer(sample))
    return n_tokens * 30 >= len(sample)
