"""Context ingestion — chunk + embed long content per turn."""

from __future__ import annotations

import base64
import logging
from collections.abc import Iterable
from typing import Any

from langchain_text_splitters import (
    Language,
    RecursiveCharacterTextSplitter,
)

from openbb_agent_server.memory.classifier import looks_like_code
from openbb_agent_server.memory.store import MemoryStore
from openbb_agent_server.memory.translation import NvidiaTranslator
from openbb_agent_server.runtime.principal import UserPrincipal

_EXT_TO_LANGUAGE: dict[str, Language] = {
    ".py": Language.PYTHON,
    ".pyi": Language.PYTHON,
    ".ipynb": Language.PYTHON,
    ".js": Language.JS,
    ".mjs": Language.JS,
    ".cjs": Language.JS,
    ".jsx": Language.JS,
    ".ts": Language.TS,
    ".tsx": Language.TS,
    ".go": Language.GO,
    ".rs": Language.RUST,
    ".java": Language.JAVA,
    ".kt": Language.KOTLIN,
    ".kts": Language.KOTLIN,
    ".scala": Language.SCALA,
    ".c": Language.C,
    ".h": Language.C,
    ".cc": Language.CPP,
    ".cpp": Language.CPP,
    ".cxx": Language.CPP,
    ".hh": Language.CPP,
    ".hpp": Language.CPP,
    ".cs": Language.CSHARP,
    ".rb": Language.RUBY,
    ".php": Language.PHP,
    ".pl": Language.PERL,
    ".pm": Language.PERL,
    ".swift": Language.SWIFT,
    ".lua": Language.LUA,
    ".r": Language.R,
    ".ex": Language.ELIXIR,
    ".exs": Language.ELIXIR,
    ".sol": Language.SOL,
    ".ps1": Language.POWERSHELL,
    ".html": Language.HTML,
    ".htm": Language.HTML,
    ".md": Language.MARKDOWN,
    ".tex": Language.LATEX,
}


def _detect_language(filename: str | None) -> Language | None:
    if not filename:
        return None
    lower = filename.lower()
    for ext, lang in _EXT_TO_LANGUAGE.items():
        if lower.endswith(ext):
            return lang
    return None


def _likely_non_english(text: str, *, target_lang: str = "English") -> bool:
    """Quick heuristic: does this chunk look like it needs translation?"""
    if target_lang.strip().lower() != "english":
        return True
    sample = text[:4000]
    if not sample:
        return False
    non_ascii = sum(1 for ch in sample if ord(ch) > 127)
    # ≥5% non-ASCII → likely non-English.
    return non_ascii * 20 >= len(sample)


logger = logging.getLogger("openbb_agent_server.memory.ingestion")


def chunk_text(
    text: str,
    *,
    chunk_chars: int = 1500,
    overlap: int = 200,
    language: Language | None = None,
) -> list[str]:
    """Split ``text`` into semantic chunks using LangChain's recursive splitter."""
    if chunk_chars <= 0:
        raise ValueError("chunk_chars must be positive")
    if overlap < 0 or overlap >= chunk_chars:
        raise ValueError("overlap must be in [0, chunk_chars)")
    if not text.strip():
        return []
    if language is not None:
        splitter = RecursiveCharacterTextSplitter.from_language(
            language=language,
            chunk_size=chunk_chars,
            chunk_overlap=overlap,
        )
    else:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_chars,
            chunk_overlap=overlap,
        )
    return [c for c in splitter.split_text(text) if c.strip()]


_TEXT_MIMES: frozenset[str] = frozenset(
    {
        "application/json",
        "application/xml",
        "application/x-yaml",
        "application/yaml",
        "application/x-toml",
        "application/toml",
    }
)

_TEXT_EXTS: tuple[str, ...] = (
    ".md",
    ".txt",
    ".csv",
    ".tsv",
    ".psv",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".html",
    ".htm",
)


def _decode_bytes_to_text(raw: bytes) -> str:
    """Decode bytes preferring UTF-8 with a latin-1 fallback."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _load_via_loader(path: str, ext: str) -> str | None:
    """Route a tempfile through the matching LangChain document loader."""
    from langchain_community.document_loaders import (
        BSHTMLLoader,
        CSVLoader,
        JSONLoader,
        TextLoader,
    )

    loader: Any
    try:
        if ext == ".csv":
            loader = CSVLoader(file_path=path)
        elif ext in {".tsv", ".psv"}:
            sep = "\t" if ext == ".tsv" else "|"
            loader = CSVLoader(file_path=path, csv_args={"delimiter": sep})
        elif ext in {".html", ".htm"}:
            loader = BSHTMLLoader(file_path=path)
        elif ext == ".json":
            loader = JSONLoader(file_path=path, jq_schema=".", text_content=False)
        else:
            loader = TextLoader(file_path=path, autodetect_encoding=True)
        docs = loader.load()
    except Exception:
        return None
    return "\n\n".join(d.page_content for d in docs if d.page_content)


def _decode_file_text(name: str, mime: str, b64: str | None) -> str | None:
    """Decode an uploaded file into plaintext using LangChain document loaders."""
    if not b64:
        return None
    lower_name = name.lower()
    lower_mime = (mime or "").lower()
    text_like = (
        lower_mime.startswith("text/")
        or lower_mime in _TEXT_MIMES
        or lower_name.endswith(_TEXT_EXTS)
    )
    if not text_like:
        return None
    try:
        raw = base64.b64decode(b64, validate=False)
    except (ValueError, TypeError):
        logger.warning("ingest: skipping %s — base64 decode failed", name)
        return None

    ext = ""
    for candidate in _TEXT_EXTS:
        if lower_name.endswith(candidate):
            ext = candidate
            break

    import contextlib
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=ext or ".txt", delete=False) as f:
        f.write(raw)
        path = f.name
    try:
        try:
            loaded = _load_via_loader(path, ext)
        except Exception:
            loaded = None
        if loaded is not None:
            return loaded
        return _decode_bytes_to_text(raw)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(path)


def _iter_long_messages(
    messages: Iterable[Any], threshold: int
) -> Iterable[tuple[int, str]]:
    """Yield ``(index, text)`` for human messages whose content exceeds threshold."""
    for idx, m in enumerate(messages):
        role = getattr(m, "role", None) or (
            m.get("role") if isinstance(m, dict) else None
        )
        if role != "human":
            continue
        content = getattr(m, "content", None)
        if content is None and isinstance(m, dict):
            content = m.get("content")
        if not isinstance(content, str):
            continue
        if len(content) >= threshold:
            yield idx, content


async def ingest_request_context(
    *,
    principal: UserPrincipal,
    store: MemoryStore,
    body: Any,
    trace_id: str,
    char_threshold: int = 2000,
    chunk_chars: int = 1500,
    chunk_overlap: int = 200,
    translator: NvidiaTranslator | None = None,
    translate_target_lang: str = "English",
) -> int:
    """Ingest long content from a ``QueryRequest`` into the memory store."""
    if not principal.has_scope("memory:write"):
        logger.debug(
            "ingest: skipping — principal lacks memory:write scope (user_id=%s)",
            principal.user_id,
        )
        return 0

    written = 0
    # Each source carries enough metadata (filename + mime) for the
    # classifier to pick text vs. code embedders correctly.
    sources: list[tuple[str, str, str | None, str | None]] = []
    # tuple: (source_label, full_text, filename, mime)

    for f in getattr(body, "uploaded_files", []) or []:
        fname = getattr(f, "name", "") or "uploaded_file"
        fmime = getattr(f, "mime", "") or ""
        text = _decode_file_text(fname, fmime, getattr(f, "data_base64", None))
        if text and len(text) >= char_threshold:
            sources.append((f"file:{fname}", text, fname, fmime))

    for idx, text in _iter_long_messages(
        getattr(body, "messages", []) or [], char_threshold
    ):
        sources.append((f"message:{idx}", text, None, None))

    if not sources:
        return 0

    logger.debug(
        "ingest: %d source(s) over %d chars; chunking…",
        len(sources),
        char_threshold,
    )

    for label, full_text, fname, fmime in sources:
        is_code = looks_like_code(full_text, filename=fname, mime=fmime)
        kind = "context_code" if is_code else "context_text"
        language = _detect_language(fname) if is_code else None
        chunks = chunk_text(
            full_text,
            chunk_chars=chunk_chars,
            overlap=chunk_overlap,
            language=language,
        )
        for ci, chunk in enumerate(chunks):
            stored_text = chunk
            header_extra = ""

            if (
                translator is not None
                and not is_code
                and _likely_non_english(chunk, target_lang=translate_target_lang)
            ):
                try:
                    translated = await translator.translate(
                        chunk,
                        source_language="auto",
                        target_language=translate_target_lang,
                    )
                    if translated:
                        stored_text = translated
                        header_extra = f", translated→{translate_target_lang}"
                except Exception:
                    logger.warning(
                        "ingest: translation failed for %s chunk %d; storing original",
                        label,
                        ci,
                        exc_info=True,
                    )

            tagged = (
                f"[{label} chunk {ci + 1}/{len(chunks)}{header_extra}]\n{stored_text}"
            )
            try:
                await store.write(
                    principal=principal,
                    text=tagged,
                    kind=kind,
                    source_trace_id=trace_id,
                )
                written += 1
            except PermissionError:
                logger.warning("ingest: PermissionError mid-batch; aborting")
                return written
            except Exception:
                logger.warning(
                    "ingest: failed to write chunk from %s (idx=%d)",
                    label,
                    ci,
                    exc_info=True,
                )
    logger.debug("ingest: wrote %d chunk(s)", written)
    return written
