"""Deterministic feature-hashed embeddings + cosine helper."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence

from langchain_core.embeddings import Embeddings


class HashEmbeddings(Embeddings):
    """Zero-config feature-hashed embeddings for tests and dev fallback."""

    def __init__(self, *, dim: int = 256) -> None:
        if dim <= 0:
            raise ValueError("dim must be positive")
        self.dim = dim

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed_one(text)

    def _embed_one(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        words = text.lower().split()
        features = list(words)
        features.extend(f"{a}_{b}" for a, b in zip(words, words[1:]))
        for f in features:
            h = hashlib.blake2s(f.encode("utf-8"), digest_size=8).digest()
            bucket = int.from_bytes(h[:4], "little") % self.dim
            sign = 1.0 if (h[4] & 1) else -1.0
            vec[bucket] += sign
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity, robust to length mismatch (uses the shorter)."""
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    num = sum(a[i] * b[i] for i in range(n))
    da = math.sqrt(sum(x * x for x in a[:n])) or 1.0
    db = math.sqrt(sum(x * x for x in b[:n])) or 1.0
    return num / (da * db)


__all__ = ["Embeddings", "HashEmbeddings", "cosine"]
