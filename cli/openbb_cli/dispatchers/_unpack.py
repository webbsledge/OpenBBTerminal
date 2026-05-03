"""Shared response unwrapping used by both the HTTP dispatcher and codegen.

The same envelope-stripping logic the generated extension applies at runtime
also runs against the spec-mode HTTP backend's responses, so what the user
sees in the REPL matches what an installed extension would surface — single-
key envelope wrappers stripped, single-element lists unpacked, sibling
scalar metadata split off from the row array.
"""

from __future__ import annotations

from typing import Any


def unpack_response(
    payload: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Split a raw API payload into (rows, metadata).

    Single-element list and single-key envelope wrappers are stripped so the
    caller never has to handle them. When the payload is a dict mixing one
    array field with scalar metadata fields, the array becomes ``rows`` and
    everything else becomes ``metadata``. Arrays of scalars (strings,
    numbers) wrap each element as ``{"value": x}`` so downstream typed-row
    construction has something to instantiate.
    """
    if isinstance(payload, list):
        if len(payload) == 1:
            return unpack_response(payload[0])
        rows = [r for r in payload if isinstance(r, dict)]
        if rows:
            return rows, {}
        return [{"value": r} for r in payload], {}
    if not isinstance(payload, dict):
        return [{"value": payload}], {}
    list_keys = [k for k, v in payload.items() if isinstance(v, list)]
    if len(list_keys) == 1:
        key = list_keys[0]
        items = payload[key]
        rows = [r for r in items if isinstance(r, dict)]
        if not rows and items:
            rows = [{"value": r} for r in items]
        metadata = {k: v for k, v in payload.items() if k != key}
        return rows, metadata
    if len(payload) == 1:
        only_value = next(iter(payload.values()))
        if isinstance(only_value, (list, dict)):
            return unpack_response(only_value)
    return [payload], {}
