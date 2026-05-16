"""Snowflake Cortex helpers — SQL functions + REST endpoints."""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
from base64 import b64encode
from typing import Any

import httpx
import jwt as pyjwt

from openbb_agent_server.plugins.tools.snowflake_tools.client import (
    QueryResult,
    SnowflakeClient,
    SnowflakeCredentials,
)

logger = logging.getLogger("openbb_agent_server.tools.snowflake.cortex")


def cortex_complete(
    client: SnowflakeClient,
    *,
    prompt: str,
    model: str = "claude-3-5-sonnet",
    options: dict[str, Any] | None = None,
) -> str:
    """Call ``SNOWFLAKE.CORTEX.COMPLETE`` and return the text response."""
    if options:
        sql = (
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%(model)s, %(prompt)s, "
            "PARSE_JSON(%(options)s)) AS response"
        )
        result = client.execute(
            sql,
            {
                "model": model,
                "prompt": prompt,
                "options": json.dumps(options),
            },
        )
    else:
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%(model)s, %(prompt)s) AS response"
        result = client.execute(sql, {"model": model, "prompt": prompt})
    return _scalar(result)


def cortex_summarize(client: SnowflakeClient, *, text: str) -> str:
    return _scalar(
        client.execute(
            "SELECT SNOWFLAKE.CORTEX.SUMMARIZE(%(text)s) AS response",
            {"text": text},
        )
    )


def cortex_sentiment(client: SnowflakeClient, *, text: str) -> float:
    raw = _scalar(
        client.execute(
            "SELECT SNOWFLAKE.CORTEX.SENTIMENT(%(text)s) AS response",
            {"text": text},
        )
    )
    return float(raw)


def cortex_translate(
    client: SnowflakeClient,
    *,
    text: str,
    target_language: str,
    source_language: str = "",
) -> str:
    return _scalar(
        client.execute(
            "SELECT SNOWFLAKE.CORTEX.TRANSLATE(%(t)s, %(src)s, %(dst)s) AS response",
            {"t": text, "src": source_language, "dst": target_language},
        )
    )


def cortex_classify_text(
    client: SnowflakeClient,
    *,
    text: str,
    categories: list[str],
) -> dict[str, Any]:
    raw = _scalar(
        client.execute(
            "SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(%(t)s, %(c)s) AS response",
            {"t": text, "c": categories},
        )
    )
    return _parse_json(raw)


def cortex_extract_answer(
    client: SnowflakeClient,
    *,
    question: str,
    context: str,
) -> dict[str, Any]:
    raw = _scalar(
        client.execute(
            "SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(%(q)s, %(c)s) AS response",
            {"q": question, "c": context},
        )
    )
    return _parse_json(raw)


def cortex_embed(
    client: SnowflakeClient,
    *,
    text: str,
    model: str = "snowflake-arctic-embed-l-v2.0",
    dim: int = 1024,
) -> list[float]:
    fn = "EMBED_TEXT_1024" if dim == 1024 else "EMBED_TEXT_768"
    raw = _scalar(
        client.execute(
            f"SELECT SNOWFLAKE.CORTEX.{fn}(%(model)s, %(text)s) AS response",
            {"model": model, "text": text},
        )
    )
    if isinstance(raw, str):
        raw = _parse_json(raw)
    return [float(x) for x in raw]


def _scalar(result: QueryResult) -> Any:
    if not result.rows or not result.rows[0]:
        raise RuntimeError("cortex call returned no rows")
    return result.rows[0][0]


def _parse_json(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw


def _account_host(creds: SnowflakeCredentials) -> str:
    if creds.host:
        return creds.host.rstrip("/")
    if not creds.account:
        raise RuntimeError("Snowflake account is required for Cortex REST calls")
    region_part = f".{creds.region}" if creds.region else ""
    return f"https://{creds.account}{region_part}.snowflakecomputing.com"


def _public_key_fingerprint(private_key_pem: str, passphrase: str | None) -> str:
    """Compute the SHA-256 fingerprint Snowflake expects for KeyPair JWTs."""
    from cryptography.hazmat.primitives import serialization

    priv = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=passphrase.encode() if passphrase else None,
    )
    pub_der = priv.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    digest = hashlib.sha256(pub_der).digest()
    return "SHA256:" + b64encode(digest).decode()


def keypair_jwt(creds: SnowflakeCredentials, *, lifetime_seconds: int = 3600) -> str:
    """Mint a Snowflake-compatible KeyPair JWT for the REST API."""
    if not (creds.account and creds.user and creds.private_key):
        raise RuntimeError(
            "KeyPair JWT requires account, user, and private_key on the credentials"
        )
    fingerprint = _public_key_fingerprint(
        creds.private_key, creds.private_key_passphrase
    )
    now = _dt.datetime.now(_dt.timezone.utc)
    qualified_user = f"{creds.account.upper()}.{creds.user.upper()}"
    payload = {
        "iss": f"{qualified_user}.{fingerprint}",
        "sub": qualified_user,
        "iat": int(now.timestamp()),
        "exp": int(now.timestamp()) + lifetime_seconds,
    }
    return pyjwt.encode(payload, creds.private_key, algorithm="RS256")


def auth_headers(creds: SnowflakeCredentials) -> dict[str, str]:
    """Build the auth headers Snowflake REST endpoints accept."""
    if creds.token and creds.authenticator in {
        "oauth",
        "programmatic_access_token",
    }:
        kind = (
            "OAUTH" if creds.authenticator == "oauth" else "PROGRAMMATIC_ACCESS_TOKEN"
        )
        return {
            "Authorization": f"Bearer {creds.token}",
            "X-Snowflake-Authorization-Token-Type": kind,
        }
    if creds.private_key:
        return {
            "Authorization": f"Bearer {keypair_jwt(creds)}",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
    raise RuntimeError(
        "Cortex REST endpoints require OAuth, PAT, or KeyPair JWT credentials"
    )


def cortex_search(
    creds: SnowflakeCredentials,
    *,
    database: str,
    schema: str,
    service: str,
    query: str,
    columns: list[str] | None = None,
    limit: int = 10,
    filter_: dict[str, Any] | None = None,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Call a Cortex Search service."""
    url = (
        f"{_account_host(creds)}/api/v2/databases/{database}"
        f"/schemas/{schema}/cortex-search-services/{service}:query"
    )
    body: dict[str, Any] = {"query": query, "limit": limit}
    if columns:
        body["columns"] = columns
    if filter_:
        body["filter"] = filter_
    headers = auth_headers(creds)
    headers["Content-Type"] = "application/json"
    closing = client is None
    client = client or httpx.Client(timeout=60.0)
    try:
        resp = client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json()
    finally:
        if closing:
            client.close()


def cortex_analyst(
    creds: SnowflakeCredentials,
    *,
    messages: list[dict[str, Any]],
    semantic_model: str | None = None,
    semantic_view: str | None = None,
    stream: bool = False,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Call Cortex Analyst (text-to-SQL via semantic models)."""
    if not (semantic_model or semantic_view):
        raise RuntimeError("Cortex Analyst requires semantic_model or semantic_view")
    url = f"{_account_host(creds)}/api/v2/cortex/analyst/message"
    body: dict[str, Any] = {"messages": messages, "stream": stream}
    if semantic_model:
        body["semantic_model_file"] = semantic_model
    if semantic_view:
        body["semantic_view"] = semantic_view
    headers = auth_headers(creds)
    headers["Content-Type"] = "application/json"
    closing = client is None
    client = client or httpx.Client(timeout=60.0)
    try:
        resp = client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json()
    finally:
        if closing:
            client.close()
