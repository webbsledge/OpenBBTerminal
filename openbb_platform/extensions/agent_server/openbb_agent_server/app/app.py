"""FastAPI app factory + lifespan."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from openbb_agent_server.app.config import (
    agent_section,
    bootstrap_launcher_config,
)
from openbb_agent_server.app.router import build_router
from openbb_agent_server.app.settings import AgentServerSettings
from openbb_agent_server.memory.factory import (
    make_embeddings,
    make_reranker,
    make_translator,
)
from openbb_agent_server.memory.sqlite_store import SqliteMemoryStore
from openbb_agent_server.observability.logging import install_trace_logging
from openbb_agent_server.persistence.sqlite_store import SqliteHistoryStore
from openbb_agent_server.runtime import registry, services
from openbb_agent_server.runtime.identity import warn_if_pepper_unset
from openbb_agent_server.runtime.pdf_store import PdfStore
from openbb_agent_server.runtime.plugins import AuthBackend, CheckpointerProvider
from openbb_agent_server.runtime.widget_store import WidgetDataStore

logger = logging.getLogger("openbb_agent_server.app")


def _load_auth(settings: AgentServerSettings) -> AuthBackend:
    return registry.load(
        "openbb_agent_server.auth",
        settings.auth_backend,
        settings.auth_config,
    )


def _load_checkpointer_provider(
    settings: AgentServerSettings,
) -> CheckpointerProvider:
    return registry.load(
        "openbb_agent_server.checkpointers",
        settings.checkpointer_provider,
        settings.checkpointer_config,
    )


def create_app(settings: AgentServerSettings | None = None) -> FastAPI:
    """Build the FastAPI app."""
    if settings is None:
        explicit = os.environ.get("OPENBB_AGENT_BOOTSTRAP_TOML") or None
        cfg = bootstrap_launcher_config(explicit_path=explicit)
        settings = AgentServerSettings.from_toml(agent_section(cfg))
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    install_trace_logging()
    # One deterministic boot-time config check — keeps the missing-pepper
    # notice out of per-request logs.
    warn_if_pepper_unset()

    auth = _load_auth(settings)
    checkpointer_provider = _load_checkpointer_provider(settings)
    db_url = settings.resolved_db_url()
    history = SqliteHistoryStore(db_url)
    embeddings = make_embeddings(
        settings.embeddings_provider,
        model=settings.embeddings_model,
        config=settings.embeddings_config,
    )
    code_embeddings = None
    if settings.embeddings_code_provider:
        code_embeddings = make_embeddings(
            settings.embeddings_code_provider,
            model=settings.embeddings_code_model,
            config=settings.embeddings_code_config,
        )
    reranker = (
        make_reranker(
            settings.reranker_provider,
            model=settings.reranker_model,
            config=settings.reranker_config,
        )
        if settings.reranker_provider
        else None
    )
    memory = SqliteMemoryStore(
        db_url,
        embeddings=embeddings,
        code_embeddings=code_embeddings,
        reranker=reranker,
        rerank_fanout=settings.rerank_fanout,
    )
    translator = (
        make_translator(
            settings.translation_provider,
            model=settings.translation_model,
            config=settings.translation_config,
        )
        if settings.translation_provider
        else None
    )
    widget_store = WidgetDataStore(db_url, embeddings=embeddings)
    pdf_store = PdfStore(db_url, embeddings=embeddings)
    services.set_services(widget_store=widget_store, pdf_store=pdf_store)
    checkpointer: Any = None

    services.set_services(history=history, memory=memory)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nonlocal checkpointer
        await history.init_schema()
        checkpointer = await checkpointer_provider.open(settings)
        services.set_services(checkpointer=checkpointer)
        _app.state.checkpointer = checkpointer
        try:
            yield
        finally:
            try:
                await checkpointer_provider.close(checkpointer)
            finally:
                await history.aclose()
                services.reset()

    app = FastAPI(
        title=settings.metadata.name,
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    app.include_router(
        build_router(
            settings=settings,
            auth=auth,
            history=history,
            memory=memory,
            translator=translator,
            widget_store=widget_store,
        ),
    )
    app.state.settings = settings
    app.state.auth = auth
    app.state.history = history
    app.state.memory = memory
    return app
