"""SQLAlchemy ORM tables."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base."""

    type_annotation_map = {
        dict[str, Any]: JSON,
        list[Any]: JSON,
    }


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    display_name: Mapped[str | None] = mapped_column(String, nullable=True)
    email: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    last_seen_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    quota_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    memory_opt_in: Mapped[bool] = mapped_column(Boolean, default=False)


class ApiKey(Base):
    __tablename__ = "api_keys"

    key_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.user_id", ondelete="CASCADE")
    )
    hashed_secret: Mapped[str] = mapped_column(String, nullable=False)
    label: Mapped[str | None] = mapped_column(String, nullable=True)
    scopes: Mapped[list[Any]] = mapped_column(JSON, default=list)
    created_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    revoked_at: Mapped[_dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (Index("ix_api_keys_user", "user_id"),)


class Conversation(Base):
    __tablename__ = "conversations"

    conversation_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.user_id", ondelete="CASCADE")
    )
    title: Mapped[str | None] = mapped_column(String, nullable=True)
    summary_blob_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    updated_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )

    __table_args__ = (Index("ix_conversations_user", "user_id", "updated_at"),)

    messages: Mapped[list[Message]] = relationship(
        cascade="all, delete-orphan",
        back_populates="conversation",
    )


class Trace(Base):
    __tablename__ = "traces"

    trace_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.user_id", ondelete="CASCADE")
    )
    conversation_id: Mapped[str | None] = mapped_column(String, nullable=True)
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)
    started_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    ended_at: Mapped[_dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(String, default="running")

    __table_args__ = (Index("ix_traces_user", "user_id", "started_at"),)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conversation_id: Mapped[str] = mapped_column(
        ForeignKey("conversations.conversation_id", ondelete="CASCADE")
    )
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    widget_refs: Mapped[list[Any]] = mapped_column(JSON, default=list)
    file_refs: Mapped[list[Any]] = mapped_column(JSON, default=list)
    trace_id: Mapped[str | None] = mapped_column(String, nullable=True)
    ts: Mapped[_dt.datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (
        Index("ix_messages_user_conv_seq", "user_id", "conversation_id", "seq"),
    )

    conversation: Mapped[Conversation] = relationship(back_populates="messages")


class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    conversation_id: Mapped[str | None] = mapped_column(String, nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String, nullable=True)
    model: Mapped[str | None] = mapped_column(String, nullable=True)
    system_prompt_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, default="running")
    started_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )
    ended_at: Mapped[_dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (Index("ix_runs_user", "user_id", "started_at"),)


class ToolCall(Base):
    __tablename__ = "tool_calls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    tool_name: Mapped[str] = mapped_column(String, nullable=False)
    args_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    result_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    side: Mapped[str] = mapped_column(String, default="server")
    state: Mapped[str] = mapped_column(String, default="complete")
    ts: Mapped[_dt.datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (Index("ix_tool_calls_user_trace", "user_id", "trace_id", "seq"),)


class Usage(Base):
    __tablename__ = "usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cache_read: Mapped[int] = mapped_column(Integer, default=0)
    cache_creation: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[float] = mapped_column(default=0.0)
    ts: Mapped[_dt.datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (Index("ix_usage_user_trace", "user_id", "trace_id", "seq"),)


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False)
    payload_blob_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    mime: Mapped[str | None] = mapped_column(String, nullable=True)
    ts: Mapped[_dt.datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (Index("ix_artifacts_user_trace", "user_id", "trace_id", "seq"),)


class CitationRow(Base):
    __tablename__ = "citations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    source_url: Mapped[str | None] = mapped_column(String, nullable=True)
    page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bbox_json: Mapped[list[Any] | None] = mapped_column(JSON, nullable=True)
    text_snippet: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (Index("ix_citations_user_trace", "user_id", "trace_id", "seq"),)


class WidgetData(Base):
    """One ingested widget-data response."""

    __tablename__ = "widget_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    conversation_id: Mapped[str] = mapped_column(String, nullable=False)
    widget_uuid: Mapped[str] = mapped_column(String, nullable=False)
    widget_name: Mapped[str | None] = mapped_column(String, nullable=True)
    origin: Mapped[str | None] = mapped_column(String, nullable=True)
    input_args: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    columns: Mapped[list[Any] | None] = mapped_column(JSON, nullable=True)
    rows: Mapped[list[Any]] = mapped_column(JSON, default=list)
    ingested_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True),
        default=_now,
    )

    __table_args__ = (
        Index(
            "ix_widget_data_lookup",
            "user_id",
            "conversation_id",
            "widget_uuid",
            "ingested_at",
        ),
    )


class PdfDocument(Base):
    """One ingested PDF with its metadata and table of contents."""

    __tablename__ = "pdf_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    file_key: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str | None] = mapped_column(String, nullable=True)
    mime: Mapped[str | None] = mapped_column(String, nullable=True)
    total_pages: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    toc_json: Mapped[list[Any]] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String, default="pending")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )

    __table_args__ = (Index("ix_pdf_documents_lookup", "user_id", "file_key"),)


class PdfPage(Base):
    """One parsed page of a :class:`PdfDocument`."""

    __tablename__ = "pdf_pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pdf_id: Mapped[int] = mapped_column(
        ForeignKey("pdf_documents.id", ondelete="CASCADE"),
        nullable=False,
    )
    page: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, default="")
    words_json: Mapped[list[Any]] = mapped_column(JSON, default=list)

    __table_args__ = (Index("ix_pdf_pages_doc_page", "pdf_id", "page"),)


class PendingRun(Base):
    """State blob for resuming a run that yielded on a client-side tool call."""

    __tablename__ = "pending_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    state_blob: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[_dt.datetime] = mapped_column(
        DateTime(timezone=True), default=_now
    )

    __table_args__ = (Index("ix_pending_runs_user", "user_id"),)
