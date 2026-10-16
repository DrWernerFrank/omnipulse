"""Persistence layer: PostgreSQL + TimescaleDB + pgvector (full) or in-memory (lite)."""

from __future__ import annotations

from .base import Store
from .memory import MemoryStore


def make_store(settings) -> Store:
    if settings.mode == "lite":
        return MemoryStore(max_events=settings.memory_max_events, dim=settings.embed_dim)
    from .postgres import PostgresStore

    return PostgresStore(settings.database_url, dim=settings.embed_dim, retention_days=settings.retention_days)


__all__ = ["Store", "make_store", "MemoryStore"]
