"""Shared test config for the merged suite: v2 (app.*) + v1 (omnipulse.*).

Points both settings singletons at a clean env so tests never touch real
env vars, Redis, or Postgres.
"""
import os
from pathlib import Path

import pytest

# ---- v2 (OMNIPULSE_*) — must run before any app import resolves settings ----
os.environ.setdefault("OMNIPULSE_LLM_PROVIDER", "none")
os.environ.pop("OMNIPULSE_DATABASE_URL", None)
os.environ.pop("OMNIPULSE_REDIS_URL", None)

# ---- v1 (OMNI_*) — lite mode, in-memory bus/store, no network sources ----
EMPTY = Path(__file__).parent / "empty_sources.json"
EMPTY.write_text('{"version": 1, "sources": [{"id": "test_wire", "name": "Test Wire", "type": "webhook", '
                 '"category": "wire", "credibility_tier": 1, "publisher": "wire"}, {"id": "test_blog", "name": "Test Blog", '
                 '"type": "webhook", "category": "tech", "credibility_tier": 2, "publisher": "blog"}, {"id": "test_social", '
                 '"name": "Test Social", "type": "webhook", "category": "social", "credibility_tier": 3, "publisher": "social"}]}')

os.environ.update(OMNI_MODE="lite", OMNI_SOURCES_FILE=str(EMPTY), OMNI_EMBEDDER="hash",
                  OMNI_FETCH_FULL_ARTICLE="false", OMNI_LOG_LEVEL="WARNING",
                  OMNI_LLM_PROVIDER="none")  # never fire real LLM calls from tests (local .env guard)


@pytest.fixture
def settings():
    from omnipulse.settings import Settings

    return Settings()
