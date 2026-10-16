"""Shared test config: point the settings singleton at a clean env so tests
never touch real env vars, Redis, or Postgres."""
import os

# must run before any app import resolves settings
os.environ.setdefault("OMNIPULSE_LLM_PROVIDER", "none")
os.environ.pop("OMNIPULSE_DATABASE_URL", None)
os.environ.pop("OMNIPULSE_REDIS_URL", None)
