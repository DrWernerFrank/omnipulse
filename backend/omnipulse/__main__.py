"""CLI entry point.

    python -m omnipulse all      # API + ingestion + enrichment in one process (required for lite mode)
    python -m omnipulse api      # API / streams / MCP only (full mode)
    python -m omnipulse engine   # ingestion + enrichment only (full mode)
    python -m omnipulse lite     # shorthand: OMNI_MODE=lite + role all
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys


def _logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "websockets", "trafilatura", "mcp", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def _engine_main(settings) -> None:
    from .runtime import Runtime

    rt = await Runtime.create(settings, "engine")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await rt.close()


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    role = args[0] if args else "all"
    if role == "lite":
        os.environ["OMNI_MODE"] = "lite"
        role = "all"
    if role not in ("all", "api", "engine"):
        raise SystemExit(__doc__)

    from .settings import get_settings

    settings = get_settings()
    _logging(settings.log_level)

    if role == "engine":
        asyncio.run(_engine_main(settings))
        return

    import uvicorn

    from .api.app import create_app

    uvicorn.run(
        create_app(settings, role),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        proxy_headers=True,
        ws_ping_interval=20,
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
