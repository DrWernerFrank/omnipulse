"""Connector base class and shared helpers."""

from __future__ import annotations

import calendar
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, ClassVar

from ..schema import RawItem, SourceConfig

if TYPE_CHECKING:
    from ..settings import Settings
    from .http import Fetcher

Emit = Callable[[RawItem], Awaitable[None]]


class SeenSet:
    """Bounded LRU set of item GUIDs so unchanged feeds don't re-emit items."""

    def __init__(self, items: list[str] | None = None, maxlen: int = 3000) -> None:
        self._order: deque[str] = deque(maxlen=maxlen)
        self._set: set[str] = set()
        for i in items or ():
            self.add(i)

    def __contains__(self, key: str) -> bool:
        return key in self._set

    def add(self, key: str) -> None:
        if key in self._set:
            return
        if len(self._order) == self._order.maxlen:
            self._set.discard(self._order[0])
        self._order.append(key)
        self._set.add(key)

    def export(self, n: int = 1000) -> list[str]:
        return list(self._order)[-n:]


@dataclass
class ConnectorContext:
    fetcher: "Fetcher"
    settings: "Settings"


class Connector:
    kind: ClassVar[str] = ""
    streaming: ClassVar[bool] = False

    def __init__(self, cfg: SourceConfig, ctx: ConnectorContext, state: dict[str, Any]) -> None:
        self.cfg = cfg
        self.ctx = ctx
        self.state = state  # persisted across restarts (etag, cursors, baselines...)

    @property
    def headers(self) -> dict[str, str]:
        h = dict(self.cfg.headers)
        if self.cfg.params.get("contact_ua"):
            # Some government sites (SEC, BLS) reject anonymous agents but accept "Name email".
            h.setdefault("User-Agent", self.ctx.settings.sec_user_agent)
        return h

    async def poll(self) -> list[RawItem]:  # polling connectors
        raise NotImplementedError

    async def stream(self, emit: Emit) -> None:  # streaming connectors (run forever)
        raise NotImplementedError

    async def conditional_get(self, url: str, **kw):
        """GET with ETag / If-Modified-Since persisted in connector state."""
        res = await self.ctx.fetcher.get(
            url, etag=self.state.get("etag"), last_modified=self.state.get("last_modified"),
            headers=self.headers | kw.pop("headers", {}), **kw,
        )
        if res.etag:
            self.state["etag"] = res.etag
        if res.last_modified:
            self.state["last_modified"] = res.last_modified
        return res

    def item(self, **kw: Any) -> RawItem:
        return RawItem(source_id=self.cfg.id, **kw)

    async def close(self) -> None:
        return None


def struct_to_dt(st) -> datetime | None:
    if not st:
        return None
    try:
        return datetime.fromtimestamp(calendar.timegm(st), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    v = value.strip()
    try:
        if re.fullmatch(r"\d{8}T\d{6}Z", v):  # GDELT seendate
            return datetime.strptime(v, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        import email.utils

        try:
            return email.utils.parsedate_to_datetime(v).astimezone(timezone.utc)
        except Exception:
            return None
