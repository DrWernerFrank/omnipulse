import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../lib/api";
import { controllers } from "../lib/controllers";
import { STATUS_CLASS, ago } from "../lib/format";
import { refreshSources } from "../lib/stream";
import { useUI } from "../store";
import { Pane } from "./Pane";

const RANK: Record<string, number> = {
  dead: 0, degraded: 1, stale: 2, starting: 3, ok: 4, waiting_key: 5, paused: 6, disabled: 7,
};

export function SourcesPane({ area }: { area: string }) {
  const sources = useUI((s) => s.sources);
  const health = useUI((s) => s.health);
  const sel = useUI((s) => s.sourceSel);
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [now, setNow] = useState(Date.now());
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(t);
  }, []);

  const rows = useMemo(
    () =>
      [...sources].sort((a, b) => {
        const sa = health[a.id]?.status ?? "starting";
        const sb = health[b.id]?.status ?? "starting";
        return (RANK[sa] ?? 9) - (RANK[sb] ?? 9) || a.id.localeCompare(b.id);
      }),
    [sources, health],
  );

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const s of sources) {
      const st = health[s.id]?.status ?? "starting";
      c[st] = (c[st] ?? 0) + 1;
    }
    return c;
  }, [sources, health]);

  useEffect(() => {
    const ui = useUI.getState;
    const current = () => rows.find((r) => r.id === sel);
    const scrollTo = (i: number) => listRef.current?.children[i]?.scrollIntoView({ block: "nearest" });
    controllers.sources = {
      move: (d) => {
        const cur = sel ? rows.findIndex((r) => r.id === sel) : -1;
        const next = Math.max(0, Math.min(rows.length - 1, cur + d));
        if (rows[next]) {
          ui().set({ sourceSel: rows[next].id });
          scrollTo(next);
        }
      },
      edge: (end) => {
        const i = end === "top" ? 0 : rows.length - 1;
        if (rows[i]) {
          ui().set({ sourceSel: rows[i].id });
          scrollTo(i);
        }
      },
      open: () => {
        const s = current();
        if (s) {
          ui().setFilter(`src:${s.id}`);
          ui().setFocus("firehose");
        }
      },
      key: (k) => {
        const s = current();
        if (k === "r") {
          refreshSources();
          return true;
        }
        if (!s) return false;
        if (k === "p") {
          const pause = s.enabled;
          api
            .pauseSource(s.id, pause)
            .then(() => {
              ui().notify(`${pause ? "paused" : "resumed"} ${s.id}`, "ok");
              refreshSources();
            })
            .catch((e: Error) => ui().notify(e.message, "err"));
          return true;
        }
        if (k === "x") {
          if (confirmDelete !== s.id) {
            setConfirmDelete(s.id);
            ui().notify(`press x again to remove ${s.id}`, "info");
            return true;
          }
          setConfirmDelete(null);
          api
            .deleteSource(s.id)
            .then(() => {
              ui().notify(`removed ${s.id}`, "ok");
              refreshSources();
            })
            .catch((e: Error) => ui().notify(e.message, "err"));
          return true;
        }
        return false;
      },
    };
  }, [rows, sel, confirmDelete]);

  const right = (
    <>
      <span className="text-up">{counts.ok ?? 0} ok</span>
      {((counts.degraded ?? 0) + (counts.stale ?? 0)) > 0 && (
        <span className="text-amber">{(counts.degraded ?? 0) + (counts.stale ?? 0)} degraded</span>
      )}
      {(counts.dead ?? 0) > 0 && <span className="text-down">{counts.dead} dead</span>}
      <span>{sources.length} total</span>
    </>
  );

  return (
    <Pane id="sources" title="Sources" right={right} area={area}>
      <div ref={listRef} className="absolute inset-0 overflow-y-auto">
        {rows.map((s) => {
          const h = health[s.id];
          const st = h?.status ?? s.health ?? "starting";
          const err = h?.last_error ?? s.detail ?? "";
          return (
            <div
              key={s.id}
              onClick={() => useUI.getState().set({ sourceSel: s.id })}
              onDoubleClick={() => controllers.sources?.open()}
              title={`${s.name}\n${st}${err ? `\n${err}` : ""}`}
              className={`flex h-[20px] items-center gap-2 border-l-2 px-2 text-[11px] whitespace-nowrap ${
                s.id === sel ? "border-amber bg-amber/12" : "border-transparent hover:bg-panel2"
              }`}
            >
              <span className={`inline-block h-[7px] w-[7px] shrink-0 rounded-full ${STATUS_CLASS[st] ?? "bg-dim"}`} />
              <span className="w-[150px] shrink-0 truncate text-bright">{s.id}</span>
              <span className="min-w-0 flex-1 truncate text-dim">{s.name}</span>
              <span className="w-[42px] shrink-0 text-right text-dim">{h?.interval ? `${Math.round(h.interval)}s` : "—"}</span>
              <span className={`w-[34px] shrink-0 text-right text-dim ${s.enabled ? "" : "text-amber"}`}>
                {s.enabled ? "" : "off"}
              </span>
              <span className="w-[34px] shrink-0 text-right text-dim">{ago(h?.last_success ?? s.last_success, now)}</span>
              <span className="w-[110px] shrink-0 truncate text-down">{st === "ok" ? "" : err}</span>
            </div>
          );
        })}
      </div>
    </Pane>
  );
}
