import { useEffect, useMemo, useState } from "react";
import { useTerminal } from "../../store";
import { sentimentClass } from "../lib/format";
import { DB, useUI } from "../store";

function useClock(): string {
  const [t, setT] = useState(() => new Date());
  useEffect(() => {
    const i = window.setInterval(() => setT(new Date()), 1000);
    return () => window.clearInterval(i);
  }, []);
  return t.toISOString().slice(11, 19);
}

/** Tickers mentioned most in the last hour, with mean sentiment. Recomputed at most every 5 s. */
function useTrending(): { t: string; n: number; s: number }[] {
  const rev = useUI((s) => s.rev);
  const [slot, setSlot] = useState(0);
  useEffect(() => {
    const i = window.setInterval(() => setSlot((x) => x + 1), 5000);
    return () => window.clearInterval(i);
  }, []);
  return useMemo(() => {
    const cutoff = new Date(Date.now() - 3600_000).toISOString();
    const agg = new Map<string, { n: number; s: number }>();
    for (const id of DB.order) {
      const ev = DB.events.get(id);
      if (!ev || ev.ingested_at < cutoff) continue;
      for (const t of ev.enrichment.tickers) {
        const a = agg.get(t) ?? { n: 0, s: 0 };
        a.n += 1;
        a.s += ev.enrichment.sentiment;
        agg.set(t, a);
      }
    }
    return [...agg.entries()]
      .map(([t, a]) => ({ t, n: a.n, s: a.s / a.n }))
      .sort((a, b) => b.n - a.n)
      .slice(0, 14);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [slot, rev > 0]);
}

export function TopBar() {
  const conn = useUI((s) => s.conn);
  const rate = useUI((s) => s.ratePerMin);
  const stats = useUI((s) => s.stats);
  const health = useUI((s) => s.health);
  const sources = useUI((s) => s.sources);
  const setFilter = useUI((s) => s.setFilter);
  const setAppearance = useTerminal((s) => s.setAppearance);
  const clock = useClock();
  const trending = useTrending();
  const healthy = Object.values(health).filter((h) => h.status === "ok").length;

  const connCls = conn === "live" ? "text-up" : conn === "connecting" ? "text-amber" : "text-down";
  return (
    <div className="shrink-0 border-b border-line bg-panel">
      <div className="flex h-[26px] items-center gap-4 px-2 text-[11px] whitespace-nowrap">
        <span className="font-bold tracking-[0.2em] text-amber">OMNIPULSE</span>
        <span className={connCls}>
          {conn === "live" ? "● LIVE" : conn === "connecting" ? "◌ CONNECTING" : "✕ OFFLINE"}
        </span>
        <span className="text-dim">
          RATE <span className="text-bright">{rate}</span>/min
        </span>
        {stats && (stats.lag_p50_ms || stats.lag_p95_ms) && (
          <span className="text-dim" title="published → ingested latency, p50 / p95">
            LAG <span className="text-bright">{stats.lag_p50_ms}</span>/{stats.lag_p95_ms}ms
          </span>
        )}
        <span className="hidden text-dim md:inline">
          EVENTS <span className="text-bright">{DB.order.length.toLocaleString()}</span>
        </span>
        <span className="hidden text-dim md:inline">
          STORIES <span className="text-bright">{DB.clusters.size.toLocaleString()}</span>
        </span>
        <span className="hidden text-dim lg:inline">
          SOURCES <span className="text-up">{healthy}</span>/{sources.length}
        </span>
        {stats && (
          <span className="hidden text-dim lg:inline">
            TOTAL <span className="text-bright">{stats.events_total.toLocaleString()}</span>
          </span>
        )}
        <span className="ml-auto text-bright">{clock} UTC</span>
        <button
          type="button"
          onClick={() => setAppearance("tape")}
          title="Switch terminal skin → tape (v2)"
          className="border border-line bg-panel2 px-1.5 py-[1px] text-[10px] text-cyan hover:border-cyan/60"
        >
          TAPE ⇄
        </button>
      </div>
      {trending.length > 0 && (
        <div className="flex h-[20px] items-center gap-3 overflow-hidden border-t border-line px-2 text-[11px] whitespace-nowrap">
          <span className="text-dim">1H TICKERS</span>
          {trending.map((x) => (
            <button key={x.t} type="button" onClick={() => setFilter(`$${x.t}`)} className="hover:underline">
              <span className="text-cyan">${x.t}</span>
              <span className="text-dim"> {x.n} </span>
              <span className={sentimentClass(x.s)}>{x.s >= 0 ? "▲" : "▼"}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
