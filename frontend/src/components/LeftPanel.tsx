import { useEffect, useState } from "react";
import { api } from "../api";
import { useTerminal } from "../store";
import type { ClusterSummary, SourceState } from "../types";
import { CATEGORIES } from "../types";

function FiltersPane() {
  const filters = useTerminal((s) => s.filters);
  const setFilters = useTerminal((s) => s.setFilters);
  const toggleCategory = useTerminal((s) => s.toggleCategory);

  return (
    <div className="flex flex-col gap-3 p-2">
      <div>
        <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">Search</div>
        <input
          id="tape-search"
          value={filters.q}
          onChange={(e) => setFilters({ q: e.target.value })}
          placeholder="/ to focus — headline match"
          className="w-full rounded-sm border border-bg-border bg-bg-raised px-2 py-1 text-[12px] text-dim-text2 outline-none focus:border-accent-dim"
        />
      </div>

      <div>
        <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">
          Categories <span className="normal-case text-dim-text/60">(1-9)</span>
        </div>
        <div className="grid grid-cols-2 gap-1">
          {CATEGORIES.map((c, i) => {
            const on = filters.categories.includes(c);
            return (
              <button
                key={c}
                onClick={() => toggleCategory(c)}
                className={`flex items-center gap-1.5 rounded-sm border px-1.5 py-[3px] text-left text-[11px] ${
                  on
                    ? "border-accent/40 bg-accent/10 text-accent-text"
                    : "border-bg-border text-dim-text2 hover:border-bg-border hover:text-dim-text"
                }`}
              >
                <span className="w-2">{on ? "●" : "○"}</span>
                <span className="flex-1 truncate">{c}</span>
                {i < 9 && <kbd className="!border-0 !bg-transparent !p-0">{i + 1}</kbd>}
              </button>
            );
          })}
        </div>
      </div>

      <div>
        <div className="mb-1 flex items-baseline justify-between">
          <span className="text-[9px] uppercase tracking-widest text-dim-text">Min urgency</span>
          <span className="text-[11px] text-amber-text">
            {filters.minUrgency === 0 ? "ALL" : `≥ ${filters.minUrgency}`}
          </span>
        </div>
        <input
          type="range"
          min={0}
          max={10}
          value={filters.minUrgency}
          onChange={(e) => setFilters({ minUrgency: Number(e.target.value) })}
          className="w-full accent-accent"
        />
        <div className="mt-0.5 flex justify-between text-[9px] text-dim-text">
          <span>u cycles · all</span>
          <span>flash only</span>
        </div>
      </div>

      <div>
        <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">Tickers</div>
        <input
          value={filters.tickers.join(",")}
          onChange={(e) =>
            setFilters({
              tickers: e.target.value
                .split(",")
                .map((t) => t.trim().toUpperCase())
                .filter(Boolean),
            })
          }
          placeholder="NVDA, BTC, MSFT"
          className="w-full rounded-sm border border-bg-border bg-bg-raised px-2 py-1 text-[12px] uppercase text-dim-text2 outline-none focus:border-accent-dim"
        />
      </div>
    </div>
  );
}

function SourcesPane() {
  const sources = useTerminal((s) => s.sources);
  const [busy, setBusy] = useState<string | null>(null);

  const sorted = [...sources].sort((a, b) => {
    const rank = (h: string) => (h === "ok" ? 0 : h === "degraded" ? 1 : h === "waiting_key" ? 2 : h === "dead" ? 3 : 4);
    return rank(a.health) - rank(b.health) || a.name.localeCompare(b.name);
  });

  const act = async (id: string, fn: (id: string) => Promise<unknown>) => {
    setBusy(id);
    try {
      await fn(id);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="flex flex-col gap-0.5 overflow-y-auto p-2">
      <div className="mb-1 flex items-center justify-between text-[9px] uppercase tracking-widest text-dim-text">
        <span>Registry ({sources.length})</span>
        <span className="normal-case text-dim-text/60">s cycles panes</span>
      </div>
      {sorted.map((s: SourceState) => (
        <div
          key={s.source_id}
          className="group flex items-center gap-2 rounded-sm px-1.5 py-[3px] text-[11px] hover:bg-bg-hover"
        >
          <span
            className={`h-1.5 w-1.5 shrink-0 rounded-full dot-${s.health}`}
            title={s.health}
          />
          <span className="min-w-0 flex-1 truncate text-dim-text2" title={s.source_id}>
            {s.name}
          </span>
          <span className="shrink-0 text-[9px] text-dim-text">
            {s.interval_sec ? `${s.interval_sec}s` : "stream"}
          </span>
          <button
            disabled={busy === s.source_id}
            onClick={() =>
              act(s.source_id, s.enabled ? api.pauseSource : api.resumeSource)
            }
            className={`shrink-0 rounded-sm border px-1 text-[9px] ${
              s.enabled
                ? "border-bg-border text-dim-text2 hover:text-amber-text"
                : "border-amber-flash/40 text-amber-text"
            }`}
          >
            {s.enabled ? "pause" : "resume"}
          </button>
        </div>
      ))}
      {!sources.length && (
        <div className="p-2 text-[11px] text-dim-text">awaiting source states…</div>
      )}
    </div>
  );
}

function ClustersPane() {
  const clusters = useTerminal((s) => s.clusters);
  const openCluster = useTerminal((s) => s.openCluster);

  const sorted = [...clusters].sort((a, b) => b.velocity_15m - a.velocity_15m || b.event_count - a.event_count);

  return (
    <div className="flex flex-col gap-0.5 overflow-y-auto p-2">
      <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">
        Story clusters ({clusters.length}) <span className="normal-case text-dim-text/60">· by velocity</span>
      </div>
      {sorted.map((c: ClusterSummary) => (
        <button
          key={c.cluster_id}
          onClick={async () => {
            const events = await api.clusterEvents(c.cluster_id, 50);
            openCluster(c, events);
          }}
          className="flex items-center gap-2 rounded-sm px-1.5 py-[3px] text-left text-[11px] hover:bg-bg-hover"
          title={c.title}
        >
          <span className="w-8 shrink-0 text-right text-dim-text">{c.velocity_15m}/15m</span>
          <span className="w-8 shrink-0 text-right text-dim-text2">{c.event_count}×</span>
          <span className="min-w-0 flex-1 truncate text-dim-text2">{c.title}</span>
          {c.tickers.slice(0, 2).map((t) => (
            <span key={t} className="chip chip-Macro !text-[9px]">{t}</span>
          ))}
        </button>
      ))}
      {!clusters.length && (
        <div className="p-2 text-[11px] text-dim-text">no clusters yet — waiting for events…</div>
      )}
    </div>
  );
}

export function LeftPanel() {
  const leftPane = useTerminal((s) => s.leftPane);
  const setSources = useTerminal((s) => s.setSources);
  const setClusters = useTerminal((s) => s.setClusters);

  useEffect(() => {
    api.sources().then(setSources).catch(() => {});
    api.clusters(100).then(setClusters).catch(() => {});
    const t = setInterval(() => {
      api.sources().then(setSources).catch(() => {});
      api.clusters(100).then(setClusters).catch(() => {});
    }, 30_000);
    return () => clearInterval(t);
  }, [setSources, setClusters]);

  return (
    <div className="flex w-[270px] shrink-0 flex-col border-r border-bg-border bg-bg-panel">
      <div className="flex shrink-0 border-b border-bg-border text-[10px]">
        {(["filters", "sources", "clusters"] as const).map((p) => (
          <button
            key={p}
            onClick={() => useTerminal.setState({ leftPane: p })}
            className={`flex-1 py-1 uppercase tracking-wider ${
              leftPane === p
                ? "border-b-2 border-accent bg-bg-raised text-accent-text"
                : "text-dim-text hover:text-dim-text2"
            }`}
          >
            {p}
          </button>
        ))}
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {leftPane === "filters" && <FiltersPane />}
        {leftPane === "sources" && <SourcesPane />}
        {leftPane === "clusters" && <ClustersPane />}
      </div>
    </div>
  );
}
