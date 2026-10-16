import { useEffect, useState } from "react";
import { useTerminal } from "../store";

function Clock() {
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);
  return (
    <span className="text-dim-text">
      {now.toISOString().slice(11, 19)} <span className="text-dim-text">UTC</span>
    </span>
  );
}

export function TopBar() {
  const stats = useTerminal((s) => s.stats);
  const connection = useTerminal((s) => s.connection);
  const paused = useTerminal((s) => s.filters.paused);
  const togglePaused = useTerminal((s) => s.togglePaused);
  const setPalette = useTerminal((s) => s.setPalette);

  const conn =
    connection === "live" ? (
      <span className="flex items-center gap-1.5 text-accent-text">
        <span className="dot-ok pulse-dot h-2 w-2 rounded-full" /> LIVE
      </span>
    ) : connection === "connecting" ? (
      <span className="flex items-center gap-1.5 text-amber-text">
        <span className="dot-degraded pulse-dot h-2 w-2 rounded-full" /> SYNCING
      </span>
    ) : (
      <span className="flex items-center gap-1.5 text-red-text">
        <span className="dot-dead h-2 w-2 rounded-full" /> DOWN
      </span>
    );

  return (
    <div className="flex h-[34px] shrink-0 items-center gap-4 border-b border-bg-border bg-bg-panel px-3">
      <div className="flex items-center gap-2">
        <span className="text-[15px] font-bold tracking-tight text-accent">⌁ OmniPulse</span>
        <span className="hidden rounded-sm border border-bg-border px-1 text-[9px] text-dim-text md:inline">
          TERMINAL
        </span>
      </div>

      {conn}

      <button
        onClick={togglePaused}
        className={`rounded-sm border px-2 py-[1px] text-[10px] ${
          paused
            ? "border-amber-flash/50 bg-amber-500/10 text-amber-text"
            : "border-bg-border text-dim-text2 hover:text-dim-text"
        }`}
        title="Space — pause/resume tape"
      >
        {paused ? "▶ RESUME" : "❙❙ FREEZE"}
      </button>

      <div className="flex flex-1 items-center justify-end gap-4 text-[11px]">
        {stats && (
          <>
            <span>
              <span className="text-dim-text">ev/min </span>
              <span className="text-accent-text">{stats.events_per_min.toFixed(1)}</span>
            </span>
            <span>
              <span className="text-dim-text">lag </span>
              <span className="text-dim-text2">
                p50 {stats.lag_p50_ms}ms · p95 {stats.lag_p95_ms}ms
              </span>
            </span>
            <span>
              <span className="text-dim-text">src </span>
              <span className="text-dim-text2">
                {stats.sources_online}/{stats.sources_total}
              </span>
            </span>
            <span>
              <span className="text-dim-text">24h </span>
              <span className="text-dim-text2">{stats.events_total.toLocaleString()}</span>
            </span>
          </>
        )}
        <Clock />
        <button
          onClick={() => setPalette(true)}
          className="rounded-sm border border-bg-border px-2 py-[1px] text-[10px] text-dim-text2 hover:text-dim-text"
          title="Ctrl+K"
        >
          ⌘K
        </button>
      </div>
    </div>
  );
}
