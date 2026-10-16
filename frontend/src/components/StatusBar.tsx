import { useEffect, useRef } from "react";
import { drainPaused } from "../api";
import { useTerminal } from "../store";

export function StatusBar() {
  const stats = useTerminal((s) => s.stats);
  const connection = useTerminal((s) => s.connection);
  const events = useTerminal((s) => s.events);
  const clusters = useTerminal((s) => s.clusters);

  return (
    <div className="flex h-[22px] shrink-0 items-center gap-3 border-t border-bg-border bg-bg-panel px-3 text-[10px] text-dim-text">
      <span className={connection === "live" ? "text-accent-text" : "text-amber-text"}>
        {connection === "live"
          ? "◉ STREAMING"
          : connection === "connecting"
            ? "◌ SYNCING"
            : "✕ DISCONNECTED"}
      </span>
      <span>{events.length.toLocaleString()} in buffer</span>
      <span>{clusters.length} clusters</span>
      {stats && (
        <span>
          dedup {stats.pipeline?.duplicates.toLocaleString() ?? 0} · joins{" "}
          {stats.pipeline?.cluster_joins.toLocaleString() ?? 0}
        </span>
      )}
      {stats && stats.dlq_size > 0 && (
        <span className="text-amber-text">DLQ {stats.dlq_size}</span>
      )}
      <span className="flex-1" />
      <span>bus:{stats?.bus_mode ?? "—"}</span>
      <span>store:{stats?.store_mode ?? "—"}</span>
      <span className="text-dim-text/70">
        <kbd>j/k</kbd> move <kbd>Space</kbd> freeze <kbd>/</kbd> search <kbd>⌘K</kbd> palette{" "}
        <kbd>f/s</kbd> panes <kbd>d</kbd> detail <kbd>1-9</kbd> cats <kbd>u</kbd> urgency <kbd>Esc</kbd> close
      </span>
    </div>
  );
}

/** When the tape un-freezes, drain everything buffered while paused. */
export function usePausedDrain() {
  const paused = useTerminal((s) => s.filters.paused);
  const pushEvent = useTerminal((s) => s.pushEvent);
  const prevRef = useRef(paused);

  useEffect(() => {
    if (prevRef.current && !paused) {
      for (const ev of drainPaused()) pushEvent(ev);
    }
    prevRef.current = paused;
  }, [paused, pushEvent]);
}
