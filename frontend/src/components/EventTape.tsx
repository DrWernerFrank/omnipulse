import { memo, useCallback, useEffect, useMemo, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useTerminal } from "../store";
import type { NewsEvent } from "../types";

// "AI/Tech" -> "AITech": safe CSS class fragment (chip-* selectors)
export function chipClass(cat: string): string {
  return `chip-${cat.replace(/[^A-Za-z0-9]/g, "")}`;
}
const urgencyClass = (u: number): string =>
  u >= 9 ? "u-high" : u >= 7 ? "u-med" : "";

const urgencyGlyph = (u: number): string =>
  u >= 9 ? "▲▲" : u >= 7 ? "▲" : u >= 5 ? "·" : " ";

function timeOf(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const diff = (now.getTime() - d.getTime()) / 1000;
  if (diff < 60) return `${Math.max(0, Math.floor(diff))}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  return d.toISOString().slice(5, 10);
}

const sentimentBar = (s: number): string => {
  if (s >= 0.25) return "▲";
  if (s <= -0.25) return "▼";
  return "·";
};

interface RowProps {
  ev: NewsEvent;
  selected: boolean;
  flash: boolean;
  onSelect: (ev: NewsEvent) => void;
}

const TapeRow = memo(({ ev, selected, flash, onSelect }: RowProps) => {
  const e = ev.enrichment;
  return (
    <div
      className={`tape-row h-[26px] text-[12px] ${selected ? "selected" : ""} ${flash ? "flash" : ""}`}
      onClick={() => onSelect(ev)}
    >
      <span className={`w-9 shrink-0 text-right ${urgencyClass(e.urgency)}`}>
        {urgencyGlyph(e.urgency)}
        <span className="ml-0.5 text-dim-text">{e.urgency}</span>
      </span>
      <span className="w-11 shrink-0 text-dim-text">{timeOf(ev.published_at)}</span>
      <span className="w-28 shrink-0 truncate text-dim-text2" title={ev.source.name}>
        {ev.source.name}
      </span>
      <span
        className={`w-8 shrink-0 text-center ${e.sentiment > 0.2 ? "sent-pos" : e.sentiment < -0.2 ? "sent-neg" : "text-dim-text"}`}
      >
        {sentimentBar(e.sentiment)}
      </span>
      <span className="min-w-0 flex-1 truncate" title={ev.content.raw_title}>
        {e.urgency >= 8 && <span className="mr-1 text-red-flash font-bold">⚡ </span>}
        {ev.content.flash_headline || ev.content.raw_title}
      </span>
      <span className="flex shrink-0 gap-1 overflow-hidden">
        {ev.enrichment.tickers.slice(0, 3).map((t) => (
          <span key={t} className="chip chip-Macro !text-[10px]">{t}</span>
        ))}
      </span>      <span className="flex w-[170px] shrink-0 justify-end gap-1 overflow-hidden">
        {e.categories.slice(0, 2).map((c) => (
          <span key={c} className={chipClass(c)}>{c}</span>
        ))}
      </span>
    </div>
  );
});
TapeRow.displayName = "TapeRow";

export function EventTape() {
  const events = useTerminal((s) => s.events);
  const selected = useTerminal((s) => s.selected);
  const select = useTerminal((s) => s.select);
  const lastEventAt = useTerminal((s) => s.lastEventAt);
  const parentRef = useRef<HTMLDivElement>(null);
  const cursor = useRef<number>(-1);

  const virtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 26,
    overscan: 30,
  });

  // keyboard: j/k or arrows move selection; Enter opens detail
  const onKeyDown = useCallback(
    (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      const count = events.length;
      if (!count) return;
      if (e.key === "j" || e.key === "ArrowDown") {
        e.preventDefault();
        cursor.current = Math.min(count - 1, cursor.current + 1);
        select(events[cursor.current]);
        virtualizer.scrollToIndex(cursor.current, { align: "auto" });
      } else if (e.key === "k" || e.key === "ArrowUp") {
        e.preventDefault();
        cursor.current = Math.max(0, cursor.current - 1);
        select(events[cursor.current]);
        virtualizer.scrollToIndex(cursor.current, { align: "auto" });
      } else if (e.key === "g") {
        cursor.current = 0;
        select(events[0]);
        virtualizer.scrollToIndex(0);
      } else if (e.key === "G") {
        cursor.current = count - 1;
        select(events[cursor.current]);
        virtualizer.scrollToIndex(count - 1);
      } else if (e.key === "Enter") {
        if (cursor.current >= 0 && events[cursor.current]) select(events[cursor.current]);
      }
    },
    [events, select, virtualizer]
  );

  useEffect(() => {
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onKeyDown]);

  // new events shift the cursor; keep pointing at the same logical item
  useEffect(() => {
    if (cursor.current >= 0 && cursor.current < events.length) {
      // selection follows the data, not the index
    }
  }, [events]);

  const items = virtualizer.getVirtualItems();
  const flashCutoff = Date.now() - lastEventAt < 2000 ? events[0]?.event_id : null;

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-bg">
      {/* column header */}
      <div className="flex h-[24px] shrink-0 items-center gap-3 border-b border-bg-border bg-bg-panel px-2 text-[10px] uppercase tracking-wider text-dim-text">
        <span className="w-9 text-right">URG</span>
        <span className="w-11">AGE</span>
        <span className="w-28">SOURCE</span>
        <span className="w-8 text-center">SNT</span>
        <span className="flex-1">HEADLINE</span>
        <span className="w-[170px] text-right">TICKERS / TAGS</span>
      </div>
      <div ref={parentRef} className="min-h-0 flex-1 overflow-auto">
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((vRow) => {
            const ev = events[vRow.index];
            if (!ev) return null;
            return (
              <div
                key={ev.event_id}
                data-index={vRow.index}
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  width: "100%",
                  height: vRow.size,
                  transform: `translateY(${vRow.start}px)`,
                }}
              >
                <TapeRow
                  ev={ev}
                  selected={selected?.event_id === ev.event_id}
                  flash={flashCutoff === ev.event_id}
                  onSelect={(evv) => {
                    cursor.current = events.findIndex((x) => x.event_id === evv.event_id);
                    select(evv);
                  }}
                />
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
