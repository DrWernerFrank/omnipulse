import { useVirtualizer } from "@tanstack/react-virtual";
import { memo, useEffect, useLayoutEffect, useMemo, useRef } from "react";
import { controllers } from "../lib/controllers";
import { CATEGORY_SHORT, VERIFY_CLASS, VERIFY_GLYPH, hhmmss, sentimentClass, signed, tierClass, urgencyClass } from "../lib/format";
import { isEmptyFilter, matches } from "../lib/query";
import { DB, useUI } from "../store";
import type { NewsEvent } from "../types";
import { Pane } from "./Pane";

const ROW = 22;

const Row = memo(function Row({
  ev,
  selected,
  arrival,
  clusterSize,
  onClick,
}: {
  ev: NewsEvent;
  selected: boolean;
  arrival: number | undefined;
  clusterSize: number;
  onClick: () => void;
}) {
  const e = ev.enrichment;
  const fresh = arrival !== undefined && performance.now() - arrival < 2500;
  const flash = fresh ? (e.urgency >= 9 ? "row-flash-red" : e.urgency >= 7 ? "row-flash" : "") : "";
  return (
    <div
      onClick={onClick}
      title={`${ev.content.raw_title}\npublished ${ev.published_at} · ingested ${ev.ingested_at}`}
      className={`flex h-[22px] cursor-default items-center gap-2 border-l-2 px-2 whitespace-nowrap ${flash} ${
        selected ? "border-amber bg-amber/12" : e.urgency >= 8 ? "border-down/80" : "border-transparent hover:bg-panel2"
      }`}
    >
      <span className="w-[58px] shrink-0 text-dim">{hhmmss(ev.published_at)}</span>
      <span className={`w-[22px] shrink-0 text-center ${urgencyClass(e.urgency)}`}>{e.urgency}</span>
      <span className={`w-[42px] shrink-0 text-right ${sentimentClass(e.sentiment)}`}>{signed(e.sentiment)}</span>
      <span className={`w-[10px] shrink-0 ${VERIFY_CLASS[e.verification_status]}`} title={e.verification_status}>
        {VERIFY_GLYPH[e.verification_status]}
      </span>
      <span className={`w-[132px] shrink-0 truncate uppercase ${tierClass(ev.source.credibility_tier)}`}>
        {ev.source.name}
      </span>
      <span className={`min-w-0 flex-1 truncate ${e.urgency >= 7 ? "text-bright" : "text-text"}`}>
        {ev.content.flash_headline}
      </span>
      {e.tickers.length > 0 && (
        <span className="shrink-0 text-cyan">{e.tickers.slice(0, 3).map((t) => "$" + t).join(" ")}</span>
      )}
      <span className="w-[86px] shrink-0 truncate text-right text-[10px] text-violet">
        {e.categories.map((c) => CATEGORY_SHORT[c] ?? c).join("·")}
      </span>
      <span className="w-[26px] shrink-0 text-right text-[10px] text-amber">{clusterSize > 1 ? `×${clusterSize}` : ""}</span>
    </div>
  );
});

export function FirehosePane({ area }: { area: string }) {
  const rev = useUI((s) => s.rev);
  const filter = useUI((s) => s.filter);
  const filterText = useUI((s) => s.filterText);
  const frozen = useUI((s) => s.frozen);
  const paused = useUI((s) => s.paused);
  const selectedId = useUI((s) => s.selectedId);
  const follow = useUI((s) => s.follow);
  const rate = useUI((s) => s.ratePerMin);
  const select = useUI((s) => s.select);
  const parentRef = useRef<HTMLDivElement>(null);
  const anchor = useRef<{ id: string; offset: number } | null>(null);

  const ids = useMemo(() => {
    const order = frozen ?? DB.order;
    if (isEmptyFilter(filter)) return order.slice();
    return order.filter((id) => {
      const ev = DB.events.get(id);
      return ev ? matches(filter, ev) : false;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rev, filter, frozen]);

  const virt = useVirtualizer({
    count: ids.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW,
    overscan: 30,
  });

  // Keep the reader's place when new rows land above it (unless following the head).
  useLayoutEffect(() => {
    const el = parentRef.current;
    if (!el || !anchor.current) return;
    if (follow) {
      el.scrollTop = 0;
      return;
    }
    const idx = ids.indexOf(anchor.current.id);
    if (idx >= 0) el.scrollTop = idx * ROW + anchor.current.offset;
  }, [ids, follow]);

  const onScroll = () => {
    const el = parentRef.current;
    if (!el) return;
    const top = Math.floor(el.scrollTop / ROW);
    anchor.current = ids[top] ? { id: ids[top], offset: el.scrollTop - top * ROW } : null;
    const atHead = el.scrollTop < ROW;
    if (atHead !== useUI.getState().follow) useUI.setState({ follow: atHead });
  };

  useEffect(() => {
    controllers.firehose = {
      move: (d) => {
        const cur = selectedId ? ids.indexOf(selectedId) : -1;
        const next = Math.max(0, Math.min(ids.length - 1, cur + d));
        if (ids[next]) {
          select(ids[next]);
          virt.scrollToIndex(next, { align: "auto" });
          if (next > 0 && useUI.getState().follow) useUI.setState({ follow: false });
        }
      },
      edge: (end) => {
        const i = end === "top" ? 0 : ids.length - 1;
        if (ids[i]) {
          select(ids[i]);
          virt.scrollToIndex(i);
          useUI.setState({ follow: end === "top" });
        }
      },
      open: () => useUI.getState().setFocus("detail"),
    };
  }, [ids, selectedId, select, virt]);

  const pending = paused ? DB.order.length - (frozen?.length ?? 0) : 0;
  const right = (
    <>
      {paused ? <span className="blink font-bold text-down">PAUSED +{pending}</span> : follow ? <span className="text-up">● FOLLOW</span> : <span>SCROLL</span>}
      <span>{rate}/min</span>
      <span>
        {ids.length.toLocaleString()}
        {filterText ? ` / ${DB.order.length.toLocaleString()}` : ""} rows
      </span>
    </>
  );

  return (
    <Pane id="firehose" title="Firehose" right={right} area={area}>
      <div ref={parentRef} onScroll={onScroll} className="absolute inset-0 overflow-y-auto overflow-x-hidden">
        {ids.length === 0 ? (
          <div className="p-4 text-dim">
            {DB.order.length ? "No events match the current filter. Press Esc to clear." : "Waiting for the first events…"}
          </div>
        ) : (
          <div style={{ height: virt.getTotalSize(), position: "relative" }}>
            {virt.getVirtualItems().map((v) => {
              const ev = DB.events.get(ids[v.index]);
              if (!ev) return null;
              return (
                <div key={v.key} style={{ position: "absolute", top: 0, left: 0, right: 0, transform: `translateY(${v.start}px)` }}>
                  <Row
                    ev={ev}
                    selected={ev.event_id === selectedId}
                    arrival={DB.arrivals.get(ev.event_id)}
                    clusterSize={DB.clusters.get(ev.cluster_id)?.source_count ?? 1}
                    onClick={() => select(ev.event_id)}
                  />
                </div>
              );
            })}
          </div>
        )}
      </div>
    </Pane>
  );
}
