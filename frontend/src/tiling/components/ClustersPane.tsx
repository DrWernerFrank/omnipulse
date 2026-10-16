import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useMemo, useRef, useState } from "react";
import { controllers } from "../lib/controllers";
import { VERIFY_CLASS, VERIFY_GLYPH, ago, sentimentClass, signed, urgencyClass } from "../lib/format";
import type { Filter } from "../lib/query";
import { DB, useUI } from "../store";
import type { Cluster } from "../types";
import { Pane } from "./Pane";

const ROW = 22;

function clusterMatches(c: Cluster, f: Filter): boolean {
  if (c.max_urgency < f.minUrgency) return false;
  if (f.categories.length && !f.categories.some((x) => c.categories.includes(x))) return false;
  if (f.tickers.length && !f.tickers.some((x) => c.tickers.includes(x))) return false;
  if (f.locations.length && !f.locations.some((x) => c.locations.includes(x))) return false;
  if (f.text.length && f.text.some((t) => !c.headline.toLowerCase().includes(t))) return false;
  return true;
}

export function ClustersPane({ area }: { area: string }) {
  const clusterRev = useUI((s) => s.clusterRev);
  const filter = useUI((s) => s.filter);
  const sel = useUI((s) => s.clusterSel);
  const [showAll, setShowAll] = useState(false);
  const [now, setNow] = useState(Date.now());
  const parentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(t);
  }, []);

  const rows = useMemo(() => {
    const out: Cluster[] = [];
    for (const c of DB.clusters.values()) {
      if (!showAll && c.source_count < 2) continue;
      if (clusterMatches(c, filter)) out.push(c);
    }
    out.sort((a, b) => (a.last_updated < b.last_updated ? 1 : -1));
    return out.slice(0, 2000);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterRev, filter, showAll]);

  const virt = useVirtualizer({ count: rows.length, getScrollElement: () => parentRef.current, estimateSize: () => ROW, overscan: 20 });

  const openCluster = (c: Cluster) => {
    const ui = useUI.getState();
    ui.set({ clusterSel: c.cluster_id, selectedId: c.primary_event_id || null });
  };

  useEffect(() => {
    controllers.clusters = {
      move: (d) => {
        const cur = sel ? rows.findIndex((c) => c.cluster_id === sel) : -1;
        const next = Math.max(0, Math.min(rows.length - 1, cur + d));
        if (rows[next]) {
          openCluster(rows[next]);
          virt.scrollToIndex(next);
        }
      },
      edge: (end) => {
        const i = end === "top" ? 0 : rows.length - 1;
        if (rows[i]) {
          openCluster(rows[i]);
          virt.scrollToIndex(i);
        }
      },
      open: () => {
        if (sel) {
          useUI.getState().setFilter(`cluster:${sel}`);
          useUI.getState().setFocus("firehose");
        }
      },
      key: (k) => {
        if (k === "a") {
          setShowAll((v) => !v);
          return true;
        }
        return false;
      },
    };
  }, [rows, sel, virt]);

  const right = (
    <>
      <span>{showAll ? "ALL" : "MULTI-SOURCE"} (a)</span>
      <span>{rows.length} stories</span>
    </>
  );

  return (
    <Pane id="clusters" title="Story Clusters" right={right} area={area}>
      <div ref={parentRef} className="absolute inset-0 overflow-y-auto overflow-x-hidden">
        {rows.length === 0 && (
          <div className="p-4 text-dim">No corroborated stories yet — clusters appear when 2+ independent sources report the same event. Press a to show all.</div>
        )}
        <div style={{ height: virt.getTotalSize(), position: "relative" }}>
          {virt.getVirtualItems().map((v) => {
            const c = rows[v.index];
            const active = c.cluster_id === sel;
            return (
              <div
                key={c.cluster_id}
                onClick={() => openCluster(c)}
                onDoubleClick={() => controllers.clusters?.open()}
                style={{ position: "absolute", top: 0, left: 0, right: 0, transform: `translateY(${v.start}px)` }}
                className={`flex h-[22px] items-center gap-2 border-l-2 px-2 whitespace-nowrap ${
                  active ? "border-amber bg-amber/12" : "border-transparent hover:bg-panel2"
                }`}
                title={`${c.sources.join(", ")}\nfirst: ${c.primary_source.name} @ ${c.primary_source.published_at}`}
              >
                <span className="w-[30px] shrink-0 text-dim">{ago(c.last_updated, now)}</span>
                <span className={`w-[22px] shrink-0 text-center ${urgencyClass(c.max_urgency)}`}>{c.max_urgency}</span>
                <span className="w-[34px] shrink-0 text-right text-amber">×{c.source_count}</span>
                <span className={`w-[10px] shrink-0 ${VERIFY_CLASS[c.verification_status]}`}>{VERIFY_GLYPH[c.verification_status]}</span>
                <span className={`w-[42px] shrink-0 text-right ${sentimentClass(c.avg_sentiment)}`}>{signed(c.avg_sentiment)}</span>
                <span className="min-w-0 flex-1 truncate text-bright">{c.headline}</span>
                {c.tickers.length > 0 && <span className="shrink-0 text-cyan">{c.tickers.slice(0, 3).map((t) => "$" + t).join(" ")}</span>}
                <span className="w-[110px] shrink-0 truncate text-right text-[10px] uppercase text-dim">{c.primary_source.name}</span>
              </div>
            );
          })}
        </div>
      </div>
    </Pane>
  );
}
