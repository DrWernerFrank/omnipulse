import DOMPurify from "dompurify";
import { marked } from "marked";
import { useEffect, useMemo, useState } from "react";
import { api } from "../lib/api";
import { VERIFY_CLASS, VERIFY_GLYPH, hhmmss, sentimentClass, signed, tierClass, urgencyClass } from "../lib/format";
import { DB, useUI } from "../store";
import type { Cluster, NewsEvent } from "../types";
import { Pane } from "./Pane";

DOMPurify.addHook("afterSanitizeAttributes", (node) => {
  if (node.tagName === "A") {
    node.setAttribute("target", "_blank");
    node.setAttribute("rel", "noopener noreferrer");
  }
});

function Bar({ value, min, max, cls }: { value: number; min: number; max: number; cls: string }) {
  const pct = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
  return (
    <span className="relative inline-block h-[6px] w-[70px] bg-line align-middle">
      <span className={`absolute inset-y-0 left-0 ${cls}`} style={{ width: `${pct}%` }} />
    </span>
  );
}

function Chip({ children, cls = "text-text", onClick }: { children: React.ReactNode; cls?: string; onClick?: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`border border-line bg-panel2 px-1.5 py-[1px] text-[11px] hover:border-amber/60 ${cls}`}
    >
      {children}
    </button>
  );
}

export function DetailPane({ area }: { area: string }) {
  const selectedId = useUI((s) => s.selectedId);
  const rev = useUI((s) => s.rev);
  const setFilter = useUI((s) => s.setFilter);
  const [cluster, setCluster] = useState<{ cluster: Cluster; events: NewsEvent[] } | null>(null);

  const ev = useMemo(() => (selectedId ? DB.events.get(selectedId) ?? null : null), [selectedId, rev]);
  const clusterSize = ev ? DB.clusters.get(ev.cluster_id)?.size ?? 1 : 1;

  useEffect(() => {
    setCluster(null);
    if (!ev || clusterSize < 2) return;
    let alive = true;
    api.cluster(ev.cluster_id).then((c) => alive && setCluster(c)).catch(() => undefined);
    return () => {
      alive = false;
    };
  }, [ev?.cluster_id, clusterSize]);

  const html = useMemo(() => {
    if (!ev?.content.clean_markdown) return "";
    return DOMPurify.sanitize(marked.parse(ev.content.clean_markdown, { async: false }) as string);
  }, [ev?.content.clean_markdown]);

  if (!ev) {
    return (
      <Pane id="detail" title="Reader" area={area}>
        <div className="p-4 leading-6 text-dim">
          Select an event (j/k, Enter) to read its flash headline, executive summary, entities, story timeline and
          clean article Markdown.
          <br />
          Press <span className="text-amber">?</span> for all keyboard shortcuts.
        </div>
      </Pane>
    );
  }

  const e = ev.enrichment;
  const latency = Date.parse(ev.ingested_at) - Date.parse(ev.published_at);
  const right = (
    <>
      <span className={VERIFY_CLASS[e.verification_status]}>
        {VERIFY_GLYPH[e.verification_status]} {e.verification_status}
      </span>
      <span>{ev.event_id}</span>
    </>
  );

  return (
    <Pane id="detail" title="Reader" right={right} area={area}>
      <div className="absolute inset-0 overflow-y-auto px-3 py-2">
        <div className="mb-1 flex items-center gap-2 text-[11px]">
          <span className={`px-1 ${urgencyClass(e.urgency)}`}>U{e.urgency}</span>
          <span className={`uppercase ${tierClass(ev.source.credibility_tier)}`}>{ev.source.name}</span>
          <span className="text-dim">T{ev.source.credibility_tier}</span>
          <span className="text-dim">
            {ev.published_at.slice(0, 10)} {hhmmss(ev.published_at)}Z
          </span>
          {latency > 0 && latency < 86400000 && (
            <span className="text-dim" title="ingested_at − published_at">
              +{latency < 60000 ? `${(latency / 1000).toFixed(1)}s` : `${Math.round(latency / 60000)}m`} to wire
            </span>
          )}
        </div>
        <h1 className="mb-2 text-[15px] font-bold leading-snug text-bright">{ev.content.flash_headline}</h1>
        {ev.content.raw_title !== ev.content.flash_headline && (
          <div className="mb-2 text-[11px] text-dim">{ev.content.raw_title}</div>
        )}

        <div className="mb-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[11px]">
          <span className="text-dim">SENTIMENT</span>
          <span className={sentimentClass(e.sentiment)}>
            <Bar value={e.sentiment} min={-1} max={1} cls={e.sentiment >= 0 ? "bg-up" : "bg-down"} /> {signed(e.sentiment)}
          </span>
          <span className="text-dim">URGENCY</span>
          <span>
            <Bar value={e.urgency} min={0} max={10} cls={e.urgency >= 8 ? "bg-down" : "bg-amber"} /> {e.urgency}/10
          </span>
          <span className="text-dim">NOVELTY</span>
          <span>
            <Bar value={e.novelty} min={0} max={1} cls="bg-cyan" /> {e.novelty.toFixed(2)}
          </span>
        </div>

        <div className="mb-2 flex flex-wrap gap-1">
          {e.categories.map((c) => (
            <Chip key={c} cls="text-violet" onClick={() => setFilter(`cat:${c}`)}>{c}</Chip>
          ))}
          {e.tickers.map((t) => (
            <Chip key={t} cls="text-cyan" onClick={() => setFilter(`$${t}`)}>${t}</Chip>
          ))}
          {e.locations.map((l) => (
            <Chip key={l} cls="text-amber" onClick={() => setFilter(`loc:${l}`)}>{l}</Chip>
          ))}
        </div>
        {e.entities.length > 0 && (
          <div className="mb-3 flex flex-wrap gap-1">
            {e.entities.slice(0, 16).map((x) => (
              <Chip key={x.type + x.name} onClick={() => setFilter(`ent:${x.name.toLowerCase().split(/\s+/)[0]}`)}>
                <span className="text-dim">{x.type[0]}·</span>
                {x.name}
              </Chip>
            ))}
          </div>
        )}

        <div className="mb-3 border-l-2 border-amber pl-2">
          <div className="mb-1 text-[10px] uppercase tracking-wider text-dim">Executive summary</div>
          <ul className="space-y-1">
            {ev.content.executive_summary.map((b, i) => (
              <li key={i} className="flex gap-2">
                <span className="text-amber">▸</span>
                <span>{b}</span>
              </li>
            ))}
          </ul>
        </div>

        {cluster && cluster.events.length > 1 && (
          <div className="mb-3">
            <div className="mb-1 text-[10px] uppercase tracking-wider text-dim">
              Story timeline · {cluster.cluster.source_count} sources · {cluster.cluster.syndication_count} syndications
            </div>
            {cluster.events.map((m, i) => (
              <button
                type="button"
                key={m.event_id}
                onClick={() => {
                  if (!DB.events.has(m.event_id)) DB.events.set(m.event_id, m);
                  useUI.getState().select(m.event_id);
                }}
                className={`flex w-full gap-2 text-left text-[11px] hover:bg-panel2 ${m.event_id === ev.event_id ? "text-amber" : ""}`}
              >
                <span className="w-[58px] shrink-0 text-dim">{hhmmss(m.published_at)}</span>
                <span className="w-[16px] shrink-0 text-dim">{i === 0 ? "1st" : ""}</span>
                <span className="w-[120px] shrink-0 truncate uppercase">{m.source.name}</span>
                <span className="min-w-0 flex-1 truncate">{m.content.flash_headline}</span>
              </button>
            ))}
          </div>
        )}

        <div className="mb-2 flex items-center gap-3 text-[11px]">
          {ev.source.url && (
            <a href={ev.source.url} target="_blank" rel="noopener noreferrer" className="text-cyan hover:underline">
              open original ↗ (o)
            </a>
          )}
          <span className="text-dim">copy JSON (y) · cluster filter (c)</span>
        </div>
        {html ? (
          <article className="md border-t border-line pt-2" dangerouslySetInnerHTML={{ __html: html }} />
        ) : (
          <div className="border-t border-line pt-2 text-dim">No article body in the feed for this item.</div>
        )}
      </div>
    </Pane>
  );
}
