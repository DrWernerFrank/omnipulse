import { useTerminal } from "../store";

// "AI/Tech" -> "AITech": safe CSS class fragment (chip-* selectors)
const chipClass = (cat: string): string => `chip-${cat.replace(/[^A-Za-z0-9]/g, "")}`;

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex gap-2 text-[11px]">
      <span className="w-20 shrink-0 text-dim-text">{label}</span>
      <span className="min-w-0 flex-1 text-dim-text2">{children}</span>
    </div>
  );
}

export function DetailDrawer() {
  const selected = useTerminal((s) => s.select);
  const ev = useTerminal((s) => s.selected);
  const openCluster = useTerminal((s) => s.openCluster);
  const closeCluster = useTerminal((s) => s.closeCluster);
  const selectedCluster = useTerminal((s) => s.selectedCluster);
  const clusterEvents = useTerminal((s) => s.clusterEvents);
  const setFilters = useTerminal((s) => s.setFilters);

  if (selectedCluster) {
    return (
      <div className="flex w-[420px] shrink-0 flex-col border-l border-bg-border bg-bg-panel">
        <div className="flex items-start gap-2 border-b border-bg-border p-2">
          <div className="min-w-0 flex-1">
            <div className="mb-1 text-[9px] uppercase tracking-widest text-accent-text">
              Story cluster · {selectedCluster.event_count} events · {selectedCluster.velocity_15m}/15m
            </div>
            <div className="text-[13px] leading-snug text-dim-text2">{selectedCluster.title}</div>
          </div>
          <button onClick={closeCluster} className="text-dim-text hover:text-dim-text2">esc</button>
        </div>
        <div className="border-b border-bg-border p-2">
          <Row label="Broke by">
            {selectedCluster.breaking_source || "—"}
          </Row>
          <Row label="Sources">
            {selectedCluster.sources.join(", ") || "—"}
          </Row>
          <Row label="Tickers">
            <span className="flex flex-wrap gap-1">
              {selectedCluster.tickers.map((t) => (
                <button key={t} className="chip chip-Macro"
                  onClick={() => setFilters({ tickers: [t] })}>{t}</button>
              ))}
            </span>
          </Row>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto p-2">
          <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">
            Timeline (how it developed)
          </div>
          {clusterEvents.map((e) => (
            <div key={e.event_id} className="mb-1.5 border-l border-bg-border pl-2 text-[11px]">
              <div className="text-dim-text">
                {new Date(e.published_at).toISOString().slice(11, 19)} · {e.source.name}
              </div>
              <div className="text-dim-text2">
                {e.content.flash_headline || e.content.raw_title}
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (!ev) {
    return (
      <div className="flex w-[420px] shrink-0 flex-col items-center justify-center gap-2 border-l border-bg-border bg-bg-panel text-[11px] text-dim-text">
        <span>j / k to move · Enter to inspect</span>
        <span>select an event row to open its dossier</span>
      </div>
    );
  }

  const e = ev.enrichment;
  return (
    <div className="flex w-[420px] shrink-0 flex-col border-l border-bg-border bg-bg-panel">
      <div className="flex items-start gap-2 border-b border-bg-border p-2">
        <div className="min-w-0 flex-1">
          <div className="mb-1 flex items-center gap-2">
            <span className={`chip ${e.verification_status === "CONFIRMED" ? "chip-Macro" : "chip-Breaking"}`}>
              {e.verification_status}
            </span>
            <span className="text-[10px] text-dim-text">
              {new Date(ev.published_at).toISOString().replace("T", " ").slice(0, 19)}Z
            </span>
          </div>
          <div className="text-[13px] font-medium leading-snug text-dim-text2">
            {ev.content.raw_title}
          </div>
        </div>
        <button onClick={() => selected(null)} className="text-dim-text hover:text-dim-text2">esc</button>
      </div>

      <div className="border-b border-bg-border p-2">
        <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">Flash summary</div>
        <div className="mb-2 text-[13px] text-accent-text">{ev.content.flash_headline}</div>
        <ul className="space-y-1 text-[11px] text-dim-text2">
          {ev.content.executive_summary.map((b, i) => (
            <li key={i} className="flex gap-1.5">
              <span className="text-accent-dim">▸</span>
              <span className="min-w-0">{b}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="grid grid-cols-3 gap-1 border-b border-bg-border p-2 text-[11px]">
        <div>
          <div className="text-[9px] uppercase text-dim-text">Urgency</div>
          <div className={e.urgency >= 8 ? "u-high text-[15px]" : "u-med text-[15px]"}>{e.urgency}/10</div>
        </div>
        <div>
          <div className="text-[9px] uppercase text-dim-text">Sentiment</div>
          <div className={`text-[15px] ${e.sentiment > 0.2 ? "sent-pos" : e.sentiment < -0.2 ? "sent-neg" : ""}`}>
            {e.sentiment > 0 ? "+" : ""}{e.sentiment.toFixed(2)}
          </div>
        </div>
        <div>
          <div className="text-[9px] uppercase text-dim-text">Novelty</div>
          <div className="text-[15px] text-dim-text2">{e.novelty.toFixed(2)}</div>
        </div>
      </div>

      <div className="border-b border-bg-border p-2">
        <Row label="Source">
          {ev.source.name} · tier {ev.source.credibility_tier} · {ev.source.id}
        </Row>
        <Row label="Entities">
          <span className="flex flex-wrap gap-1">
            {e.entities.slice(0, 10).map((ent) => (
              <span key={ent.name} className={chipClass("AI/Tech")}>{ent.name}</span>
            ))}
          </span>
        </Row>
        <Row label="Tickers">
          <span className="flex flex-wrap gap-1">
            {e.tickers.length ? e.tickers.map((t) => (
              <button key={t} className="chip chip-Macro"
                onClick={() => setFilters({ tickers: [t] })}>{t}</button>
            )) : <span className="text-dim-text">—</span>}
          </span>
        </Row>
        <Row label="Locations">
          {e.locations.join(" · ") || "—"}
        </Row>
        <Row label="Cluster">
          <button
            className="text-blue-text hover:underline"
            onClick={async () => {
              const res = await fetch(`/api/clusters/${ev.cluster_id}/events?limit=50`);
              const events = await res.json();
              const cres = await fetch(`/api/clusters/${ev.cluster_id}`);
              openCluster(await cres.json(), events);
            }}
          >
            {ev.cluster_id}
          </button>
        </Row>
        {ev.source.url && (
          <Row label="Link">
            <a
              href={ev.source.url}
              target="_blank"
              rel="noreferrer"
              className="text-blue-text hover:underline"
            >
              open origin ↗
            </a>
          </Row>
        )}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-2">
        <div className="mb-1 text-[9px] uppercase tracking-widest text-dim-text">Clean body</div>
        <pre className="whitespace-pre-wrap break-words font-mono text-[11px] leading-relaxed text-dim-text2">
          {ev.content.clean_markdown || "— no fulltext for this item —"}
        </pre>
      </div>
    </div>
  );
}
