import { COMMANDS } from "../lib/commands";
import { useUI } from "../store";

const KEYS: [string, string][] = [
  ["/", "filter (DSL below)"],
  [":", "command mode"],
  ["j / k  ↓ / ↑", "move selection"],
  ["PgDn / PgUp  ^d / ^u", "page"],
  ["g / G", "top / bottom"],
  ["Enter", "open (reader · cluster filter · source filter)"],
  ["1 2 3 4  Tab", "focus pane"],
  ["z", "zoom focused pane"],
  ["L", "cycle layout (quad · wide · reader)"],
  ["space", "pause / resume the live view"],
  ["f", "follow the head of the stream"],
  ["u", "cycle urgency floor (off · 5 · 7 · 9)"],
  ["c", "show the selected event's whole story cluster"],
  ["o", "open original article"],
  ["y", "copy selected event as JSON (agent-ready)"],
  ["m", "toggle sound on urgency ≥ 9"],
  ["Esc", "clear filter / unzoom / close"],
  ["a", "(clusters) show all vs multi-source only"],
  ["p · x · r", "(sources) pause/resume · remove (x twice) · refresh"],
];

const DSL: [string, string][] = [
  ["u>=7  u:9  u<4", "urgency"],
  ["cat:crypto,macro", "categories (ai, geo, cyber, reg, eq, sci, brk…)"],
  ["$NVDA  tk:BTC,ETH", "tickers"],
  ["src:reuters  scat:wire", "source id/name · source category"],
  ["loc:US,CN", "ISO country codes"],
  ["tier<=2  tier:1", "credibility tier"],
  ["sent>0.3  sent<-0.3", "sentiment"],
  ["nov>=0.5", "novelty"],
  ["vs:confirmed", "verification status"],
  ["ent:openai", "entity name"],
  ['"rate cut"  -opinion', "phrase must / must not appear"],
];

export function HelpOverlay() {
  const open = useUI((s) => s.help);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-bg/80 p-4" onClick={() => useUI.setState({ help: false })}>
      <div className="max-h-full w-full max-w-[880px] overflow-y-auto border border-amber/60 bg-panel p-4 text-[12px]" onClick={(e) => e.stopPropagation()}>
        <div className="mb-3 flex justify-between">
          <span className="font-bold tracking-[0.2em] text-amber">OMNIPULSE · KEYBOARD</span>
          <span className="text-dim">Esc to close</span>
        </div>
        <div className="grid gap-6 md:grid-cols-2">
          <div>
            {KEYS.map(([k, d]) => (
              <div key={k} className="flex gap-3 py-[1px]">
                <span className="w-[150px] shrink-0 text-amber">{k}</span>
                <span>{d}</span>
              </div>
            ))}
          </div>
          <div>
            <div className="mb-1 text-[10px] uppercase tracking-wider text-dim">Filter DSL — keys AND, repeated values OR</div>
            {DSL.map(([k, d]) => (
              <div key={k} className="flex gap-3 py-[1px]">
                <span className="w-[170px] shrink-0 text-cyan">{k}</span>
                <span>{d}</span>
              </div>
            ))}
            <div className="mb-1 mt-4 text-[10px] uppercase tracking-wider text-dim">Commands</div>
            {COMMANDS.map(([k, d]) => (
              <div key={k} className="flex gap-3 py-[1px]">
                <span className="w-[170px] shrink-0 text-violet">{k}</span>
                <span>{d}</span>
              </div>
            ))}
            <div className="mt-4 text-dim">
              Agents: OpenAPI at <span className="text-bright">/openapi.json</span> · REST at{" "}
              <span className="text-bright">/api/events?compact=true</span> · WS{" "}
              <span className="text-bright">/ws</span> · SSE <span className="text-bright">/stream/events</span> · NDJSON{" "}
              <span className="text-bright">/api/events.jsonl</span> · MCP via{" "}
              <span className="text-bright">python -m app.mcp_server</span> (stdio).
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
