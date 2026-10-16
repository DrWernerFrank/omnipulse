// Tiling-skin state: hot data in module-level structures + a zustand UI store.
// Cluster rows keep the v1 pane model, derived from v2 ClusterSummary frames
// plus client-side aggregates over the local event window.
import { create } from "zustand";
import type { Cluster, ClusterSummary, Layout, NewsEvent, Pane, SourceHealth, SourceRow, Verification } from "./types";
import { localStorageGet, localStorageSet } from "./lib/storage";
import { emptyFilter, parseFilter, type Filter } from "./lib/query";

export const MAX_EVENTS = 20000;

export const DB = {
  events: new Map<string, NewsEvent>(),
  order: [] as string[], // newest arrival first (by ingested_at)
  clusters: new Map<string, Cluster>(),
  arrivals: new Map<string, number>(), // client receive time, for flash highlighting
};

// ---------------------------------------------------------------------------
// cluster derivation: v2 ClusterSummary + aggregates over locally seen events
// ---------------------------------------------------------------------------

interface ClusterAgg {
  maxUrg: number;
  sentSum: number;
  sentN: number;
  locations: Set<string>;
  verif: Verification;
  latestIng: string;
  primary: { event_id: string; published_at: string; source: { id: string; name: string; credibility_tier: number } } | null;
}

const clusterAgg = new Map<string, ClusterAgg>();
const clusterSummaries = new Map<string, ClusterSummary>();
const touchedClusters = new Set<string>();

function foldEvent(a: ClusterAgg, ev: NewsEvent): void {
  const e = ev.enrichment;
  if (e.urgency > a.maxUrg) a.maxUrg = e.urgency;
  a.sentSum += e.sentiment;
  a.sentN += 1;
  for (const l of e.locations) a.locations.add(l);
  if (!a.latestIng || ev.ingested_at > a.latestIng) {
    a.latestIng = ev.ingested_at;
    a.verif = e.verification_status;
  }
  if (!a.primary || ev.published_at < a.primary.published_at) {
    a.primary = {
      event_id: ev.event_id,
      published_at: ev.published_at,
      source: { id: ev.source.id, name: ev.source.name, credibility_tier: ev.source.credibility_tier },
    };
  }
}

/** Build a v1-shaped Cluster from a v2 ClusterSummary, with aggregates from the
 *  local window (agg) or an explicit event list (DetailPane drill-down). */
export function clusterFromSummary(cs: ClusterSummary, events?: NewsEvent[]): Cluster {
  let agg = clusterAgg.get(cs.cluster_id);
  if (!agg && events?.length) {
    agg = { maxUrg: 0, sentSum: 0, sentN: 0, locations: new Set(), verif: "DEVELOPING", latestIng: "", primary: null };
    for (const ev of events) foldEvent(agg, ev);
  }
  const primary = agg?.primary;
  return {
    cluster_id: cs.cluster_id,
    first_seen: cs.first_seen ?? "",
    last_updated: cs.latest_at ?? "",
    headline: cs.title,
    primary_event_id: primary?.event_id ?? "",
    primary_source: primary
      ? { ...primary.source, published_at: primary.published_at }
      : {
          id: cs.breaking_source ?? cs.sources[0] ?? "",
          name: cs.breaking_source ?? cs.sources[0] ?? "",
          credibility_tier: 3,
          published_at: "",
        },
    size: cs.event_count,
    event_count: cs.event_count,
    syndication_count: Math.max(0, (cs.sources.length || 1) - 1),
    sources: cs.sources,
    source_count: cs.sources.length,
    categories: cs.categories,
    tickers: cs.tickers,
    locations: agg ? [...agg.locations] : [],
    max_urgency: agg?.maxUrg ?? 0,
    avg_sentiment: agg && agg.sentN ? agg.sentSum / agg.sentN : 0,
    verification_status: agg?.verif ?? "DEVELOPING",
  };
}

/** Store a cluster summary frame and refresh the derived row. */
export function applyClusterSummary(cs: ClusterSummary): void {
  clusterSummaries.set(cs.cluster_id, cs);
  DB.clusters.set(cs.cluster_id, clusterFromSummary(cs));
}

/** Re-derive clusters that received events since the last flush (stream.ts drains this). */
export function refreshTouchedClusters(): boolean {
  if (!touchedClusters.size) return false;
  let changed = false;
  for (const id of touchedClusters) {
    const cs = clusterSummaries.get(id);
    if (cs) {
      DB.clusters.set(id, clusterFromSummary(cs));
      changed = true;
    }
  }
  touchedClusters.clear();
  return changed;
}

// ---------------------------------------------------------------------------
// event window: arrival-ordered ring with binary-search insertion (v1 semantics)
// ---------------------------------------------------------------------------

/** Ordering key: arrival time for fresh items, but publish time once an item is
 *  over an hour old, so backfill bursts read chronologically. */
export function orderKey(ev: NewsEvent): string {
  const cap = new Date(Date.parse(ev.published_at) + 3600_000).toISOString();
  return ev.ingested_at < cap ? ev.ingested_at : cap;
}

function key(id: string): string {
  const ev = DB.events.get(id);
  return ev ? orderKey(ev) : "";
}

export function upsertEvent(ev: NewsEvent, live: boolean): boolean {
  const isNew = !DB.events.has(ev.event_id);
  DB.events.set(ev.event_id, ev);
  if (!isNew) return false;
  if (live) DB.arrivals.set(ev.event_id, performance.now());

  let agg = clusterAgg.get(ev.cluster_id);
  if (!agg) {
    agg = { maxUrg: 0, sentSum: 0, sentN: 0, locations: new Set(), verif: "DEVELOPING", latestIng: "", primary: null };
    clusterAgg.set(ev.cluster_id, agg);
  }
  foldEvent(agg, ev);
  touchedClusters.add(ev.cluster_id);

  const k = orderKey(ev);
  if (!DB.order.length || k >= key(DB.order[0])) {
    DB.order.unshift(ev.event_id);
  } else {
    let lo = 0;
    let hi = DB.order.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (key(DB.order[mid]) > k) lo = mid + 1;
      else hi = mid;
    }
    DB.order.splice(lo, 0, ev.event_id);
  }
  while (DB.order.length > MAX_EVENTS) {
    const old = DB.order.pop()!;
    DB.events.delete(old);
    DB.arrivals.delete(old);
  }
  return true;
}

export function clearEvents(): void {
  DB.events.clear();
  DB.order = [];
  DB.arrivals.clear();
}

// ---------------------------------------------------------------------------
// UI store (v1 terminal state; data stays in DB, React subscribes to revs)
// ---------------------------------------------------------------------------

type Conn = "connecting" | "live" | "down";
type Toast = { msg: string; kind: "info" | "ok" | "err"; at: number } | null;

interface UI {
  rev: number;
  clusterRev: number;
  filterText: string;
  filter: Filter;
  paused: boolean;
  frozen: string[] | null;
  selectedId: string | null;
  clusterSel: string | null;
  sourceSel: string | null;
  focus: Pane;
  zoom: Pane | null;
  layout: Layout;
  conn: Conn;
  lastMsg: number;
  ratePerMin: number;
  sources: SourceRow[];
  health: Record<string, SourceHealth>;
  stats: import("./types").Stats | null;
  help: boolean;
  sound: boolean;
  follow: boolean;
  toast: Toast;
  command: { open: boolean; seed: string };

  bump: (events: boolean, clusters: boolean) => void;
  setFilter: (text: string) => void;
  togglePause: () => void;
  select: (id: string | null) => void;
  setFocus: (p: Pane) => void;
  toggleZoom: () => void;
  cycleLayout: () => void;
  setLayout: (l: Layout) => void;
  set: (patch: Partial<UI>) => void;
  notify: (msg: string, kind?: "info" | "ok" | "err") => void;
  openCommand: (seed: string) => void;
}

const LAYOUTS: Layout[] = ["quad", "wide", "reader"];

export const useUI = create<UI>((set, get) => ({
  rev: 0,
  clusterRev: 0,
  filterText: localStorageGet("omni.filter") ?? "",
  filter: parseFilter(localStorageGet("omni.filter") ?? ""),
  paused: false,
  frozen: null,
  selectedId: null,
  clusterSel: null,
  sourceSel: null,
  focus: "firehose",
  zoom: null,
  layout: (localStorageGet("omni.layout") as Layout) || "quad",
  conn: "connecting",
  lastMsg: 0,
  ratePerMin: 0,
  sources: [],
  health: {},
  stats: null,
  help: false,
  sound: localStorageGet("omni.sound") === "1",
  follow: true,
  toast: null,
  command: { open: false, seed: "" },

  bump: (events, clusters) =>
    set((s) => ({ rev: events ? s.rev + 1 : s.rev, clusterRev: clusters ? s.clusterRev + 1 : s.clusterRev })),
  setFilter: (text) => {
    localStorageSet("omni.filter", text);
    set({ filterText: text, filter: text.trim() ? parseFilter(text) : emptyFilter() });
  },
  togglePause: () => {
    const paused = !get().paused;
    set({ paused, frozen: paused ? [...DB.order] : null });
  },
  select: (id) => set({ selectedId: id }),
  setFocus: (p) => set({ focus: p }),
  toggleZoom: () => set((s) => ({ zoom: s.zoom ? null : s.focus })),
  cycleLayout: () => get().setLayout(LAYOUTS[(LAYOUTS.indexOf(get().layout) + 1) % LAYOUTS.length]),
  setLayout: (l) => {
    localStorageSet("omni.layout", l);
    set({ layout: l, zoom: null });
  },
  set: (patch) => set(patch),
  notify: (msg, kind = "info") => set({ toast: { msg, kind, at: Date.now() } }),
  openCommand: (seed) => set({ command: { open: true, seed } }),
}));

export const currentOrder = (): string[] => useUI.getState().frozen ?? DB.order;
