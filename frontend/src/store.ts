import { create } from "zustand";
import type { ClusterSummary, NewsEvent, SourceState, Stats } from "./types";

const MAX_ROWS = 10_000; // ring capacity; virtualized list renders only what's visible

// event ids already on the tape (or deliberately filtered): kills duplicates
// from snapshot-seed vs live-WS overlap and duplicate frames on reconnect
const seenIds = new Set<string>();

export type Appearance = "tape" | "tiling";
const APPEARANCE_KEY = "omnipulse.appearance";

function loadAppearance(): Appearance {
  try {
    return localStorage.getItem(APPEARANCE_KEY) === "tiling" ? "tiling" : "tape";
  } catch {
    return "tape";
  }
}

export interface Filters {
  q: string;
  categories: string[];
  tickers: string[];
  minUrgency: number; // 0 = off
  paused: boolean;    // freeze tape (buffer still fills in background)
}

interface TerminalState {
  events: NewsEvent[];
  clusters: ClusterSummary[];
  sources: SourceState[];
  stats: Stats | null;
  selected: NewsEvent | null;
  selectedCluster: ClusterSummary | null;
  clusterEvents: NewsEvent[];
  filters: Filters;
  connection: "connecting" | "live" | "down";
  paletteOpen: boolean;
  leftPane: "filters" | "sources" | "clusters";
  rightPane: boolean;
  lastEventAt: number; // for flash animation
  appearance: Appearance; // terminal skin: tape (v2) or tiling (classic v1)

  pushEvent: (ev: NewsEvent) => void;
  setClusters: (cs: ClusterSummary[]) => void;
  setSources: (ss: SourceState[]) => void;
  setStats: (s: Stats) => void;
  setConnection: (c: TerminalState["connection"]) => void;
  select: (ev: NewsEvent | null) => void;
  openCluster: (c: ClusterSummary, events: NewsEvent[]) => void;
  closeCluster: () => void;
  setFilters: (patch: Partial<Filters>) => void;
  toggleCategory: (cat: string) => void;
  togglePaused: () => void;
  togglePane: (pane: "leftPane" | "rightPane" | null, value?: never) => void;
  setPalette: (open: boolean) => void;
  clearTape: () => void;
  setAppearance: (a: Appearance) => void;
}

export const useTerminal = create<TerminalState>((set, get) => ({
  events: [],
  clusters: [],
  sources: [],
  stats: null,
  selected: null,
  selectedCluster: null,
  clusterEvents: [],
  filters: { q: "", categories: [], tickers: [], minUrgency: 0, paused: false },
  connection: "connecting",
  paletteOpen: false,
  leftPane: "filters",
  rightPane: true,
  lastEventAt: 0,
  appearance: loadAppearance(),

  pushEvent: (ev) => {
    const { events, filters } = get();
    if (seenIds.has(ev.event_id)) return;
    // client-side re-filter (server already filters; this catches local-only modes)
    if (!matchLocal(ev, filters)) return;
    if (seenIds.size > 40_000) {
      // bound memory on long sessions: keep only ids still on the tape
      seenIds.clear();
      for (const e of events) seenIds.add(e.event_id);
    }
    seenIds.add(ev.event_id);
    const next = [ev, ...events];
    if (next.length > MAX_ROWS) next.length = MAX_ROWS;
    set({ events: next, lastEventAt: Date.now() });
  },

  setClusters: (clusters) => set({ clusters }),
  setSources: (sources) => set({ sources }),
  setStats: (stats) => set({ stats }),
  setConnection: (connection) => set({ connection }),

  select: (selected) => set({ selected, rightPane: selected ? true : get().rightPane }),
  openCluster: (selectedCluster, clusterEvents) =>
    set({ selectedCluster, clusterEvents, rightPane: true }),
  closeCluster: () => set({ selectedCluster: null, clusterEvents: [] }),

  setFilters: (patch) => {
    set({ filters: { ...get().filters, ...patch } });
    // re-apply local filters to the buffer
    const { events, filters: f } = get();
    set({ events: events.filter((e) => matchLocal(e, f)) });
  },

  toggleCategory: (cat) => {
    const { filters } = get();
    const has = filters.categories.includes(cat);
    const categories = has
      ? filters.categories.filter((c) => c !== cat)
      : [...filters.categories, cat];
    get().setFilters({ categories });
  },

  togglePaused: () => set({ filters: { ...get().filters, paused: !get().filters.paused } }),

  togglePane: (pane) => {
    if (pane === "leftPane") {
      const order: Array<TerminalState["leftPane"]> = ["filters", "sources", "clusters"];
      const cur = order.indexOf(get().leftPane);
      set({ leftPane: order[(cur + 1) % order.length] });
    } else if (pane === "rightPane") {
      set({ rightPane: !get().rightPane });
    }
  },

  setPalette: (paletteOpen) => set({ paletteOpen }),
  clearTape: () => {
    seenIds.clear();
    set({ events: [] });
  },
  setAppearance: (appearance) => {
    try {
      localStorage.setItem(APPEARANCE_KEY, appearance);
    } catch {
      /* private mode */
    }
    set({ appearance });
  },
}));

function matchLocal(ev: NewsEvent, f: Filters): boolean {
  if (f.categories.length && !ev.enrichment.categories.some((c) => f.categories.includes(c)))
    return false;
  if (f.minUrgency > 0 && ev.enrichment.urgency < f.minUrgency) return false;
  if (f.q) {
    const hay = `${ev.content.raw_title} ${ev.content.flash_headline}`.toLowerCase();
    if (!hay.includes(f.q.toLowerCase())) return false;
  }
  if (f.tickers.length && !ev.enrichment.tickers.some((t) => f.tickers.includes(t)))
    return false;
  return true;
}
