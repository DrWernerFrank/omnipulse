import { create } from "zustand";
import { localStorageGet, localStorageSet } from "./lib/api";
import { emptyFilter, parseFilter, type Filter } from "./lib/query";
import type { Cluster, Layout, NewsEvent, Pane, SourceHealth, SourceRow } from "./types";

export const MAX_EVENTS = 20000;

/**
 * Hot data lives in mutable module-level structures; React subscribes to cheap revision
 * counters instead of copying 20k-row maps on every streamed batch.
 */
export const DB = {
  events: new Map<string, NewsEvent>(),
  order: [] as string[], // newest arrival first (by ingested_at)
  clusters: new Map<string, Cluster>(),
  arrivals: new Map<string, number>(), // client receive time, for flash highlighting
};

/**
 * Ordering key: arrival time for fresh items (a late-arriving story is still news to you), but
 * publish time once an item is over an hour old, so backfill bursts read chronologically.
 */
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
  stats: Record<string, any> | null;
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
