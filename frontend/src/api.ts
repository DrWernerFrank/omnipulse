import { useTerminal } from "./store";
import type { ClusterSummary, NewsEvent, SourceState, Stats, WSFrame } from "./types";

const API = import.meta.env.DEV ? "" : ""; // same-origin via nginx proxy

//
// WebSocket firehose — reconnect with exponential backoff.
// Sends the active filter so the server only pushes matching events.
//
export function connectStream(): () => void {
  let ws: WebSocket | null = null;
  let closed = false;
  let backoff = 500;
  let retryTimer: number | undefined;

  const filterParams = (): URLSearchParams => {
    const st = useTerminal.getState();
    const p = new URLSearchParams();
    if (st.filters.q) p.set("q", st.filters.q);
    if (st.filters.categories.length) p.set("categories", st.filters.categories.join(","));
    if (st.filters.tickers.length) p.set("tickers", st.filters.tickers.join(","));
    if (st.filters.minUrgency > 0) p.set("min_urgency", String(st.filters.minUrgency));
    return p;
  };

  const buildUrl = (): string => {
    const p = filterParams();
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${location.host}/ws?${p.toString()}`;
  };

  // Instant tape: the WS only carries events from now on, so on first load we
  // seed the tape with the most recent buffer via REST. Feels immediate
  // instead of blank-until-next-event.
  const seedSnapshot = async () => {
    const st = useTerminal.getState();
    if (st.events.length > 0 || st.filters.paused) return;
    try {
      const p = filterParams();
      p.set("limit", "100");
      const evs = await api.events(p);
      for (let i = evs.length - 1; i >= 0; i--) st.pushEvent(evs[i]); // oldest first
    } catch {
      /* live stream will populate the tape anyway */
    }
  };

  const connect = () => {
    if (closed) return;
    useTerminal.getState().setConnection("connecting");
    ws = new WebSocket(buildUrl());
    ws.onopen = () => {
      backoff = 500;
      useTerminal.getState().setConnection("live");
      void seedSnapshot();
    };
    ws.onmessage = (msg) => {
      let frame: WSFrame;
      try {
        frame = JSON.parse(msg.data as string);
      } catch {
        return;
      }
      handleFrame(frame);
    };
    ws.onclose = () => {
      useTerminal.getState().setConnection("down");
      if (!closed) {
        retryTimer = window.setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, 8000);
      }
    };
    ws.onerror = () => ws?.close();
  };

  // resubscribe when filters change (server-side filtering)
  const unsub = useTerminal.subscribe((state, prev) => {
    const f = state.filters;
    const pf = prev.filters;
    if (
      ws && ws.readyState === WebSocket.OPEN &&
      (f.q !== pf.q ||
        f.categories !== pf.categories ||
        f.tickers !== pf.tickers ||
        f.minUrgency !== pf.minUrgency)
    ) {
      ws.send(JSON.stringify({ type: "resubscribe", filters: {
        q: f.q || undefined,
        categories: f.categories.length ? f.categories : undefined,
        tickers: f.tickers.length ? f.tickers : undefined,
        min_urgency: f.minUrgency > 0 ? f.minUrgency : undefined,
      }}));
    }
  });

  connect();
  return () => {
    closed = true;
    if (retryTimer) clearTimeout(retryTimer);
    unsub();
    ws?.close();
  };
}

function handleFrame(frame: WSFrame) {
  const st = useTerminal.getState();
  switch (frame.t) {
    case "event":
      if (!st.filters.paused) st.pushEvent(frame.d as NewsEvent);
      else pausedBuffer.push(frame.d as NewsEvent);
      break;
    case "cluster":
      // merge into cluster list, keep top 100 by recency
      {
        const cs = frame.d as ClusterSummary;
        const rest = st.clusters.filter((c) => c.cluster_id !== cs.cluster_id);
        st.setClusters([cs, ...rest].slice(0, 100));
      }
      break;
    case "stats":
      st.setStats(frame.d as Stats);
      break;
    case "source_state":
      {
        const ss = frame.d as SourceState;
        const rest = st.sources.filter((s) => s.source_id !== ss.source_id);
        st.setSources([ss, ...rest]);
      }
      break;
    default:
      break;
  }
}

// paused-tape buffer: cap at 500 so unpausing doesn't dump a wall
const pausedBuffer: NewsEvent[] = [];
export function drainPaused(): NewsEvent[] {
  const out = pausedBuffer.splice(0, pausedBuffer.length);
  return out.reverse();
}

//
// REST helpers (source management, cluster drill-down, initial snapshots)
//
async function j<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${API}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

export const api = {
  events: (params: URLSearchParams) =>
    j<NewsEvent[]>(`/api/events?${params.toString()}`),
  event: (id: string) => j<NewsEvent>(`/api/events/${id}`),
  clusters: (limit = 50) => j<ClusterSummary[]>(`/api/clusters?limit=${limit}`),
  cluster: (id: string) => j<ClusterSummary>(`/api/clusters/${id}`),
  clusterEvents: (id: string, limit = 50) =>
    j<NewsEvent[]>(`/api/clusters/${id}/events?limit=${limit}`),
  sources: () => j<SourceState[]>(`/api/sources`),
  pauseSource: (id: string) => fetch(`/api/sources/${id}/pause`, { method: "POST" }),
  resumeSource: (id: string) => fetch(`/api/sources/${id}/resume`, { method: "POST" }),
  removeSource: (id: string) => fetch(`/api/sources/${id}`, { method: "DELETE" }),
  addSource: (cfg: Record<string, unknown>) =>
    j<unknown>(`/api/sources`, { method: "POST", body: JSON.stringify(cfg) }),
  stats: () => j<Stats>(`/api/stats`),
};
