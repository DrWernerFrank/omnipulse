// Live firehose for the tiling skin: one unfiltered /ws connection to the v2
// engine (the skin filters client-side, exactly like the v1 terminal did).
import type { ClusterSummary, NewsEvent, SourceHealth, Stats, WSFrame } from "../types";
import { api, wsUrl } from "./api";
import { applyClusterSummary, refreshTouchedClusters, upsertEvent, useUI } from "../store";

const FLUSH_MS = 100;

let socket: WebSocket | null = null;
let buffer: WSFrame[] = [];
let retry = 0;
let stopped = false;
const arrivals: number[] = [];
let audio: AudioContext | null = null;

function beep(): void {
  try {
    audio ??= new AudioContext();
    const o = audio.createOscillator();
    const g = audio.createGain();
    o.type = "square";
    o.frequency.value = 880;
    g.gain.setValueAtTime(0.05, audio.currentTime);
    g.gain.exponentialRampToValueAtTime(0.0001, audio.currentTime + 0.18);
    o.connect(g).connect(audio.destination);
    o.start();
    o.stop(audio.currentTime + 0.2);
  } catch {
    /* audio blocked until user gesture */
  }
}

function flush(): void {
  if (!buffer.length) return;
  const batch = buffer;
  buffer = [];
  let events = false;
  let clusters = false;
  let flash = false;
  let stats: Stats | undefined;
  const healthPatch: Record<string, SourceHealth> = {};
  let gotHealth = false;
  const now = performance.now();
  for (const frame of batch) {
    switch (frame.t) {
      case "event": {
        const ev = frame.d as NewsEvent;
        if (upsertEvent(ev, true)) {
          arrivals.push(now);
          if (ev.enrichment.urgency >= 9) flash = true;
        }
        events = true;
        break;
      }
      case "cluster":
        applyClusterSummary(frame.d as ClusterSummary);
        clusters = true;
        break;
      case "stats":
        stats = frame.d as Stats;
        break;
      case "source_state": {
        const s = frame.d as {
          source_id: string; health: string; detail: string | null;
          last_success: string | null; interval_sec: number | null;
        };
        healthPatch[s.source_id] = {
          id: s.source_id, status: s.health, interval: s.interval_sec,
          last_success: s.last_success, last_error: s.detail,
        };
        gotHealth = true;
        break;
      }
      default:
        break; // hello / ping
    }
  }
  while (arrivals.length && now - arrivals[0] > 60000) arrivals.shift();
  if (refreshTouchedClusters()) clusters = true;
  const ui = useUI.getState();
  if (gotHealth) useUI.setState({ health: { ...ui.health, ...healthPatch } });
  if (events || clusters) ui.bump(events, clusters);
  useUI.setState({ lastMsg: Date.now(), ratePerMin: arrivals.length, ...(stats ? { stats } : {}) });
  if (flash && ui.sound) beep();
}

function connect(): void {
  useUI.setState({ conn: "connecting" });
  socket = new WebSocket(wsUrl());
  socket.onopen = () => {
    retry = 0;
    useUI.setState({ conn: "live" });
  };
  socket.onmessage = (m) => {
    let frame: WSFrame;
    try {
      frame = JSON.parse(m.data as string);
    } catch {
      return;
    }
    buffer.push(frame);
  };
  socket.onclose = () => {
    useUI.setState({ conn: "down" });
    if (stopped) return;
    const delay = Math.min(15000, 500 * 2 ** retry++) * (0.7 + Math.random() * 0.6);
    setTimeout(connect, delay);
  };
  socket.onerror = () => socket?.close();
}

export async function loadInitial(): Promise<void> {
  const ui = useUI.getState();
  const [ev, cl] = await Promise.allSettled([api.events(1000), api.clusters()]);
  if (ev.status === "fulfilled") for (const e of ev.value) upsertEvent(e, false);
  if (cl.status === "fulfilled") for (const c of cl.value) applyClusterSummary(c);
  const failed = [ev, cl].find((r) => r.status === "rejected") as PromiseRejectedResult | undefined;
  if (failed) ui.notify(`backfill failed: ${(failed.reason as Error).message}`, "err");
  refreshTouchedClusters();
  ui.bump(true, true);
  refreshSources();
}

export async function refreshSources(): Promise<void> {
  try {
    const rows = await api.sources();
    const health: Record<string, SourceHealth> = {};
    for (const r of rows) {
      health[r.id] = {
        id: r.id, status: r.health, interval: r.interval_sec,
        last_success: r.last_success, last_error: r.detail,
      };
    }
    useUI.setState({ sources: rows, health: { ...useUI.getState().health, ...health } });
  } catch {
    /* transient */
  }
}

export function startStream(): () => void {
  stopped = false;
  connect();
  const flusher = window.setInterval(flush, FLUSH_MS);
  const statsTimer = window.setInterval(async () => {
    try {
      useUI.setState({ stats: await api.stats() });
    } catch {
      /* ignore */
    }
  }, 10000);
  const srcTimer = window.setInterval(refreshSources, 30000);
  api.stats().then((s) => useUI.setState({ stats: s })).catch(() => undefined);
  return () => {
    window.clearInterval(flusher);
    window.clearInterval(statsTimer);
    window.clearInterval(srcTimer);
    stopped = true;
    socket?.close();
  };
}
