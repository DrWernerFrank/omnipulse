import { DB, upsertEvent, useUI } from "../store";
import type { Envelope, SourceHealth } from "../types";
import { api, wsUrl } from "./api";

const FLUSH_MS = 100;

let socket: WebSocket | null = null;
let buffer: Envelope[] = [];
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
  const health: Record<string, SourceHealth> = {};
  let gotHealth = false;
  const now = performance.now();
  for (const env of batch) {
    switch (env.type) {
      case "EVENT_CREATED":
      case "EVENT_UPDATED": {
        const live = env.type === "EVENT_CREATED" && !env.backfill;
        if (upsertEvent(env.data, live) && live) {
          arrivals.push(now);
          if (env.data.enrichment.urgency >= 9) flash = true;
        }
        events = true;
        break;
      }
      case "CLUSTER_UPDATED":
        DB.clusters.set(env.data.cluster_id, env.data);
        clusters = true;
        break;
      case "SOURCE_HEALTH":
        for (const h of env.data.sources) health[h.id] = h;
        gotHealth = true;
        break;
      case "HEARTBEAT":
        if (env.data && Object.keys(env.data).length) useUI.setState((s) => ({ stats: { ...(s.stats ?? {}), pipeline: env.data } }));
        break;
    }
  }
  while (arrivals.length && now - arrivals[0] > 60000) arrivals.shift();
  const ui = useUI.getState();
  if (gotHealth) useUI.setState({ health });
  if (events || clusters) ui.bump(events, clusters);
  useUI.setState({ lastMsg: Date.now(), ratePerMin: arrivals.length });
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
    let env: Envelope;
    try {
      env = JSON.parse(m.data as string);
    } catch {
      return;
    }
    if (env.type === "BATCH") buffer.push(...env.data);
    else buffer.push(env);
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
  const [ev, cl] = await Promise.allSettled([api.events(1500), api.clusters()]);
  if (ev.status === "fulfilled") for (const e of ev.value.items) upsertEvent(e, false);
  if (cl.status === "fulfilled") for (const c of cl.value.items) DB.clusters.set(c.cluster_id, c);
  const failed = [ev, cl].find((r) => r.status === "rejected") as PromiseRejectedResult | undefined;
  if (failed) ui.notify(`backfill failed: ${(failed.reason as Error).message}`, "err");
  ui.bump(true, true);
  refreshSources();
}

export async function refreshSources(): Promise<void> {
  try {
    const s = await api.sources();
    const health: Record<string, SourceHealth> = {};
    for (const r of s.items) if (r.health) health[r.id] = r.health;
    useUI.setState({ sources: s.items, health: { ...useUI.getState().health, ...health } });
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
