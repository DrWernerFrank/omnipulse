// REST client for the tiling skin — v2 /api endpoints behind v1-style call signatures.
import type { ClusterSummary, NewsEvent, SourceRow, Stats } from "../types";
import type { Cluster } from "../types";
import { clusterFromSummary } from "../store";
import { localStorageGet } from "./storage";

const BASE = (import.meta.env.VITE_OMNI_API as string | undefined) ?? "";

function headers(): HeadersInit {
  const key = localStorageGet("omni.apiKey");
  return key ? { "X-API-Key": key, "Content-Type": "application/json" } : { "Content-Type": "application/json" };
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(BASE + path, { ...init, headers: { ...headers(), ...(init?.headers ?? {}) } });
  if (!r.ok) {
    let detail = `${r.status}`;
    try {
      const body = await r.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail ?? body);
    } catch {
      /* not json */
    }
    throw new Error(`${detail} (${r.status})`);
  }
  if (r.status === 204) return undefined as T;
  return (await r.json()) as T;
}

function wsUrl(): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}${BASE}/ws`;
}
export { wsUrl };

export const api = {
  events: (limit = 1000) => req<NewsEvent[]>(`/api/events?limit=${limit}`),
  event: (id: string) => req<NewsEvent>(`/api/events/${id}`),

  clusters: () => req<ClusterSummary[]>("/api/clusters?limit=200"),
  cluster: async (id: string): Promise<{ cluster: Cluster; events: NewsEvent[] }> => {
    const [cs, events] = await Promise.all([
      req<ClusterSummary>(`/api/clusters/${id}`),
      req<NewsEvent[]>(`/api/clusters/${id}/events?limit=50`),
    ]);
    return { cluster: clusterFromSummary(cs, events), events };
  },

  sources: async (): Promise<SourceRow[]> =>
    req<
      Array<{
        id: string; name: string; health: string; enabled: boolean;
        detail: string | null; last_success: string | null; interval_sec: number | null;
      }>
    >("/api/sources"),

  addSource: (cfg: Record<string, unknown>) =>
    req<{ id: string }>("/api/sources", { method: "POST", body: JSON.stringify(cfg) }),
  pauseSource: (id: string, pause: boolean) =>
    req<unknown>(`/api/sources/${id}/${pause ? "pause" : "resume"}`, { method: "POST" }),
  deleteSource: (id: string) => req<unknown>(`/api/sources/${id}`, { method: "DELETE" }),

  stats: () => req<Stats>("/api/stats"),
};
