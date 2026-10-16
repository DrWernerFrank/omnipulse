import type { Cluster, NewsEvent, SourceRow } from "../types";

const BASE = (import.meta.env.VITE_OMNI_API as string | undefined) ?? "";

function headers(): HeadersInit {
  const key = localStorageGet("omni.apiKey");
  return key ? { "X-API-Key": key, "Content-Type": "application/json" } : { "Content-Type": "application/json" };
}

export function localStorageGet(k: string): string | null {
  try {
    return localStorage.getItem(k);
  } catch {
    return null;
  }
}

export function localStorageSet(k: string, v: string): void {
  try {
    localStorage.setItem(k, v);
  } catch {
    /* private mode */
  }
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
    throw new Error(detail);
  }
  return r.json() as Promise<T>;
}

export const api = {
  events: (limit = 1500) => req<{ items: NewsEvent[] }>(`/v1/events?limit=${limit}`),
  clusters: () => req<{ items: Cluster[] }>(`/v1/clusters?hours=24&limit=500&slim=true`),
  cluster: (id: string) => req<{ cluster: Cluster; events: NewsEvent[] }>(`/v1/clusters/${id}?format=full&limit=60`),
  event: (id: string) => req<NewsEvent>(`/v1/events/${id}`),
  sources: () => req<{ items: SourceRow[] }>(`/v1/sources`),
  stats: () => req<Record<string, any>>(`/v1/stats`),
  addSource: (body: Record<string, unknown>) =>
    req<{ source: SourceRow; validation: { ok: boolean; items?: number } | null }>(`/v1/sources`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  pauseSource: (id: string, pause: boolean) =>
    req(`/v1/sources/${id}/${pause ? "pause" : "resume"}`, { method: "POST" }),
  deleteSource: (id: string) => req(`/v1/sources/${id}`, { method: "DELETE" }),
};

export function wsUrl(): string {
  const base = BASE || window.location.origin;
  const u = new URL("/v1/stream", base);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  u.searchParams.set("format", "full");
  u.searchParams.set("types", "EVENT_CREATED,EVENT_UPDATED,CLUSTER_UPDATED,SOURCE_HEALTH,HEARTBEAT");
  const key = localStorageGet("omni.apiKey");
  if (key) u.searchParams.set("api_key", key);
  return u.toString();
}
