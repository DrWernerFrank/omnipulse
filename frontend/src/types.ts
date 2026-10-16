// Canonical NewsEvent + supporting wire types (mirror of backend schemas.py)

export interface Entity { name: string; type: string }
export interface SourceRef {
  id: string; name: string; category: string;
  credibility_tier: number; url: string | null;
}
export interface Content {
  raw_title: string; flash_headline: string;
  executive_summary: string[]; clean_markdown: string;
}
export interface Enrichment {
  categories: string[]; tickers: string[]; entities: Entity[];
  locations: string[]; sentiment: number; urgency: number;
  novelty: number; verification_status: "CONFIRMED" | "DEVELOPING" | "UNVERIFIED";
}
export interface NewsEvent {
  event_id: string; cluster_id: string;
  published_at: string; ingested_at: string;
  source: SourceRef; content: Content; enrichment: Enrichment;
}

export interface ClusterSummary {
  cluster_id: string; title: string; event_count: number;
  sources: string[]; categories: string[]; tickers: string[];
  breaking_source: string | null;
  first_seen: string | null; latest_at: string | null; velocity_15m: number;
}

export interface SourceState {
  source_id: string; name: string; health: string;
  enabled: boolean; detail: string | null;
  last_success: string | null; interval_sec: number | null;
}

export interface Stats {
  ts: string; events_total: number; events_1h: number; events_15m: number;
  events_per_min: number; lag_p50_ms: number; lag_p95_ms: number;
  clusters_24h: number; sources_online: number; sources_total: number;
  sources_paused: number; dlq_size: number; bus_mode: string; store_mode: string;
  top_tickers: [string, number][];
  pipeline?: { processed: number; duplicates: number; cluster_joins: number; new_clusters: number };
}

export const CATEGORIES = [
  "Macro", "Equities", "Crypto", "AI/Tech", "Geopolitics",
  "Cybersecurity", "Science", "Regulatory", "Breaking",
] as const;

export type FrameType = "event" | "cluster" | "stats" | "source_state" | "ping" | "hello";
export interface WSFrame { t: FrameType; d: unknown }
