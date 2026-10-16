export type Category =
  | "Macro"
  | "Equities"
  | "Crypto"
  | "AI/Tech"
  | "Geopolitics"
  | "Cybersecurity"
  | "Science"
  | "Regulatory"
  | "Breaking";

export type Verification = "CONFIRMED" | "DEVELOPING" | "UNVERIFIED";

export interface NewsEvent {
  event_id: string;
  cluster_id: string;
  published_at: string;
  ingested_at: string;
  source: { id: string; name: string; category: string; credibility_tier: 1 | 2 | 3; url: string };
  content: { raw_title: string; flash_headline: string; executive_summary: string[]; clean_markdown: string };
  enrichment: {
    categories: Category[];
    tickers: string[];
    entities: { name: string; type: string }[];
    locations: string[];
    sentiment: number;
    urgency: number;
    novelty: number;
    verification_status: Verification;
  };
}

export interface ClusterMember {
  event_id: string;
  source_id: string;
  source_name: string;
  tier: number;
  published_at: string;
  headline: string;
}

export interface Cluster {
  cluster_id: string;
  first_seen: string;
  last_updated: string;
  headline: string;
  primary_event_id: string;
  primary_source: { id: string; name: string; credibility_tier: number; published_at: string };
  size: number;
  event_count: number;
  syndication_count: number;
  sources: string[];
  source_count: number;
  categories: Category[];
  tickers: string[];
  locations: string[];
  max_urgency: number;
  avg_sentiment: number;
  verification_status: Verification;
  members?: ClusterMember[];
  syndications?: { source_id: string; source_name: string; url: string; published_at: string }[];
}

export interface SourceHealth {
  id: string;
  status: "healthy" | "degraded" | "dead" | "paused" | "disabled" | "starting" | "push";
  interval: number;
  last_poll: string | null;
  last_success: string | null;
  last_item_at: string | null;
  last_error: string | null;
  consecutive_failures: number;
  items_total: number;
  polls_total: number;
  avg_latency_ms: number;
}

export interface SourceRow {
  id: string;
  name: string;
  type: string;
  url: string;
  category: string;
  credibility_tier: number;
  group: string;
  enabled: boolean;
  poll_interval: number;
  health: SourceHealth | null;
}

export type Envelope =
  | { type: "EVENT_CREATED" | "EVENT_UPDATED"; ts: string; data: NewsEvent; backfill?: boolean }
  | { type: "CLUSTER_UPDATED"; ts: string; data: Cluster }
  | { type: "SOURCE_HEALTH"; ts: string; data: { sources: SourceHealth[] } }
  | { type: "HEARTBEAT"; ts: string; data: Record<string, unknown> }
  | { type: "WELCOME" | "SUBSCRIBED" | "PONG"; ts?: string; data?: unknown }
  | { type: "BATCH"; data: Envelope[] };

export type Pane = "firehose" | "clusters" | "detail" | "sources";
export type Layout = "quad" | "wide" | "reader";
