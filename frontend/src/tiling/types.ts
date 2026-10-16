// Classic tiling-terminal skin (ported from the v1 terminal) — types.
// Wire shapes are the v2 engine's; cluster/source rows keep the v1 pane model.
import type { NewsEvent as WireEvent, ClusterSummary, Stats, WSFrame } from "../types";

export type { WireEvent, ClusterSummary, Stats, WSFrame };

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

export type NewsEvent = WireEvent;

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
  categories: string[];
  tickers: string[];
  locations: string[];
  max_urgency: number;
  avg_sentiment: number;
  verification_status: Verification;
  members?: ClusterMember[];
}

export interface SourceHealth {
  id: string;
  status: string; // v2 health: ok|stale|degraded|dead|paused|waiting_key|starting
  interval: number | null;
  last_success: string | null;
  last_error: string | null;
}

export interface SourceRow {
  id: string;
  name: string;
  enabled: boolean;
  interval_sec: number | null;
  detail: string | null;
  last_success: string | null;
  health: string;
}

export type Pane = "firehose" | "clusters" | "detail" | "sources";
export type Layout = "quad" | "wide" | "reader";
