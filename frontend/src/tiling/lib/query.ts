// Client-side twin of the backend DSL — same grammar, applied to the local event window.
import type { Category, NewsEvent } from "../types";

export interface Filter {
  categories: Category[];
  tickers: string[];
  sources: string[];
  sourceCategories: string[];
  locations: string[];
  entities: string[];
  verification: string[];
  clusters: string[];
  minUrgency: number;
  maxUrgency: number;
  minTier: number;
  maxTier: number;
  minSentiment: number | null;
  maxSentiment: number | null;
  minNovelty: number;
  text: string[];
  exclude: string[];
}

export const emptyFilter = (): Filter => ({
  categories: [],
  tickers: [],
  sources: [],
  sourceCategories: [],
  locations: [],
  entities: [],
  verification: [],
  clusters: [],
  minUrgency: 1,
  maxUrgency: 10,
  minTier: 1,
  maxTier: 3,
  minSentiment: null,
  maxSentiment: null,
  minNovelty: 0,
  text: [],
  exclude: [],
});

export const TAXONOMY: Category[] = [
  "Macro", "Equities", "Crypto", "AI/Tech", "Geopolitics", "Cybersecurity", "Science", "Regulatory", "Breaking",
];

const ALIASES: Record<string, Category> = {
  ai: "AI/Tech", tech: "AI/Tech", aitech: "AI/Tech", geo: "Geopolitics", world: "Geopolitics",
  cyber: "Cybersecurity", security: "Cybersecurity", infosec: "Cybersecurity", reg: "Regulatory",
  regulation: "Regulatory", eq: "Equities", stocks: "Equities", equity: "Equities", macro: "Macro",
  econ: "Macro", economy: "Macro", crypto: "Crypto", web3: "Crypto", sci: "Science", science: "Science",
  breaking: "Breaking", brk: "Breaking", flash: "Breaking",
};

export function resolveCategory(token: string): Category | null {
  const t = token.toLowerCase().replace(/[/-]/g, "").trim();
  if (!t) return null;
  if (ALIASES[t]) return ALIASES[t];
  return TAXONOMY.find((c) => c.toLowerCase().replace("/", "").startsWith(t)) ?? null;
}

function tokenize(s: string): string[] {
  const out: string[] = [];
  const re = /"([^"]*)"|(\S+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(s))) out.push(m[1] ?? m[2]);
  return out;
}

export function parseFilter(dsl: string): Filter {
  const f = emptyFilter();
  for (const tok of tokenize(dsl)) {
    if (tok.startsWith("$") && tok.length > 1) {
      f.tickers.push(...tok.slice(1).split(",").filter(Boolean).map((t) => t.toUpperCase()));
      continue;
    }
    const num = /^(u|urg|urgency|tier|sent|sentiment|nov|novelty)(>=|<=|>|<|=|:)(-?[\d.]+)$/i.exec(tok);
    if (num) {
      const key = num[1].toLowerCase();
      const op = num[2];
      const v = parseFloat(num[3]);
      if (Number.isNaN(v)) continue;
      if (key.startsWith("u")) {
        if (op === ">=" || op === ":" || op === "=") f.minUrgency = v;
        if (op === "=") f.maxUrgency = v;
        if (op === ">") f.minUrgency = v + 1;
        if (op === "<=") f.maxUrgency = v;
        if (op === "<") f.maxUrgency = v - 1;
      } else if (key === "tier") {
        if (op === ":" || op === "=") f.minTier = f.maxTier = v;
        if (op === "<=") f.maxTier = v;
        if (op === "<") f.maxTier = v - 1;
        if (op === ">=") f.minTier = v;
        if (op === ">") f.minTier = v + 1;
      } else if (key.startsWith("sen")) {
        if (op.startsWith(">") || op === ":" || op === "=") f.minSentiment = v;
        else f.maxSentiment = v;
      } else {
        f.minNovelty = v;
      }
      continue;
    }
    const colon = tok.indexOf(":");
    if (colon > 0 && !tok.startsWith("-") && !tok.startsWith("http")) {
      const key = tok.slice(0, colon).toLowerCase();
      const vals = tok.slice(colon + 1).split(",").filter(Boolean);
      switch (key) {
        case "cat": case "category": case "c":
          vals.forEach((v) => { const c = resolveCategory(v); if (c) f.categories.push(c); });
          break;
        case "tk": case "ticker": case "t":
          f.tickers.push(...vals.map((v) => v.toUpperCase().replace(/^\$/, "")));
          break;
        case "src": case "source": case "s":
          f.sources.push(...vals.map((v) => v.toLowerCase()));
          break;
        case "scat": case "sourcecat": case "type":
          f.sourceCategories.push(...vals.map((v) => v.toLowerCase()));
          break;
        case "loc": case "location": case "country":
          f.locations.push(...vals.map((v) => v.toUpperCase()));
          break;
        case "ent": case "entity": case "e":
          f.entities.push(...vals.map((v) => v.toLowerCase()));
          break;
        case "vs": case "verified": case "verification":
          f.verification.push(...vals.map((v) => v.toUpperCase()));
          break;
        case "cluster": case "cls":
          f.clusters.push(...vals);
          break;
        default:
          f.text.push(tok.toLowerCase());
      }
      continue;
    }
    if (tok.startsWith("-") && tok.length > 1) {
      f.exclude.push(tok.slice(1).toLowerCase());
      continue;
    }
    f.text.push(tok.toLowerCase());
  }
  return f;
}

export function isEmptyFilter(f: Filter): boolean {
  return JSON.stringify(f) === JSON.stringify(emptyFilter());
}

export function matches(f: Filter, ev: NewsEvent): boolean {
  const e = ev.enrichment;
  const s = ev.source;
  if (e.urgency < f.minUrgency || e.urgency > f.maxUrgency) return false;
  if (s.credibility_tier < f.minTier || s.credibility_tier > f.maxTier) return false;
  if (f.minSentiment !== null && e.sentiment < f.minSentiment) return false;
  if (f.maxSentiment !== null && e.sentiment > f.maxSentiment) return false;
  if (e.novelty < f.minNovelty) return false;
  if (f.categories.length && !f.categories.some((c) => e.categories.includes(c))) return false;
  if (f.tickers.length && !f.tickers.some((t) => e.tickers.includes(t))) return false;
  if (f.locations.length && !f.locations.some((l) => e.locations.includes(l))) return false;
  if (f.verification.length && !f.verification.includes(e.verification_status)) return false;
  if (f.clusters.length && !f.clusters.includes(ev.cluster_id)) return false;
  if (f.sources.length) {
    const id = s.id.toLowerCase();
    const name = s.name.toLowerCase();
    if (!f.sources.some((x) => id.includes(x) || name.includes(x))) return false;
  }
  if (f.sourceCategories.length && !f.sourceCategories.includes(s.category.toLowerCase())) return false;
  if (f.entities.length) {
    const names = e.entities.map((x) => x.name.toLowerCase()).join(" | ");
    if (!f.entities.some((x) => names.includes(x))) return false;
  }
  if (f.text.length || f.exclude.length) {
    const hay = (ev.content.flash_headline + " " + ev.content.raw_title).toLowerCase();
    if (f.text.some((t) => !hay.includes(t))) return false;
    if (f.exclude.some((x) => hay.includes(x))) return false;
  }
  return true;
}
