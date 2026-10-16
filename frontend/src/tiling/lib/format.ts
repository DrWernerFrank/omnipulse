export const hhmmss = (iso: string) => iso.slice(11, 19);

export function ago(iso: string | null | undefined, now = Date.now()): string {
  if (!iso) return "—";
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  return `${Math.floor(s / 86400)}d`;
}

export const signed = (n: number, digits = 2) => (n >= 0 ? "+" : "") + n.toFixed(digits);

export function urgencyClass(u: number): string {
  if (u >= 9) return "text-bg bg-down font-bold";
  if (u >= 7) return "text-amber font-bold";
  if (u >= 5) return "text-bright";
  return "text-dim";
}

export function sentimentClass(s: number): string {
  if (s >= 0.25) return "text-up";
  if (s <= -0.25) return "text-down";
  return "text-dim";
}

export function tierClass(t: number): string {
  return t === 1 ? "text-bright" : t === 2 ? "text-text" : "text-dim";
}

export const VERIFY_GLYPH: Record<string, string> = { CONFIRMED: "●", DEVELOPING: "◐", UNVERIFIED: "○" };
export const VERIFY_CLASS: Record<string, string> = {
  CONFIRMED: "text-up",
  DEVELOPING: "text-amber",
  UNVERIFIED: "text-dim",
};

export const CATEGORY_SHORT: Record<string, string> = {
  Macro: "MACRO", Equities: "EQ", Crypto: "CRYPTO", "AI/Tech": "AI", Geopolitics: "GEO",
  Cybersecurity: "CYBER", Science: "SCI", Regulatory: "REG", Breaking: "BRK",
};

// v2 engine health values (plus transitional "starting")
export const STATUS_CLASS: Record<string, string> = {
  ok: "bg-up", waiting_key: "bg-cyan", stale: "bg-amber", degraded: "bg-amber",
  dead: "bg-down", paused: "bg-dim", disabled: "bg-line", starting: "bg-violet",
};
