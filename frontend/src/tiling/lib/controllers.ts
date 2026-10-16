import type { Pane } from "../types";

/** Each list pane registers how keyboard navigation drives it; the global key handler dispatches. */
export interface ListController {
  move: (delta: number) => void;
  edge: (end: "top" | "bottom") => void;
  open: () => void;
  key?: (k: string) => boolean; // pane-local keys; return true if handled
}

export const controllers: Partial<Record<Pane, ListController>> = {};
