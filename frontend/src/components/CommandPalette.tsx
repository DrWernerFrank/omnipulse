import { useEffect, useRef, useState } from "react";
import { useTerminal } from "../store";
import { CATEGORIES } from "../types";

interface Cmd {
  id: string;
  label: string;
  hint?: string;
  run: () => void;
}

export function CommandPalette() {
  const open = useTerminal((s) => s.paletteOpen);
  const setPalette = useTerminal((s) => s.setPalette);
  const st = useTerminal.getState;
  const [q, setQ] = useState("");
  const [cursor, setCursor] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setQ("");
      setCursor(0);
      setTimeout(() => inputRef.current?.focus(), 10);
    }
  }, [open]);

  const commands: Cmd[] = [
    {
      id: "freeze",
      label: st().filters.paused ? "Resume tape" : "Freeze tape",
      hint: "Space",
      run: () => st().togglePaused(),
    },
    { id: "clear", label: "Clear tape", hint: "", run: () => st().clearTape() },
    { id: "filters", label: "Pane: filters", hint: "f", run: () => useTerminal.setState({ leftPane: "filters" }) },
    { id: "sources", label: "Pane: source registry", hint: "s", run: () => useTerminal.setState({ leftPane: "sources" }) },
    { id: "clusters", label: "Pane: story clusters", run: () => useTerminal.setState({ leftPane: "clusters" }) },
    {
      id: "urgent",
      label: "Filter: urgency ≥ 8 (flash only)",
      run: () => st().setFilters({ minUrgency: 8 }),
    },
    {
      id: "all-urgency",
      label: "Filter: all urgencies",
      run: () => st().setFilters({ minUrgency: 0 }),
    },
    {
      id: "clear-filters",
      label: "Reset all filters",
      run: () => st().setFilters({ q: "", categories: [], tickers: [], minUrgency: 0 }),
    },
    {
      id: "breaking",
      label: "Category: Breaking only",
      run: () => st().setFilters({ categories: ["Breaking"] }),
    },
    {
      id: "toggle-right",
      label: "Toggle detail panel",
      hint: "d",
      run: () => useTerminal.setState((s) => ({ rightPane: !s.rightPane })),
    },
    {
      id: "skin-tiling",
      label: "Skin: switch to the tiling terminal (classic v1)",
      hint: "TL",
      run: () => st().setAppearance("tiling"),
    },
  ];

  const filtered = commands.filter((c) => c.label.toLowerCase().includes(q.toLowerCase()));

  if (!open) return null;

  const commit = (idx: number) => {
    filtered[idx]?.run();
    setPalette(false);
  };

  return (
    <div
      className="absolute inset-0 z-50 flex items-start justify-center bg-black/60 pt-[12vh]"
      onClick={() => setPalette(false)}
    >
      <div
        className="w-[560px] rounded border border-bg-border bg-bg-panel shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setCursor(0);
          }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setCursor((c) => Math.min(filtered.length - 1, c + 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setCursor((c) => Math.max(0, c - 1));
            } else if (e.key === "Enter") {
              commit(cursor);
            } else if (e.key === "Escape") {
              setPalette(false);
            }
          }}
          placeholder="type a command… (freeze, urgent, clear, filters, breaking)"
          className="w-full border-b border-bg-border bg-transparent px-3 py-2.5 text-[14px] text-dim-text2 outline-none placeholder:text-dim-text/60"
        />
        <div className="max-h-[320px] overflow-y-auto p-1">
          {filtered.map((c, i) => (
            <button
              key={c.id}
              onClick={() => commit(i)}
              onMouseEnter={() => setCursor(i)}
              className={`flex w-full items-center gap-3 rounded-sm px-2.5 py-1.5 text-left text-[12px] ${
                i === cursor ? "bg-accent/10 text-accent-text" : "text-dim-text2 hover:bg-bg-hover"
              }`}
            >
              <span className="flex-1">{c.label}</span>
              {c.hint && <kbd>{c.hint}</kbd>}
            </button>
          ))}
          {!filtered.length && (
            <div className="p-3 text-center text-[12px] text-dim-text">no matching command</div>
          )}
        </div>
        <div className="flex items-center gap-2 border-t border-bg-border px-3 py-1.5 text-[10px] text-dim-text">
          <kbd>↑↓</kbd> navigate <kbd>↵</kbd> run <kbd>esc</kbd> close · also try
          <span className="text-dim-text2">/</span>search
          <span className="text-dim-text2">·</span>1-9 categories
        </div>
      </div>
    </div>
  );
}

export const CATEGORY_KEYS = CATEGORIES; // re-export for hotkey wiring
