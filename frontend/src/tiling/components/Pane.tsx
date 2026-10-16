import type { ReactNode } from "react";
import { useUI } from "../store";
import type { Pane as PaneId } from "../types";

const NUM: Record<PaneId, number> = { firehose: 1, clusters: 2, detail: 3, sources: 4 };

export function Pane({
  id,
  title,
  right,
  children,
  area,
}: {
  id: PaneId;
  title: string;
  right?: ReactNode;
  children: ReactNode;
  area: string;
}) {
  const focused = useUI((s) => s.focus === id);
  const setFocus = useUI((s) => s.setFocus);
  return (
    <section
      style={{ gridArea: area }}
      onMouseDown={() => setFocus(id)}
      className={`flex min-h-0 min-w-0 flex-col border bg-panel ${focused ? "border-amber/70" : "border-line"}`}
      aria-label={title}
    >
      <header className="flex h-[22px] shrink-0 items-center justify-between gap-2 border-b border-line bg-panel2 px-2 text-[11px] uppercase tracking-wider">
        <div className="flex items-center gap-2 truncate">
          <span className={focused ? "bg-amber px-1 font-bold text-bg" : "px-1 text-dim"}>{NUM[id]}</span>
          <span className={focused ? "font-bold text-amber" : "text-text"}>{title}</span>
        </div>
        <div className="flex items-center gap-3 truncate normal-case tracking-normal text-dim">{right}</div>
      </header>
      <div className="relative min-h-0 flex-1">{children}</div>
    </section>
  );
}
