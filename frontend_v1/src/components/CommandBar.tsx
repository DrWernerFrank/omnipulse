import { useEffect, useRef, useState } from "react";
import { runCommand } from "../lib/commands";
import { useUI } from "../store";

export function CommandBar() {
  const filterText = useUI((s) => s.filterText);
  const command = useUI((s) => s.command);
  const toast = useUI((s) => s.toast);
  const focus = useUI((s) => s.focus);
  const layout = useUI((s) => s.layout);
  const zoom = useUI((s) => s.zoom);
  const sound = useUI((s) => s.sound);
  const [value, setValue] = useState(filterText);
  const [history, setHistory] = useState<string[]>([]);
  const [hIdx, setHIdx] = useState(-1);
  const [showToast, setShowToast] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (command.open) {
      setValue(command.seed === "/" ? filterText : command.seed);
      setHIdx(-1);
      requestAnimationFrame(() => input.current?.focus());
    }
  }, [command]);

  useEffect(() => {
    if (document.activeElement !== input.current) setValue(filterText);
  }, [filterText]);

  useEffect(() => {
    if (!toast) return;
    setShowToast(true);
    const t = window.setTimeout(() => setShowToast(false), 4000);
    return () => window.clearTimeout(t);
  }, [toast]);

  const close = () => {
    useUI.setState({ command: { open: false, seed: "" } });
    input.current?.blur();
  };

  const submit = () => {
    const v = value.trim();
    if (v) setHistory((h) => [v, ...h.filter((x) => x !== v)].slice(0, 50));
    if (v.startsWith(":")) {
      runCommand(v);
      setValue(useUI.getState().filterText);
    } else {
      useUI.getState().setFilter(v);
    }
    close();
  };

  const isCmd = value.startsWith(":");
  return (
    <div className="flex h-[24px] shrink-0 items-center gap-2 border-t border-line bg-panel px-2 text-[12px]">
      <span className={isCmd ? "font-bold text-violet" : "font-bold text-amber"}>{isCmd ? "CMD" : "FILTER"}</span>
      <input
        ref={input}
        value={value}
        spellCheck={false}
        placeholder='/ to filter: u>=7 cat:crypto $NVDA src:reuters loc:US tier<=2 "rate cut" -opinion   ·   : for commands   ·   ? for help'
        onChange={(e) => setValue(e.target.value)}
        onFocus={() => useUI.setState({ command: { open: true, seed: value } })}
        onBlur={() => useUI.setState({ command: { open: false, seed: "" } })}
        onKeyDown={(e) => {
          if (e.key === "Enter") submit();
          else if (e.key === "Escape") {
            setValue(filterText);
            close();
          } else if (e.key === "ArrowUp" && history.length) {
            const i = Math.min(history.length - 1, hIdx + 1);
            setHIdx(i);
            setValue(history[i]);
            e.preventDefault();
          } else if (e.key === "ArrowDown") {
            const i = hIdx - 1;
            setHIdx(Math.max(-1, i));
            setValue(i >= 0 ? history[i] : "");
            e.preventDefault();
          }
          e.stopPropagation();
        }}
        className="min-w-0 flex-1 bg-transparent text-bright placeholder:text-dim/60 focus:outline-none"
        aria-label="filter or command"
      />
      {showToast && toast && (
        <span className={`max-w-[45%] truncate ${toast.kind === "err" ? "text-down" : toast.kind === "ok" ? "text-up" : "text-amber"}`}>
          {toast.msg}
        </span>
      )}
      <span className="hidden shrink-0 text-[11px] text-dim md:inline">
        {focus.toUpperCase()} · {zoom ? "ZOOM" : layout.toUpperCase()} {sound ? "· ♪" : ""} · ? help
      </span>
    </div>
  );
}
