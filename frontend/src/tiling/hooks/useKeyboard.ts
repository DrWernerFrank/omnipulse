import { useEffect } from "react";
import { controllers } from "../lib/controllers";
import { localStorageSet } from "../lib/storage";
import { DB, useUI } from "../store";
import type { Pane } from "../types";

const PANES: Pane[] = ["firehose", "clusters", "detail", "sources"];
const URGENCY_STEPS = [0, 5, 7, 9];

function visiblePanes(): Pane[] {
  const { layout, zoom } = useUI.getState();
  if (zoom) return [zoom];
  if (layout === "reader") return ["firehose", "detail"];
  if (layout === "wide") return ["firehose", "detail", "clusters"];
  return PANES;
}

function cycleUrgency(): void {
  const ui = useUI.getState();
  const text = ui.filterText.replace(/\bu(>=|:)\d+\s*/g, "").trim();
  const cur = ui.filter.minUrgency;
  const idx = URGENCY_STEPS.findIndex((s) => s > cur);
  const next = idx === -1 ? 0 : URGENCY_STEPS[idx];
  ui.setFilter(next ? `u>=${next} ${text}`.trim() : text);
  ui.notify(next ? `urgency ≥ ${next}` : "urgency floor off");
}

export function useKeyboard(): void {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)) return;
      if (e.altKey || e.metaKey) return;
      const ui = useUI.getState();
      const ctl = controllers[ui.focus];
      const k = e.key;

      if (ui.help) {
        if (k === "Escape" || k === "?") useUI.setState({ help: false });
        e.preventDefault();
        return;
      }
      if (e.ctrlKey) {
        if (k === "d") ctl?.move(15);
        else if (k === "u") ctl?.move(-15);
        else return;
        e.preventDefault();
        return;
      }
      if (ctl?.key?.(k)) {
        e.preventDefault();
        return;
      }
      const selected = ui.selectedId ? DB.events.get(ui.selectedId) : undefined;
      switch (k) {
        case "/":
          ui.openCommand("/");
          break;
        case ":":
          ui.openCommand(":");
          break;
        case "?":
          useUI.setState({ help: true });
          break;
        case "j":
        case "ArrowDown":
          ctl?.move(1);
          break;
        case "k":
        case "ArrowUp":
          ctl?.move(-1);
          break;
        case "PageDown":
          ctl?.move(20);
          break;
        case "PageUp":
          ctl?.move(-20);
          break;
        case "g":
          ctl?.edge("top");
          break;
        case "G":
          ctl?.edge("bottom");
          break;
        case "Home":
          ctl?.edge("top");
          break;
        case "End":
          ctl?.edge("bottom");
          break;
        case "Enter":
          ctl?.open();
          break;
        case "1":
        case "2":
        case "3":
        case "4": {
          const p = PANES[Number(k) - 1];
          ui.set({ focus: p, zoom: ui.zoom ? p : null });
          break;
        }
        case "Tab": {
          const vis = visiblePanes();
          const i = vis.indexOf(ui.focus);
          ui.setFocus(vis[(i + (e.shiftKey ? vis.length - 1 : 1)) % vis.length]);
          break;
        }
        case "z":
          ui.toggleZoom();
          break;
        case "L":
          ui.cycleLayout();
          break;
        case " ":
          ui.togglePause();
          break;
        case "f":
          useUI.setState({ follow: true });
          if (ui.paused) ui.togglePause();
          controllers.firehose?.edge("top");
          break;
        case "u":
          cycleUrgency();
          break;
        case "m": {
          const sound = !ui.sound;
          localStorageSet("omni.sound", sound ? "1" : "0");
          ui.set({ sound });
          ui.notify(`sound ${sound ? "on (urgency ≥ 9)" : "off"}`);
          break;
        }
        case "c":
          if (selected) {
            const f = `cluster:${selected.cluster_id}`;
            ui.setFilter(ui.filterText === f ? "" : f);
          }
          break;
        case "o":
          if (selected?.source.url) window.open(selected.source.url, "_blank", "noopener,noreferrer");
          break;
        case "y":
          if (selected) {
            navigator.clipboard
              .writeText(JSON.stringify(selected, null, 2))
              .then(() => ui.notify(`copied ${selected.event_id}`, "ok"))
              .catch(() => ui.notify("clipboard unavailable", "err"));
          }
          break;
        case "Escape":
          if (ui.zoom) ui.toggleZoom();
          else if (ui.filterText) ui.setFilter("");
          else ui.select(null);
          break;
        default:
          return;
      }
      e.preventDefault();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
}
