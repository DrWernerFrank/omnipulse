import { clearEvents, DB, useUI } from "../store";
import type { Layout } from "../types";
import { api, localStorageSet } from "./api";
import { refreshSources } from "./stream";

export const COMMANDS: [string, string][] = [
  [":add <url> [name…]", "add an RSS/Atom source (validated with a live test poll)"],
  [":rm <source_id>", "remove a source"],
  [":pause [source_id]", "pause the stream view, or a source"],
  [":resume [source_id]", "resume the stream view, or a source"],
  [":layout quad|wide|reader", "switch tiling layout"],
  [":clear", "clear the local event buffer"],
  [":export", "download visible events as JSONL"],
  [":sound on|off", "beep on urgency ≥ 9"],
  [":key <api_key>", "store an API key for write operations"],
];

function download(name: string, text: string): void {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type: "application/x-ndjson" }));
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

export async function runCommand(line: string): Promise<void> {
  const ui = useUI.getState();
  const [cmd, ...args] = line.replace(/^:/, "").trim().split(/\s+/);
  try {
    switch (cmd) {
      case "add": {
        const url = args[0];
        if (!url) throw new Error("usage: :add <url> [name]");
        let name = args.slice(1).join(" ");
        if (!name) name = new URL(url).hostname.replace(/^www\./, "");
        ui.notify(`validating ${url}…`);
        const r = await api.addSource({ name, url, type: "rss", poll_interval: 60, min_interval: 30 });
        ui.notify(`added ${r.source.id} (${r.validation?.items ?? 0} items in test poll)`, "ok");
        refreshSources();
        break;
      }
      case "rm":
        await api.deleteSource(args[0]);
        ui.notify(`removed ${args[0]}`, "ok");
        refreshSources();
        break;
      case "pause":
      case "resume":
        if (args[0]) {
          await api.pauseSource(args[0], cmd === "pause");
          ui.notify(`${cmd}d ${args[0]}`, "ok");
          refreshSources();
        } else if (ui.paused !== (cmd === "pause")) {
          ui.togglePause();
        }
        break;
      case "layout":
        if (!["quad", "wide", "reader"].includes(args[0])) throw new Error("layouts: quad, wide, reader");
        ui.setLayout(args[0] as Layout);
        break;
      case "clear":
        clearEvents();
        ui.bump(true, false);
        break;
      case "export": {
        const lines = (ui.frozen ?? DB.order).map((id) => JSON.stringify(DB.events.get(id))).join("\n");
        download(`omnipulse-${new Date().toISOString().slice(0, 19)}.jsonl`, lines);
        break;
      }
      case "sound":
        localStorageSet("omni.sound", args[0] === "off" ? "0" : "1");
        ui.set({ sound: args[0] !== "off" });
        ui.notify(`sound ${args[0] === "off" ? "off" : "on"}`, "ok");
        break;
      case "key":
        localStorageSet("omni.apiKey", args[0] ?? "");
        ui.notify("API key stored for this browser", "ok");
        break;
      case "help":
        ui.set({ help: true });
        break;
      default:
        throw new Error(`unknown command :${cmd} — press ? for help`);
    }
  } catch (e) {
    ui.notify((e as Error).message, "err");
  }
}
