import { useEffect } from "react";
import { connectStream } from "./api";
import { CommandPalette } from "./components/CommandPalette";
import { DetailDrawer } from "./components/DetailDrawer";
import { EventTape } from "./components/EventTape";
import { LeftPanel } from "./components/LeftPanel";
import { StatusBar, usePausedDrain } from "./components/StatusBar";
import { TopBar } from "./components/TopBar";
import { useTerminal } from "./store";
import { CATEGORIES } from "./types";

export default function App() {
  const rightPane = useTerminal((s) => s.rightPane);
  const setPalette = useTerminal((s) => s.setPalette);
  usePausedDrain();

  useEffect(() => connectStream(), []);

  // global keyboard map (tape-specific j/k handled inside EventTape)
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      const typing = tag === "INPUT" || tag === "TEXTAREA";
      const st = useTerminal.getState();

      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPalette(!st.paletteOpen);
        return;
      }
      if (typing) return;

      switch (e.key) {
        case " ":
          e.preventDefault();
          st.togglePaused();
          break;
        case "f":
          useTerminal.setState({ leftPane: "filters" });
          break;
        case "s":
          useTerminal.setState({
            leftPane: st.leftPane === "sources" ? "clusters" : "sources",
          });
          break;
        case "d":
          useTerminal.setState((s) => ({ rightPane: !s.rightPane }));
          break;
        case "u": {
          const ladder = [0, 5, 7, 8, 9, 10];
          const cur = ladder.indexOf(st.filters.minUrgency);
          st.setFilters({ minUrgency: ladder[(cur + 1) % ladder.length] });
          break;
        }
        case "/":
          e.preventDefault();
          useTerminal.setState({ leftPane: "filters" });
          setTimeout(() => document.getElementById("tape-search")?.focus(), 30);
          break;
        case "Escape":
          if (st.paletteOpen) st.setPalette(false);
          else if (st.selectedCluster) st.closeCluster();
          else st.select(null);
          break;
        default:
          if (/^[1-9]$/.test(e.key)) {
            const cat = CATEGORIES[Number(e.key) - 1];
            if (cat) st.toggleCategory(cat);
          }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [setPalette]);

  return (
    <div className="relative flex h-full flex-col bg-bg text-dim-text2">
      <TopBar />
      <div className="flex min-h-0 flex-1">
        <LeftPanel />
        <EventTape />
        {rightPane && <DetailDrawer />}
      </div>
      <StatusBar />
      <CommandPalette />
    </div>
  );
}
