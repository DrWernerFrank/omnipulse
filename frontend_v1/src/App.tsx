import { useEffect } from "react";
import { ClustersPane } from "./components/ClustersPane";
import { CommandBar } from "./components/CommandBar";
import { DetailPane } from "./components/DetailPane";
import { FirehosePane } from "./components/FirehosePane";
import { HelpOverlay } from "./components/HelpOverlay";
import { SourcesPane } from "./components/SourcesPane";
import { TopBar } from "./components/TopBar";
import { useKeyboard } from "./hooks/useKeyboard";
import { loadInitial, startStream } from "./lib/stream";
import { useUI } from "./store";
import type { Layout, Pane } from "./types";

const GRIDS: Record<Layout, { cols: string; rows: string; areas: string; panes: Pane[] }> = {
  quad: {
    cols: "minmax(0,1.55fr) minmax(0,1fr)",
    rows: "minmax(0,1.7fr) minmax(0,1fr)",
    areas: `"firehose detail" "clusters sources"`,
    panes: ["firehose", "clusters", "detail", "sources"],
  },
  wide: {
    cols: "minmax(0,1.9fr) minmax(0,1fr)",
    rows: "minmax(0,1.4fr) minmax(0,1fr)",
    areas: `"firehose detail" "firehose clusters"`,
    panes: ["firehose", "clusters", "detail"],
  },
  reader: {
    cols: "minmax(0,1fr) minmax(0,1.25fr)",
    rows: "minmax(0,1fr)",
    areas: `"firehose detail"`,
    panes: ["firehose", "detail"],
  },
};

const COMPONENT: Record<Pane, (p: { area: string }) => JSX.Element> = {
  firehose: FirehosePane,
  clusters: ClustersPane,
  detail: DetailPane,
  sources: SourcesPane,
};

export default function App() {
  const layout = useUI((s) => s.layout);
  const zoom = useUI((s) => s.zoom);
  useKeyboard();

  useEffect(() => {
    loadInitial();
    return startStream();
  }, []);

  const grid = zoom
    ? { cols: "minmax(0,1fr)", rows: "minmax(0,1fr)", areas: `"${zoom}"`, panes: [zoom] }
    : GRIDS[layout];

  return (
    <div className="flex h-full flex-col">
      <TopBar />
      <main
        className="omni-grid grid min-h-0 flex-1 gap-[3px] bg-bg p-[3px]"
        style={{ gridTemplateColumns: grid.cols, gridTemplateRows: grid.rows, gridTemplateAreas: grid.areas }}
      >
        {grid.panes.map((p) => {
          const C = COMPONENT[p];
          return <C key={p} area={p} />;
        })}
      </main>
      <CommandBar />
      <HelpOverlay />
    </div>
  );
}
