/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Bloomberg-adjacent phosphor palette (tape skin)
        bg: {
          DEFAULT: "#060807",
          panel: "#0a0d0b",
          raised: "#0e120f",
          hover: "#131a15",
          border: "#1c2620",
        },
        accent: {
          DEFAULT: "#34e06a",
          dim: "#1f8f47",
          text: "#8ff0b5",
        },
        amber: { DEFAULT: "#ffb000", flash: "#ffb020", text: "#ffd27a" },
        red: { flash: "#ff4d4d", text: "#ff8f8f" },
        blue: { flash: "#4dc9ff", text: "#9fdcff" },
        dim: { DEFAULT: "#6c7884", text: "#5c6b61", text2: "#8a998f" },
        // classic tiling-terminal skin (v1) — its own cool-toned theme
        ink: "#06080a",
        panel: "#0b0f13",
        panel2: "#11171d",
        line: "#1c242c",
        bright: "#f3f6f9",
        text: "#c4cdd6",
        up: "#2fd47a",
        down: "#ff4d4f",
        cyan: { DEFAULT: "#45c4ff" },
        violet: { DEFAULT: "#b48cff" },
      },
      fontFamily: {
        mono: [
          "JetBrains Mono", "Cascadia Code", "SF Mono", "Consolas",
          "Roboto Mono", "monospace",
        ],
      },
      animation: {
        "flash-in": "flashIn 0.35s ease-out",
        "pulse-dot": "pulseDot 1.4s ease-in-out infinite",
      },
      keyframes: {
        flashIn: {
          "0%": { backgroundColor: "rgba(52,224,106,0.22)" },
          "100%": { backgroundColor: "transparent" },
        },
        pulseDot: {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.25" },
        },
      },
    },
  },
  plugins: [],
};
