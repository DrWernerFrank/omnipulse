/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Bloomberg-adjacent phosphor palette
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
        amber: { flash: "#ffb020", text: "#ffd27a" },
        red: { flash: "#ff4d4d", text: "#ff8f8f" },
        blue: { flash: "#4dc9ff", text: "#9fdcff" },
        dim: { text: "#5c6b61", text2: "#8a998f" },
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
