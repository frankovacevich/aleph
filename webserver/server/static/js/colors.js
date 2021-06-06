
// =============================================================================
// COLORS
// =============================================================================

COLORS = {};
COLORS["primary"] = "#5bbadc";
COLORS["background"] = "#FFFFFF";
COLORS["text"] = "#000000";
COLORS["contrast"] = "#FFFFFF";

COLORS["text_inactive"] = "#808080";
COLORS["ok"] = "#73d216";
COLORS["error"] = "#dc0000";
COLORS["info"] = "#0088e8";
COLORS["warning"] = "#fbb901";
COLORS["inactive"] = "#aea79f";

// =============================================================================
// COLOR PALETTES
// =============================================================================
function INSERT_COLOR_IN_COLOR_PALETTES(color){
  for(const p in COLOR_PALETTES){
    COLOR_PALETTES[p].splice(0,0,color);
  }
}

COLOR_PALETTES = {};

DEFAULT_PALETTE = "main";

COLOR_PALETTES["main"] = [
  "#d500f9",
  "#3d5afe",
  "#00b0ff",
  "#1de9b6",
  "#00e676",
  "#76ff03",
  "#c6ff00",
  "#ffc400",
  "#ff3d00",
  "#f50057",
];

COLOR_PALETTES["alternative"] = [
  "#d90d39",
  "#f8432d",
  "#ff8e25",
  "#ef55f1",
  "#c543fa",
  "#6324f5",
  "#2e21ea",
  "#3d719a",
  "#31ac28",
  "#96d310",
];

COLOR_PALETTES["creamy"] = [
  "#ea989d",
  "#b9d7f7",
  "#c6bcdd",
  "#9bfdce",
  "#bee8bb",
  "#f9f2c0",
  "#5f8c96",
  "#9b91ac",
  "#ae896f",
  "#cc6d55",
];

COLOR_PALETTES["contrast"] = [
  "#8ae234",
  "#fcaf3e",
  "#729fcf",
  "#ad7fa8",
  "#ef2929",
  "#4e9a06",
  "#ce5c00",
  "#204a87",
  "#5c3566",
  "#a40000",
];

COLOR_PALETTES["soft"] = [
  "#EF9A9A",
  "#CE93D8",
  "#B39DDB",
  "#90CAF9",
  "#80CBC4",
  "#A5D6A7",
  "#FFE082",
  "#FFCC80",
  "#FFAB91",
  "#B0BEC5",
];

COLOR_PALETTES["accent"] = [
  "#D50000",
  "#C51162",
  "#6200EA",
  "#2962FF",
  "#00BFA5",
  "#00C853",
  "#FFAB00",
  "#FF6D00",
  "#DD2C00",
  "#263238",
];
