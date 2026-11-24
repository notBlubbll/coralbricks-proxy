// coralbricks-usage — OpenCode Desktop GUI part.
//
// A "Coralbricks usage" side panel: a stacked bar chart of the proxy's traffic
// (one colour per registered key), summary cards, per-key totals and a compact
// per-model table. Windows: 24u / 24h / 7d / 30d / all. Data comes from the
// coralbricks-proxy's GET /usage, GET /usage/series and GET /keys endpoints.
//
// Loaded by the desktop from this prebuilt CommonJS bundle:
//   Function("require","module","exports", code)(sharedRequire, module, module.exports)
// `@opencode/plugin/gui` and `solid-js` are provided by the app at runtime.
//
// Plain DOM, no JSX and no build step: the panel builds its nodes once and
// updates them in place.
const { Plugin, Panel, MenuItem, Command, ComposerPill } = require("@opencode/plugin/gui");
const { createSignal } = require("solid-js");

// Live retry state from the proxy's /status, read by the composer pill.
const [proxyStatus, setProxyStatus] = createSignal(null);
// One shared 1s ticker; reading `tick()` in a pill makes the host re-run it.
const [tick, setTick] = createSignal(0);

const WINDOWS = ["24u", "24h", "7d", "30d", "all"];
const METRICS = [
  ["requests", "Requests"],
  ["tokens", "Tokens"],
  ["cost", "Cost"],
];
const PALETTE = ["#4f9cf9", "#f0a93b", "#57c785", "#c57bf0", "#f06d6d", "#5ad0d0", "#b0b64a", "#9b8cff"];
const BORDER = "1px solid rgba(127,127,127,.18)";
// Shown in place of a saved key. The real value is only fetched on Show.
const KEY_MASK = "••••••••••••••••";
const BTN = [
  "padding:3px 9px",
  "border-radius:7px",
  "border:1px solid rgba(127,127,127,.35)",
  "background:rgba(127,127,127,.08)",
  "color:inherit",
  "cursor:pointer",
  "font:inherit",
  "font-weight:600",
  "white-space:nowrap",
].join(";");
const FIELD = [
  "width:100%",
  "box-sizing:border-box",
  "height:26px",
  "padding:0 8px",
  "border-radius:7px",
  "border:1px solid rgba(127,127,127,.35)",
  "background:transparent",
  "color:inherit",
  "font:inherit",
  "font-variant-numeric:tabular-nums",
].join(";");

function buttonEl(text, title, extra) {
  const b = document.createElement("button");
  b.type = "button";
  b.textContent = text;
  b.title = title || "";
  b.style.cssText = BTN + ";" + (extra || "");
  return b;
}

function idleText(ms) {
  const s = Math.max(0, Math.round((Number(ms) || 0) / 1000));
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m";
  return Math.floor(s / 3600) + "h";
}

function shortId(id) {
  const s = String(id || "");
  return s.length > 14 ? `${s.slice(0, 8)}…${s.slice(-4)}` : s;
}

function formatSeconds(total) {
  if (total < 60) return `${total}s`;
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return seconds ? `${minutes}m ${seconds}s` : `${minutes}m`;
}

// Elapsed time of the routed session, or null when it is not working.
function elapsedSeconds(session) {
  const data = session && session.server && session.server.data;
  if (!data) return null;
  const id = session.id;
  let running = false;
  try {
    running = typeof data.session.status === "function" && data.session.status(id) === "running";
  } catch {}
  const info = data.session.get(id);
  const busy = info && info.time && info.time.busy;
  const idle = info && info.time && info.time.idle;
  const byTime = typeof busy === "number" && (typeof idle !== "number" || busy > idle);
  if (!running && !byTime) return null;
  if (typeof busy !== "number") return 0;
  const elapsed = Math.floor((Date.now() - busy) / 1000);
  return elapsed >= 0 && elapsed < 24 * 60 * 60 ? elapsed : 0;
}

function fmt(n) {
  const v = Number(n) || 0;
  if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(1) + "k";
  return String(Math.round(v));
}

function money(n) {
  const v = Number(n) || 0;
  if (v === 0) return "$0.00";
  if (v < 0.01) return "$" + v.toFixed(4);
  return "$" + v.toFixed(2);
}

// The proxy records the model id the client asked for; this keeps the table
// readable without hiding unknown ids.
const MODEL_NAMES = {
  "coralbricks/deepseek-v4.1-flash-value": "DeepSeek V4.1 Flash Value",
  "deepseek-v4.1-flash-value": "DeepSeek V4.1 Flash Value",
  "deepseek-v4.1-flash-standard": "DeepSeek V4.1 Flash",
  "deepseek-v4.1-flash": "DeepSeek V4.1 Flash",
};
const displayModel = (id) => MODEL_NAMES[id] || String(id || "");

function colorFor(key) {
  const n = Number(key);
  if (!n) return "rgba(127,127,127,.45)";
  return PALETTE[(n - 1) % PALETTE.length];
}

function clockLabel(t, bucketMs) {
  const d = new Date(t);
  if (bucketMs >= 86400000) {
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  }
  return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

function metricValue(bucket, metric) {
  if (!bucket) return 0;
  if (metric === "requests") return bucket.requests || 0;
  if (metric === "tokens") return (bucket.input || 0) + (bucket.output || 0);
  if (metric === "cost") return bucket.cost || 0;
  return 0;
}

function keyMetric(kb, metric) {
  if (!kb) return 0;
  if (metric === "requests") return kb.requests || 0;
  if (metric === "tokens") return (kb.input || 0) + (kb.output || 0);
  if (metric === "cost") return kb.cost || 0;
  return 0;
}

function buildPanel(endpoint, refreshMs, headingText, getSessionId, getTitle) {
  const seriesUrl = endpoint.replace(/\/usage\/?$/, "/usage/series");
  const keysUrl = endpoint.replace(/\/usage\/?$/, "/keys");
  const sessionId = () => {
    try { return typeof getSessionId === "function" ? getSessionId() : null; } catch { return null; }
  };
  const titleOf = (id) => {
    try { return typeof getTitle === "function" ? getTitle(id) : null; } catch { return null; }
  };

  const root = document.createElement("div");
  root.style.cssText = [
    "padding:14px",
    "font:12px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif",
    "color:inherit",
    "height:100%",
    "overflow:auto",
    "box-sizing:border-box",
    "display:flex",
    "flex-direction:column",
    "gap:12px",
    "user-select:text",
    "-webkit-user-select:text",
    "cursor:auto",
  ].join(";");

  const state = {
    win: "24h",
    metric: "tokens",
    series: null,
    usage: null,
    keyReport: { keys: [], pins: {}, sessions: [] },
    error: null,
    keySig: "",
    keysOpen: false,
    editingKey: false,
    sessionSig: "",
  };

  // ---- header -------------------------------------------------------------
  const head = document.createElement("div");
  head.style.cssText = "display:flex;align-items:center;gap:6px;flex-wrap:wrap";

  const heading = document.createElement("div");
  heading.textContent = headingText || "Usage";
  heading.style.cssText = "font-weight:650;font-size:13px;margin-right:auto;letter-spacing:.01em";
  head.appendChild(heading);

  const pillStyle = [
    "padding:3px 9px",
    "border-radius:999px",
    "border:1px solid rgba(127,127,127,.3)",
    "background:transparent",
    "color:inherit",
    "cursor:pointer",
    "font:inherit",
    "line-height:1.5",
  ].join(";");

  const winButtons = {};
  for (const w of WINDOWS) {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = w;
    if (w === "24u") b.title = "Since 00:00 UTC today";
    if (w === "24h") b.title = "Rolling last 24 hours";
    b.style.cssText = pillStyle;
    b.onclick = () => { state.win = w; refresh(); };
    winButtons[w] = b;
    head.appendChild(b);
  }
  root.appendChild(head);

  // ---- chart card ---------------------------------------------------------
  const chartCard = document.createElement("div");
  chartCard.style.cssText = [
    "padding:10px 11px 8px",
    "border-radius:10px",
    "border:" + BORDER,
    "background:rgba(127,127,127,.06)",
    "display:flex",
    "flex-direction:column",
    "gap:8px",
  ].join(";");
  root.appendChild(chartCard);

  const metricRow = document.createElement("div");
  metricRow.style.cssText = "display:flex;align-items:center;gap:6px;flex-wrap:wrap";
  const metricButtons = {};
  for (const [id, label] of METRICS) {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = label;
    b.style.cssText = pillStyle;
    b.onclick = () => { state.metric = id; paint(); };
    metricButtons[id] = b;
    metricRow.appendChild(b);
  }
  const chartTotal = document.createElement("div");
  chartTotal.style.cssText = "margin-left:auto;font-variant-numeric:tabular-nums;opacity:.7";
  metricRow.appendChild(chartTotal);
  chartCard.appendChild(metricRow);

  const chartBox = document.createElement("div");
  chartBox.style.cssText = "position:relative;height:168px";
  const maxLabel = document.createElement("div");
  maxLabel.style.cssText = "position:absolute;top:-2px;left:0;font-size:10px;opacity:.45;pointer-events:none";
  chartBox.appendChild(maxLabel);
  const chart = document.createElement("div");
  chart.style.cssText = "position:absolute;inset:12px 0 0 0;display:flex;align-items:flex-end;gap:2px;border-bottom:" + BORDER;
  chartBox.appendChild(chart);
  chartCard.appendChild(chartBox);

  const axis = document.createElement("div");
  axis.style.cssText = "display:flex;justify-content:space-between;font-size:10px;opacity:.45;font-variant-numeric:tabular-nums";
  const axisStart = document.createElement("span");
  const axisMid = document.createElement("span");
  const axisEnd = document.createElement("span");
  axis.appendChild(axisStart);
  axis.appendChild(axisMid);
  axis.appendChild(axisEnd);
  chartCard.appendChild(axis);

  const emptyNote = document.createElement("div");
  emptyNote.textContent = "no usage in this window yet";
  emptyNote.style.cssText = "opacity:.5;font-size:11px;display:none";
  chartCard.appendChild(emptyNote);

  // ---- summary cards ------------------------------------------------------
  const cards = document.createElement("div");
  cards.style.cssText = "display:grid;grid-template-columns:repeat(auto-fit,minmax(88px,1fr));gap:8px";
  root.appendChild(cards);

  const cardValues = {};
  for (const [key, label] of [["requests", "Requests"], ["input", "Input"], ["output", "Output"], ["cost", "Cost"], ["tps", "tok/s"]]) {
    const card = document.createElement("div");
    card.style.cssText = [
      "padding:9px 11px",
      "border-radius:9px",
      "background:rgba(127,127,127,.08)",
      "border:" + BORDER,
      "min-width:0",
    ].join(";");
    const k = document.createElement("div");
    k.textContent = label;
    k.style.cssText = "font-size:10px;letter-spacing:.06em;text-transform:uppercase;opacity:.55";
    const v = document.createElement("div");
    v.textContent = "—";
    v.style.cssText = "font-size:16px;font-weight:650;margin-top:3px;font-variant-numeric:tabular-nums;white-space:nowrap";
    card.appendChild(k);
    card.appendChild(v);
    cards.appendChild(card);
    cardValues[key] = v;
  }

  // ---- per-key legend -----------------------------------------------------
  const legend = document.createElement("div");
  legend.style.cssText = "display:flex;flex-direction:column;gap:4px";
  root.appendChild(legend);

  // ---- sessions -----------------------------------------------------------
  // Who is using which key right now and where each session is pinned. The
  // routed session is marked "(this)".
  const sessionsBox = document.createElement("div");
  sessionsBox.style.cssText = "border-top:" + BORDER + ";padding-top:10px;display:flex;flex-direction:column;gap:6px";
  const sessionsHead = document.createElement("div");
  sessionsHead.style.cssText = "font-size:11px;font-weight:650;letter-spacing:.04em;text-transform:uppercase;opacity:.55";
  const sessionsList = document.createElement("div");
  sessionsList.style.cssText = "display:flex;flex-direction:column;gap:3px";
  sessionsBox.appendChild(sessionsHead);
  sessionsBox.appendChild(sessionsList);

  // ---- keys manager -------------------------------------------------------
  const keysBox = document.createElement("div");
  keysBox.style.cssText = "border-top:" + BORDER + ";padding-top:10px;display:flex;flex-direction:column;gap:6px";
  const keysHead = document.createElement("div");
  keysHead.style.cssText = "font-size:11px;font-weight:650;letter-spacing:.04em;text-transform:uppercase;opacity:.55;cursor:pointer;user-select:none;-webkit-user-select:none";
  keysHead.setAttribute("role", "button");
  keysHead.tabIndex = 0;
  const keysList = document.createElement("div");
  keysList.style.cssText = "display:flex;flex-direction:column;gap:3px";
  const keysNote = document.createElement("div");
  keysNote.style.cssText = "opacity:.5;font-size:11px";
  keysNote.textContent = "saved to keys.json — each session sticks to one key";
  keysBox.appendChild(keysHead);
  keysBox.appendChild(keysList);
  keysBox.appendChild(keysNote);

  const addRow = document.createElement("div");
  addRow.style.cssText = "display:grid;grid-template-columns:minmax(0,1fr) 58px;gap:6px;align-items:center;margin-top:4px";
  const addInput = document.createElement("input");
  addInput.type = "password";
  addInput.autocomplete = "off";
  addInput.spellcheck = false;
  addInput.placeholder = "paste a cb_… key to add";
  addInput.style.cssText = FIELD;
  const addBtn = buttonEl("Add", "Add a key to the pool");
  const doAdd = () => {
    const v = addInput.value.trim();
    if (!v) return;
    addInput.value = "";
    saveKeys({ add: v });
  };
  addBtn.onclick = doAdd;
  addInput.onkeydown = (e) => {
    if (e.key === "Enter") { e.preventDefault(); doAdd(); }
  };
  addRow.appendChild(addInput);
  addRow.appendChild(addBtn);
  keysBox.appendChild(addRow);

  function paintKeysOpen() {
    const report = state.keyReport || {};
    const n = (report.keys || []).length;
    keysHead.textContent = `${state.keysOpen ? "▾" : "▸"} Keys` + (n ? ` · ${n}` : "");
    keysHead.setAttribute("aria-expanded", String(state.keysOpen));
    keysHead.title = state.keysOpen ? "Hide the key manager" : "Show the key manager";
    keysList.style.display = state.keysOpen ? "flex" : "none";
    keysNote.style.display = state.keysOpen ? "block" : "none";
    addRow.style.display = state.keysOpen ? "grid" : "none";
  }
  const toggleKeys = () => {
    state.keysOpen = !state.keysOpen;
    paintKeysOpen();
    renderKeys();
  };
  keysHead.onclick = toggleKeys;
  keysHead.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggleKeys(); }
  };

  async function saveKeys(payload) {
    try {
      const r = await fetch(keysUrl, { method: "POST", cache: "no-store", body: JSON.stringify(payload) });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
      state.keyReport = j;
      state.keySig = "";
      state.sessionSig = "";
      keysNote.textContent = "saved to keys.json — each session sticks to one key";
      paintKeysOpen();
      renderKeys();
      paintSessions();
      paintLegend();
    } catch (e) {
      keysNote.textContent = "key update failed: " + ((e && e.message) || "unreachable");
    }
  }

  function renderKeys() {
    paintKeysOpen();
    keysList.textContent = "";
    const report = state.keyReport || {};
    const keys = report.keys || [];
    const pins = report.pins || {};
    if (!keys.length) {
      const empty = document.createElement("div");
      empty.textContent = "no keys configured — add a cb_… key below";
      empty.style.cssText = "opacity:.5;font-size:11px";
      keysList.appendChild(empty);
      return;
    }
    const here = sessionId();
    keys.forEach((current) => {
      const i = current.index;
      const row = document.createElement("div");
      row.style.cssText = "display:grid;grid-template-columns:minmax(0,1fr) 52px 50px 58px 26px;gap:5px;align-items:center";

      const input = document.createElement("input");
      input.type = "password";
      input.readOnly = true;
      input.autocomplete = "off";
      input.spellcheck = false;
      input.value = KEY_MASK;
      input.title = `key ${i} · ${current.hint} · ${current.sessions || 0} live session(s)`;
      input.style.cssText = FIELD;

      const reveal = buttonEl("Show", `Show key ${i}`, "height:26px;padding:0 7px");
      reveal.onclick = async () => {
        if (state.editingKey) return;
        if (input.type === "text") {
          input.type = "password";
          input.value = KEY_MASK;
          reveal.textContent = "Show";
          return;
        }
        try {
          const r = await fetch(`${keysUrl}/reveal?slot=${i}`, { cache: "no-store" });
          const j = await r.json();
          if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
          input.type = "text";
          input.value = j.key;
          reveal.textContent = "Hide";
        } catch (e) {
          keysNote.textContent = "reveal failed: " + ((e && e.message) || "unreachable");
        }
      };

      const finishEdit = () => {
        state.editingKey = false;
        input.readOnly = true;
        input.type = "password";
        input.value = KEY_MASK;
        input.placeholder = "";
        edit.textContent = "Edit";
        reveal.style.opacity = "1";
        reveal.disabled = false;
      };
      const edit = buttonEl("Edit", `Replace key ${i}`, "height:26px;padding:0 7px");
      edit.onclick = () => {
        if (!state.editingKey) {
          state.editingKey = true;
          input.readOnly = false;
          input.type = "password";
          input.value = "";
          input.placeholder = "new cb_… key";
          edit.textContent = "Save";
          reveal.style.opacity = ".45";
          reveal.disabled = true;
          input.focus();
          return;
        }
        const v = input.value.trim();
        if (!v) { finishEdit(); return; }
        state.editingKey = false;
        saveKeys({ set: i, key: v });
      };
      input.onkeydown = (e) => {
        if (!state.editingKey) return;
        if (e.key === "Enter") { e.preventDefault(); edit.onclick(); }
        if (e.key === "Escape") { e.preventDefault(); finishEdit(); }
      };

      const pinned = !!here && Number(pins[here]) === i;
      const pinCell = buttonEl(
        pinned ? "Pinned" : "Pin",
        here ? `Pin this session to key ${i}` : "Open a session first",
        "height:26px;padding:0 6px;" + (pinned ? "border:1px solid rgba(120,200,140,.65);background:rgba(120,200,140,.16)" : ""),
      );
      pinCell.disabled = !here;
      pinCell.style.opacity = here ? "1" : ".45";
      pinCell.onclick = () => { if (here) saveKeys({ pin: { session: here, key: i } }); };

      const delCell = buttonEl("×", `Remove key ${i}`, "height:26px;padding:0");
      delCell.onclick = () => saveKeys({ remove: i });

      row.appendChild(input);
      row.appendChild(reveal);
      row.appendChild(edit);
      row.appendChild(pinCell);
      row.appendChild(delCell);
      keysList.appendChild(row);
    });
  }

  // ---- model table --------------------------------------------------------
  const table = document.createElement("table");
  table.style.cssText = "width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;user-select:text;-webkit-user-select:text";
  root.appendChild(table);
  // Sessions sit under the table and the key editor at the very bottom, the
  // same order as the original proxy panel.
  root.appendChild(sessionsBox);

  const footer = document.createElement("div");
  footer.style.cssText = "margin-top:auto;padding-top:6px;opacity:.45;font-size:11px";
  footer.textContent = endpoint;
  root.appendChild(footer);
  root.appendChild(keysBox);

  function pillState() {
    for (const w of WINDOWS) {
      const on = w === state.win;
      winButtons[w].style.background = on ? "rgba(127,127,127,.22)" : "transparent";
      winButtons[w].style.fontWeight = on ? "650" : "400";
    }
    for (const [id] of METRICS) {
      const on = id === state.metric;
      metricButtons[id].style.background = on ? "rgba(127,127,127,.22)" : "transparent";
      metricButtons[id].style.fontWeight = on ? "650" : "400";
    }
  }

  function th(text, align, title) {
    const el = document.createElement("th");
    el.textContent = text;
    el.style.cssText = [
      "text-align:" + (align || "left"),
      "padding:4px 6px",
      "font-size:10px",
      "font-weight:600",
      "letter-spacing:.05em",
      "text-transform:uppercase",
      "opacity:.55",
      "border-bottom:" + BORDER,
      "white-space:nowrap",
    ].join(";");
    if (title) el.title = title;
    return el;
  }

  function td(text, align, opts = {}) {
    const el = document.createElement("td");
    el.textContent = text;
    el.style.cssText = [
      "text-align:" + (align || "left"),
      "padding:4px 6px",
      "border-bottom:" + (opts.last ? "none" : BORDER),
      opts.muted ? "opacity:.5" : "",
      opts.strong ? "font-weight:650" : "",
      "white-space:nowrap",
    ].join(";");
    if (opts.title) el.title = opts.title;
    return el;
  }

  function paintChart() {
    pillState();
    chart.textContent = "";
    const buckets = (state.series && state.series.buckets) || [];
    const metric = state.metric;
    const values = buckets.map((b) => metricValue(b, metric));
    const max = Math.max(0, ...values);
    const bucketMs = (state.series && state.series.bucketMs) || 3600000;

    maxLabel.textContent = max > 0
      ? (metric === "cost" ? money(max) : fmt(max))
      : "";
    emptyNote.style.display = max > 0 ? "none" : "block";

    for (const b of buckets) {
      const col = document.createElement("div");
      col.style.cssText = "flex:1 1 0;min-width:2px;height:100%;display:flex;flex-direction:column-reverse;position:relative";
      const keys = Object.keys(b.byKey || {}).sort((a, z) => Number(a) - Number(z));
      for (const k of keys) {
        const v = keyMetric(b.byKey[k], metric);
        if (v <= 0) continue;
        const seg = document.createElement("div");
        seg.style.cssText = `height:${(v / max) * 100}%;background:${colorFor(k)};opacity:.85;min-height:1px`;
        col.appendChild(seg);
      }
      const start = clockLabel(b.t, bucketMs);
      const end = clockLabel(b.t + bucketMs, bucketMs);
      const perKey = keys
        .map((k) => `  key ${k}: ${fmt(b.byKey[k].requests)} req · ${fmt((b.byKey[k].input || 0) + (b.byKey[k].output || 0))} tok`)
        .join("\n");
      col.title = `${start} – ${end}\n${fmt(b.requests)} requests · ${fmt(b.input)} in · ${fmt(b.output)} out · ${money(b.cost)}`
        + (perKey ? "\n" + perKey : "");
      chart.appendChild(col);
    }

    const first = buckets.length ? clockLabel(buckets[0].t, bucketMs) : "—";
    const lastT = buckets.length ? buckets[buckets.length - 1].t + bucketMs : Date.now();
    const mid = buckets.length ? clockLabel(buckets[Math.floor(buckets.length / 2)].t, bucketMs) : "";
    axisStart.textContent = first;
    axisMid.textContent = mid;
    axisEnd.textContent = clockLabel(lastT, bucketMs);

    const total = metricsTotal(buckets, metric);
    chartTotal.textContent = metric === "cost" ? money(total) : `${fmt(total)} ${metric === "tokens" ? "tokens" : "requests"}`;
  }

  function metricsTotal(buckets, metric) {
    let out = 0;
    for (const b of buckets) out += metricValue(b, metric);
    return out;
  }

  function paintCards() {
    const w = state.usage && state.usage.windows && state.usage.windows[state.win];
    cardValues.requests.textContent = w ? fmt(w.requests) : "—";
    cardValues.input.textContent = w ? fmt(w.input) : "—";
    cardValues.output.textContent = w ? fmt(w.output) : "—";
    cardValues.cost.textContent = w ? money(w.cost) : "—";
    cardValues.tps.textContent = w && w.avgTps ? String(w.avgTps) : "—";
  }

  function paintLegend() {
    legend.textContent = "";
    const buckets = (state.series && state.series.buckets) || [];
    const byKey = {};
    for (const b of buckets) {
      for (const [k, kb] of Object.entries(b.byKey || {})) {
        const t = (byKey[k] = byKey[k] || { requests: 0, tokens: 0, cost: 0 });
        t.requests += kb.requests || 0;
        t.tokens += (kb.input || 0) + (kb.output || 0);
        t.cost += kb.cost || 0;
      }
    }
    const keys = (state.keyReport && state.keyReport.keys) || [];
    if (!keys.length) {
      const none = document.createElement("div");
      none.textContent = "no keys registered";
      none.style.cssText = "opacity:.5;font-size:11px";
      legend.appendChild(none);
      return;
    }
    for (const k of keys) {
      const t = byKey[String(k.index)] || { requests: 0, tokens: 0, cost: 0 };
      const row = document.createElement("div");
      row.style.cssText = "display:flex;align-items:center;gap:8px;font-variant-numeric:tabular-nums";
      const dot = document.createElement("span");
      dot.style.cssText = `width:9px;height:9px;border-radius:50%;background:${colorFor(k.index)};flex:none;opacity:.9`;
      const name = document.createElement("span");
      name.textContent = `Key ${k.index} · ${k.hint}`;
      name.style.cssText = "min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
      name.title = `${k.sessions || 0} live session(s)`;
      const val = document.createElement("span");
      val.textContent = `${fmt(t.requests)} req · ${fmt(t.tokens)} tok · ${money(t.cost)}`;
      val.style.cssText = "margin-left:auto;opacity:.75;white-space:nowrap";
      row.appendChild(dot);
      row.appendChild(name);
      row.appendChild(val);
      legend.appendChild(row);
    }
  }

  function paintTable() {
    table.textContent = "";
    const models = (state.usage && state.usage.models && state.usage.models[state.win]) || {};
    const names = Object.keys(models);
    if (!names.length) {
      const row = document.createElement("tr");
      const cell = td("no requests in this window", "left", { last: true, muted: true });
      cell.colSpan = 6;
      row.appendChild(cell);
      table.appendChild(row);
      return;
    }
    const head = document.createElement("tr");
    head.appendChild(th("Model", "left"));
    head.appendChild(th("Req", "right"));
    head.appendChild(th("In", "right", "Prompt tokens, cached tokens included"));
    head.appendChild(th("Out", "right", "Completion tokens"));
    head.appendChild(th("tok/s", "right", "Decode speed: output tokens after the first, over the time between the first and last streamed token"));
    head.appendChild(th("$", "right"));
    table.appendChild(head);
    names.sort((a, b) => (models[b].requests || 0) - (models[a].requests || 0));
    names.forEach((name, i) => {
      const m = models[name];
      const last = i === names.length - 1;
      const tr = document.createElement("tr");
      tr.title = name;
      tr.appendChild(td(displayModel(name), "left", { last }));
      tr.appendChild(td(fmt(m.requests), "right", { last }));
      tr.appendChild(td(fmt(m.input), "right", { last }));
      tr.appendChild(td(fmt(m.output), "right", { last }));
      tr.appendChild(td(m.avgTps ? String(m.avgTps) : "—", "right", { last, muted: !m.avgTps }));
      tr.appendChild(td(money(m.cost), "right", { last }));
      table.appendChild(tr);
    });
  }

  // ---- sessions -----------------------------------------------------------
  // One row per live session: which key it is glued to, the model it last used,
  // its state and a pin selector. The routed session is marked "(this)".
  function paintSessions() {
    const report = state.keyReport || {};
    const sessions = report.sessions || [];
    const here = sessionId();
    sessionsHead.textContent = `Sessions${sessions.length ? ` · ${sessions.length}` : ""}`;

    // idleMs moves every second; round it so the table does not rebuild on each
    // 5s refresh unless something visible changed.
    const sig = sessions
      .map((s) => `${s.id}|${s.key}|${s.active}|${s.requests}|${s.model}|${Math.floor((s.idleMs || 0) / 10000)}`)
      .join(",") + `|${here || ""}`;
    if (sig === state.sessionSig) return;
    state.sessionSig = sig;

    sessionsList.textContent = "";
    if (!sessions.length) {
      const empty = document.createElement("div");
      empty.textContent = "no sessions in the last 30 minutes";
      empty.style.cssText = "opacity:.5;font-size:11px";
      sessionsList.appendChild(empty);
      return;
    }

    // Same shape as the original proxy panel: Session · Key · Model · Req ·
    // State. Pinning lives in the key editor at the bottom (Pin = glue the
    // routed session to that key).
    const tbl = document.createElement("table");
    tbl.style.cssText = "width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;user-select:text;-webkit-user-select:text";
    const head = document.createElement("tr");
    head.appendChild(th("Session", "left", "OpenCode session id; (this) is the tab you are looking at"));
    head.appendChild(th("Key", "center", "the key this session's requests are charged to; pin it from the key editor below"));
    head.appendChild(th("Model", "left"));
    head.appendChild(th("Req", "right", "requests this session sent through the proxy"));
    head.appendChild(th("State", "left"));
    tbl.appendChild(head);

    sessions.forEach((s, i) => {
      const last = i === sessions.length - 1;
      const isThis = !!here && s.id === here;
      const tr = document.createElement("tr");
      tr.title = s.id;

      tr.appendChild(td(`${titleOf(s.id) || shortId(s.id)}${isThis ? "  (this)" : ""}`, "left", {
        last, strong: isThis,
      }));
      tr.appendChild(td(s.key ? `Key ${s.key}` : "no key", "center", {
        last, muted: !s.key, strong: s.active > 0,
      }));
      tr.appendChild(td(s.model ? displayModel(s.model) : "–", "left", { last, muted: !s.model }));
      tr.appendChild(td(fmt(s.requests), "right", { last }));
      tr.appendChild(td(s.active > 0 ? "running" : idleText(s.idleMs), "left", {
        last, strong: s.active > 0, muted: s.active <= 0,
      }));
      tbl.appendChild(tr);
    });

    sessionsList.appendChild(tbl);
  }

  function paint() {
    paintChart();
    paintCards();
    paintLegend();
    paintSessions();
    paintTable();
    paintKeysOpen();
    const report = state.keyReport || {};
    if (state.keysOpen && !state.editingKey) {
      const sig = JSON.stringify([report.keys || [], report.pins || {}, sessionId() || ""]);
      if (sig !== state.keySig) {
        state.keySig = sig;
        renderKeys();
      }
    }
    footer.textContent = state.error
      ? `${endpoint} — ${state.error}`
      : `${endpoint} · updated ${new Date().toLocaleTimeString()}`;
  }

  async function refresh() {
    try {
      const [seriesRes, usageRes, keysRes] = await Promise.all([
        fetch(`${seriesUrl}?window=${encodeURIComponent(state.win)}`, { cache: "no-store" }),
        fetch(endpoint, { cache: "no-store" }),
        fetch(keysUrl, { cache: "no-store" }).catch(() => null),
      ]);
      state.series = seriesRes.ok ? await seriesRes.json() : null;
      state.usage = usageRes.ok ? await usageRes.json() : null;
      state.keyReport = keysRes && keysRes.ok ? await keysRes.json() : { keys: [], pins: {}, sessions: [] };
      state.error = state.series ? null : "proxy unreachable";
    } catch (e) {
      state.error = String(e && e.message ? e.message : e);
    }
    paint();
  }

  paint();
  refresh();
  const timer = setInterval(refresh, refreshMs);
  try { timer.unref?.(); } catch {}

  return root;
}

module.exports = {
  default: Plugin.define({
    i18n: { en: { title: "Coralbricks usage", heading: "Usage" } },
    setup(ctx) {
      const endpoint = (ctx.options && ctx.options.usageEndpoint) || "http://127.0.0.1:17332/usage";
      const refreshMs = Math.max(1000, Number(ctx.options && ctx.options.refreshMs) || 5000);
      const key = `${ctx.id}:usage`;
      const statusUrl = endpoint.replace(/\/usage\/?$/, "/status");

      // Proxy retry state for the pill: poll /status once a second.
      const statusTimer = setInterval(async () => {
        try {
          const r = await fetch(statusUrl, { cache: "no-store" });
          setProxyStatus(r.ok ? await r.json() : null);
        } catch {
          setProxyStatus(null);
        }
      }, 1000);

      // Composer pill: elapsed time for the routed session plus the proxy's
      // retry state while a request is being re-sent.
      const tickTimer = setInterval(() => setTick((n) => n + 1), 1000);

      ctx.add(ComposerPill, {
        id: "working",
        order: 5,
        show: (input) => {
          tick();
          const seconds = elapsedSeconds(input.session);
          if (seconds === null) return undefined;
          const s = proxyStatus();
          const e = s && s.sessions && input.session ? s.sessions[input.session.id] : null;
          const code = e && Number.isFinite(e.status) ? e.status : null;
          // Only a real failure status is worth showing; an internal retry of a
          // 200 stream (cut/empty) must not read "retrying … HTTP 200".
          const showCode = code !== null && code >= 400;
          const suffix = e && e.retries > 0
            ? ` · retrying ${e.retries}${showCode ? ` · HTTP ${code}` : ""}`
            : "";
          return {
            label: `Working (${formatSeconds(seconds)}${suffix})`,
            icon: "status",
            tone: e && e.retries > 0 ? "warning" : undefined,
            title: e && e.retries > 0
              ? (showCode
                ? `${e.model || "request"} returned ${code}; the proxy is retrying`
                : `${e.model || "request"} was cut early; the proxy is re-sending it`)
              : "Session is running",
          };
        },
      });

      const tab = {
        id: "usage",
        get title() { return ctx.t("title"); },
        closable: "compact",
        fallback: true,
      };

      const open = () => {
        const session = ctx.sessions.current();
        if (session) ctx.layout.open(key, session);
      };

      ctx.add(Panel, {
        id: "usage",
        region: "side",
        width: "narrow",
        mobile: {
          get title() { return ctx.t("title"); },
          order: 40,
          kind: "menu",
          icon: "status",
          cached: true,
        },
        list: (input) => (input.open.includes("usage") ? [tab] : []),
        render: () => buildPanel(endpoint, refreshMs, ctx.t("heading"), () => {
          const s = ctx.sessions && ctx.sessions.current ? ctx.sessions.current() : null;
          return s && s.id ? s.id : null;
        }, (id) => {
          // Titles come from the app's own session data, which knows every
          // session the server has, not just the one on screen.
          const s = ctx.sessions && ctx.sessions.current ? ctx.sessions.current() : null;
          const info = s && s.server && s.server.data && s.server.data.session && s.server.data.session.get(id);
          return (info && (info.title || info.name)) || null;
        }),
      });

      ctx.add(MenuItem, () => ({
        menu: "session.panel",
        id: "usage",
        title: ctx.t("title"),
        icon: "status",
        description: "usage graph · tokens · cost",
        run: open,
      }));

      ctx.add(Command, () => ({
        id: "usage",
        title: ctx.t("title"),
        section: "navigation",
        run: open,
      }));

      return () => {
        clearInterval(statusTimer);
        clearInterval(tickTimer);
      };
    },
  }),
};
