// Second pass: confirm the GLMs produce real content with a sane budget, and
// diff /v1/models against the current config for anything new.
import fs from "node:fs";
const cfg = JSON.parse(fs.readFileSync("C:/Users/User/.config/opencode/opencode.json", "utf8").replace(/^\uFEFF/, ""));
const key = cfg.providers.coralbricks.settings.apiKey;

async function probe(model, maxTokens) {
  const r = await fetch("https://coralbricks.ai/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
    signal: AbortSignal.timeout(90000),
    body: JSON.stringify({ model, stream: false, max_tokens: maxTokens, messages: [{ role: "user", content: "Say OK" }] }),
  });
  const t = await r.text();
  try {
    const j = JSON.parse(t);
    const msg = j?.choices?.[0]?.message ?? {};
    return { status: r.status, content: (msg.content || "").trim().slice(0, 40), reasoning: (msg.reasoning || "").length };
  } catch { return { status: r.status, content: null, err: t.slice(0, 100) }; }
}

for (const m of ["glm-5.3", "glm-5.3-flash"]) {
  const r = await probe(m, 400);
  console.log(`${m.padEnd(15)} status=${r.status} content=${JSON.stringify(r.content)} reasoningChars=${r.reasoning ?? "-"}`);
  await new Promise((res) => setTimeout(res, 3000));
}

const cat = await (await fetch("https://coralbricks.ai/v1/models", { headers: { authorization: `Bearer ${key}` } })).json();
const chat = cat.data.filter((m) => (m.endpoints || []).includes("chat/completions"));
const configured = new Set(Object.keys(cfg.providers.coralbricks.models));
console.log(`\nchat-capable in /v1/models (${chat.length}):`);
for (const m of chat) console.log(`  ${configured.has(m.id) ? "have " : "NEW  "} ${m.id}`);
console.log(`\nconfigured but NOT chat-capable: ${[...configured].filter((id) => !chat.some((m) => m.id === id)).join(", ")}`);
