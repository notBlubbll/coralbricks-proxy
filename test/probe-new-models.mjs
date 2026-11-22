// Final verification pass for the NEW chat-capable models found in /v1/models.
import fs from "node:fs";
const cfg = JSON.parse(fs.readFileSync("C:/Users/User/.config/opencode/opencode.json", "utf8").replace(/^\uFEFF/, ""));
const key = cfg.providers.coralbricks.settings.apiKey;

const NEW = ["glm-5.2", "gemma-4-31b-it", "kimi-k2.6", "kimi-k2.7-code", "stealth/funfly", "economy/deepseek-v4.1-flash", "stealth/autistic-parrot"];
const cat = await (await fetch("https://coralbricks.ai/v1/models", { headers: { authorization: `Bearer ${key}` } })).json();
const byId = Object.fromEntries(cat.data.map((m) => [m.id, m]));

async function probe(model) {
  const r = await fetch("https://coralbricks.ai/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
    signal: AbortSignal.timeout(120000),
    body: JSON.stringify({ model, stream: false, max_tokens: 300, messages: [{ role: "user", content: "Say OK" }] }),
  });
  const t = await r.text();
  try {
    const j = JSON.parse(t);
    const msg = j?.choices?.[0]?.message ?? {};
    return {
      status: r.status,
      content: (msg.content || "").trim().slice(0, 30),
      reasoningField: typeof msg.reasoning === "string" ? "reasoning" : (typeof msg.reasoning_content === "string" ? "reasoning_content" : "-"),
      tools: byId[model]?.chat_capabilities?.tools ?? "?",
      vision: byId[model]?.chat_vision ?? "?",
      ctx: byId[model]?.context_length ?? "?",
    };
  } catch { return { status: r.status, content: null, err: t.slice(0, 90) }; }
}

for (const m of NEW) {
  const meta = byId[m] ?? {};
  const r = await probe(m);
  console.log(`${m.padEnd(30)} status=${r.status} tools=${r.tools} vision=${r.vision} ctx=${r.ctx} reasoningField=${r.reasoningField} content=${JSON.stringify(r.content ?? r.err)}`);
  await new Promise((res) => setTimeout(res, 3000));
}
