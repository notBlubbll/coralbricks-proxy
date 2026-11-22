// Reachability probe for every model in the current coralbricks config.
// Goes DIRECT to https://coralbricks.ai/v1 (bypasses the proxy) so the status
// codes are the upstream's truth: 404 = dead id, 400 unsupported endpoint =
// non-chat, 429/5xx = valid id but transiently limited, 200 = verified.
import fs from "node:fs";

const cfg = JSON.parse(fs.readFileSync("C:/Users/User/.config/opencode/opencode.json", "utf8").replace(/^\uFEFF/, ""));
const key = cfg.providers.coralbricks.settings.apiKey;
const models = Object.keys(cfg.providers.coralbricks.models);

async function probe(model) {
  const t0 = Date.now();
  try {
    const r = await fetch("https://coralbricks.ai/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
      signal: AbortSignal.timeout(90000),
      body: JSON.stringify({ model, stream: false, max_tokens: 24, messages: [{ role: "user", content: "Say OK" }] }),
    });
    const text = await r.text();
    let verdict, note = "";
    if (r.status === 200) {
      let ok = false, content = "";
      try {
        const j = JSON.parse(text);
        const c = j?.choices?.[0]?.message?.content;
        if (typeof c === "string") { ok = true; content = c.trim().slice(0, 40); }
      } catch {}
      verdict = ok ? "WORKS" : "200-odd";
      note = content;
    } else if (/not found/i.test(text)) {
      verdict = "DEAD";
      note = text.replace(/\s+/g, " ").slice(0, 90);
    } else if (/does not support the chat/i.test(text)) {
      verdict = "NON-CHAT";
      note = "no chat/completions endpoint";
    } else if (r.status === 429 || /rate limit|per min|capacity|overloaded/i.test(text)) {
      verdict = "VALID-LIMITED"; // id recognized, transiently limited
      note = text.replace(/\s+/g, " ").slice(0, 90);
    } else {
      verdict = `HTTP-${r.status}`;
      note = text.replace(/\s+/g, " ").slice(0, 90);
    }
    return { model, status: r.status, ms: Date.now() - t0, verdict, note };
  } catch (e) {
    const msg = String(e?.message || e);
    return { model, status: "ERR", ms: Date.now() - t0, verdict: msg.includes("timeout") ? "TIMEOUT" : "ERR", note: msg.slice(0, 90) };
  }
}

const results = [];
for (const m of models) {
  const r = await probe(m);
  results.push(r);
  console.log(`${r.verdict.padEnd(14)} ${m.padEnd(26)} status=${String(r.status).padEnd(5)} ms=${String(r.ms).padEnd(6)} ${r.note}`);
  await new Promise((res) => setTimeout(res, 3000)); // stay under the per-minute limit
}
console.log("\nSUMMARY " + JSON.stringify(Object.fromEntries(results.map((r) => [r.model, r.verdict]))));
