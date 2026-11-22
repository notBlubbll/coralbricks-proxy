// coralbricks-proxy/index.js — OpenCode V2 plugin: autostarting retry-proxy for coralbricks.
//
// Purpose: opencode's V2 router throws
//     "Invalid coralbricks/openai-compatible-chat stream event"
// whenever a coralbricks SSE `data:` payload fails the openai-compatible-chat
// schema decode (router: `Invalid ${route} stream event`). This proxy sits
// between opencode (baseURL http://127.0.0.1:17332/v1) and
// https://inference.coralbricks.ai/v1 and makes that error impossible to reach the client:
//
//   - PRE-COMMIT: upstream SSE events are buffered and validated; NOTHING is
//     forwarded until the stream proves valid. Any failure (non-2xx, network
//     error, invalid event, in-band error payload, stall) = failed attempt
//     -> the WHOLE request is retried with the same buffered body, up to
//     10 retries (11 attempts) with backoff. Only after that does the last
//     upstream failure pass through.
//   - POST-COMMIT: every event is still validated before it is written;
//     invalid events are DROPPED inline (logged) so the client parser can
//     never see one; valid events pass through byte-exact.
//   - UPSTREAM DEATH after commit: synthesizes a schema-VALID error chunk
//     (`{"error":{"message":...,"type":"proxy_error"}}` + [DONE]) so the
//     client sees a clean provider error — never a parse error.
//   - REPAIR: semantically-null typed fields (`delta.content: null` etc.)
//     are stripped (schema-safe, meaning-preserving); `{"error":"str"}`
//     is reshaped to `{"error":{"message":"str"}}`.
//
// Auth/model passthrough: with a key pool configured (keys.json next to this
// file, or CORALBRICKS_PROXY_KEYS), the proxy owns the credential — each session
// is pinned to one registered key and the client's Authorization header is
// ignored. Without a pool the inbound header is forwarded unchanged. Optional
// `x-cb-key` / `x-cb-upstream` headers override per request and are stripped.
// The request body (incl. the model id) is forwarded byte-exact, except for
// `stream_options.include_usage` (added to streaming chat calls so per-request
// token counts come back for GET /usage).
//
// rev: 1 — initial port of the tinyproxy pattern (hot-reload guard via
//      globalThis, Bun crash-proofing, Expect: 100-continue, idle watchdog,
//      per-attempt init rebuild, bulk-JSON-to-SSE conversion, EADDRINUSE
//      retry-listen so a standalone test instance can be taken over).
// rev: 2 — real-world hardening after the "Build All Projects in
//      Infinite-Cubes" post-mortem:
//      - raw node:http(s) sockets instead of fetch (Bun fetch concatenates
//        retried bytes into the same body on mid-body death);
//      - agent:false + explicit destroy (a keep-alive socket abandoned at
//        [DONE] re-delivered stale body bytes into the NEXT request);
//      - upstream base path prefix stripped once (base .../v1 + client path
//        /v1/chat/completions = /v1/v1/... 404);
//      - patient retry classes: 429 / Serverless TPM / AiError 8006
//        "request body exceeds the maximum supported size" — flaky per
//        attempt, the SAME body succeeds on a later retry; 25-min budget,
//        never consumes the 10 hard retries;
//      - 200 + JSON error bodies are failures, never converted to chunks
//        (that conversion was the "Invalid ... stream event" producer);
//      - give-up emits the real upstream error as a schema-valid SSE event.
// rev: 2.1 — memory-leak fix: the per-response idle watchdog interval in
//      makeReader never stopped on terminal paths (up.destroy() after [DONE]
//      emits 'close', not 'error'/'end'), leaking one interval + its closures
//      per response (~2GB RSS / 8h observed). Timer now cleared on 'close',
//      'end' and 'error'.
// rev: 2.2 — header-timeout coverage: for streaming requests the proxy now
//      sends SSE response headers immediately and emits `: keepalive` comments
//      every 15s so the client never times out waiting for headers while the
//      proxy is running vision handoff or retrying pre-commit upstream attempts.
//      Pre-commit retries continue internally against fresh upstream attempts as
//      long as no real data event has been committed to the client; only
//      keepalive comments flow meanwhile. This prevents "Timed out waiting for
//      response headers" without weakening stream validation (invalid events are
//      still never forwarded).
// rev: 2.3 — finish_reason guarantee: coralbricks sometimes ends a stream with
//      [DONE] (or a clean socket close) without ever sending a chunk carrying
//      finish_reason. OpenCode's openai-compatible router rejects that with
//      "OpenAI Chat stream ended without finish_reason". The proxy now injects
//      {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]} before
//      [DONE]/close whenever no finish_reason was seen, so the client always
//      sees a complete turn.
// rev: 2.4 — post-commit continuation: when coralbricks cuts a stream mid-answer
//      (no finish_reason/[DONE]) or sends a transient error after content, the
//      proxy retries the SAME request and skips the prefix already sent,
//      streaming only the new suffix. If the retry diverges, it closes the
//      partial answer cleanly instead of showing an error/retry loop.
// rev: 2.5 — abrupt-stop continuation: models sometimes send
//      finish_reason=stop mid-sentence. If the streamed text looks incomplete
//      (prose ending in a word/comma/colon, or an unclosed code fence), the
//      proxy treats it as a cut and uses the same prefix-skip continuation.
// rev: 2.6 — empty-turn recovery: a role-only chunk can commit headers before
//      a 503, leaving zero visible content. The proxy now retries that case
//      instead of closing an empty turn.
// rev: 2.7 — punctuation coverage: a stop after a Unicode dash (—, –, ―, −)
//      is treated as mid-thought, like an ASCII hyphen; opening brackets too.
// rev: 2.8 — finish-chunk content: the terminal chunk can carry new text
//      together with finish_reason; include it in the incompleteness check.
// rev: 2.9 — tool-call handoff during continuation + code-dump detection: if a
//      continuation retry chooses a tool call, hand it to the client instead
//      of ending partial; large file/code dumps ending at "}" are treated as
//      cuts (one retry, no-progress guard stops repeats).
// rev: 2.10 — code-dump continuation after tool calls: a prior Read/tool call
//      no longer blocks the dump retry; finish diagnostics log toolCalls.
// rev: 2.11 — broad code-dump fallback: any large code-like output (braces/
//      semicolons) is treated as a possible cut, not only path/DSML-marked.
// rev: 2.12 — text tool calls: retry when content contains "edit/write/read/
//      shell <path>" or DSML instead of structured tool_calls; hand off the
//      retry's real tool call.
// rev: 2.13 — DSML read conversion: a DSML read block streamed as text is
//      converted to a real read tool_calls delta; finish stop -> tool_calls.
// rev: 2.14 — buffer-until-complete (default): SSE responses are buffered and
//      flushed only after a complete turn; failures retry transparently with
//      no partial output. CORALBRICKS_PROXY_BUFFER=0 restores streaming.
// rev: 2.15 — DSML read variants: accept the bare path before the parameter
//      tags; buffer mode refuses to flush [DONE] without finish_reason when
//      the answer is incomplete.
// rev: 2.16 — DSML edit conversion: path + oldString + newString -> real edit
//      tool call; parameter values stop at closing DSML tags.
// rev: 2.17 — DSML grep conversion: pattern + path + include -> real grep
//      tool call (checked before read).
// rev: 2.18 — DSML write conversion: content + path -> real write tool call
//      (requires a closing tag; checked before grep/edit/read).
// rev: 2.19 — bare image path -> read tool call; tool calls after an early
//      finish_reason=stop remove the stop and emit a tool_calls finish.
// rev: 2.20 — split DSML consolidation: the whole buffered response is
//      joined, converted into one tool call, and DSML text dropped; bare
//      reads omit offset/limit when absent.
// rev: 2.21 — incomplete edit (oldString XOR newString) is a broken tool call:
//      buffer mode retries the whole request instead of converting to read/write.
// rev: 2.22 — DSML shell conversion: command + timeout -> real shell tool call.
// rev: 2.23 — truncated edit (both parameters, unclosed newString) and any
//      unparseable DSML shape are broken turns: buffer mode retries instead of
//      forwarding raw DSML; a failed buffered attempt no longer poisons the
//      next attempt's completeness check (sentContent reset).
// rev: 2.24 — /status tracks each streaming request separately (image
//      generations cannot overwrite retry state), reports `first try` while an
//      attempt is in flight, and marks the retry before the backoff wait.
// rev: 2.25 — DSML split across many small chunks is converted by the buffer
//      post-process instead of being treated as a cut and retried forever; a
//      raw-SSE scan guarantees unconvertible DSML is retried, never forwarded.
//      /status groups streams by x-opencode-session-id so the pill shows
//      `failed: N` for its own session only.
// rev: 2.26 — never-forward transients (503) now obey the 10-retry hard cap,
//      and every give-up appends a visible warning instead of closing
//      silently; a discarded buffered attempt still counts as partial.
// rev: 3.0 — coralbricks edition. Upstream is inference.coralbricks.ai/v1 with
//      the single deepseek-v4.1-flash-value model. Credentials are owned by the
//      proxy: keys.json holds the registered keys, each session is pinned to one
//      of them (least-live first, persisted, two-week TTL) and never flips
//      mid-turn. GET/POST /keys manage the pool, GET /keys/reveal reads one key
//      (loopback only), GET /usage reports per-model and per-key tokens, cost,
//      TTFB and tok/s over 24u/24h/7d/30d/all. Streaming chat calls get
//      stream_options.include_usage so the upstream returns real token counts.
// Standalone: `bun index.js` runs the same server outside opencode with env
// overrides CORALBRICKS_PROXY_PORT / CORALBRICKS_PROXY_UPSTREAM / CORALBRICKS_PROXY_KEY /
// CORALBRICKS_PROXY_DEBUG=1 (verbose event tracing).

import { createServer, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const TAG = "[coralbricks-proxy]";
// Live request telemetry for GET /status (drives the working pill's retry
// suffix). Streaming requests are tracked per-request so one stream's retry
// state can never leak into another stream (or a concurrent image generation).
const proxyState = { active: 0, streams: new Map(), streamSeq: 0, updated: 0 };
const UPSTREAM_DEFAULT = "https://inference.coralbricks.ai/v1";
const LISTEN_HOST = "127.0.0.1";
const PORT_DEFAULT = 17332;
const MAX_RETRIES = 10; // user spec: give up only after 10 retries (11 attempts)
const BACKOFF_MS = [300, 600, 1000, 2000, 3000]; // then 3000 flat
const RETRY_AFTER_CAP_MS = 60000;
// "Serverless token-per-minute limit reached" (429 status or in-band stream
// error): retry until it works — patient backoff within a budget (default
// 25 min, just under opencode's own 30-min request timeout), honoring
// Retry-After when present. Rate-limit retries do NOT consume the 10 hard
// retries; only hard failures (5xx non-RL, network, invalid events) do.
const RATE_LIMIT_BUDGET_MS_DEFAULT = 25 * 60 * 1000;
const RATE_LIMIT_BACKOFF_DEFAULT = [5000, 7000, 10000, 15000, 20000, 30000];
const RATE_LIMIT_RE = /(rate.?limit|per.?(minute|hour)|too many requests|quota|\b429\b|serverless|overloaded|capacity)/i;
// "AiError ... request body exceeds the maximum supported size" (code 8006,
// arrives as HTTP 200 + JSON error body, or 413): NOT deterministic — the
// same body succeeds on a later attempt, so retry patiently like rate limits.
const PATIENT_SIZE_RE = /(exceeds the maximum supported size|payload too large|request (body|entity) too large|"code"\s*:\s*8006|\b8006\b)/i;
// Transient upstream availability errors ("Service temporarily unavailable",
// 503 "temporarily unavailable", bad gateway, connection resets, overloaded):
// the SAME request succeeds on a later attempt, so retry patiently like rate
// limits instead of burning the 10 hard retries in ~30s and surfacing the
// error to the client (which then retries the whole turn).
const TRANSIENT_RE = /(service temporarily unavailable|temporarily unavailable|try again later|bad gateway|gateway time-?out|upstream (?:connect|connection|request)|connection (?:reset|refused|closed)|unexpected eof|overloaded|capacity)/i;
// Errors that must NEVER reach the client: the desktop treats them as a
// retryable turn failure and loops ("Attempt N - retrying"). Pre-commit they
// are retried with no budget (until success or client disconnect); post-commit
// they are suppressed and the stream is closed cleanly with [DONE]. Configurable
// via CORALBRICKS_PROXY_NEVER_FORWARD (a JS regex source, case-insensitive).
const NEVER_FORWARD_RE = (() => {
  const src = process.env.CORALBRICKS_PROXY_NEVER_FORWARD || "service temporarily unavailable";
  try { return new RegExp(src, "i"); } catch { return /service temporarily unavailable/i; }
})();

// ---- model-based vision handoff ----
// A text-only target model that receives image parts cannot see them. The
// proxy captions each image with a vision model and replaces the image part
// with the caption text, then forwards the request to the ORIGINAL model —
// one request in, one stream out, invisible to the client. Captions are
// cached by image hash: the same screenshot resent on later turns is free.
const VISION_MODEL_DEFAULT = "coralbricks/deepseek-v4.1-flash-value";
// The only coralbricks model accepts image input (verified against /v1/models:
// supports_image_input=true), so nothing is captioned by default. List a model
// here only if it really cannot see.
const VISION_TARGETS_DEFAULT = "";
const VISION_PROMPT_DEFAULT = "Describe this image in full detail for a text-only assistant that cannot see it. Transcribe all text, code, UI labels, error messages, tables and diagrams exactly. Be complete but concise.";
const VISION_CACHE_TTL_MS = 60 * 60 * 1000;
const VISION_CACHE_MAX = 200;
const VISION_ENABLED = !/^(0|false|off)$/i.test(process.env.CORALBRICKS_PROXY_VISION || "");
const VISION_MODEL = process.env.CORALBRICKS_PROXY_VISION_MODEL || VISION_MODEL_DEFAULT;
const VISION_TARGETS = (process.env.CORALBRICKS_PROXY_VISION_TARGETS || VISION_TARGETS_DEFAULT).split(",").map((s) => s.trim()).filter(Boolean);
const VISION_PROMPT = process.env.CORALBRICKS_PROXY_VISION_PROMPT || VISION_PROMPT_DEFAULT;
const visionCache = new Map();
function visionCacheGet(k) {
  const e = visionCache.get(k);
  if (!e) return null;
  if (Date.now() - e.time > VISION_CACHE_TTL_MS) { visionCache.delete(k); return null; }
  return e.text;
}
function visionCacheSet(k, text) {
  if (visionCache.size >= VISION_CACHE_MAX) { try { visionCache.delete(visionCache.keys().next().value); } catch {} }
  visionCache.set(k, { text, time: Date.now() });
}
const FIRST_BYTE_MS = 120000; // pre-commit: time to first event
const IDLE_MS = 300000; // max gap between SSE events
const ATTEMPT_TIMEOUT_MS = 1800000; // per attempt, mirrors provider timeout
const KEEPALIVE_MS = 15000; // SSE comment keepalive while headers sent but no data committed
const EARLY_HEADER_MS = 20000; // send SSE headers+keepalives if no data committed within this window (prevents client header timeout)
// Buffer the whole SSE response and flush only when it completes. Transparent
// retries, no partial/cut output. Set CORALBRICKS_PROXY_BUFFER=0 for streaming.
const BUFFER_UNTIL_COMPLETE = !/^(0|false|off)$/i.test(process.env.CORALBRICKS_PROXY_BUFFER || "1");
const LOG_MAX_BYTES = 2 * 1024 * 1024;

// A "stop" that ends mid-sentence is treated as a cut, not a completed turn.
// Conservative: only prose-like endings (letters/digits/comma/semicolon/colon/
// hyphen/backtick), and only after enough text to avoid retrying short answers.
function looksIncomplete(text) {
  const t = String(text).trimEnd();
  if (t.length < 80) return false;
  if (((t.match(/```/g) || []).length % 2) === 1) return true; // unclosed code fence
  const last = t[t.length - 1];
  if (/[.!?…"'”’)\]}»]/.test(last)) return false;
  if (/[(\[{]/.test(last)) return true;
  if (last === ":") return true;
  // Include Unicode dashes (em/en/horizontal bar/minus): a model that stops
  // right after "—" is mid-thought, not done.
  return /[A-Za-z0-9,;:_`\-\u2010-\u2015\u2212]/.test(last);
}

// A large code/file dump that stops at a closing brace can still be a partial
// file. Treat it as a cut (one retry; no-progress guard stops repeats).
function looksLikePartialCode(text) {
  const t = String(text);
  if (t.length < 1500) return false;
  const hasFile = /(^|\n)[A-Za-z]:\\[^\n]+\.(?:ts|tsx|js|jsx|py|cs|cpp|h|json|md)\b/.test(t)
    || /(^|\n)\/[^\n]+\.(?:ts|tsx|js|jsx|py|cs|cpp|h|json|md)\b/.test(t);
  if (hasFile || t.includes("< | DSML |") || t.includes("</ | DSML |")) return true;
  // Fallback: any large code-like output (braces/semicolons) that stopped.
  // The no-progress guard limits this to one extra retry.
  return /[{};]/.test(t);
}

// The model sometimes writes a tool call as text ("edit C:\...", "write ...",
// DSML blocks) instead of a structured tool_calls delta. Retry so it can emit
// a real tool call; the continuation handoff then forwards it.
function looksLikeTextToolCall(text) {
  const t = String(text);
  return /(^|\n)\s*(edit|write|read|shell|bash|apply_patch|patch)\s+[A-Za-z]:\\/i.test(t)
    || /(^|\n)\s*(edit|write|read|shell|bash|apply_patch|patch)\s+\/[^\n]+/i.test(t)
    || t.includes("< | DSML |")
    || t.includes("</ | DSML |");
}

// A bare path followed by file content (no edit/write keyword, no DSML tags)
// is a malformed tool call, not an answer. The narration before it is not
// reliable ("Let me fix that."), so the shape decides: a line-start path whose
// very next text is code (not a fenced example) is a file dump. Buffer mode
// retries it. A fenced code block after a path is a legitimate answer.
function looksLikeBarePathEdit(text) {
  const t = String(text);
  if (t.length < 200) return false;
  const m = t.match(/(?:^|\n)\s*([A-Za-z]:\\[^\n]*?\.(?:ts|tsx|js|jsx|py|cs|cpp|h|json|md|txt|yml|yaml|toml))\b/);
  if (!m) return false;
  const before = t.slice(0, m.index);
  // Inside a fenced block the path is part of an example, not a dump.
  if (((before.match(/```/g) || []).length % 2) === 1) return false;
  const after = t.slice(m.index + m[0].length);
  if (after.length < 80) return false;
  const fence = after.indexOf("```");
  const head = fence >= 0 ? after.slice(0, fence) : after.slice(0, 240);
  return /(\bconst\b|\blet\b|\bvar\b|\bfunction\b|\bclass\b|\bimport\b|\bexport\b|\bawait\b|\breturn\b|\bif\s*\(|\bfor\s*\(|=>|[;{}])/.test(head);
}

// Conservative DSML read parser: coralbricks/deepseek sometimes streams
//   < | DSML | parameter name="limit" string="false">100
//   < | DSML | parameter name="offset" string="false">140
//   < | DSML | parameter name="path" string="true">C:\...\file.ts
// as text. Convert it to a real, read-only `read` tool call.
function parseDsmlRead(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return null;
  const param = (name) => {
    const re = new RegExp(`<\\s*\\|\\s*DSML\\s*\\|\\s*parameter\\s+name="${name}"\\s+string="(?:true|false)">([\\s\\S]*?)(?=<\\s*\\/?\\s*\\|\\s*DSML\\s*\\||$)`);
    const m = t.match(re);
    return m ? m[1].trim() : null;
  };
  let filePath = param("path");
  if (!filePath) {
    // Some DSML read blocks put the bare path first:
    //   C:\...\file.ts</ | DSML | parameter > < | DSML | parameter name="offset" ...
    const bare = t.match(/(?:^|\n)\s*([A-Za-z]:\\[^\n<]+?|\/[^\n<]+?)\s*<\s*\/\s*\|\s*DSML\s*\|\s*parameter\s*>/);
    if (bare) filePath = bare[1].trim();
  }
  if (!filePath && !param("oldString") && !param("newString")) {
    // Malformed edit/read: bare path followed by file content instead of
    // parameters. Treat it as a read of that path (never a write).
    const first = t.match(/^\s*([A-Za-z]:\\[^\n]*?\.(?:ts|tsx|js|jsx|py|cs|cpp|h|json|md))\b/);
    if (first) filePath = first[1];
  }
  if (!filePath) return null;
  const args = { filePath };
  const offsetStr = param("offset");
  const offset = offsetStr === null ? NaN : Number(offsetStr);
  if (Number.isFinite(offset)) args.offset = offset;
  const limitStr = param("limit");
  const limit = limitStr === null ? NaN : Number(limitStr);
  if (Number.isFinite(limit)) args.limit = limit;
  return { name: "read", arguments: args };
}

// A bare image path at the end of a "let me look/check/upload..." message is
// a malformed read. Convert it to a read-only tool call.
function parseBareImageRead(text, prior) {
  const t = String(text).trim();
  const m = t.match(/([A-Za-z]:\\[^\n]*?\.(?:jpg|jpeg|png|gif|webp))\s*$/i);
  if (!m) return null;
  const before = `${String(prior || "")}\n${t.slice(0, m.index)}`.toLowerCase();
  if (!/(look|view|read|check|see|inspect|upload|avatar|image)/.test(before)) return null;
  return { name: "read", arguments: { filePath: m[1] } };
}

// A broken edit is a broken tool call. Buffer mode retries the whole request
// instead of converting it to a read/write. Broken means: only one of
// oldString/newString, or a newString whose tag never closes (truncated turn).
function isIncompleteDsmlEdit(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return false;
  const hasOld = /parameter\s+name="oldString"/.test(t);
  const hasNew = /parameter\s+name="newString"/.test(t);
  if (hasOld !== hasNew) return true;
  if (!hasOld) return false;
  const newIdx = t.search(/parameter\s+name="newString"/);
  const closeRe = /<\/\s*\|\s*DSML\s*\|\s*parameter\s*>/g;
  closeRe.lastIndex = Math.max(0, newIdx);
  return !closeRe.test(t);
}

// Conservative DSML shell parser: `command` + optional `timeout`.
function parseDsmlShell(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return null;
  const param = (name) => {
    const re = new RegExp(`<\\s*\\|\\s*DSML\\s*\\|\\s*parameter\\s+name="${name}"\\s+string="(?:true|false)">([\\s\\S]*?)(?=<\\s*\\/?\\s*\\|\\s*DSML\\s*\\||$)`);
    const m = t.match(re);
    return m ? m[1].trim() : null;
  };
  const command = param("command");
  if (!command) return null;
  const args = { command };
  const timeoutStr = param("timeout");
  const timeout = timeoutStr === null ? NaN : Number(timeoutStr);
  if (Number.isFinite(timeout)) args.timeout = timeout;
  return { name: "shell", arguments: args };
}

// Conservative DSML write parser: `content` + `path`. Requires a closing
// DSML tag so a truncated block is never written to disk.
function parseDsmlWrite(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return null;
  const param = (name) => {
    const re = new RegExp(`<\\s*\\|\\s*DSML\\s*\\|\\s*parameter\\s+name="${name}"\\s+string="(?:true|false)">([\\s\\S]*?)(?=<\\s*\\/?\\s*\\|\\s*DSML\\s*\\||$)`);
    const m = t.match(re);
    return m ? m[1].trim() : null;
  };
  const content = param("content");
  if (!content) return null;
  const filePath = param("path");
  if (!filePath) return null;
  const closed = t.includes("</ | DSML | parameter>") || t.includes("</ | DSML | parameter >") || t.includes("</ | DSML | invoke>");
  if (!closed) return null;
  return { name: "write", arguments: { filePath, content } };
}

// Conservative DSML grep parser: a `pattern` parameter means grep, not read.
function parseDsmlGrep(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return null;
  const param = (name) => {
    const re = new RegExp(`<\\s*\\|\\s*DSML\\s*\\|\\s*parameter\\s+name="${name}"\\s+string="(?:true|false)">([\\s\\S]*?)(?=<\\s*\\/?\\s*\\|\\s*DSML\\s*\\||$)`);
    const m = t.match(re);
    return m ? m[1].trim() : null;
  };
  const pattern = param("pattern");
  if (!pattern) return null;
  const args = { pattern };
  const path = param("path");
  if (path) args.path = path;
  const include = param("include");
  if (include) args.include = include;
  return { name: "grep", arguments: args };
}

// Conservative DSML edit parser. Requires a path plus BOTH oldString and
// newString, so a truncated block is never turned into a file write.
function parseDsmlEdit(text) {
  const t = String(text);
  if (!t.includes("< | DSML |") && !t.includes("</ | DSML |")) return null;
  const param = (name) => {
    const re = new RegExp(`<\\s*\\|\\s*DSML\\s*\\|\\s*parameter\\s+name="${name}"\\s+string="(?:true|false)">([\\s\\S]*?)(?=<\\s*\\/?\\s*\\|\\s*DSML\\s*\\||$)`);
    const m = t.match(re);
    return m ? m[1].trim() : null;
  };
  let filePath = param("path");
  if (!filePath) {
    const bare = t.match(/(?:^|\n)\s*edit\s+([A-Za-z]:\\[^\n<]+?|\/[^\n<]+?)\s*<\s*\/\s*\|\s*DSML\s*\|\s*parameter\s*>/i)
      || t.match(/(?:^|\n)\s*edit\s+([A-Za-z]:\\[^\n<]+?|\/[^\n<]+?)\s*(?=<\s*\|\s*DSML\s*\|)/i);
    if (bare) filePath = bare[1].trim();
  }
  if (!filePath) return null;
  const oldString = param("oldString");
  const newString = param("newString");
  if (!oldString || !newString) return null;
  // The newString must itself be closed: a truncated block would otherwise
  // become an edit that writes half a file.
  const newIdx = t.search(/parameter\s+name="newString"/);
  const closeRe = /<\/\s*\|\s*DSML\s*\|\s*parameter\s*>/g;
  closeRe.lastIndex = Math.max(0, newIdx);
  if (!closeRe.test(t)) return null;
  return { name: "edit", arguments: { filePath, oldString, newString } };
}

// True when the text contains DSML markers (raw tool-call syntax).
function hasDsmlMarkers(text) {
  const t = String(text);
  return t.includes("< | DSML |") || t.includes("</ | DSML |");
}

// Try every DSML parser. Buffer mode uses this to decide whether a turn is
// safe to forward: raw DSML that no parser can convert is a broken turn.
function parseAnyDsml(text) {
  return parseDsmlShell(text) || parseDsmlWrite(text) || parseDsmlGrep(text) || parseDsmlEdit(text) || parseDsmlRead(text);
}

const PLUGIN_DIR = path.dirname(fileURLToPath(import.meta.url));
const LOG_FILE = process.env.CORALBRICKS_PROXY_LOG_FILE || path.join(PLUGIN_DIR, ".coralbricks-proxy.log");
let logWrites = 0;

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  try { console.log(`${TAG} ${msg}`); } catch {}
  try {
    if ((logWrites++ & 63) === 0) {
      try { if (fs.statSync(LOG_FILE).size > LOG_MAX_BYTES) fs.truncateSync(LOG_FILE, 0); } catch {}
    }
    fs.appendFileSync(LOG_FILE, line + "\n");
  } catch {}
}

const DEBUG = /^(1|true|on)$/i.test(process.env.CORALBRICKS_PROXY_DEBUG || "");
function dbg(msg) { if (DEBUG) log(`dbg ${msg}`); }

// ===========================================================================
// ---- key pool --------------------------------------------------------------
// coralbricks meters each key separately, so N keys = N independent sets of
// limits. Keys are registered in keys.json next to this file (env
// CORALBRICKS_PROXY_KEYS adds more) and every request is charged to exactly one
// of them: a session is glued to a key on first sight and stays there, so a
// turn never flips credentials mid-flight. New sessions go to the key with the
// fewest live sessions. There is no round-robin.
// Management: GET /keys, POST /keys ({"keys":[...]}, {"add":"cb_..."},
// {"remove":<slot>}, {"set":<slot>,"key":"..."}, {"pin":{"session":"ses_…","key":<slot>}},
// {"unpin":"ses_…"}), GET /keys/reveal?slot=N (loopback only).
// ===========================================================================
const KEYS_FILE = process.env.CORALBRICKS_PROXY_KEYS_FILE || path.join(PLUGIN_DIR, "keys.json");
// Older builds kept the file hidden; it is moved to keys.json on first start.
const LEGACY_KEYS_FILE = path.join(PLUGIN_DIR, ".coralbricks-keys.json");
const MAX_KEYS = 8;
const CORS_HEADERS = { "cache-control": "no-store", "access-control-allow-origin": "*" };
let keyPool = [];
// sessionId -> 1-based key slot. Persisted, so a restart does not reshuffle.
const sessionKeyPins = new Map();
// A session counts toward its key's load only while it is live (seen in the
// last half hour); pins themselves are forgotten after two weeks.
const SESSION_LIVE_MS = 30 * 60 * 1000;
const SESSION_PIN_TTL_MS = 14 * 24 * 60 * 60 * 1000;
// sessionId -> { last, active, requests, model }
const sessionActivity = new Map();

function touchSession(sid, model) {
  const a = sessionActivity.get(sid) || { last: 0, active: 0, requests: 0, model: null };
  a.last = Date.now();
  a.requests++;
  if (model) a.model = model;
  sessionActivity.set(sid, a);
  saveKeysSoon();
  return a;
}

let keysSaveTimer = null;
function saveKeysSoon() {
  if (keysSaveTimer) return;
  keysSaveTimer = setTimeout(() => { keysSaveTimer = null; saveKeys(); }, 30000);
  try { keysSaveTimer.unref?.(); } catch {}
}

// After key N is removed, later keys shift down one slot. Pins follow their key
// rather than their number, so a session never silently lands on a different
// credential; sessions on the removed key are reassigned on their next request.
function remapPinsAfterRemoval(removed) {
  for (const [sid, slot] of [...sessionKeyPins]) {
    if (slot === removed) sessionKeyPins.delete(sid);
    else if (slot > removed) sessionKeyPins.set(sid, slot - 1);
  }
}

function loadKeys() {
  let fromFile = [];
  let migrate = false;
  let source = KEYS_FILE;
  if (!fs.existsSync(KEYS_FILE) && fs.existsSync(LEGACY_KEYS_FILE)) {
    source = LEGACY_KEYS_FILE;
    migrate = true;
  }
  try {
    const j = JSON.parse(fs.readFileSync(source, "utf8"));
    if (Array.isArray(j.keys)) fromFile = j.keys;
    if (j && typeof j.pins === "object" && j.pins) {
      for (const [sid, slot] of Object.entries(j.pins)) {
        if (typeof sid === "string" && Number.isInteger(slot)) sessionKeyPins.set(sid, slot);
      }
    }
    if (j && typeof j.seen === "object" && j.seen) {
      for (const [sid, ts] of Object.entries(j.seen)) {
        if (Number.isFinite(ts)) sessionActivity.set(sid, { last: ts, active: 0, requests: 0, model: null });
      }
    }
  } catch {}
  {
    const now = Date.now();
    for (const sid of [...sessionKeyPins.keys()]) {
      const a = sessionActivity.get(sid);
      if (!a) {
        // pinned by an older file without a timestamp: keep it, but not as live
        sessionActivity.set(sid, { last: now - SESSION_LIVE_MS - 1, active: 0, requests: 0, model: null });
      } else if (now - a.last > SESSION_PIN_TTL_MS) {
        sessionKeyPins.delete(sid);
        sessionActivity.delete(sid);
      }
    }
  }
  const fromEnv = String(process.env.CORALBRICKS_PROXY_KEYS || "").split(",");
  keyPool = [...new Set([...fromEnv, ...fromFile].map((k) => String(k || "").trim()).filter(Boolean))].slice(0, MAX_KEYS);

  if (migrate) {
    saveKeys();
    if (fs.existsSync(KEYS_FILE)) {
      try { fs.unlinkSync(LEGACY_KEYS_FILE); } catch {}
      log(`moved ${path.basename(LEGACY_KEYS_FILE)} -> ${path.basename(KEYS_FILE)} (${keyPool.length} key(s) kept)`);
    }
  }
  if (keyPool.length) log(`key pool: ${keyPool.length} key(s) loaded from ${path.basename(source)}`);
}

function saveKeys() {
  try {
    const pins = {};
    const seen = {};
    for (const [sid, slot] of sessionKeyPins) {
      pins[sid] = slot;
      const a = sessionActivity.get(sid);
      if (a && a.last) seen[sid] = a.last;
    }
    fs.writeFileSync(KEYS_FILE, JSON.stringify({ keys: keyPool, pins, seen }, null, 2) + "\n", "utf8");
    try { fs.chmodSync(KEYS_FILE, 0o600); } catch {}
  } catch (e) {
    log(`could not save keys: ${e?.message || e}`);
  }
}

function maskKey(k) {
  const s = String(k || "");
  return s.length <= 4 ? "…" : "…" + s.slice(-4);
}

// Which key slot a session uses: its pin, or the least-pinned live slot, which
// is then remembered for good.
function keySlotForSession(sessionId) {
  if (keyPool.length === 0) return 0;
  if (!sessionId) return 1;
  const pinned = sessionKeyPins.get(sessionId);
  if (Number.isInteger(pinned) && pinned >= 1 && pinned <= keyPool.length) return pinned;
  const counts = liveSessionCounts();
  const slot = counts.indexOf(Math.min(...counts)) + 1;
  sessionKeyPins.set(sessionId, slot);
  saveKeys();
  log(`session ${sessionId} pinned to key ${slot} (${maskKey(keyPool[slot - 1])})`);
  return slot;
}

function liveSessionCounts() {
  const counts = new Array(keyPool.length).fill(0);
  const now = Date.now();
  for (const [sid, slot] of sessionKeyPins) {
    const a = sessionActivity.get(sid);
    if (!a || now - a.last > SESSION_LIVE_MS) continue;
    if (Number.isInteger(slot) && slot >= 1 && slot <= keyPool.length) counts[slot - 1]++;
  }
  return counts;
}

function keysReport() {
  const now = Date.now();
  const pins = {};
  for (const [sid, slot] of sessionKeyPins) pins[sid] = slot;
  const live = liveSessionCounts();
  const sessions = [];
  for (const [id, a] of sessionActivity) {
    if (now - a.last > SESSION_LIVE_MS) continue;
    sessions.push({
      id,
      key: sessionKeyPins.get(id) || 0,
      active: a.active,
      requests: a.requests,
      model: a.model,
      idleMs: Math.max(0, now - a.last),
    });
  }
  sessions.sort((x, y) => (y.active > 0) - (x.active > 0) || x.idleMs - y.idleMs);
  return {
    keys: keyPool.map((k, i) => ({ index: i + 1, hint: maskKey(k), sessions: live[i] || 0 })),
    pins,
    sessions,
    max: MAX_KEYS,
    file: KEYS_FILE,
  };
}

loadKeys();

// ===========================================================================
// ---- usage accounting ------------------------------------------------------
// Every completed request appends one compact record to a JSONL file and to an
// in-memory ring, so GET /usage can report 24u / 24h / 7d / 30d / all-time
// windows, per model and per key. Records are pruned at 30 days; lifetime
// totals live in the JSON snapshot.
//
// Tokens are the upstream's own counts (the proxy asks for
// stream_options.include_usage on streaming chat requests). If a model answers
// without usage, the counts are estimated from bytes and flagged `est`.
// ===========================================================================
const USAGE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const USAGE_FILE = process.env.CORALBRICKS_PROXY_USAGE_FILE || path.join(PLUGIN_DIR, ".coralbricks-usage.json");
const USAGE_LOG = process.env.CORALBRICKS_PROXY_USAGE_LOG
  || USAGE_FILE.replace(/\.json$/i, ".jsonl");
const USAGE_LOG_MAX_BYTES = 5 * 1024 * 1024;
const usage = { since: Date.now(), records: [], lifetime: {}, loaded: false };

// USD per 1M tokens. Mirrors what the same model costs on opencode-go
// (`deepseek-v4.1-flash`, DeepSeek V4.1 Flash 1017): $0.15 in / $0.60 out /
// $0.003 cache read, from the models.dev table. coralbricks' own /v1/models
// numbers are much lower and its per-request `cost` is 0, so this table is what
// the usage report prices with.
// Override with CORALBRICKS_PROXY_PRICING="model=in/out/cached,...".
const PRICING_DEFAULT = {
  "deepseek-v4.1-flash-value": { in: 0.15, out: 0.60, cached: 0.003 },
  "deepseek-v4.1-flash-standard": { in: 0.15, out: 0.60, cached: 0.003 },
  "deepseek-v4.1-flash": { in: 0.15, out: 0.60, cached: 0.003 },
  "deepseek-v4.1-flash-1017": { in: 0.15, out: 0.60, cached: 0.003 },
  "coralbricks/deepseek-v4.1-flash-value": { in: 0.15, out: 0.60, cached: 0.003 },
};
function parsePricing(src) {
  const out = {};
  for (const entry of String(src).split(",")) {
    const e = entry.trim();
    if (!e) continue;
    const eq = e.indexOf("=");
    if (eq < 0) continue;
    const model = e.slice(0, eq).trim();
    const parts = e.slice(eq + 1).split("/").map((n) => Number(n));
    if (!model || parts.length < 2 || parts.some((n) => !Number.isFinite(n))) continue;
    out[model] = { in: parts[0], out: parts[1], cached: parts[2] ?? parts[0] };
  }
  return out;
}
const PRICING = { ...PRICING_DEFAULT, ...parsePricing(process.env.CORALBRICKS_PROXY_PRICING || "") };

// Cost of one record. Cached prompt tokens are billed at the cached-read rate
// instead of the full input rate. An upstream-reported positive cost wins.
function recordCost(r) {
  if (typeof r.usd === "number" && r.usd > 0) return r.usd;
  const p = PRICING[r.m];
  if (!p) return 0;
  const cached = Math.min(r.c || 0, r.i || 0);
  const fresh = Math.max(0, (r.i || 0) - cached);
  return (fresh * p.in + cached * p.cached + (r.o || 0) * p.out) / 1e6;
}

function loadUsage() {
  if (usage.loaded) return;
  usage.loaded = true;
  try {
    const snap = JSON.parse(fs.readFileSync(USAGE_FILE, "utf8"));
    if (snap?.since) usage.since = snap.since;
    if (snap?.lifetime) usage.lifetime = snap.lifetime;
  } catch {}
  try {
    const cutoff = Date.now() - USAGE_RETENTION_MS;
    const lines = fs.readFileSync(USAGE_LOG, "utf8").split("\n");
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const r = JSON.parse(line);
        if (r && r.t >= cutoff) usage.records.push(r);
      } catch {}
    }
  } catch {}
  if (usage.records.length) log(`usage: ${usage.records.length} record(s) loaded from ${path.basename(USAGE_LOG)}`);
}

let usageSaveTimer = null;
function saveUsageSoon() {
  if (usageSaveTimer) return;
  usageSaveTimer = setTimeout(() => {
    usageSaveTimer = null;
    try {
      fs.writeFileSync(USAGE_FILE, JSON.stringify({ since: usage.since, lifetime: usage.lifetime }));
    } catch {}
  }, 2000);
  try { usageSaveTimer.unref?.(); } catch {}
}

function appendUsageRecord(rec) {
  usage.records.push(rec);
  try {
    try {
      if (fs.statSync(USAGE_LOG).size > USAGE_LOG_MAX_BYTES) fs.truncateSync(USAGE_LOG, 0);
    } catch {}
    fs.appendFileSync(USAGE_LOG, JSON.stringify(rec) + "\n");
  } catch {}
  const lt = (usage.lifetime[rec.m] = usage.lifetime[rec.m] || { requests: 0, input: 0, output: 0, reasoning: 0, cached: 0, genMs: 0 });
  lt.requests++;
  lt.input += rec.i || 0;
  lt.output += rec.o || 0;
  lt.reasoning += rec.r || 0;
  lt.cached += rec.c || 0;
  lt.genMs += rec.gm || 0;
  saveUsageSoon();
}

// Usage counters found inside one SSE `data:` payload (or a bulk JSON reply).
// Attached to the live request while it streams.
function noteUsageEvent(acc, data) {
  if (!acc || typeof data !== "string" || !data.startsWith("{")) return;
  let j;
  try { j = JSON.parse(data); } catch { return; }
  const u = j?.usage;
  if (u && typeof u === "object") {
    if (Number.isFinite(u.prompt_tokens)) acc.prompt = u.prompt_tokens;
    if (Number.isFinite(u.completion_tokens)) acc.completion = u.completion_tokens;
    if (Number.isFinite(u.total_tokens)) acc.total = u.total_tokens;
    const rt = u.completion_tokens_details?.reasoning_tokens;
    if (Number.isFinite(rt)) acc.reasoning = rt;
    const ct = u.prompt_tokens_details?.cached_tokens;
    if (Number.isFinite(ct)) acc.cached = ct;
    const cw = u.cost_details?.total;
    if (Number.isFinite(cw) && cw > 0) acc.usd = cw;
    else if (Number.isFinite(u.cost) && u.cost > 0) acc.usd = u.cost;
    acc.sawUsage = true;
  }
  const d = j?.choices?.[0]?.delta;
  if (d && typeof d === "object") {
    const txt = typeof d.content === "string" ? d.content : "";
    const rs = typeof d.reasoning === "string" ? d.reasoning
      : typeof d.reasoning_content === "string" ? d.reasoning_content : "";
    if (txt || rs) {
      const now = Date.now();
      if (!acc.firstTokenAt) acc.firstTokenAt = now;
      acc.lastTokenAt = now;
      if (txt) { acc.tokenFrames++; acc.chars += txt.length; }
      if (rs) acc.sawReasoning = true;
    }
  }
}

function newUsageAcc() {
  return { prompt: 0, completion: 0, total: 0, reasoning: 0, cached: 0, usd: null,
    firstTokenAt: 0, lastTokenAt: 0, tokenFrames: 0, sawReasoning: false, sawUsage: false, chars: 0 };
}

// Token counts found in a non-streaming JSON body.
function parseUsageFromText(text) {
  if (!text) return null;
  const num = (key) => {
    const re = new RegExp(`"${key}"\\s*:\\s*(\\d+)`, "g");
    let last = null, m;
    while ((m = re.exec(text))) last = Number(m[1]);
    return last;
  };
  const prompt = num("prompt_tokens");
  const completion = num("completion_tokens");
  if (prompt == null && completion == null) return null;
  return { prompt: prompt || 0, completion: completion || 0, reasoning: num("reasoning_tokens") || 0, cached: num("cached_tokens") || 0 };
}

function pruneUsage() {
  const cutoff = Date.now() - USAGE_RETENTION_MS;
  if (usage.records.length === 0) return;
  let i = 0;
  while (i < usage.records.length && usage.records[i].t < cutoff) i++;
  if (i > 0) usage.records.splice(0, i);
}

// Requests shorter than this stay out of tok/s: a handful of tokens over a few
// milliseconds is network framing, not decode speed.
const MIN_TPS_TOKENS = 16;
const MIN_TPS_WINDOW_MS = 250;

// slot: only records served on that key (1-based); undefined = all keys.
function windowStats(model, since, slot) {
  const acc = { requests: 0, input: 0, output: 0, reasoning: 0, cached: 0, estimated: 0, cost: 0 };
  let realMs = 0, realReq = 0;
  let realIn = 0, realCache = 0;
  let tpsTokens = 0, tpsMs = 0, tpsN = 0;
  let ttftSum = 0, ttftN = 0;
  const rates = [];
  let lastRate = 0;
  for (const r of usage.records) {
    if (r.t < since) continue;
    if (model && r.m !== model) continue;
    if (slot !== undefined && (r.k || 0) !== slot) continue;
    acc.requests += r.n || 1;
    acc.input += r.i || 0;
    acc.output += r.o || 0;
    acc.reasoning += r.r || 0;
    acc.cached += r.c || 0;
    acc.cost += recordCost(r);
    if (r.est) acc.estimated++;
    realReq++;
    realMs += r.ms || 0;
    realIn += r.i || 0;
    realCache += r.c || 0;
    if (r.ttft > 0) { ttftSum += r.ttft; ttftN++; }
    // Decode speed: tokens produced after the first one, over the span between
    // the first and last text frame. Non-streamed reasoning happened before the
    // first frame, so it is taken out of the count.
    if (r.gm >= MIN_TPS_WINDOW_MS) {
      const hidden = r.r > 0 && !r.sr ? r.r : 0;
      const n = (r.o || 0) - hidden - 1;
      if (n >= MIN_TPS_TOKENS) {
        tpsTokens += n;
        tpsMs += r.gm;
        tpsN++;
        lastRate = n / (r.gm / 1000);
        rates.push(lastRate);
      }
    }
  }
  acc.cost = +acc.cost.toFixed(6);
  acc.avgTps = tpsMs > 0 ? +(tpsTokens / (tpsMs / 1000)).toFixed(1) : 0;
  acc.meanTps = rates.length ? +(rates.reduce((s, v) => s + v, 0) / rates.length).toFixed(1) : 0;
  acc.lastTps = +lastRate.toFixed(1);
  rates.sort((a, b) => a - b);
  acc.medTps = rates.length ? +rates[Math.floor(rates.length / 2)].toFixed(1) : 0;
  acc.tpsN = tpsN;
  acc.ttftMs = ttftN > 0 ? Math.round(ttftSum / ttftN) : 0;
  acc.ttftN = ttftN;
  acc.avgMs = realReq > 0 ? Math.round(realMs / realReq) : 0;
  acc.cachedReal = realCache;
  acc.inputReal = realIn;
  acc.cachedPct = realIn > 0 ? +((realCache / realIn) * 100).toFixed(1) : 0;
  return acc;
}

function usageReport() {
  pruneUsage();
  const now = Date.now();
  const d = new Date(now);
  const windows = {
    // 24u is UTC midnight to now; 24h is the rolling last 24 hours.
    "24u": Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()),
    "24h": now - 24 * 60 * 60 * 1000,
    "7d": now - 7 * 24 * 60 * 60 * 1000,
    "30d": now - 30 * 24 * 60 * 60 * 1000,
    all: 0,
  };
  const out = { proxy: "coralbricks-proxy", since: usage.since, updated: now, keys: keyPool.length, pricing: PRICING, windows: {}, models: {} };
  for (const [key, since] of Object.entries(windows)) {
    out.windows[key] = windowStats(null, since);
    out.models[key] = {};
    const names = new Set(usage.records.filter((r) => r.t >= since).map((r) => r.m));
    for (const m of names) out.models[key][m] = windowStats(m, since);
  }
  // Per key, so each credential's own spend and traffic can be read. The
  // panel shows the combined `windows`; this is for looking at one key.
  out.perKey = {};
  for (const [key, since] of Object.entries(windows)) {
    const slots = new Set(usage.records.filter((r) => r.t >= since).map((r) => r.k || 0));
    out.perKey[key] = {};
    for (const k of slots) {
      const models = {};
      const names = new Set(usage.records.filter((r) => r.t >= since && (r.k || 0) === k).map((r) => r.m));
      for (const m of names) models[m] = windowStats(m, since, k);
      out.perKey[key][k] = { total: windowStats(null, since, k), models };
    }
  }
  out.lifetime = usage.lifetime;
  return out;
}

// ---- graph series ----------------------------------------------------------
// The same records as /usage, grouped into time buckets and by key, so the
// desktop addon can draw a stacked bar chart (one colour per credential).
// `window` accepts 24u / 24h / 7d / 30d / all; `bucket` overrides the bucket
// size in ms; `slot` narrows to one key.
function seriesWindow(key) {
  const now = Date.now();
  const d = new Date(now);
  switch (key) {
    case "24u":
      return { since: Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()), bucketMs: 3600000 };
    case "24h":
      return { since: now - 24 * 3600000, bucketMs: 3600000 };
    case "7d":
      return { since: now - 7 * 24 * 3600000, bucketMs: 6 * 3600000 };
    case "30d":
      return { since: now - 30 * 24 * 3600000, bucketMs: 24 * 3600000 };
    default: {
      const first = usage.records.length ? usage.records[0].t : now;
      const span = Math.max(24 * 3600000, now - first);
      // Never more than 120 bars, however long the history runs.
      const bucketMs = span > 120 * 24 * 3600000
        ? Math.ceil(span / 120 / 86400000) * 86400000
        : 24 * 3600000;
      return { since: Math.min(first, now), bucketMs };
    }
  }
}

function seriesReport(windowKey = "24h", bucketOverride = 0, slot) {
  pruneUsage();
  const now = Date.now();
  const w = seriesWindow(windowKey);
  const bucketMs = Number(bucketOverride) > 0 ? Math.round(Number(bucketOverride)) : w.bucketMs;
  const since = w.since;
  const count = Math.max(1, Math.min(240, Math.ceil((now - since) / bucketMs) + 1));
  const start = Math.floor(since / bucketMs) * bucketMs;
  const buckets = Array.from({ length: count }, (_, i) => ({
    t: start + i * bucketMs, requests: 0, input: 0, output: 0, reasoning: 0, cached: 0, cost: 0, byKey: {},
  }));
  for (const r of usage.records) {
    if (r.t < since) continue;
    if (slot !== undefined && (r.k || 0) !== slot) continue;
    const idx = Math.min(count - 1, Math.floor((r.t - start) / bucketMs));
    if (idx < 0) continue;
    const b = buckets[idx];
    const k = String(r.k || 0);
    const kb = (b.byKey[k] = b.byKey[k] || { requests: 0, input: 0, output: 0, cost: 0 });
    const n = r.n || 1;
    const cost = recordCost(r);
    b.requests += n;
    b.input += r.i || 0;
    b.output += r.o || 0;
    b.reasoning += r.r || 0;
    b.cached += r.c || 0;
    b.cost += cost;
    kb.requests += n;
    kb.input += r.i || 0;
    kb.output += r.o || 0;
    kb.cost += cost;
  }
  for (const b of buckets) {
    b.cost = +b.cost.toFixed(6);
    for (const kb of Object.values(b.byKey)) kb.cost = +kb.cost.toFixed(6);
  }
  return {
    proxy: "coralbricks-proxy",
    window: windowKey,
    since,
    updated: now,
    bucketMs,
    keys: keyPool.length,
    buckets,
  };
}

loadUsage();

// ---------- SSE eventizer ----------
// Splits a byte stream into raw SSE events (terminated by a blank line),
// preserving each event's raw text for byte-exact forwarding.
function makeEventizer() {
  let buf = "";
  function findTerminator(s) {
    let best = -1, len = 0;
    for (const [t, l] of [["\r\n\r\n", 4], ["\n\n", 2], ["\r\r", 2]]) {
      const i = s.indexOf(t);
      if (i >= 0 && (best < 0 || i < best)) { best = i; len = l; }
    }
    return best < 0 ? null : { end: best, after: best + len };
  }
  return {
    feed(text) {
      buf += text;
      const out = [];
      let t;
      while ((t = findTerminator(buf))) {
        out.push(buf.slice(0, t.end));
        buf = buf.slice(t.after);
      }
      return out;
    },
    flush() {
      if (buf.trim() === "") { buf = ""; return []; }
      const r = buf; buf = ""; return [r];
    },
  };
}

// Extract the joined `data:` payload of a raw SSE event ("" when none).
function eventData(raw) {
  const lines = raw.split(/\r\n|\r|\n/);
  const datas = [];
  for (const line of lines) {
    if (line.startsWith(":")) continue; // comment
    if (line.startsWith("data:")) {
      let v = line.slice(5);
      if (v.startsWith(" ")) v = v.slice(1);
      datas.push(v);
    } else if (line.trim() === "") {
      // blank lines inside a captured chunk belong to no event (the
      // terminator itself) — ignore
    } else if (/^[A-Za-z-]+\s*:/.test(line)) {
      // id:/event:/retry: fields — ignored for validation, raw event is
      // forwarded as-is so they survive
    } else {
      // non-field garbage line inside an event: the SSE decoder would feed
      // it as data too; treat it as data to keep the payload complete
      datas.push(line);
    }
  }
  return datas.join("\n");
}

// ---------- schema mirror (what opencode's router will accept) ----------
// analyzeEvent(joinedData) -> {kind:"pass"} | {kind:"repair", text} | {kind:"invalid", reason}
// Mirrors the lenient openai-compatible-chat chunk schema: unknown keys are
// allowed; known typed fields must have the right JSON types; error must be
// an object with a string message; [DONE] / null / no-data always pass.
function safeStringify(v) {
  try { const s = JSON.stringify(v); return s === undefined ? String(v) : s; }
  catch { return String(v); }
}

function analyzeEvent(data) {
  if (data === "") return { kind: "pass" }; // comment/field-only event
  if (data === "[DONE]") return { kind: "pass" };
  if (data === "null") return { kind: "pass" }; // opencode's SSE framing skips it
  let v;
  try { v = JSON.parse(data); } catch { return { kind: "invalid", reason: "not-json" }; }
  if (v === null) return { kind: "pass" };
  if (typeof v !== "object") return { kind: "invalid", reason: `not-object(${typeof v})` };
  if (Array.isArray(v)) return { kind: "invalid", reason: "array" };

  let copy = null;
  const ensure = () => { if (!copy) copy = JSON.parse(JSON.stringify(v)); return copy; };

  // error: must be {message: string, ...}
  if (v.error !== undefined && v.error !== null) {
    const e = v.error;
    if (typeof e !== "object" || Array.isArray(e)) {
      const r = ensure();
      r.error = { message: typeof e === "string" ? e : safeStringify(e), type: "upstream_error", code: "proxy_normalized" };
    } else if (typeof e.message !== "string") {
      const r = ensure();
      r.error = { ...e, message: e.message === undefined || e.message === null ? "upstream error" : safeStringify(e.message) };
    }
  }

  const t = copy || v;

  // choices: array of objects; delta typed fields
  if (t.choices !== undefined && t.choices !== null) {
    if (!Array.isArray(t.choices)) return { kind: "invalid", reason: "choices-not-array" };
    for (let ci = 0; ci < t.choices.length; ci++) {
      const c = t.choices[ci];
      if (c === null || typeof c !== "object" || Array.isArray(c)) return { kind: "invalid", reason: "choice-not-object" };
      const d = c.delta;
      if (d !== undefined && d !== null) {
        if (typeof d !== "object" || Array.isArray(d)) return { kind: "invalid", reason: "delta-not-object" };
        for (const k of ["content", "reasoning", "reasoning_text", "reasoning_content", "refusal", "role"]) {
          const val = d[k];
          if (val === null) {
            // semantically-absent; strip so the decode cannot reject it
            const r = ensure();
            const rc = r.choices?.[ci];
            if (rc && rc.delta) delete rc.delta[k];
          } else if (val !== undefined && typeof val !== "string") {
            return { kind: "invalid", reason: `delta.${k}-not-string` };
          }
        }
        if (d.tool_calls !== undefined && d.tool_calls !== null) {
          if (!Array.isArray(d.tool_calls)) return { kind: "invalid", reason: "tool_calls-not-array" };
          for (const tc of d.tool_calls) {
            if (tc === null || typeof tc !== "object" || Array.isArray(tc)) return { kind: "invalid", reason: "tool_call-not-object" };
            if (tc.function !== undefined && tc.function !== null && (typeof tc.function !== "object" || Array.isArray(tc.function))) {
              return { kind: "invalid", reason: "tool_call.function-not-object" };
            }
          }
        }
      }
      if (c.finish_reason !== undefined && c.finish_reason !== null && typeof c.finish_reason !== "string") {
        return { kind: "invalid", reason: "finish_reason-not-string" };
      }
      if (c.native_finish_reason !== undefined && c.native_finish_reason !== null && typeof c.native_finish_reason !== "string") {
        return { kind: "invalid", reason: "native_finish_reason-not-string" };
      }
    }
  }

  // usage: object; token counts numeric
  const u = t.usage;
  if (u !== undefined && u !== null) {
    if (typeof u !== "object" || Array.isArray(u)) return { kind: "invalid", reason: "usage-not-object" };
    for (const k of ["prompt_tokens", "completion_tokens", "total_tokens"]) {
      if (u[k] !== undefined && u[k] !== null && typeof u[k] !== "number") return { kind: "invalid", reason: `usage.${k}-not-number` };
    }
  }

  if (copy) return { kind: "repair", text: JSON.stringify(copy) };
  return { kind: "pass" };
}

// An in-band provider error payload: {"error": ...} — pre-commit this means
// the attempt failed; post-commit it is forwarded (valid schema shape).
function isErrorPayload(data) {
  if (!data.startsWith("{")) return false;
  try {
    const j = JSON.parse(data);
    return j && typeof j === "object" && !Array.isArray(j) && j.error !== undefined && j.error !== null;
  } catch { return false; }
}

// Reshape a non-stream chat.completion (stream:true answered with JSON) into
// a single chunk opencode's chat protocol can decode (tinyproxy v13 lesson).
function bulkToChunkJSON(obj) {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
  if (!Array.isArray(obj.choices)) return null;
  return JSON.stringify({
    id: obj.id,
    object: "chat.completion.chunk",
    created: obj.created,
    model: obj.model,
    choices: obj.choices.map((c, i) => ({
      index: c?.index ?? i,
      delta: c?.message && typeof c.message === "object" ? c.message : {},
      finish_reason: c?.finish_reason ?? "stop",
    })),
    usage: obj.usage,
  });
}

// ---------- vision handoff helpers ----------
function isImagePart(p) {
  if (!p || typeof p !== "object") return false;
  return p.type === "image_url" || p.type === "image" || p.type === "input_image";
}

function imageUrlOf(p) {
  if (p.type === "image_url") {
    if (typeof p.image_url === "string") return p.image_url;
    if (p.image_url && typeof p.image_url.url === "string") return p.image_url.url;
    return null;
  }
  if (p.type === "image") {
    if (p.source && typeof p.source.data === "string") {
      return `data:${p.source.media_type || "image/png"};base64,${p.source.data}`;
    }
    return p.url || null;
  }
  if (p.type === "input_image") return typeof p.image_url === "string" ? p.image_url : null;
  return null;
}

function collectImageParts(body) {
  const out = [];
  const msgs = Array.isArray(body?.messages) ? body.messages : [];
  for (let mi = 0; mi < msgs.length; mi++) {
    const c = msgs[mi]?.content;
    if (!Array.isArray(c)) continue;
    for (let pi = 0; pi < c.length; pi++) {
      if (isImagePart(c[pi])) {
        const url = imageUrlOf(c[pi]);
        if (url) out.push({ mi, pi, url });
      }
    }
  }
  return out;
}

// One non-streaming vision call against the same upstream + credentials.
async function analyzeImage({ base, auth, url }) {
  const body = {
    model: VISION_MODEL,
    stream: false,
    max_tokens: 2000,
    messages: [{
      role: "user",
      content: [
        { type: "text", text: VISION_PROMPT },
        { type: "image_url", image_url: { url } },
      ],
    }],
  };
  const headers = { "content-type": "application/json", ...(auth ? { authorization: auth } : {}) };
  const target = `${base}/chat/completions`;
  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const im = await upstreamRequest(target, { method: "POST", headers, body: Buffer.from(JSON.stringify(body), "utf8") });
      const text = await consumeBody(im);
      const status = im.statusCode;
      try { im.destroy(); } catch {}
      if (status >= 400) {
        lastErr = new Error(`vision status ${status}: ${snippet(text, 200)}`);
      } else {
        const j = JSON.parse(text);
        const content = j?.choices?.[0]?.message?.content;
        if (typeof content === "string" && content.trim()) return content.trim();
        if (Array.isArray(content)) {
          const joined = content.map((p) => (typeof p?.text === "string" ? p.text : "")).join("").trim();
          if (joined) return joined;
        }
        lastErr = new Error(`vision response had no content: ${snippet(text, 160)}`);
      }
    } catch (e) {
      lastErr = e;
    }
    if (attempt < 3) await sleep(400 * attempt);
  }
  throw lastErr || new Error("vision analysis failed");
}

// ---------- helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const RL_BUDGET_MS = Number(process.env.CORALBRICKS_PROXY_RL_BUDGET_MS) || RATE_LIMIT_BUDGET_MS_DEFAULT;
const RL_BACKOFF = (process.env.CORALBRICKS_PROXY_RL_BACKOFF_MS || "").split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => n > 0);
const rateLimitBackoffMs = (streak) => (RL_BACKOFF.length ? RL_BACKOFF[Math.min(streak, RL_BACKOFF.length - 1)] : RATE_LIMIT_BACKOFF_DEFAULT[Math.min(streak, RATE_LIMIT_BACKOFF_DEFAULT.length - 1)]) || 30000;
const HARD_BACKOFF = (process.env.CORALBRICKS_PROXY_BACKOFF_MS || "").split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => n > 0);
const backoffMs = (retryCount) => (HARD_BACKOFF.length ? HARD_BACKOFF[Math.min(retryCount, HARD_BACKOFF.length - 1)] : BACKOFF_MS[Math.min(retryCount, BACKOFF_MS.length - 1)]) || 3000;

// node:http(s) upstream call. Bun's fetch AUTO-RETRIES on mid-body
// connection death and concatenates the retried bytes into the same response
// body (observed: content duplicated 3x downstream). Raw sockets deliver
// exactly-once and give full retry control.
function upstreamRequest(target, opts) {
  return new Promise((resolve, reject) => {
    let url;
    try { url = new URL(target); } catch (e) { reject(e); return; }
    const isHttps = url.protocol === "https:";
    const mod = isHttps ? httpsRequest : httpRequest;
    const r = mod({
      method: opts.method || "GET",
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname + url.search,
      headers: opts.headers || {},
      // Dedicated socket per attempt: SSE responses are abandoned the moment
      // [DONE] arrives (never drained), and a pooled keep-alive socket with
      // unread tail bytes poisons the NEXT request — its parser re-delivers
      // stale body bytes (observed: duplicated SSE events with stray CRLF
      // framing). A fresh socket per request makes that impossible.
      agent: false,
    }, (im) => { clearTimeout(attemptTimer); resolve(im); });
    r.on("error", (e) => { clearTimeout(attemptTimer); reject(e); });
    const attemptTimer = setTimeout(() => {
      try { r.destroy(new Error("attempt-timeout")); } catch {}
    }, ATTEMPT_TIMEOUT_MS);
    try { attemptTimer.unref?.(); } catch {}
    if (opts.body) r.write(opts.body);
    r.end();
  });
}

// One chunk from an IncomingMessage with an idle deadline. Resolves null on
// end, rejects on error/timeout. Listeners are attached EXACTLY ONCE per
// response (attach/detach per read loses 'end'/'error' events that fire in
// the gap between reads — the reader then hangs until the idle timeout,
// which stalled the whole request for minutes). The buffer is drained one
// chunk at a time from a captured queue; data arriving between awaits is
// parked there, never lost.
function makeReader(im) {
  const queue = [];
  let wake = null; // pending resolver for the next awaited chunk
  let done = false;
  let error = null;
  // Memory-leak fix: this interval previously ran for the process lifetime —
  // one per response — because `up.destroy()` after [DONE] emits 'close'
  // (not 'error', not 'end'), so nothing ever cleared it. ~2GB RSS after 8h
  // on one host. It now dies with the socket via the 'close' handler, and
  // 'end'/'error' clear it explicitly.
  let timer = null;
  const killTimer = () => { if (timer) { clearInterval(timer); timer = null; } };
  const onData = (chunk) => {
    if (wake) { const w = wake; wake = null; w.resolve(chunk); }
    else queue.push(chunk);
  };
  const onEnd = () => {
    killTimer();
    done = true;
    if (wake) { const w = wake; wake = null; w.resolve(null); }
  };
  const onErr = (e) => {
    killTimer();
    error = e || new Error("stream error");
    if (wake) { const w = wake; wake = null; w.reject(error); }
  };
  // The success path (up.destroy() after [DONE]) terminates via 'close' only.
  timer = setInterval(() => {
    if (wake && wake.deadline < Date.now()) {
      const w = wake; wake = null;
      try { im.destroy(new Error("idle-timeout")); } catch {}
      w.reject(new Error("idle-timeout"));
    }
  }, 250);
  try { timer.unref?.(); } catch {}
  im.on("close", killTimer); // every socket teardown path, incl. destroy()
  im.on("data", onData);
  im.on("end", onEnd);
  im.on("error", onErr);
  return function readWithIdle(ms) {
    return new Promise((resolve, reject) => {
      if (queue.length > 0) { resolve(queue.shift()); return; }
      if (error) { reject(error); return; }
      if (done) { resolve(null); return; }
      wake = { resolve, reject, deadline: Date.now() + ms };
    });
  };
}

async function consumeBody(im) {
  const read = makeReader(im);
  const parts = [];
  for (;;) {
    const chunk = await read(IDLE_MS);
    if (chunk === null) break;
    parts.push(chunk);
  }
  return Buffer.concat(parts).toString("utf8");
}

function parseRetryAfterMs(v) {
  if (typeof v !== "string" || !v.trim()) return 0;
  const s = v.trim();
  if (/^\d+$/.test(s)) return Math.min(parseInt(s, 10) * 1000, RETRY_AFTER_CAP_MS);
  const d = Date.parse(s);
  if (Number.isNaN(d)) return 0;
  return Math.min(Math.max(0, d - Date.now()), RETRY_AFTER_CAP_MS);
}

function snippet(s, n = 220) {
  const one = String(s).replace(/\s+/g, " ");
  return one.length > n ? one.slice(0, n) + "…" : one;
}

function buildForwardHeaders(inbound, extra) {
  const h = {};
  for (const [k, v] of Object.entries(inbound)) {
    const lk = k.toLowerCase();
    if (["host", "connection", "content-length", "accept-encoding", "expect", "keep-alive",
      "transfer-encoding", "upgrade", "proxy-connection", "x-cb-key", "x-cb-upstream",
      "x-lf-key", "x-lf-upstream"].includes(lk)) continue;
    h[k] = v;
  }
  return Object.assign(h, extra || {});
}

function pickResponseHeaders(upstreamHeaders, retryCount) {
  const h = {};
  for (const [k, v] of Object.entries(upstreamHeaders || {})) {
    const lk = k.toLowerCase();
    if (["content-length", "content-encoding", "transfer-encoding", "connection", "keep-alive",
      "content-type"].includes(lk)) continue;
    h[k] = v;
  }
  h["content-type"] = upstreamHeaders?.["content-type"] || "text/event-stream";
  h["x-cb-retries"] = String(retryCount);
  h["x-accel-buffering"] = "no";
  h["cache-control"] = "no-cache";
  return h;
}

// ---------- the proxy server ----------
async function startServer(upstreamBase, port, mode) {
  async function handle(req, res) {
    const started = Date.now();
    const pathAndQuery = req.url || "/";
    const sendJson = (status, obj, extra = {}) => {
      try { res.writeHead(status, { "content-type": "application/json", ...CORS_HEADERS, ...extra }); } catch {}
      try { res.end(typeof obj === "string" ? obj : JSON.stringify(obj, null, 2) + "\n"); } catch {}
    };
    const url = new URL(pathAndQuery, `http://${LISTEN_HOST}`);
    const pathname = url.pathname;

    // CORS preflight for the panel's POST /keys.
    if (req.method === "OPTIONS") {
      try {
        res.writeHead(204, {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "GET, POST, OPTIONS",
          "access-control-allow-headers": "content-type",
          "access-control-max-age": "600",
        });
        res.end();
      } catch {}
      return;
    }

    if (pathname === "/healthz" || pathname === "/healthz/") {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("coralbricks-proxy OK");
      return;
    }

    // ---- key management ----
    if (pathname === "/keys/reveal") {
      // The only route that returns a full key: one key per request, loopback
      // only, and never to a browser page (http/https Origin).
      const origin = req.headers.origin || "";
      const ra = req.socket.remoteAddress || "";
      if (!["127.0.0.1", "::1", "::ffff:127.0.0.1"].includes(ra) || /^https?:\/\//i.test(origin)) {
        return sendJson(403, { error: "not available to this origin" });
      }
      const slot = Number(url.searchParams.get("slot"));
      if (!Number.isInteger(slot) || slot < 1 || slot > keyPool.length) return sendJson(404, { error: "no such key" });
      return sendJson(200, { slot, key: keyPool[slot - 1] }, { "access-control-allow-origin": origin || "null" });
    }

    if (pathname === "/keys" || pathname === "/keys/") {
      if (req.method === "GET") return sendJson(200, keysReport());
      if (req.method === "POST") {
        const chunks2 = [];
        for await (const c of req) chunks2.push(c);
        let body = {};
        try { body = JSON.parse(Buffer.concat(chunks2).toString("utf8") || "{}"); } catch {}
        if (Array.isArray(body.keys)) {
          keyPool = [...new Set(body.keys.map((k) => String(k).trim()).filter(Boolean))].slice(0, MAX_KEYS);
        } else if (Number.isInteger(body.set) && typeof body.key === "string" && body.key.trim()) {
          const n = body.set;
          if (n < 1 || n > MAX_KEYS) return sendJson(400, { error: `slot must be 1..${MAX_KEYS}` });
          while (keyPool.length < n) keyPool.push("");
          keyPool[n - 1] = body.key.trim();
          keyPool = keyPool.filter(Boolean).slice(0, MAX_KEYS);
        } else if (body.pin && typeof body.pin.session === "string" && Number.isInteger(body.pin.key)) {
          const slot = body.pin.key;
          if (slot < 1 || slot > keyPool.length) return sendJson(400, { error: `no key ${slot}; ${keyPool.length} configured` });
          sessionKeyPins.set(body.pin.session, slot);
          log(`pinned session ${body.pin.session} to key ${slot}`);
        } else if (typeof body.unpin === "string") {
          sessionKeyPins.delete(body.unpin);
          log(`unpinned session ${body.unpin}`);
        } else if (typeof body.add === "string" && body.add.trim()) {
          const k = body.add.trim();
          if (keyPool.includes(k)) return sendJson(409, { error: "key already configured" });
          if (keyPool.length >= MAX_KEYS) return sendJson(409, { error: `at most ${MAX_KEYS} keys` });
          keyPool.push(k);
        } else if (Number.isInteger(body.remove) && body.remove >= 1) {
          if (body.remove <= keyPool.length) {
            keyPool.splice(body.remove - 1, 1);
            remapPinsAfterRemoval(body.remove);
          }
        } else {
          return sendJson(400, { error: 'send {"keys":[...]}, {"add":"<key>"}, {"remove":<slot>} or {"set":<slot>,"key":"<key>"}' });
        }
        saveKeys();
        log(`keys updated: ${keyPool.length} configured (${keyPool.map(maskKey).join(", ") || "none"})`);
        return sendJson(200, keysReport());
      }
    }

    // ---- usage report ----
    if (pathname === "/usage/series" || pathname === "/usage/series/") {
      const win = String(url.searchParams.get("window") || "24h");
      const bucket = Number(url.searchParams.get("bucket")) || 0;
      const slotRaw = url.searchParams.get("slot");
      const slot = slotRaw === null || slotRaw === "" ? undefined : Number(slotRaw);
      return sendJson(200, seriesReport(win, bucket, Number.isFinite(slot) ? slot : undefined));
    }
    if (pathname === "/usage" || pathname === "/usage/") {
      return sendJson(200, usageReport());
    }

    if (pathname === "/status" || pathname === "/status/") {
      let best = null;
      const sessions = {};
      for (const s of proxyState.streams.values()) {
        if (!best || s.retries > best.retries || (s.retries === best.retries && s.updated > best.updated)) best = s;
        if (s.sessionId) {
          const cur = sessions[s.sessionId];
          if (!cur || s.retries > cur.retries || (s.retries === cur.retries && s.updated > cur.updated)) {
            sessions[s.sessionId] = { retries: s.retries, attempts: s.attempts, status: s.status ?? null, model: s.model, updated: s.updated, key: s.keySlot || 0 };
          }
        }
      }
      const live = liveSessionCounts();
      return sendJson(200, {
        proxy: "coralbricks-proxy",
        upstream: upstreamBase,
        active: proxyState.active,
        streams: proxyState.streams.size,
        retries: best ? best.retries : 0,
        attempts: best ? best.attempts : 0,
        model: best ? best.model : null,
        updated: proxyState.updated,
        sessions,
        keys: keyPool.length,
        keySlots: keyPool.map((k, i) => ({ index: i + 1, hint: maskKey(k), sessions: live[i] || 0 })),
        usage: {
          since: usage.since,
          today: windowStats(null, Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate())),
          lifetime: usage.lifetime,
        },
      });
    }
    proxyState.active++;
    proxyState.updated = Date.now();
    res.on("close", () => {
      proxyState.active = Math.max(0, proxyState.active - 1);
      proxyState.updated = Date.now();
    });

    // buffer the request body (retries need a replayable body)
    const chunks = [];
    let reqSize = 0;
    req.on("data", (c) => { chunks.push(c); reqSize += c.length; });
    await new Promise((resolve) => req.on("end", resolve));
    let rawBody = Buffer.concat(chunks);

    // stream wanted? (chat completions with stream:true)
    let wantsStream = false;
    let model = "?";
    let reqJson = null;
    try {
      reqJson = JSON.parse(rawBody.toString("utf8"));
      wantsStream = reqJson?.stream === true;
      model = reqJson?.model || "?";
    } catch {}
    // Ask the upstream for its token counts even when the caller did not: usage
    // accounting is worthless without them, and the field is standard.
    if (wantsStream && reqJson && typeof reqJson === "object" && !Array.isArray(reqJson)) {
      if (!reqJson.stream_options || typeof reqJson.stream_options !== "object") reqJson.stream_options = {};
      if (reqJson.stream_options.include_usage !== true) {
        reqJson.stream_options.include_usage = true;
        rawBody = Buffer.from(JSON.stringify(reqJson), "utf8");
        reqSize = rawBody.length;
      }
    }
    // Only streaming chat requests drive the retry pill. Non-stream image
    // generations must not overwrite a streaming request's retry state.
    let streamId = null;
    // OpenCode sends x-opencode-session-id on every provider request, so the
    // pill can show only its own session's failures and the key pool can glue a
    // session to one key.
    const sessionId = String(
      req.headers["x-opencode-session-id"] || req.headers["x-opencode-session"] || req.headers["x-session-id"] || "",
    ) || null;
    // Which credential this request goes out on. An explicit x-cb-key override
    // wins; otherwise the session's pinned key; also see the auth block below.
    const keySlot = keyPool.length > 0 ? keySlotForSession(sessionId) : 0;
    const sessionAct = sessionId ? touchSession(sessionId, model) : null;
    if (sessionAct) sessionAct.active++;
    res.on("close", () => { if (sessionAct) sessionAct.active = Math.max(0, sessionAct.active - 1); });
    if (wantsStream) {
      streamId = ++proxyState.streamSeq;
      proxyState.streams.set(streamId, { sessionId, keySlot, status: null, attempts: 1, retries: 0, model, updated: Date.now() });
      res.on("close", () => {
        proxyState.streams.delete(streamId);
        proxyState.updated = Date.now();
      });
    }

    // One usage accumulator per request, filled from the forwarded events.
    const usageAcc = newUsageAcc();
    let usageRecorded = false;
    const recordUsageOnce = () => {
      if (usageRecorded) return;
      usageRecorded = true;
      const estimated = !usageAcc.sawUsage;
      const prompt = usageAcc.prompt || (estimated ? Math.ceil(reqSize / 4) : 0);
      const completion = usageAcc.completion || (estimated ? Math.ceil(usageAcc.chars / 4) : 0);
      if (!prompt && !completion) return;
      const ttft = usageAcc.firstTokenAt ? Math.max(0, usageAcc.firstTokenAt - started) : 0;
      const gm = usageAcc.firstTokenAt && usageAcc.lastTokenAt && usageAcc.tokenFrames >= 2
        ? Math.max(0, usageAcc.lastTokenAt - usageAcc.firstTokenAt)
        : 0;
      appendUsageRecord({
        t: Date.now(),
        m: model,
        k: keySlot,
        i: prompt,
        o: completion,
        r: usageAcc.reasoning,
        c: usageAcc.cached,
        gm,
        ttft,
        sr: usageAcc.sawReasoning ? 1 : 0,
        ms: Math.max(0, Math.round(Date.now() - started)),
        ...(estimated ? { est: 1 } : {}),
        ...(typeof usageAcc.usd === "number" ? { usd: usageAcc.usd } : {}),
      });
    };

    // shape diagnostic: log non-text content part types (never their payload)
    if (reqJson) {
      try {
        const detail = [];
        for (const m of (reqJson.messages || [])) {
          if (!Array.isArray(m?.content)) continue;
          for (const p of m.content) {
            if (p && typeof p === "object" && p.type && p.type !== "text") {
              detail.push(`${p.type}{${Object.keys(p).join(",")}}`);
            }
          }
        }
        if (detail.length) log(`non-text parts: ${detail.slice(0, 6).join(" ")}`);
      } catch {}
    }

    let clientGone = false;
    res.on("close", () => { clientGone = true; });
    const rawWrite = res.write.bind(res);

    // Header-timeout coverage: for streaming requests, if no real data is
    // committed within EARLY_HEADER_MS (vision handoff or pre-commit retries
    // taking long), send SSE headers and emit keepalive comments so the client
    // never times out waiting for headers. Only comments flow until a real,
    // validated data event is committed; pre-commit retries can still continue
    // internally as long as no data has been written. Fast responses keep the
    // original behavior (headers/status/retry-count set at commit/give-up).
    let earlyStream = false;
    let dataCommitted = false; // first real (non-comment) event written
    let streamEnded = false;
    let keepaliveTimer = null;
    let earlyHeaderTimer = null;
    const stopKeepalive = () => { if (keepaliveTimer) { clearInterval(keepaliveTimer); keepaliveTimer = null; } };
    const stopEarlyHeaderTimer = () => { if (earlyHeaderTimer) { clearTimeout(earlyHeaderTimer); earlyHeaderTimer = null; } };
    const sendKeepalive = () => {
      if (clientGone || streamEnded || dataCommitted) { stopKeepalive(); return; }
      try { rawWrite(": keepalive\n\n"); } catch {}
    };
    const startEarlyStream = () => {
      if (!wantsStream || earlyStream || res.headersSent || dataCommitted) return;
      try {
        res.writeHead(200, {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "x-accel-buffering": "no",
        });
        earlyStream = true;
        keepaliveTimer = setInterval(sendKeepalive, KEEPALIVE_MS);
        try { keepaliveTimer.unref?.(); } catch {}
        log(`early SSE headers sent (keepalive active) to prevent client header timeout`);
      } catch (e) {
        log(`early stream header failed: ${e?.message || e}`);
      }
    };
    const endStream = () => {
      if (streamEnded) return;
      streamEnded = true;
      stopEarlyHeaderTimer();
      stopKeepalive();
      try { res.end(); } catch {}
    };
    const ensureStreamHeaders = (upHeaders, retryCount) => {
      if (res.headersSent) return;
      stopEarlyHeaderTimer();
      const headers = pickResponseHeaders(upHeaders || {}, retryCount || 0);
      headers["content-type"] = "text/event-stream";
      try { res.writeHead(200, headers); } catch {}
    };

    if (wantsStream) {
      earlyHeaderTimer = setTimeout(() => { earlyHeaderTimer = null; startEarlyStream(); }, EARLY_HEADER_MS);
      try { earlyHeaderTimer.unref?.(); } catch {}
    }

    // resolve upstream + key (header overrides stripped before forward)
    const upOverride = req.headers["x-cb-upstream"] || req.headers["x-lf-upstream"];
    const base = (typeof upOverride === "string" && upOverride.trim()) ? upOverride.trim().replace(/\/+$/, "") : upstreamBase;
    // The client hits http://127.0.0.1:17332/v1/... and the upstream base
    // already carries /v1 — appending both yielded /v1/v1/... (404 from
    // coralbricks). Strip the upstream base's own path prefix from the incoming
    // path exactly once.
    let suffix = pathAndQuery || "/";
    try {
      const basePrefix = new URL(base).pathname.replace(/\/+$/, "");
      if (basePrefix && basePrefix !== "/") {
        if (suffix === basePrefix) suffix = "/";
        else if (suffix.startsWith(basePrefix + "/")) suffix = suffix.slice(basePrefix.length);
      }
    } catch {}
    // Credential for this request: explicit override > the session's pooled key
    // > the caller's own Authorization > the single-key fallback env var.
    const keyOverride = req.headers["x-cb-key"] || req.headers["x-lf-key"];
    let auth = null;
    if (typeof keyOverride === "string" && keyOverride.trim()) {
      const k = keyOverride.trim();
      auth = /^bearer /i.test(k) ? k : `Bearer ${k}`;
    } else if (keySlot >= 1 && keySlot <= keyPool.length) {
      auth = `Bearer ${keyPool[keySlot - 1]}`;
    } else {
      auth = req.headers["authorization"];
      if (!auth && process.env.CORALBRICKS_PROXY_KEY) auth = `Bearer ${process.env.CORALBRICKS_PROXY_KEY}`;
    }
    const fwdHeaders = buildForwardHeaders(req.headers, auth ? { authorization: auth } : {});
    const target = `${base}${suffix}`;
    const isStreamCt = (ct) => String(ct || "").toLowerCase().includes("text/event-stream");

    // ---- model-based vision handoff ----
    // Target model is text-only and the request carries images: caption each
    // image with the vision model, swap the image part for the caption text,
    // then continue to the ORIGINAL model with the same stream request.
    if (VISION_ENABLED && reqJson) {
      const targetModel = String(reqJson.model || "");
      const matched = VISION_TARGETS.some((t) => t === "*" || t === targetModel || (t.endsWith("*") && targetModel.startsWith(t.slice(0, -1))));
      if (matched && targetModel !== VISION_MODEL) {
        const imgs = collectImageParts(reqJson);
        if (imgs.length > 0) {
          const origSize = reqSize;
          let analyzed = 0, cached = 0, failed = 0;
          for (const img of imgs) {
            if (clientGone) break;
            const key = createHash("sha256").update(img.url).digest("hex");
            let desc = visionCacheGet(key);
            if (desc) {
              cached++;
            } else {
              try {
                desc = await analyzeImage({ base, auth, url: img.url });
                visionCacheSet(key, desc);
                analyzed++;
              } catch (e) {
                log(`vision handoff failed: ${e?.message || e}`);
                desc = null;
                failed++;
              }
            }
            const msgs = reqJson.messages;
            if (msgs?.[img.mi]?.content) {
              msgs[img.mi].content[img.pi] = {
                type: "text",
                text: desc
                  ? `[Image attached by the user — description produced by ${VISION_MODEL}:]\n${desc}`
                  : "[Image attached by the user — vision analysis unavailable]",
              };
            }
          }
          rawBody = Buffer.from(JSON.stringify(reqJson), "utf8");
          reqSize = rawBody.length;
          log(`vision handoff model=${targetModel} images=${imgs.length} analyzed=${analyzed} cached=${cached} failed=${failed} bytes ${origSize}->${reqSize}`);
        }
      }
    }

    log(`req ${req.method} ${pathAndQuery} model=${model} stream=${wantsStream} bytes=${reqSize} max_tokens=${reqJson?.max_tokens ?? reqJson?.max_completion_tokens ?? "default"} session=${sessionId || "-"} key=${keySlot || "-"}${keySlot ? `(${maskKey(keyPool[keySlot - 1])})` : ""}`);
    if (clientGone) { stopEarlyHeaderTimer(); stopKeepalive(); log(`req ${pathAndQuery}: client gone, stop`); return; }

    let lastStatus = null;
    let lastStatusBody = null;
    let lastFailure = "unknown";

    const rlDeadline = Date.now() + RL_BUDGET_MS;
    let hardFailures = 0;
    // Never-forward transients (503 "Service temporarily unavailable") are
    // retried patiently, but they still have a hard cap: an outage must end
    // with a visible warning, not an endless "Working" spinner.
    let neverForwardFailures = 0;
    let rlStreak = 0;
    let attemptsUsed = 0;
    // Post-commit continuation: when the upstream cuts a stream mid-answer
    // (no finish_reason/[DONE]) or sends a transient error after content, we
    // retry the SAME request and skip the prefix already sent to the client,
    // then stream only the new suffix. This preserves streaming without
    // duplicating or retracting content. If the retry diverges, we end with
    // the partial answer instead of showing an error/retry loop.
    let sentContent = "";
    // Buffer mode: the last failed attempt had text before it was discarded.
    // Give-up still needs to warn the user that the answer is incomplete.
    let giveUpHadContent = false;
    let continuationPrefix = null;
    let continuationAttempts = 0;
    const MAX_CONTINUATION = 3;
    for (let attempt = 1; ; attempt++) {
      attemptsUsed = attempt;
      if (wantsStream && streamId !== null) {
        const s = proxyState.streams.get(streamId);
        if (s) {
          s.attempts = attempt;
          s.retries = attempt - 1;
          s.model = model;
          s.updated = Date.now();
        }
        proxyState.updated = Date.now();
      }
      // Mark the retry BEFORE the backoff wait: the pill should say
      // "retry N" while the user is waiting, not only during the next attempt.
      const retryPause = async (ms) => {
        if (wantsStream && streamId !== null) {
          const s = proxyState.streams.get(streamId);
          if (s) { s.retries = Math.max(s.retries, attempt); s.updated = Date.now(); }
          proxyState.updated = Date.now();
        }
        await sleep(ms);
      };
      if (clientGone) { stopKeepalive(); log(`req ${pathAndQuery}: client gone, stop`); return; }
      const retryCount = attempt - 1;
      let up = null;
      try {
        // raw socket upstream call — never reuse a consumed init (tinyproxy v10)
        const reqHeaders = { ...fwdHeaders };
        if (reqSize > 0) reqHeaders["content-length"] = String(reqSize);
        up = await upstreamRequest(target, { method: req.method, headers: reqHeaders, body: reqSize > 0 ? rawBody : undefined });
        lastStatus = up.statusCode;
        // Remember the last upstream status for this stream so /status can show
        // "retrying 2 · HTTP 503" in the desktop pill.
        if (wantsStream && streamId !== null) {
          const s = proxyState.streams.get(streamId);
          if (s) { s.status = up.statusCode; s.updated = Date.now(); }
          proxyState.updated = Date.now();
        }

        if (lastStatus >= 400) {
          const text = await consumeBody(up);
          lastStatusBody = text;
          lastFailure = `status ${lastStatus}: ${snippet(text)}`;
          const retryable = lastStatus === 429 || lastStatus === 408 || lastStatus >= 500;
          if (!retryable) { log(`attempt ${attempt}: ${lastFailure}`); break; } // deterministic 4xx: pass through now
          const patient = lastStatus === 429 || lastStatus === 413
            || (lastStatus >= 400 && (RATE_LIMIT_RE.test(text) || PATIENT_SIZE_RE.test(text) || TRANSIENT_RE.test(text)));
          if (patient) {
            // token-per-minute / transient outage: retry until it works.
            // NEVER_FORWARD classes have no budget: they are retried until
            // success or client disconnect so the client never sees them.
            const neverForward = NEVER_FORWARD_RE.test(text);
            const wait = parseRetryAfterMs(up.headers["retry-after"]) || rateLimitBackoffMs(rlStreak++);
            if (neverForward) {
              neverForwardFailures++;
              if (neverForwardFailures > MAX_RETRIES) { log(`attempt ${attempt}: ${lastFailure} — never-forward retries exhausted`); break; }
            } else if (Date.now() + wait >= rlDeadline) { log(`attempt ${attempt}: ${lastFailure} — patient budget exhausted`); break; }
            log(`attempt ${attempt}: ${lastFailure} — ${neverForward ? "never-forward transient" : "transient/rate-limited"}, retrying in ${Math.round(wait / 1000)}s`);
            await retryPause(wait);
            continue;
          }
          log(`attempt ${attempt}: ${lastFailure}`);
          hardFailures++;
          if (hardFailures > MAX_RETRIES) break;
          await retryPause(parseRetryAfterMs(up.headers["retry-after"]) || backoffMs(retryCount));
          continue;
        }

        const ct = up.headers["content-type"] || "";

        // non-stream request: pass bytes through as-is
        if (!wantsStream) {
          stopEarlyHeaderTimer();
          stopKeepalive();
          const headers = pickResponseHeaders(up.headers, retryCount);
          res.writeHead(lastStatus, headers);
          const readUp = makeReader(up);
          let bodyText = "";
          for (;;) {
            const chunk = await readUp(IDLE_MS);
            if (chunk === null) break;
            if (clientGone) { try { up.destroy(); } catch {} break; }
            if (bodyText.length < 2000000) bodyText += chunk.toString("utf8");
            res.write(chunk);
          }
          res.end();
          try { up.destroy(); } catch {} // release the dedicated socket
          if (lastStatus === 200) {
            const u = parseUsageFromText(bodyText);
            if (u) {
              usageAcc.prompt = u.prompt;
              usageAcc.completion = u.completion;
              usageAcc.reasoning = u.reasoning;
              usageAcc.cached = u.cached;
              usageAcc.sawUsage = true;
            }
            recordUsageOnce();
          }
          log(`req ${pathAndQuery}: proxied non-stream ${lastStatus} in ${Date.now() - started}ms`);
          return;
        }

        // stream requested but upstream answered non-SSE (bulk-JSON to SSE, tinyproxy v13)
        if (!isStreamCt(ct)) {
          const text = await consumeBody(up);
          let payload = null;
          if (lastStatus === 200) {
            try {
              const j = JSON.parse(text);
              // ONLY a real completion converts. A 200-JSON ERROR body (e.g.
              // Cloudflare AiError 8006 "request body exceeds the maximum
              // supported size") is NOT a chunk — forwarding it as one is
              // exactly what produced "Invalid coralbricks/... stream event"
              // (opencode decodes the error-shaped object, fails the union,
              // and blames the route). Fail the attempt and retry instead.
              payload = bulkToChunkJSON(j);
            } catch { payload = null; }
          }
          if (payload !== null) {
            log(`attempt ${attempt}: bulk-JSON (ct=${ct}) converted to SSE`);
            ensureStreamHeaders(up.headers, retryCount);
            dataCommitted = true;
            stopKeepalive();
            res.write(`data: ${payload}\n\n`);
            res.write("data: [DONE]\n\n");
            endStream();
            try { up.destroy(); } catch {}
            log(`req ${pathAndQuery}: bulk-JSON converted, done in ${Date.now() - started}ms`);
            {
              const u = parseUsageFromText(text);
              if (u) {
                usageAcc.prompt = u.prompt;
                usageAcc.completion = u.completion;
                usageAcc.reasoning = u.reasoning;
                usageAcc.cached = u.cached;
                usageAcc.sawUsage = true;
              }
              recordUsageOnce();
            }
            return;
          }
          lastFailure = `stream wanted but upstream ct=${ct} status=${lastStatus}: ${snippet(text)}`;
          log(`attempt ${attempt}: ${lastFailure}`);
          // 200 + JSON error body (AiError 8006 "request body exceeds the
          // maximum supported size", rate-limit JSON): flaky per attempt —
          // the SAME body succeeds on a later retry — so retry patiently,
          // budget-bound, never counting against the 10 hard retries.
          const patientBody = lastStatus === 200 && (RATE_LIMIT_RE.test(text) || PATIENT_SIZE_RE.test(text));
          if (patientBody) {
            const wait = parseRetryAfterMs(up.headers["retry-after"]) || rateLimitBackoffMs(rlStreak++);
            if (Date.now() + wait >= rlDeadline) { log(`patient budget exhausted`); break; }
            log(`retrying in ${Math.round(wait / 1000)}s (patient: 200-error body)`);
            await retryPause(wait);
            continue;
          }
          hardFailures++;
          if (hardFailures > MAX_RETRIES) break;
          await retryPause(backoffMs(retryCount));
          continue;
        }

        // ===== SSE validation path =====
        const headers = pickResponseHeaders(up.headers, retryCount);
        headers["content-type"] = "text/event-stream";
        let bailed = false;
        let bailedRLFlag = false;
        let bailedNeverForward = false;
        let bailedPostCommit = false;
        let continuationFailed = false;
        let drops = 0;
        let sawDone = false;
        let sawFinish = false;
        let sawToolCalls = false;
        let structuredToolCalls = false;
        let syntheticToolCall = false;
        let dsmlRaw = "";
        let dsmlCallSeq = 0;
        let forwardedChars = 0;
        let lastErr = null;
        const buffered = [];
        let bufferAbrupt = false;
        let bufferFailed = false;
        if (BUFFER_UNTIL_COMPLETE) res.write = (chunk) => { buffered.push(chunk); return true; };
        const commitData = () => {
          if (!dataCommitted) {
            if (BUFFER_UNTIL_COMPLETE) {
              dataCommitted = true;
            } else {
              ensureStreamHeaders(up.headers, retryCount);
              dataCommitted = true;
              stopEarlyHeaderTimer();
              stopKeepalive();
            }
          }
        };
        // A tool call can arrive after the upstream already sent
        // finish_reason=stop (some routes split the final chunk). Remove the
        // early stop from the buffer so the client executes the tool call.
        const prepareToolCallAfterFinish = () => {
          if (!BUFFER_UNTIL_COMPLETE || !sawFinish) return;
          for (let i = buffered.length - 1; i >= 0; i--) {
            if (String(buffered[i]).includes('"finish_reason":"stop"')) { buffered.splice(i, 1); break; }
          }
          sawFinish = false;
        };
        const ev = makeEventizer();
        const decoder = new TextDecoder();
        const readUp = makeReader(up);
        let firstByteSeen = false;
        let continuationMatched = continuationPrefix === null;
        let continuationBuffer = "";
        let continuationEmitted = 0;

        // An event carrying a non-null finish_reason means the completion is
        // semantically done even if the upstream never sends [DONE]. Some
        // coralbricks routes close the stream right after finish_reason; treating
        // that as an error made the client retry whole turns.
        const finishReasonOf = (data) => {
          if (!data.startsWith("{")) return null;
          try {
            const j = JSON.parse(data);
            if (!Array.isArray(j?.choices)) return null;
            const c = j.choices.find((x) => x && x.finish_reason !== undefined && x.finish_reason !== null);
            return c ? c.finish_reason : null;
          } catch { return null; }
        };
        const hasFinishReason = (data) => finishReasonOf(data) !== null;
        const contentDelta = (data) => {
          if (!data.startsWith("{")) return "";
          try {
            const j = JSON.parse(data);
            const c = j?.choices?.[0]?.delta?.content;
            return typeof c === "string" ? c : "";
          } catch { return ""; }
        };
        const stripContent = (data) => {
          if (!data.startsWith("{")) return data;
          try {
            const j = JSON.parse(data);
            if (!Array.isArray(j?.choices)) return data;
            let changed = false;
            for (const c of j.choices) {
              if (c?.delta && typeof c.delta.content === "string") {
                delete c.delta.content;
                changed = true;
              }
            }
            return changed ? JSON.stringify(j) : data;
          } catch { return data; }
        };

        // returns {bail, rateLimited} (pre-commit failure) or null
        const handleEvent = (raw) => {
          const data = eventData(raw);
          noteUsageEvent(usageAcc, data);
          const verdict = analyzeEvent(data);
          // Continuation mode: buffer the retry's content until it grows past
          // the prefix already sent, verify it starts with that prefix, then
          // emit only the new suffix. Other events before the match are
          // dropped (the client already has them).
          if (!continuationMatched) {
            if (data.includes('"tool_calls"')) {
              if (verdict.kind === "invalid") {
                continuationFailed = true;
                return { bail: `continuation saw invalid tool_calls`, continuationFailed: true };
              }
              // The retry chose a tool call instead of regenerating text.
              // Hand it to the client (drop buffered duplicate content) so the
              // turn can continue instead of ending partial.
              continuationMatched = true;
              sawToolCalls = true;
              const tr = finishReasonOf(data);
              if (tr !== null) {
                sawFinish = true;
                log(`finish_reason=${tr} chars=${sentContent.length}`);
              }
              commitData();
              try { res.write(raw + "\n\n"); } catch {}
              log(`continuation handed off tool_calls after ${sentContent.length} chars`);
              return null;
            }
            const c = contentDelta(data);
            if (c) continuationBuffer += c;
            if (continuationBuffer.length >= continuationPrefix.length) {
              if (!continuationBuffer.startsWith(continuationPrefix)) {
                continuationFailed = true;
                return { bail: `continuation diverged after ${continuationBuffer.length} chars`, continuationFailed: true };
              }
              continuationMatched = true;
              const suffix = continuationBuffer.slice(continuationPrefix.length);
              continuationEmitted = suffix.length;
              if (suffix) {
                try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: suffix } }] })}\n\n`); } catch {}
                sentContent += suffix;
                forwardedChars += suffix.length;
              }
              const rest = stripContent(data);
              if (rest) {
                try { res.write(`data: ${rest}\n\n`); } catch {}
              }
              log(`continuation matched at ${continuationPrefix.length} chars; emitted ${suffix.length} new chars`);
              return null;
            }
            if (finishReasonOf(data) !== null || data === "[DONE]" || isErrorPayload(data)) {
              continuationFailed = true;
              return { bail: `continuation ended before prefix match`, continuationFailed: true };
            }
            return null;
          }
          if (verdict.kind === "invalid") {
            if (!dataCommitted) return { bail: `invalid event pre-commit: ${verdict.reason}: ${snippet(data)}`, rateLimited: false };
            drops++;
            log(`POST-commit DROP (${verdict.reason}): ${snippet(data)}`);
            return null;
          }
          if (isErrorPayload(data) && !dataCommitted) {
            return {
              bail: `error event pre-commit: ${snippet(data)}`,
              rateLimited: RATE_LIMIT_RE.test(data) || PATIENT_SIZE_RE.test(data) || TRANSIENT_RE.test(data),
              neverForward: NEVER_FORWARD_RE.test(data),
              errorBody: data,
            };
          }
          // Post-commit transient error: suppress it. The client cannot
          // un-see partial content, and forwarding this error makes the
          // desktop retry the whole turn ("Attempt N - retrying"). Close the
          // stream cleanly instead.
          if (isErrorPayload(data) && dataCommitted && NEVER_FORWARD_RE.test(data)) {
            if (BUFFER_UNTIL_COMPLETE && !sawFinish) {
              bufferFailed = true;
              return { bail: `buffered never-forward error`, rateLimited: false, neverForward: true, postCommit: true };
            }
            // Headers may have been committed by early SSE/keepalives or a
            // role-only chunk, but no visible content was sent. Retrying is
            // safe and recovers empty 503 turns.
            if (sentContent.length === 0 && !sawToolCalls && continuationAttempts < MAX_CONTINUATION) {
              continuationAttempts++;
              log(`POST-commit never-forward error before any content; retrying ${continuationAttempts}/${MAX_CONTINUATION}`);
              return { bail: `post-commit empty retry`, rateLimited: false, neverForward: true, postCommit: true };
            }
            if (!sawFinish && !sawToolCalls && sentContent.length > 0 && continuationAttempts < MAX_CONTINUATION
                && (continuationPrefix === null || continuationEmitted > 0)) {
              continuationPrefix = sentContent;
              continuationAttempts++;
              log(`POST-commit never-forward error; continuation ${continuationAttempts}/${MAX_CONTINUATION} after ${sentContent.length} chars`);
              return { bail: `post-commit continuation`, rateLimited: false, neverForward: true, postCommit: true };
            }
            log(`POST-commit SUPPRESS never-forward error: ${snippet(data)}`);
            if (!sawFinish) {
              try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`); } catch {}
              sawFinish = true;
            }
            try { res.write("data: [DONE]\n\n"); } catch {}
            sawDone = true;
            return null;
          }
          // Convert a text DSML edit block into a real edit tool call.
          const dsmlEdit = parseDsmlEdit(contentDelta(data));
          if (dsmlEdit) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: dsmlEdit.name, arguments: JSON.stringify(dsmlEdit.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted DSML edit to tool_calls: ${dsmlEdit.arguments.filePath}`);
            return null;
          }
          // Broken edits are judged on the whole buffered turn (a split block
          // is fine until the turn ends), so nothing is decided per chunk.
          // Convert a bare image path into a real read tool call.
          const bareImage = parseBareImageRead(contentDelta(data), sentContent);
          if (bareImage) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: bareImage.name, arguments: JSON.stringify(bareImage.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted bare image path to read tool_calls: ${bareImage.arguments.filePath}`);
            return null;
          }
          // Convert a text DSML shell block into a real shell tool call.
          const dsmlShell = parseDsmlShell(contentDelta(data));
          if (dsmlShell) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: dsmlShell.name, arguments: JSON.stringify(dsmlShell.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted DSML shell to tool_calls: ${dsmlShell.arguments.command.slice(0, 60)}`);
            return null;
          }
          // Convert a text DSML write block into a real write tool call.
          const dsmlWrite = parseDsmlWrite(contentDelta(data));
          if (dsmlWrite) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: dsmlWrite.name, arguments: JSON.stringify(dsmlWrite.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted DSML write to tool_calls: ${dsmlWrite.arguments.filePath}`);
            return null;
          }
          // Convert a text DSML grep block into a real grep tool call.
          const dsmlGrep = parseDsmlGrep(contentDelta(data));
          if (dsmlGrep) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: dsmlGrep.name, arguments: JSON.stringify(dsmlGrep.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted DSML grep to tool_calls: ${dsmlGrep.arguments.pattern}`);
            return null;
          }
          // Convert a text DSML read block into a real read tool call.
          const dsmlRead = parseDsmlRead(contentDelta(data));
          if (dsmlRead) {
            dsmlRaw += contentDelta(data);
            prepareToolCallAfterFinish();
            commitData();
            const call = {
              choices: [{
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: `call_dsml_${++dsmlCallSeq}`,
                    type: "function",
                    function: { name: dsmlRead.name, arguments: JSON.stringify(dsmlRead.arguments) },
                  }],
                },
              }],
            };
            try { res.write(`data: ${JSON.stringify(call)}\n\n`); } catch {}
            sawToolCalls = true;
            syntheticToolCall = true;
            log(`converted DSML read to tool_calls: ${dsmlRead.arguments.filePath}`);
            return null;
          }
          const finishReason = finishReasonOf(data);
          if (finishReason !== null) {
            // The final chunk can carry content together with finish_reason.
            // Include it in the completeness check; if we continue, the retry
            // regenerates it and the suffix starts with it (it was not sent).
            const finishContent = contentDelta(data);
            const combined = sentContent + finishContent;
            // DSML text is handled by the buffer post-process (the block may
            // be split across many small chunks, so no single chunk matches).
            // Retrying it here would loop forever instead of converting it.
            const dsmlText = hasDsmlMarkers(combined);
            const codeDump = !dsmlText && looksLikePartialCode(combined);
            const textTool = !dsmlText && looksLikeTextToolCall(combined);
            // A DSML/text tool call streamed as content can finish with
            // tool_calls even though no structured tool call was sent. Treat
            // it like a code dump: retry, then hand off any real tool call.
            const bareEdit = BUFFER_UNTIL_COMPLETE && looksLikeBarePathEdit(combined);
            const abrupt = (finishReason === "stop" || finishReason === "length" || finishReason === "tool_calls")
              && (looksIncomplete(combined) || codeDump || textTool || bareEdit);
            // A prior Read/tool call in the same turn must not block a code
            // dump continuation; the retry may reproduce the text or hand off
            // a new tool call.
            const allowAfterToolCalls = codeDump && (finishReason === "stop" || finishReason === "length");
            if (abrupt && BUFFER_UNTIL_COMPLETE && !sawToolCalls && !syntheticToolCall) {
              bufferAbrupt = true;
              return null;
            }
            if (abrupt && (!sawToolCalls || allowAfterToolCalls) && sentContent.length > 0 && continuationAttempts < MAX_CONTINUATION
                && (continuationPrefix === null || continuationEmitted > 0)) {
              continuationPrefix = sentContent;
              continuationAttempts++;
              log(`abrupt ${finishReason} mid-answer; continuation ${continuationAttempts}/${MAX_CONTINUATION} after ${combined.length} chars`);
              return { bail: `post-commit continuation (${finishReason})`, rateLimited: false, neverForward: true, postCommit: true };
            }
            if (syntheticToolCall && finishReason === "stop") {
              sawFinish = true;
              commitData();
              let out = data;
              try {
                const j = JSON.parse(data);
                for (const c of j.choices || []) if (c.finish_reason === "stop") c.finish_reason = "tool_calls";
                out = JSON.stringify(j);
              } catch {}
              try { res.write(`data: ${out}\n\n`); } catch {}
              log(`rewrote finish_reason stop -> tool_calls for DSML read`);
              return null;
            }
            sawFinish = true;
            log(`finish_reason=${finishReason} chars=${combined.length} toolCalls=${sawToolCalls}`);
          }
          if (verdict.kind === "repair") {
            commitData();
            dbg(`fwd repair: ${snippet(verdict.text, 80)}`);
            res.write(`data: ${verdict.text}\n\n`);
            forwardedChars += contentDelta(verdict.text).length;
            sentContent += contentDelta(verdict.text);
            if (verdict.text.includes('"tool_calls"')) { sawToolCalls = true; structuredToolCalls = true; }
            return null;
          }
          // coralbricks sometimes ends with [DONE] but never sends a chunk with a
          // finish_reason. OpenCode's openai-compatible router rejects that
          // with "OpenAI Chat stream ended without finish_reason", so inject
          // a schema-valid terminal chunk before forwarding [DONE].
          if (data === "[DONE]") {
            commitData();
            // Buffer mode: [DONE] can arrive without a finish_reason. Do not
            // flush an incomplete/DSML/text-tool-call answer just because DONE
            // is present; retry the whole request instead.
            if (BUFFER_UNTIL_COMPLETE && !sawFinish && !sawToolCalls && !syntheticToolCall) {
              const combined = sentContent;
              const dsmlText = hasDsmlMarkers(combined);
              if (looksIncomplete(combined)
                  || (!dsmlText && (looksLikePartialCode(combined) || looksLikeTextToolCall(combined)))
                  || looksLikeBarePathEdit(combined)) {
                bufferAbrupt = true;
                return null;
              }
            }
            if (!sawFinish) {
              const fin = (sawToolCalls || syntheticToolCall) ? "tool_calls" : "stop";
              try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: fin }] })}\n\n`); } catch {}
              sawFinish = true;
              log(`synthesized finish_reason (${fin}) before [DONE]`);
            }
            dbg(`fwd raw: [DONE]`);
            res.write(raw + "\n\n");
            sawDone = true;
            return null;
          }
          commitData();
          dbg(`fwd raw: ${snippet(data, 80)}`);
          res.write(raw + "\n\n");
          forwardedChars += contentDelta(data).length;
          sentContent += contentDelta(data);
          if (data.includes('"tool_calls"')) { sawToolCalls = true; structuredToolCalls = true; }
          return null;
        };

        try {
          for (;;) {
            const deadline = firstByteSeen ? IDLE_MS : FIRST_BYTE_MS;
            const chunk = await readUp(deadline);
            if (chunk === null) break;
            firstByteSeen = true;
            if (clientGone) { lastErr = "client-gone"; break; }
            dbg(`up chunk(${chunk.length}): ${snippet(chunk.toString("utf8"), 160)}`);
            const events = ev.feed(decoder.decode(chunk, { stream: true }));
            let bail = null;
            let bailRL = false;
            let bailNeverForward = false;
            let bailPostCommit = false;
            let bailContinuationFailed = false;
            for (const raw of events) {
              const r = handleEvent(raw);
              if (r) {
                bail = r.bail;
                bailRL = r.rateLimited;
                bailNeverForward = !!r.neverForward;
                bailPostCommit = !!r.postCommit;
                bailContinuationFailed = !!r.continuationFailed;
                if (r.errorBody) lastStatusBody = r.errorBody;
                break;
              }
              if (sawDone) break;
            }
            if (bail) {
              lastFailure = bail;
              log(`attempt ${attempt}: ${bail}`);
              bailed = true;
              bailedRLFlag = bailRL;
              bailedNeverForward = bailNeverForward;
              bailedPostCommit = bailPostCommit;
              continuationFailed = bailContinuationFailed;
              break;
            }
            if (sawDone) break;
          }
          if (!bailed && !sawDone) {
            for (const raw of ev.flush()) {
              const r = handleEvent(raw);
              if (r) {
                lastFailure = r.bail;
                if (r.errorBody) lastStatusBody = r.errorBody;
                log(`attempt ${attempt}: ${r.bail}`);
                bailed = true;
                bailedRLFlag = r.rateLimited;
                bailedNeverForward = !!r.neverForward;
                bailedPostCommit = !!r.postCommit;
                continuationFailed = !!r.continuationFailed;
                break;
              }
            }
          }
        } catch (streamErr) {
          lastErr = streamErr;
          log(`attempt ${attempt}: stream error: ${streamErr?.message || streamErr}`);
        }
        if (BUFFER_UNTIL_COMPLETE) {
          try { res.write = rawWrite; } catch {}
          if (lastErr === "client-gone") { buffered.length = 0; return; }
          let joinedContent = "";
          for (const chunk of buffered) {
            const m = String(chunk).match(/^data: (.*)\n\n$/s);
            if (!m) continue;
            try {
              const j = JSON.parse(m[1]);
              const c = j?.choices?.[0]?.delta?.content;
              if (typeof c === "string") joinedContent += c;
            } catch {}
          }
          const joinedAll = dsmlRaw + joinedContent;
          const incompleteEdit = isIncompleteDsmlEdit(joinedAll);
          // Raw DSML that no parser can convert (truncated block, unknown
          // shape) is a broken turn: retry instead of showing it to the client.
          // Scan the raw buffered SSE too, so a reconstruction gap can never
          // leak DSML text.
          const rawBuffered = buffered.map(String).join("");
          const dsmlPresent = hasDsmlMarkers(joinedAll) || hasDsmlMarkers(rawBuffered);
          const unconvertibleDsml = dsmlPresent && !parseAnyDsml(joinedAll);
          const complete = !bufferAbrupt && !bufferFailed && !incompleteEdit && !unconvertibleDsml && (sawDone || sawFinish);
          if (complete) {
            ensureStreamHeaders({}, retryCount);
            stopEarlyHeaderTimer();
            stopKeepalive();
            // Consolidate split DSML blocks: the path may be in one content
            // event and oldString/newString in later ones.
            let out = buffered;
            if (!structuredToolCalls) {
              let text = dsmlRaw;
              for (const chunk of buffered) {
                const m = String(chunk).match(/^data: (.*)\n\n$/s);
                if (!m) continue;
                try {
                  const j = JSON.parse(m[1]);
                  const c = j?.choices?.[0]?.delta?.content;
                  if (typeof c === "string") text += c;
                } catch {}
              }
              if (text) {
                const call = hasDsmlMarkers(text) ? parseAnyDsml(text) : parseBareImageRead(text, "");
                if (call) {
                  const p = call.arguments.filePath || call.arguments.path || "";
                  const tagA = text.indexOf("< | DSML |");
                  const tagB = text.indexOf("</ | DSML |");
                  const firstTag = Math.min(tagA < 0 ? Infinity : tagA, tagB < 0 ? Infinity : tagB);
                  const pathIdx = p ? text.indexOf(p) : -1;
                  let idx = Math.min(firstTag, pathIdx < 0 ? Infinity : pathIdx);
                  if (!Number.isFinite(idx)) idx = text.length;
                  let narration = text.slice(0, idx).trimEnd();
                  if (/^(edit|write|read|grep)$/i.test(narration)) narration = "";
                  const rebuilt = [];
                  if (narration) rebuilt.push(`data: ${JSON.stringify({ choices: [{ delta: { content: narration } }] })}\n\n`);
                  rebuilt.push(`data: ${JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, id: `call_dsml_${++dsmlCallSeq}`, type: "function", function: { name: call.name, arguments: JSON.stringify(call.arguments) } }] } }] })}\n\n`);
                  rebuilt.push(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] })}\n\n`);
                  rebuilt.push("data: [DONE]\n\n");
                  out = rebuilt;
                  log(`post-processed buffered DSML into ${call.name} tool_calls`);
                }
              }
            }
            if (out === buffered && !sawDone) buffered.push("data: [DONE]\n\n");
            for (const chunk of out) { try { rawWrite(chunk); } catch {} }
            endStream();
            try { up.destroy(); } catch {}
            log(`req ${pathAndQuery}: stream done (buffered) retries=${retryCount} chars=${sentContent.length} sawDone=${sawDone} sawFinish=${sawFinish} key=${keySlot || "-"} in ${Date.now() - started}ms`);
            recordUsageOnce();
            return;
          }
          if (!bailed) lastFailure = `buffered stream failed (sawDone=${sawDone} sawFinish=${sawFinish} err=${lastErr?.message || lastErr || "none"})`;
          log(`req ${pathAndQuery}: buffered stream failed; retrying (${lastFailure})`);
          buffered.length = 0;
          // Nothing was sent to the client in buffer mode, so the failed
          // attempt's text must not feed the next attempt's completeness
          // checks (it made every retry look like a cut answer).
          if (sentContent.length > 0) giveUpHadContent = true;
          sentContent = "";
          try { up.destroy(); } catch {}
          hardFailures++;
          if (hardFailures > MAX_RETRIES) break;
          await retryPause(backoffMs(retryCount));
          continue;
        }
        if (continuationFailed) {
          // The retry diverged or ended before reaching the prefix. We cannot
          // safely splice it, so close the partial answer cleanly.
          log(`continuation failed after ${sentContent.length} chars; ending partial stream`);
          ensureStreamHeaders({}, retryCount);
          try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`); } catch {}
          try { res.write("data: [DONE]\n\n"); } catch {}
          endStream();
          try { up.destroy(); } catch {}
          return;
        }
        if (bailedPostCommit) {
          try { up.destroy(); } catch {}
          if (continuationAttempts >= MAX_CONTINUATION) {
            log(`continuation attempts exhausted; ending partial stream`);
            ensureStreamHeaders({}, retryCount);
            try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`); } catch {}
            try { res.write("data: [DONE]\n\n"); } catch {}
            endStream();
            return;
          }
          log(`continuation retry ${continuationAttempts}/${MAX_CONTINUATION} after ${sentContent.length} chars`);
          await retryPause(backoffMs(retryCount));
          continue;
        }
        if (dataCommitted) {
          // Post-commit truncation: retry and continue after the already-sent
          // prefix when possible (no finish_reason/[DONE], no tool calls).
          if (!sawDone && !sawFinish && !sawToolCalls && sentContent.length > 0
              && continuationAttempts < MAX_CONTINUATION && !lastErr
              && (continuationPrefix === null || continuationEmitted > 0)) {
            continuationPrefix = sentContent;
            continuationAttempts++;
            log(`post-commit truncation; continuation ${continuationAttempts}/${MAX_CONTINUATION} after ${sentContent.length} chars`);
            try { up.destroy(); } catch {}
            await retryPause(backoffMs(retryCount));
            continue;
          }
          if (!sawDone) {
            if (lastErr === "client-gone") {
              // client already disconnected; nothing to heal
            } else if (sawFinish) {
              // Upstream closed after a finish_reason: the completion is
              // complete. Terminate cleanly (no error chunk -> no client retry).
              try { res.write("data: [DONE]\n\n"); } catch {}
              log(`post-commit clean finish (no [DONE]): synthesized [DONE]`);
            } else if (!lastErr) {
              // Clean close without finish_reason or [DONE]: synthesize both so
              // OpenCode's requireFinishReason check passes.
              try { res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`); } catch {}
              try { res.write("data: [DONE]\n\n"); } catch {}
              sawFinish = true;
              log(`post-commit clean close (no finish_reason/[DONE]): synthesized finish_reason + [DONE]`);
            } else {
              const msg = `Upstream stream error: ${lastErr?.message || lastErr}`;
              try { res.write(`data: ${JSON.stringify({ error: { message: msg, type: "proxy_error", code: "proxy_stream_error" } })}\n\n`); } catch {}
              log(`post-commit synth error: ${msg}`);
              try { res.write("data: [DONE]\n\n"); } catch {}
            }
          }
          endStream();
          try { up.destroy(); } catch {} // release the dedicated socket (never pooled)
          recordUsageOnce();
          log(`req ${pathAndQuery}: stream done retries=${retryCount} drops=${drops} chars=${sentContent.length} sawDone=${sawDone} sawFinish=${sawFinish} key=${keySlot || "-"} in ${Date.now() - started}ms`);
          return;
        }

        // pre-commit failure -> retry (headers may already be sent as early
        // SSE; only keepalives flowed so we can still try a fresh upstream)
        try { up && up.destroy(); } catch {} // discard the failed attempt's socket
        if (clientGone) { stopEarlyHeaderTimer(); stopKeepalive(); log(`req ${pathAndQuery}: client gone pre-commit, stop`); return; }
        if (!bailed) {
          lastFailure = `stream ended with no valid events (err=${lastErr?.message || lastErr || "none"})`;
        }
        log(`attempt ${attempt}: pre-commit failure: ${lastFailure}`);
        if (bailed && bailedRLFlag) {
          // rate-limited / transient in-stream: patient retry. NEVER_FORWARD
          // classes ignore the time budget but still obey the hard retry cap.
          const wait = rateLimitBackoffMs(rlStreak++);
          if (bailedNeverForward) {
            neverForwardFailures++;
            if (neverForwardFailures > MAX_RETRIES) { log(`never-forward retries exhausted`); break; }
          } else if (Date.now() + wait >= rlDeadline) { log(`rate-limit budget exhausted`); break; }
          log(`retrying in ${Math.round(wait / 1000)}s (${bailedNeverForward ? "never-forward transient" : "rate limit"})`);
          await retryPause(wait);
          continue;
        }
        hardFailures++;
        if (hardFailures > MAX_RETRIES) break;
        await retryPause(backoffMs(retryCount));
      } catch (err) {
        lastFailure = `attempt error: ${err?.message || err}`;
        log(`attempt ${attempt}: ${lastFailure}`);
        try { up && up.destroy(); } catch {}
        if (clientGone) { stopEarlyHeaderTimer(); stopKeepalive(); return; }
        hardFailures++;
        if (hardFailures > MAX_RETRIES) break;
        await retryPause(backoffMs(retryCount));
      }
    }

    // give-up path (only after MAX_RETRIES): surface the last failure
    if (BUFFER_UNTIL_COMPLETE) { try { res.write = rawWrite; } catch {} }
    log(`req ${pathAndQuery}: GIVING UP (attempts=${attemptsUsed}, hardFailures=${hardFailures}): ${lastFailure}`);
    // A visible, proxy-authored warning. Never dump the raw upstream error
    // (the desktop would show it as a broken stream and retry the turn), and
    // never close silently: the user must see that the answer is incomplete.
    const giveUpNotice = () => {
      const raw = String(lastFailure || "");
      const transient = NEVER_FORWARD_RE.test(raw) || (lastStatusBody && NEVER_FORWARD_RE.test(lastStatusBody));
      const reason = transient
        ? "the upstream was temporarily unavailable"
        : lastStatus && lastStatus >= 400
          ? `the upstream returned status ${lastStatus}`
          : "the upstream stream ended before the answer completed";
      const retries = Math.max(1, attemptsUsed - 1);
      return `⚠️ coralbricks-proxy: giving up after ${retries} retries — ${reason}. The answer is incomplete; please try again.`;
    };
    const emitGiveUpNotice = () => {
      const notice = giveUpNotice();
      ensureStreamHeaders({}, attemptsUsed - 1);
      try {
        res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: notice } }] })}\n\n`);
        res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`);
        res.write("data: [DONE]\n\n");
      } catch {}
      endStream();
    };
    // If content was already streamed, never surface a late error (it would
    // make the desktop retry the whole turn), but append the warning so the
    // cut answer is not mistaken for a complete one.
    if (dataCommitted && (sentContent.length > 0 || giveUpHadContent)) {
      log(`req ${pathAndQuery}: give-up after partial content (${sentContent.length} chars, had=${giveUpHadContent}); appending warning`);
      emitGiveUpNotice();
      return;
    }
    // Safety net: never forward a NEVER_FORWARD error (e.g. "Service
    // temporarily unavailable"). Emit the warning as normal assistant content
    // so the desktop does not start its "Attempt N - retrying" turn loop.
    if (NEVER_FORWARD_RE.test(lastFailure) || (lastStatusBody && NEVER_FORWARD_RE.test(lastStatusBody))) {
      log(`req ${pathAndQuery}: give-up suppressed (never-forward): ${snippet(lastFailure)}`);
      if (wantsStream || res.headersSent) {
        emitGiveUpNotice();
      } else {
        try {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ id: "proxy", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: giveUpNotice() }, finish_reason: "stop" }], usage: {} }));
        } catch {}
      }
      return;
    }
    // 200 with an in-band JSON error body (e.g. coralbricks/Cloudflare AiError
    // 8006 "request body exceeds the maximum supported size"): emit it as a
    // schema-VALID error event so the turn fails with the REAL message —
    // never as "Invalid coralbricks/openai-compatible-chat stream event".
    const emitSseError = (obj) => {
      ensureStreamHeaders({}, attemptsUsed - 1);
      try { res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch {}
      try { res.write("data: [DONE]\n\n"); } catch {}
      endStream();
    };
    if (lastStatus === 200 && lastStatusBody) {
      const trimmed = lastStatusBody.trim();
      if (isErrorPayload(trimmed)) {
        const probe = analyzeEvent(trimmed);
        const text = probe.kind === "repair" ? probe.text : trimmed;
        ensureStreamHeaders({}, attemptsUsed - 1);
        try { res.write(`data: ${text}\n\n`); } catch {}
        try { res.write("data: [DONE]\n\n"); } catch {}
        endStream();
        return;
      }
    }
    if (lastStatus && lastStatus >= 400 && lastStatusBody !== null) {
      // Deterministic upstream status: if headers already sent (early stream)
      // we must report as an in-stream error; otherwise pass status through.
      if (res.headersSent) {
        let msg = `upstream status ${lastStatus}`;
        try { const j = JSON.parse(lastStatusBody); if (j?.error?.message) msg = j.error.message; else if (typeof j?.error === "string") msg = j.error; } catch {}
        emitSseError({ error: { message: msg, type: "upstream_error", code: String(lastStatus) } });
        return;
      }
      try { res.writeHead(lastStatus, { "content-type": "application/json" }); } catch {}
      try { res.end(lastStatusBody); } catch {}
      return;
    }
    // Generic give-up: prefer in-stream error if stream headers already sent.
    if (wantsStream || res.headersSent) {
      emitSseError({ error: { message: `coralbricks-proxy: giving up after ${MAX_RETRIES} retries — last failure: ${lastFailure}`, type: "proxy_error", code: "proxy_retries_exhausted" } });
      return;
    }
    try {
      res.writeHead(502, { "content-type": "text/plain" });
      res.end(`coralbricks-proxy: ${lastFailure}`);
    } catch {}
  }

  const server = createServer();
  server.on("request", (req, res) => {
    handle(req, res).catch((err) => {
      log(`handler crash: ${err?.stack || err}`);
      try { if (!res.headersSent) res.writeHead(502, { "content-type": "text/plain" }); res.end(`coralbricks-proxy error: ${err?.message || err}`); } catch {}
    });
  });
  server.on("checkContinue", (req, res) => {
    try { res.writeContinue(); } catch {}
    handle(req, res).catch(() => {});
  });
  server.on("clientError", (err, socket) => {
    try { socket.end("HTTP/1.1 400 Bad Request\r\n\r\n"); } catch {}
  });

  // One retry timer per server instance, cancelled on takeover/shutdown.
  // Without this, every plugin reload left a zombie EADDRINUSE retry loop
  // behind (one per reload, forever): thousands of "port busy" log lines and
  // a stale-code server that could grab the port later.
  let listenRetryTimer = null;
  let shuttingDown = false;
  const scheduleListenRetry = () => {
    if (shuttingDown || listenRetryTimer) return;
    listenRetryTimer = setTimeout(() => {
      listenRetryTimer = null;
      if (!shuttingDown) listen();
    }, 5000);
    try { listenRetryTimer.unref?.(); } catch {}
  };
  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    if (listenRetryTimer) { clearTimeout(listenRetryTimer); listenRetryTimer = null; }
    try { server.closeAllConnections?.(); } catch {}
    try { server.close(() => {}); } catch {}
  };
  server.__lfShutdown = shutdown;

  // hot-reload safety (tinyproxy pattern): close a previous instance first
  try {
    const prev = globalThis.__coralbricksProxyServer;
    globalThis.__coralbricksProxyServer = null;
    if (prev) {
      try { prev.__lfShutdown?.(); } catch {}
      try { prev.closeAllConnections?.(); } catch {}
      await new Promise((resolve) => { try { prev.close(resolve); } catch { resolve(); } });
    }
  } catch {}
  globalThis.__coralbricksProxyServer = server;

  const listen = () => {
    if (shuttingDown) return;
    server.listen(port, LISTEN_HOST, () => {
      if (listenRetryTimer) { clearTimeout(listenRetryTimer); listenRetryTimer = null; }
      log(`listening (${mode}) on http://${LISTEN_HOST}:${port} -> ${upstreamBase}`);
    });
  };
  server.on("error", (err) => {
    if (shuttingDown) return;
    if (err && err.code === "EADDRINUSE") {
      log(`port ${port} busy — retrying every 5s (another coralbricks-proxy instance may be serving; takeover on release)`);
      scheduleListenRetry();
    } else {
      log(`server error: ${err?.stack || err}`);
    }
  });
  listen();

  // Bun crash-proofing (tinyproxy v16.16): an unhandled rejection in a plugin
  // can kill the whole opencode server — swallow and log instead.
  if (!globalThis.__coralbricksProxyCrashGuards) {
    globalThis.__coralbricksProxyCrashGuards = true;
    process.on("unhandledRejection", (reason) => {
      try { log(`swallowed unhandledRejection: ${(reason && (reason.stack || reason.message)) || reason}`); } catch {}
    });
    process.on("uncaughtException", (err) => {
      try { log(`swallowed uncaughtException: ${(err && (err.stack || err.message)) || err}`); } catch {}
    });
  }
}

export default {
  id: "coralbricks-proxy",
  async setup(ctx) {
    startServer(process.env.CORALBRICKS_PROXY_UPSTREAM || UPSTREAM_DEFAULT, Number(process.env.CORALBRICKS_PROXY_PORT) || PORT_DEFAULT, "plugin")
      .catch((e) => log(`startServer failed: ${e?.stack || e}`));
  },
};

if (import.meta.main) {
  startServer(process.env.CORALBRICKS_PROXY_UPSTREAM || UPSTREAM_DEFAULT, Number(process.env.CORALBRICKS_PROXY_PORT) || PORT_DEFAULT, "standalone")
    .catch((e) => log(`startServer failed: ${e?.stack || e}`));
}
