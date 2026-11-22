# coralbricks-proxy

An OpenCode V2 plugin that sits between OpenCode and the coralbricks
OpenAI-compatible endpoint (`https://inference.coralbricks.ai/v1`) and:

1. makes `Invalid coralbricks/openai-compatible-chat stream event` impossible
   to reach the client, retrying every transient failure class (rate limits,
   body-size flakes, 5xx, dropped streams) transparently, and
2. owns the credentials — three coralbricks keys are registered in `keys.json`,
   each OpenCode session is glued to one of them, and every request's token
   usage is recorded per model **and per key**.

The request body (including the model id) is forwarded byte-exact apart from
`stream_options.include_usage`, which is added to streaming chat calls so the
upstream returns its token counts.

> coralbricks currently serves exactly one model:
> `coralbricks/deepseek-v4.1-flash-value` (1,048,576 context, tools, images).
> Reasoning efforts: `/v1/models` advertises **low / high / max**, with `none`
> disabling thinking; an unsupported value (`ultra`) is rejected with a 400 that
> lists exactly those four. **`xhigh` is accepted (HTTP 200) but not
> advertised** — measured reasoning-token budgets land in the same tier as
> `max`, so the sample config exposes `xhigh` as an alias of `max`. `minimal`
> and `medium` are also silently accepted and behave like `low`/`high`.

## Why

The error above is thrown by OpenCode's route validator when an SSE `data:`
payload fails the openai-compatible-chat chunk schema. In practice the biggest
producer is the upstream answering **HTTP 200 with a JSON error body**, e.g.:

```json
{"error":{"errors":[{"message":"AiError: Invalid data for body - reason request body exceeds the maximum supported size","code":8006}],"success":false,"result":{},"messages":[]}}
```

That body is not a chunk. If it is forwarded as one, the decoder fails the
union and blames the route. This proxy treats it as a failed attempt and
retries — the same body often succeeds on a later attempt.

## Install

Copy the folder to your global plugin directory:

```
~/.config/opencode/plugins/coralbricks-proxy/
  index.js
  keys.json
```

OpenCode discovers plugins from that directory on server start — no config
entry is required. Point the provider at the proxy (see
`opencode.coralbricks.json`):

```jsonc
{
  "providers": {
    "coralbricks": {
      "package": "@opencode-ai/ai/providers/openai-compatible",
      "settings": {
        "baseURL": "http://127.0.0.1:17332/v1",
        "apiKey": "unused-with-a-key-pool",
        "timeout": 1800000,
        "headerTimeout": 600000,
        "chunkTimeout": 600000
      },
      "models": { /* deepseek-v4.1-flash-value */ }
    }
  }
}
```

With a non-empty key pool the proxy substitutes the session's registered key
for whatever `Authorization` the client sent, so the placeholder `apiKey` is
never used upstream. Keep a real key there only if you disable the pool.

## Keys

`keys.json` next to `index.js` (git-ignored — copy `keys.json.example`):

```json
{
  "keys": [
    "cb_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "cb_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "cb_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  ],
  "pins": {},
  "seen": {}
}
```

- **One session, one key.** The first request of an OpenCode session pins it to
  the key with the fewest live sessions; every later request of that session
  uses the same credential. `pins` / `seen` are written back (debounced), so a
  restart keeps the mapping. Pins expire after two weeks; a session counts as
  live for 30 minutes after its last request.
- **No round-robin.** A turn never flips keys mid-flight.
- `CORALBRICKS_PROXY_KEYS` (comma list) adds keys without editing the file;
  `CORALBRICKS_PROXY_KEYS_FILE` moves the file (tests use a throwaway one).

### Managing keys

| Route | Meaning |
|---|---|
| `GET /keys` | registered keys (masked), pins and live sessions |
| `POST /keys` `{"keys":["cb_…","cb_…"]}` | replace the whole pool |
| `POST /keys` `{"add":"cb_…"}` | append one key (max 8) |
| `POST /keys` `{"remove":2}` | drop slot 2; later slots shift down, pins follow their key |
| `POST /keys` `{"set":2,"key":"cb_…"}` | address one slot directly |
| `POST /keys` `{"pin":{"session":"ses_…","key":3}}` | force a session onto a key |
| `POST /keys` `{"unpin":"ses_…"}` | forget a pin |
| `GET /keys/reveal?slot=N` | full key value — loopback only, refused to browser origins |

## Usage tracking

Every successful request appends one record to `.coralbricks-usage.jsonl`
(pruned at 30 days, 5 MB cap) and updates the lifetime snapshot
`.coralbricks-usage.json`. `GET /usage` reports:

- windows `24u` (UTC midnight), `24h`, `7d`, `30d`, `all`;
- per model, per window: requests, input/output/reasoning/cached tokens,
  estimated-record count, USD cost, time-to-first-token, average ms, and
  decode speed (token-weighted, mean, median and last tok/s);
- `perKey[window][slot]` — the same figures per credential, so each key's own
  spend and traffic can be read;
- `lifetime` totals per model.

Token counts are the upstream's own (`usage` in the final chunk). If a model
answers without usage the record is estimated from bytes and flagged `est`.
Costs use the **OpenCode Go table for DeepSeek V4.1 Flash 1017** — input
$0.15/M, output $0.60/M, cached $0.003/M (models.dev `opencode-go/deepseek-v4.1-flash`),
so the report matches what the same model costs on opencode-go. The upstream
`cost` field wins only when it reports a positive amount; coralbricks currently
sends `0.0`. Override with
`CORALBRICKS_PROXY_PRICING="model=in/out/cached,..."`.

`GET /status` keeps the shape the desktop working pill reads
(`active`, `streams`, `retries`, `attempts`, `model`, `updated`, `sessions`)
and adds `keys`, `keySlots` and a `usage` glance (today + lifetime).

### Graph series

`GET /usage/series?window=24u|24h|7d|30d|all&slot=N&bucket=<ms>` returns the
same records bucketed over time for charting:

```json
{
  "window": "24h", "bucketMs": 3600000, "since": 1795330000000,
  "buckets": [
    { "t": 1795333200000, "requests": 4, "input": 91234, "output": 812,
      "cost": 0.0007, "byKey": { "1": { "requests": 4, "input": 91234, "output": 812, "cost": 0.0007 } } }
  ]
}
```

Bucket size defaults to 1h for `24u`/`24h`, 6h for `7d`, 1d for `30d`/`all`
(never more than 120 buckets); `bucket` overrides it, `slot` narrows to one key.
This is what the `desktop-usage-graph` addon draws as a stacked per-key bar
chart.

## Behavior

**Pre-commit** (nothing has been sent to the client yet): every upstream event
is buffered and validated. Any failure — non-2xx, invalid event, in-band error
payload, network death, stall — makes the attempt fail and the whole request is
retried with the same buffered body.

**Post-commit**: every event is still validated before it is written; invalid
events are dropped inline (logged) so the client parser can never see one. If
the upstream dies mid-stream, a schema-valid error chunk + `[DONE]` is
synthesized, so the turn fails with a real message instead of a parse error.

**Repairs** (schema-safe, meaning-preserving): `delta.content: null` and other
null-valued typed fields are stripped; `{"error":"str"}` is reshaped to
`{"error":{"message":"str"}}`.

### Retry classes

| Failure | Class | Behavior |
|---|---|---|
| `429`, Serverless token-per-minute limit (status or in-band) | patient | retry until it works: 5/7/10/15/20/30s ladder, `Retry-After` honored (cap 60s), 25-min budget |
| `AiError 8006` body-size, `413` | patient | same-body retry until it works (flaky per attempt) |
| `503 Service temporarily unavailable` (status or in-band) | never-forward | patient backoff, hard-capped at 10 retries |
| other 5xx, network death pre-commit, invalid event pre-commit | hard | 10 retries, then give up |
| invalid event post-commit | — | dropped inline |
| upstream death post-commit | — | healed with valid error chunk + `[DONE]` |
| give-up | — | forwards the last status, the real upstream error as a valid SSE event, or a visible `⚠️ coralbricks-proxy: giving up after N retries …` warning when the answer was already partial |

Patient retries never consume the 10 hard retries, but never-forward
transients have their own hard cap of 10 so an outage always ends with a
visible warning instead of an endless spinner.

### Buffer mode (default)

The proxy buffers each SSE response and flushes it only after a complete turn
(`finish_reason`/`[DONE]`). A cut, 503, or malformed stream is retried
transparently; the client never sees partial output. The tradeoff is that the
answer appears all at once instead of streaming. Set `CORALBRICKS_PROXY_BUFFER=0`
to restore streaming behavior.

### Model-based vision handoff (off by default)

coralbricks' only model accepts images, so no captions are generated by
default. For a text-only target, list it and the proxy captions each image with
the vision model, replaces the image part with the caption text, and forwards
to the original model. Captions are cached by image hash (1h, 200 entries).

| Env var | Default | Meaning |
|---|---|---|
| `CORALBRICKS_PROXY_VISION` | on | `0` disables the handoff |
| `CORALBRICKS_PROXY_VISION_TARGETS` | empty | comma list of text-only models to hand off (`*` wildcard) |
| `CORALBRICKS_PROXY_VISION_MODEL` | `coralbricks/deepseek-v4.1-flash-value` | vision model used for captions |
| `CORALBRICKS_PROXY_VISION_PROMPT` | built-in | caption prompt override |

## Standalone

The same file runs outside OpenCode:

```sh
bun index.js            # or: node index.js
```

| Env var | Default | Meaning |
|---|---|---|
| `CORALBRICKS_PROXY_PORT` | `17332` | listen port (127.0.0.1 only) |
| `CORALBRICKS_PROXY_UPSTREAM` | `https://inference.coralbricks.ai/v1` | upstream base URL |
| `CORALBRICKS_PROXY_KEY` | — | fallback bearer token when no pool is configured |
| `CORALBRICKS_PROXY_KEYS` | — | comma list added to the key pool |
| `CORALBRICKS_PROXY_KEYS_FILE` | `keys.json` | key/pin store |
| `CORALBRICKS_PROXY_DEBUG` | off | `1` = log every upstream chunk and forwarded event |
| `CORALBRICKS_PROXY_BUFFER` | on | `0` = stream instead of buffering whole turns |
| `CORALBRICKS_PROXY_RL_BUDGET_MS` | `1500000` | patient-retry wall-clock budget |
| `CORALBRICKS_PROXY_RL_BACKOFF_MS` | `5000,7000,10000,15000,20000,30000` | patient backoff ladder |
| `CORALBRICKS_PROXY_BACKOFF_MS` | `300,600,1000,2000,3000` | hard-failure backoff ladder |
| `CORALBRICKS_PROXY_PRICING` | coralbricks table | `model=in/out/cached` USD per 1M |
| `CORALBRICKS_PROXY_USAGE_FILE` | `.coralbricks-usage.json` | lifetime snapshot |
| `CORALBRICKS_PROXY_USAGE_LOG` | `.coralbricks-usage.jsonl` | per-request log |
| `CORALBRICKS_PROXY_LOG_FILE` | `.coralbricks-proxy.log` | proxy log |

Health check: `GET /healthz` → `coralbricks-proxy OK`.

Per-request overrides (stripped before forwarding): `x-cb-upstream`,
`x-cb-key`. The old `x-lf-*` names are still accepted and stripped.

## Tests

```sh
bun test/run-tests.mjs
```

Spins a mock upstream plus the proxy and asserts 73 scenarios: the full
pre-commit/post-commit retry matrix, null-field repair, mid-stream healing,
non-stream passthrough, bulk-JSON conversion, stall retry, 200+8006 same-body
retry, budget exhaustion surfacing the real error, patient TPM retry, vision
handoff and caption caching, transient 503 retries, finish_reason injection,
post-commit continuation, DSML conversion, buffer-mode scenarios, and the key
pool + usage accounting cases (three keys registered, sessions pinned to
distinct keys, usage recorded per request and per key, `/keys/reveal`
loopback-only, `/usage/series` hourly buckets with a per-key split, key
removal).

## Desktop usage graph (optional)

`desktop-usage-graph/` is an OpenCode Desktop GUI plugin
(experimental `gui-experimental.cjs` API) that adds a **Coralbricks usage**
side panel: a stacked per-key bar chart of requests/tokens/cost over 24u / 24h /
7d / 30d / all, summary cards, a per-key legend, a sessions overview (which
session uses which key, with the routed session marked `(this)`, state and a
pin selector), a keys manager (show / edit / remove / add, pin to session) and
a per-model table. It also shows the working timer plus retry state as a
composer pill. Copy the folder to
`~/.config/opencode/plugins/coralbricks-usage/`, then run `opencode reload` and
restart the desktop window. See `desktop-usage-graph/README.md`.

## Desktop working timer (optional)

`desktop-working-timer/` is a small OpenCode Desktop GUI plugin
(experimental `gui-experimental.cjs` API) that shows `Working (20s)` as a
composer pill while the routed session is running, plus that session's
failed-attempt count (`· failed: N`) while its streaming request is in flight.
It polls this proxy's `/status` on `127.0.0.1:17332`. Copy it to
`~/.config/opencode/plugins/working-timer/`, then run `opencode reload` and
restart the desktop window.

## Operational notes

- Listen address is fixed to `127.0.0.1` (never exposed).
- If the port is busy the plugin retries binding every 5s, so a standalone
  instance can hand over to the plugin (or vice versa) without manual steps.
- Logs: console (OpenCode server log) plus `.coralbricks-proxy.log` next to the
  plugin file, truncated at 2 MB.
- Request bodies are buffered in memory so retries can replay them; this is
  what makes same-body retries possible.
- `keys.json` is written with mode 0600 on POSIX.
