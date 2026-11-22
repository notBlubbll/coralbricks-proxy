# coralbricks-usage (OpenCode Desktop GUI plugin)

A usage graph addon for `coralbricks-proxy`. It adds a **Coralbricks usage**
side panel and a composer pill.

## Panel
The **Coralbricks usage** side panel shows:
- **Stacked bar chart** of the proxy's traffic, one colour per registered key.
  Bars stack key 1, key 2, key 3 (and so on) so a glance shows which
  credential carried the load.
- **Metric pills**: Requests, Tokens (input + output), Cost.
- **Window pills**: `24u` (since 00:00 UTC), `24h` (rolling), `7d`, `30d`, `all`.
- **Summary cards**: requests, input, output, cost, decode tok/s.
- **Per-key legend**: masked key hint, live session count, and that key's
  totals for the selected window.
- **Sessions overview**: one row per live session, laid out like the original
  proxy panel — Session · Key · Model · Req · State (`running` / idle time).
  The routed session is marked **(this)**.
- **Keys manager** (collapsible, anchored at the bottom like the original):
  per key — Show/Hide the real value, Edit to replace it, **Pin** to glue the
  routed session to that key, `×` to remove; plus a paste-a-`cb_…`-key field to
  add one.
- **Per-model table**: requests, input/output tokens, tok/s and cost.

Data comes from the proxy:

| Endpoint | Used for |
|---|---|
| `GET /usage/series?window=…` | chart buckets (time × key) |
| `GET /usage` | cards and per-model table |
| `GET /keys` | keys, sessions and pins |
| `POST /keys` | add / edit / remove / pin / unpin |
| `GET /keys/reveal?slot=N` | the Show button (loopback only) |
| `GET /status` | composer pill retry state |

## Composer pill

While the routed session is running it shows `Working (20s)`, plus the proxy's
retry state when a request is being re-sent (`· retrying 2 · HTTP 502`).

## Install

Copy this folder to the global plugin directory as `coralbricks-usage`:

```
~/.config/opencode/plugins/coralbricks-usage/
  index.js
  gui-experimental.cjs
```

Then reload OpenCode (`opencode reload`, or `/reload`) and restart the desktop
window so the GUI bundle loads.

The proxy must be running on its default port (`17332`). To point the addon at
another instance, set the plugin option `usageEndpoint`, e.g.
`http://127.0.0.1:17332/usage`.

## Notes

- Plain DOM, no JSX and no build step; the bundle is CommonJS and is loaded by
  the desktop's experimental GUI plugin loader.
- The panel refreshes every 5 seconds and tolerates a missing proxy: the footer
  shows `proxy unreachable` instead of failing.
- Bar tooltips show the bucket's time range, requests, tokens, cost and the
  per-key split.
