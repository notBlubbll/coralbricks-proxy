// coralbricks-usage — OpenCode V2 server part.
// The real work is the desktop GUI part (gui-experimental.cjs): a usage graph
// panel fed by the coralbricks-proxy's /usage and /usage/series endpoints. This
// tiny server part exists so the desktop app discovers the plugin and can read
// its GUI bundle; it intentionally has no server-side behavior.
export default {
  id: "coralbricks-usage",
  async setup() {},
};
