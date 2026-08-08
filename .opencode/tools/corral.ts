// corral — corral scheduler control tools.
// Talks to the corral daemon over HTTP. The daemon enforces roles
// server-side using the current agent name (context.agent).
import { tool } from "@opencode-ai/plugin"

const DAEMON = process.env.CORRAL_DAEMON_URL ?? "http://127.0.0.1:4519"

// The API key comes from the environment, or from the repo's
// .corral/api.key written by `corral init` (zero-config setup).
async function loadKey(): Promise<string> {
  if (process.env.CORRAL_DAEMON_KEY) return process.env.CORRAL_DAEMON_KEY
  try {
    const k = await Bun.file(".corral/api.key").text()
    return k.trim()
  } catch {
    return ""
  }
}

// Map the current OpenCode agent to a corral role for server-side
// enforcement. Anything unknown falls back to operator (human).
const ROLE_MAP: Record<string, string> = {
  "corral-orchestrator": "orchestrator",
  "corral-planner": "planner",
  "corral-worker": "worker",
  "corral-reviewer": "reviewer",
  "corral-merger": "merger",
}

function roleFor(agent?: string): string {
  if (agent && agent in ROLE_MAP) return ROLE_MAP[agent]
  return "operator"
}

async function call(path: string, body?: unknown, role?: string) {
  const key = await loadKey()
  const res = await fetch(`${DAEMON}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Corral-Role": role ?? "operator",
      ...(key ? { Authorization: `Bearer ${key}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!res.ok) {
    return `error ${res.status}: ${await res.text()}`
  }
  return JSON.stringify(await res.json(), null, 2)
}

export const plan = tool({
  description: "Plan a corral task graph from a goal statement (planner agent).",
  args: { goal: tool.schema.string().describe("The goal to plan a graph for") },
  async execute(args, context) {
    return call("/api/plan", { goal: args.goal }, roleFor(context.agent))
  },
})

export const start = tool({
  description: "Start a corral run from an approved graph.",
  args: { graph: tool.schema.string().describe("Graph JSON (as returned by corral_plan)") },
  async execute(args, context) {
    let graph: unknown
    try {
      graph = JSON.parse(args.graph)
    } catch {
      return "error: graph is not valid JSON"
    }
    return call("/api/runs", { graph }, roleFor(context.agent))
  },
})

export const status = tool({
  description: "Show the status of corral runs (all runs, or one run by id).",
  args: {
    runID: tool.schema.string().optional().describe("Run id; omit for all runs"),
  },
  async execute(args, context) {
    return call(args.runID ? `/api/runs/${args.runID}` : "/api/runs", undefined, roleFor(context.agent))
  },
})

export const approve = tool({
  description: "Approve a human gate (or the run's merge) by node id.",
  args: {
    runID: tool.schema.string(),
    nodeID: tool.schema.string().describe("Node id, e.g. 'gate'"),
  },
  async execute(args, context) {
    return call(`/api/runs/${args.runID}/approve`, { nodeID: args.nodeID }, roleFor(context.agent))
  },
})

export const reject = tool({
  description: "Reject a human gate by node id (blocks the run).",
  args: {
    runID: tool.schema.string(),
    nodeID: tool.schema.string(),
  },
  async execute(args, context) {
    return call(`/api/runs/${args.runID}/reject`, { nodeID: args.nodeID }, roleFor(context.agent))
  },
})

export const cancel = tool({
  description: "Cancel an in-flight node by id.",
  args: {
    runID: tool.schema.string(),
    nodeID: tool.schema.string(),
  },
  async execute(args, context) {
    return call(`/api/runs/${args.runID}/cancel`, { nodeID: args.nodeID }, roleFor(context.agent))
  },
})

export const retry = tool({
  description: "Retry a blocked, retry-waiting or failed node by id.",
  args: {
    runID: tool.schema.string(),
    nodeID: tool.schema.string(),
  },
  async execute(args, context) {
    return call(`/api/runs/${args.runID}/retry`, { nodeID: args.nodeID }, roleFor(context.agent))
  },
})

export const steer = tool({
  description: "Send an instruction to the running attempt of a node.",
  args: {
    runID: tool.schema.string(),
    nodeID: tool.schema.string(),
    message: tool.schema.string(),
  },
  async execute(args, context) {
    return call(`/api/runs/${args.runID}/steer`, {
      nodeID: args.nodeID,
      message: args.message,
    }, roleFor(context.agent))
  },
})
