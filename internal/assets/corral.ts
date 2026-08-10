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
  let res: Response
  try {
    res = await fetch(`${DAEMON}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Corral-Role": role ?? "operator",
        ...(key ? { Authorization: `Bearer ${key}` } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    })
  } catch {
    return `error: corral daemon is not running at ${DAEMON}. Start it with: corral up`
  }
  if (res.status === 401) {
    return `error 401: API key mismatch — restart the daemon so it uses .corral/api.key (corral up)`
  }
  if (!res.ok) {
    return `error ${res.status}: ${await res.text()}`
  }
  return JSON.stringify(await res.json(), null, 2)
}

// unwrapGraph accepts both the raw inner graph object and the full
// corral_plan output (wrapped as {"graph": ...}). A leading single-key
// {"graph": ...} wrapper is unwrapped so the daemon never receives a
// double-wrapped, empty graph (version 0, nodes null) that completes
// instantly with zero nodes.
function unwrapGraph(parsed: unknown): unknown {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return parsed
  }
  const obj = parsed as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1 && keys[0] === "graph") {
    return obj.graph
  }
  return parsed
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
  args: {
    graph: tool.schema.string().describe("Graph JSON: the raw graph, or the full corral_plan output wrapped as {\"graph\": ...}"),
    autoApproveGates: tool.schema.boolean().optional().describe("Approve human gates automatically instead of waiting for operator approval"),
  },
  async execute(args, context) {
    let parsed: unknown
    try {
      parsed = JSON.parse(args.graph)
    } catch {
      return "error: graph is not valid JSON"
    }
    const graph = unwrapGraph(parsed)
    const body: Record<string, unknown> = { graph }
    if (args.autoApproveGates) {
      body.autoApproveGates = true
    }
    return call("/api/runs", body, roleFor(context.agent))
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

// sseData joins the data payload of one SSE frame, or null when the
// frame carries no data (e.g. a heartbeat comment).
function sseData(frame: string): string | null {
  const lines = frame.split("\n").filter((l) => l.startsWith("data:"))
  if (lines.length === 0) return null
  return lines.map((l) => l.slice(5).trimStart()).join("\n")
}

export const watch = tool({
  description:
    "Wait for the next delta of a corral run — a node transition, a human gate awaiting approval, or the run finishing — from the daemon SSE stream. Returns the first event, or a message when none arrive within the timeout.",
  args: {
    runID: tool.schema.string().describe("Run id"),
    after: tool.schema.number().optional().describe("Only report events with a sequence number greater than this (resume from a previous watch or status call)"),
    timeout: tool.schema.number().optional().describe("Max seconds to wait for an event (default 30)"),
  },
  async execute(args, context) {
    const runID = encodeURIComponent(args.runID)
    const after = args.after ?? 0
    const timeout = args.timeout ?? 30
    const key = await loadKey()
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeout * 1000)
    try {
      const res = await fetch(`${DAEMON}/api/runs/${runID}/watch?after=${after}`, {
        headers: {
          "X-Corral-Role": roleFor(context.agent),
          ...(key ? { Authorization: `Bearer ${key}` } : {}),
        },
        signal: AbortSignal.any([controller.signal, context.abort]),
      })
      if (res.status === 401) {
        return "error 401: API key mismatch — restart the daemon so it uses .corral/api.key (corral up)"
      }
      if (!res.ok) {
        return `error ${res.status}: ${await res.text()}`
      }
      if (!res.body) {
        return "error: daemon returned no event stream"
      }
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const frames = buffer.split("\n\n")
        buffer = frames.pop() ?? ""
        for (const frame of frames) {
          const data = sseData(frame)
          if (data === null) continue
          await reader.cancel().catch(() => {})
          try {
            return JSON.stringify(JSON.parse(data), null, 2)
          } catch {
            return `event (raw): ${data}`
          }
        }
      }
      return "error: daemon closed the event stream without an event"
    } catch (err) {
      if (controller.signal.aborted) {
        return `no events within ${timeout}s (after seq ${after})`
      }
      if (context.abort.aborted) {
        return "cancelled: corral_watch aborted"
      }
      return `error: corral daemon is not running at ${DAEMON}. Start it with: corral up`
    } finally {
      clearTimeout(timer)
    }
  },
})
