package daemon

import (
	"encoding/json"
	"net/http"
)

// OpenAPI is the machine-readable contract of the daemon control API.
// The contract test validates every registered route against this
// document and live responses against its schemas.
const OpenAPI = `{
  "openapi": "3.0.3",
  "info": {"title": "corral daemon API", "version": "0.8.0"},
  "paths": {
    "/api/health": {
      "get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Health"}}}}}}
    },
    "/api/plan": {
      "post": {"responses": {"200": {"description": "planned graph", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/PlanResponse"}}}}}}
    },
    "/api/runs": {
      "post": {"responses": {"201": {"description": "created", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateRunResponse"}}}}}},
      "get": {"responses": {"200": {"description": "runs", "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/RunSummary"}}}}}}}
    },
    "/api/runs/{id}": {
      "get": {"responses": {"200": {"description": "run detail", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RunDetail"}}}}}}
    },
    "/api/runs/{id}/watch": {
      "get": {"parameters": [{"name": "since", "in": "query", "schema": {"type": "integer"}}, {"name": "timeout", "in": "query", "schema": {"type": "integer"}}], "responses": {"200": {"description": "run snapshot", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/WatchResponse"}}}}}}
    },
    "/api/runs/{id}/approve": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/reject": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/cancel": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/retry": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/steer": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/permission": {"post": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ok"}}}}}}},
    "/api/runs/{id}/export": {"get": {"responses": {"200": {"description": "audit export", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Export"}}}}}}},
    "/doc": {"get": {"responses": {"200": {"description": "openapi document"}}}}
  },
  "components": {
    "schemas": {
      "Health": {"type": "object", "required": ["healthy"], "properties": {"healthy": {"type": "boolean"}, "dir": {"type": "string"}}},
      "Ok": {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}},
      "PlanResponse": {"type": "object", "required": ["graph"], "properties": {"graph": {"$ref": "#/components/schemas/Graph"}}},
      "CreateRunResponse": {"type": "object", "required": ["runID"], "properties": {"runID": {"type": "string"}}},
      "RunSummary": {
        "type": "object", "required": ["id", "status"],
        "properties": {
          "id": {"type": "string"},
          "status": {"type": "string"},
          "done": {"type": "boolean"},
          "states": {"type": "object", "additionalProperties": {"type": "string"}}
        }
      },
      "Graph": {
        "type": "object", "required": ["nodes"],
        "properties": {
          "version": {"type": "integer"},
          "nodes": {"type": "array", "items": {"$ref": "#/components/schemas/Node"}}
        }
      },
      "Node": {
        "type": "object", "required": ["id", "type", "objective"],
        "properties": {
          "id": {"type": "string"},
          "type": {"type": "string", "enum": ["agent", "check", "merge", "human_gate"]},
          "objective": {"type": "string"},
          "acceptanceCriteria": {"type": "array", "items": {"type": "string"}},
          "role": {"type": "string"},
          "priority": {"type": "integer"},
          "dependsOn": {"type": "array", "items": {"type": "string"}},
          "writeScope": {"type": "array", "items": {"type": "string"}},
          "verification": {"$ref": "#/components/schemas/Verification"},
          "retryPolicy": {"type": "object", "properties": {"maxRetries": {"type": "integer"}, "backoff": {"type": "integer"}}},
          "budget": {"type": "object", "properties": {"maxDuration": {"type": "integer"}, "maxTokens": {"type": "integer"}, "maxCost": {"type": "number"}}}
        }
      },
      "Verification": {
        "type": "object", "required": ["kind"],
        "properties": {
          "kind": {"type": "string", "enum": ["command", "json_schema", "reviewer"]},
          "command": {"type": "array", "items": {"type": "string"}},
          "schema": {"type": "string"},
          "target": {"type": "string"},
          "reviewer": {"type": "string"}
        }
      },
      "Attempt": {
        "type": "object", "required": ["id", "no", "status"],
        "properties": {
          "id": {"type": "string"}, "no": {"type": "integer"}, "status": {"type": "string"},
          "serverID": {"type": "string"}, "sessionID": {"type": "string"},
          "worktree": {"type": "string"}, "branch": {"type": "string"},
          "startedAt": {"type": "integer"}, "finishedAt": {"type": "integer"},
          "evidence": {"type": "string"}, "cost": {"type": "number"}, "tokens": {"type": "integer"}
        }
      },
      "Event": {
        "type": "object", "required": ["seq", "type"],
        "properties": {
          "seq": {"type": "integer"}, "nodeID": {"type": "string"},
          "type": {"type": "string"}, "from": {"type": "string"}, "to": {"type": "string"},
          "attemptID": {"type": "string"}, "createdAt": {"type": "integer"}
        }
      },
      "Artifact": {
        "type": "object", "required": ["attemptID", "name", "hash"],
        "properties": {
          "attemptID": {"type": "string"}, "nodeID": {"type": "string"},
          "name": {"type": "string"}, "hash": {"type": "string"},
          "path": {"type": "string"}, "content": {"type": "string"}
        }
      },
      "RunDetail": {
        "type": "object", "required": ["runID"],
        "properties": {
          "runID": {"type": "string"}, "status": {"type": "string"}, "done": {"type": "boolean"},
          "graph": {"$ref": "#/components/schemas/Graph"},
          "autoApproveGates": {"type": "boolean"},
          "states": {"type": "object", "additionalProperties": {"type": "string"}},
          "attempts": {"type": "object", "additionalProperties": {"type": "array", "items": {"$ref": "#/components/schemas/Attempt"}}},
          "events": {"type": "array", "items": {"$ref": "#/components/schemas/Event"}}
        }
      },
      "WatchResponse": {
        "type": "object", "required": ["runID"],
        "properties": {
          "runID": {"type": "string"}, "status": {"type": "string"}, "done": {"type": "boolean"},
          "autoApproveGates": {"type": "boolean"},
          "states": {"type": "object", "additionalProperties": {"type": "string"}},
          "gatesAwaitingApproval": {"type": "array", "items": {"type": "string"}},
          "since": {"type": "integer"},
          "events": {"type": "array", "items": {"$ref": "#/components/schemas/Event"}}
        }
      },
      "Export": {
        "type": "object", "required": ["runID", "graph", "events"],
        "properties": {
          "runID": {"type": "string"}, "status": {"type": "string"},
          "exportedAt": {"type": "string"},
          "graph": {"$ref": "#/components/schemas/Graph"},
          "states": {"type": "object", "additionalProperties": {"type": "string"}},
          "events": {"type": "array", "items": {"$ref": "#/components/schemas/Event"}},
          "attempts": {"type": "object", "additionalProperties": {"type": "array", "items": {"$ref": "#/components/schemas/Attempt"}}},
          "artifacts": {"type": "object", "additionalProperties": {"type": "array", "items": {"$ref": "#/components/schemas/Artifact"}}}
        }
      }
    }
  }
}`

func (d *Daemon) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(OpenAPI))
}

var _ = json.Valid
