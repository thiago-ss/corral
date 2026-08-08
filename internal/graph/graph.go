// Package graph defines the corral task graph schema, its validation, the
// per-node state machine, deterministic ready-node computation, and the
// proposal mechanism for graph changes.
package graph

import "time"

type NodeType string

const (
	NodeAgent NodeType = "agent"      // runs an agent session against a worktree
	NodeCheck NodeType = "check"      // verification/evidence gate, no agent
	NodeMerge NodeType = "merge"      // merges accepted work into the main checkout
	NodeHuman NodeType = "human_gate" // manual approval required
)

type Priority int

const (
	PriorityLow      Priority = 10
	PriorityNormal   Priority = 50
	PriorityHigh     Priority = 90
	PriorityCritical Priority = 100
)

type NodeID string

type ArtifactRef struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Kind string `json:"kind"` // "file" | "dir" | "diff" | "url"
}

type Verification struct {
	Kind     string   `json:"kind"` // "command" | "json_schema" | "reviewer"
	Command  []string `json:"command,omitempty"`
	Schema   string   `json:"schema,omitempty"`   // JSON schema document
	Target   string   `json:"target,omitempty"`   // artifact path validated by json_schema (relative to worktree)
	Reviewer NodeID   `json:"reviewer,omitempty"` // node that performs the review
}

type RetryPolicy struct {
	MaxRetries int           `json:"maxRetries"`
	Backoff    time.Duration `json:"backoff"`
}

type Budget struct {
	MaxDuration time.Duration `json:"maxDuration"`
	MaxTokens   int           `json:"maxTokens"`
	MaxCost     float64       `json:"maxCost"`
}

type Node struct {
	ID                 NodeID            `json:"id"`
	Type               NodeType          `json:"type"`
	Objective          string            `json:"objective"`
	AcceptanceCriteria []string          `json:"acceptanceCriteria,omitempty"`
	Role               string            `json:"role,omitempty"` // "worker" | "reviewer" | "merger" | ...
	Model              string            `json:"model,omitempty"`
	Priority           Priority          `json:"priority"`
	DependsOn          []NodeID          `json:"dependsOn,omitempty"`
	InputArtifacts     []ArtifactRef     `json:"inputArtifacts,omitempty"`
	OutputArtifacts    []ArtifactRef     `json:"outputArtifacts,omitempty"`
	WriteScope         []string          `json:"writeScope,omitempty"` // declared writable paths
	Verification       *Verification     `json:"verification,omitempty"`
	RetryPolicy        RetryPolicy       `json:"retryPolicy"`
	Budget             Budget            `json:"budget"`
	Meta               map[string]string `json:"meta,omitempty"`
}

type Graph struct {
	Version int     `json:"version"`
	Nodes   []*Node `json:"nodes"`
}

const (
	MaxNodes  = 100
	MaxFanOut = 8 // max dependents (out-edges) per node
)

// Validation limits for node content.
const (
	maxObjectiveLen  = 4096
	maxCriteriaLen   = 2048
	maxCriteriaCount = 16
)
