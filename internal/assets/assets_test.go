package assets

import (
	"encoding/json"
	"os"
	"testing"
)

// TestEmbeddedMatchesRepoCopies guards against drift between the embedded
// (canonical) plugin/config and the reference copies at the repo root
// that humans browsing the repository see.
func TestEmbeddedMatchesRepoCopies(t *testing.T) {
	root := "../../"
	plugin, err := os.ReadFile(root + ".opencode/tools/corral.ts")
	if err != nil {
		t.Fatal(err)
	}
	if string(plugin) != CorralPluginTS {
		t.Error("internal/assets/corral.ts diverged from .opencode/tools/corral.ts")
	}
	cfg, err := os.ReadFile(root + "example/opencode.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(cfg) != OpenCodeConfigJSON {
		t.Error("internal/assets/opencode.json diverged from example/opencode.json")
	}
}

func TestEmbeddedContentValid(t *testing.T) {
	if CorralPluginTS == "" || !contains(CorralPluginTS, "corral_plan") {
		t.Error("embedded plugin missing corral tools")
	}
	if !contains(OpenCodeConfigJSON, `"corral-orchestrator"`) || !contains(OpenCodeConfigJSON, `"corral-planner"`) {
		t.Error("embedded config missing agents")
	}
	if !contains(CorralPluginTS, `"graph" in parsed`) || !contains(CorralPluginTS, `(parsed as { graph: unknown }).graph`) {
		t.Error("embedded start tool does not unwrap full corral_plan output")
	}
	if contains(CorralPluginTS, `return "operator"`) || contains(CorralPluginTS, `role ?? "operator"`) {
		t.Error("embedded plugin lets unknown model agents mint operator authority")
	}
	if contains(CorralPluginTS, "autoApproveGates: tool.schema") {
		t.Error("embedded plugin lets an orchestrator request its own gate pre-authorization")
	}
}

func TestEmbeddedAgentPoliciesFailClosed(t *testing.T) {
	var cfg struct {
		Agent map[string]struct {
			Tools      map[string]any `json:"tools"`
			Permission map[string]any `json:"permission"`
		} `json:"agent"`
	}
	if err := json.Unmarshal([]byte(OpenCodeConfigJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"corral-orchestrator", "corral-planner", "corral-worker", "corral-reviewer", "corral-merger"} {
		agent, ok := cfg.Agent[name]
		if !ok {
			t.Fatalf("managed agent %q missing", name)
		}
		if len(agent.Tools) != 0 {
			t.Errorf("%s has legacy tools override: %#v", name, agent.Tools)
		}
		if agent.Permission["*"] != "deny" {
			t.Errorf("%s wildcard permission = %#v, want deny", name, agent.Permission["*"])
		}
		if task, ok := agent.Permission["task"]; ok && task != "deny" {
			t.Errorf("%s can delegate around its sandbox: %#v", name, task)
		}
	}
	for name, action := range map[string]string{
		"corral_plan": "allow", "corral_start": "allow", "corral_status": "allow",
		"corral_watch": "allow", "corral_approve": "allow", "corral_reject": "allow",
		"corral_cancel": "allow", "corral_retry": "allow", "corral_steer": "allow",
	} {
		if cfg.Agent["corral-orchestrator"].Permission[name] != action {
			t.Errorf("orchestrator %s = %#v, want %s", name, cfg.Agent["corral-orchestrator"].Permission[name], action)
		}
	}
	if cfg.Agent["corral-planner"].Permission["corral_plan"] != "allow" {
		t.Error("planner cannot call corral_plan")
	}
	if cfg.Agent["corral-planner"].Permission["glob"] != "allow" {
		t.Error("planner cannot inspect repository structure")
	}
	worker := cfg.Agent["corral-worker"].Permission
	if worker["edit"] != "ask" || worker["bash"] != "ask" || worker["glob"] != "allow" {
		t.Errorf("worker policy = %#v", worker)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
