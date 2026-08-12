package assets

import (
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
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
