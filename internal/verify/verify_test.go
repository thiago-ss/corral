package verify

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/graph"
)

func node(kind string, v *graph.Verification) *graph.Node {
	n := &graph.Node{
		ID:                 "n1",
		Type:               graph.NodeAgent,
		Objective:          "work",
		AcceptanceCriteria: []string{"c"},
		Verification:       v,
	}
	_ = kind
	return n
}

type fakeRunner struct {
	calls  int
	script []struct {
		exit   int
		stdout string
		stderr string
	}
}

func (f *fakeRunner) Run(_ context.Context, _ string, _ []string, _ time.Duration) (int, string, string, error) {
	s := f.script[f.calls%len(f.script)]
	f.calls++
	return s.exit, s.stdout, s.stderr, nil
}

func msgsWithDiff() []adapter.Message {
	return []adapter.Message{
		{Role: "user", Diffs: []adapter.Diff{{File: "out.txt", Patch: "+x"}}},
		{Role: "assistant", Finish: "stop", Text: "done"},
	}
}

func proseOnly() []adapter.Message {
	return []adapter.Message{{Role: "assistant", Finish: "stop", Text: "I thought about it; everything is fine."}}
}

func TestCommandGatePassAndFail(t *testing.T) {
	eng := New(t.TempDir())
	eng.Runner = &fakeRunner{script: []struct {
		exit   int
		stdout string
		stderr string
	}{
		{exit: 1, stderr: "missing file: out.txt"},
		{exit: 0, stdout: "ok"},
	}}
	n := node("command", &graph.Verification{Kind: "command", Command: []string{"test", "-f", "out.txt"}})

	res, err := eng.Verify(context.Background(), n, "", 1, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass {
		t.Fatal("gate passed on failing command")
	}
	if res.Feedback == "" || res.Feedback != "missing file: out.txt" {
		t.Fatalf("feedback = %q, want stderr focused feedback", res.Feedback)
	}

	res, err = eng.Verify(context.Background(), n, "", 2, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Pass {
		t.Fatalf("gate failed on passing command: %+v", res)
	}
}

func TestProseAloneFails(t *testing.T) {
	eng := New(t.TempDir())
	n := node("default", nil)
	res, err := eng.Verify(context.Background(), n, "", 1, proseOnly())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass {
		t.Fatal("prose-only attempt passed the default gate")
	}
	if res.Feedback == "" {
		t.Fatal("prose-only failure has no feedback")
	}
	res, err = eng.Verify(context.Background(), n, "", 2, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Pass {
		t.Fatal("attempt with diffs failed the default gate")
	}
}

func TestJSONSchemaGate(t *testing.T) {
	worktree := t.TempDir()
	schema := `{
		"type": "object",
		"properties": {"name": {"type": "string"}, "count": {"type": "integer", "minimum": 1}},
		"required": ["name"]
	}`
	eng := New(worktree)
	n := node("json_schema", &graph.Verification{Kind: "json_schema", Schema: schema, Target: "manifest.json"})

	// Missing artifact.
	res, err := eng.Verify(context.Background(), n, worktree, 1, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass || res.Feedback == "" {
		t.Fatalf("missing artifact should fail with feedback: %+v", res)
	}

	// Invalid JSON / schema violation.
	os.WriteFile(filepath.Join(worktree, "manifest.json"), []byte(`{"name":"x","count":0}`), 0o644)
	res, err = eng.Verify(context.Background(), n, worktree, 2, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass {
		t.Fatal("schema violation passed")
	}
	if res.Feedback == "" {
		t.Fatal("schema violation has no feedback")
	}

	// Valid.
	os.WriteFile(filepath.Join(worktree, "manifest.json"), []byte(`{"name":"x","count":3}`), 0o644)
	res, err = eng.Verify(context.Background(), n, worktree, 3, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Pass {
		t.Fatalf("valid artifact failed: %+v", res)
	}
}

type fakeReviewer struct {
	script []struct {
		approved bool
		note     string
	}
	calls int
}

func (f *fakeReviewer) Review(_ context.Context, req ReviewRequest) (bool, string, error) {
	if len(f.script) == 0 {
		return true, "", nil
	}
	s := f.script[f.calls%len(f.script)]
	f.calls++
	if !s.approved && req.Feedback != "" {
		// feedback must be carried into the review request
		s.note = s.note + " [prior: " + req.Feedback + "]"
	}
	return s.approved, s.note, nil
}

func TestReviewerGate(t *testing.T) {
	eng := New(t.TempDir())
	eng.Reviewer = &fakeReviewer{script: []struct {
		approved bool
		note     string
	}{
		{approved: false, note: "wrong order of fields"},
		{approved: true},
	}}
	n := node("reviewer", &graph.Verification{Kind: "reviewer"})
	res, err := eng.Verify(context.Background(), n, "", 1, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass || res.Feedback != "wrong order of fields" {
		t.Fatalf("reviewer rejection not surfaced: %+v", res)
	}
	res, err = eng.Verify(context.Background(), n, "", 2, msgsWithDiff())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Pass {
		t.Fatalf("reviewer approval not honored: %+v", res)
	}
}

func TestCheckNodeVerdictFromMeta(t *testing.T) {
	eng := New(t.TempDir())
	n := &graph.Node{
		ID:           "check1",
		Type:         graph.NodeCheck,
		Objective:    "run checks",
		Verification: &graph.Verification{Kind: "command", Command: []string{"go", "test"}},
	}
	msgs := []adapter.Message{{Role: "assistant", Finish: "stop", Meta: map[string]string{
		"exit": "1", "stdout": "", "stderr": "FAIL: TestFoo",
	}}}
	res, err := eng.Verify(context.Background(), n, "", 1, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Pass || res.Feedback != "FAIL: TestFoo" {
		t.Fatalf("failing check verdict wrong: %+v", res)
	}
	msgs[0].Meta["exit"] = "0"
	res, err = eng.Verify(context.Background(), n, "", 1, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Pass {
		t.Fatalf("passing check verdict wrong: %+v", res)
	}
}

func TestExecRunnerReportsStartFailure(t *testing.T) {
	// A command whose binary does not exist must not be reported as a
	// successful run: callers treat a non-nil error as "could not start",
	// and must never see a clean exit 0 for a check that never ran.
	exit, stdout, stderr, err := ExecRunner{}.Run(context.Background(), t.TempDir(), []string{"definitely-not-a-real-binary-xyz"}, 0)
	if err == nil {
		t.Fatalf("expected a start error, got exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	if exit != 0 {
		t.Fatalf("start failure should keep exit 0 with a non-nil error, got exit=%d", exit)
	}
}

func TestCommandGateDoesNotPassOnMissingBinary(t *testing.T) {
	eng := New(t.TempDir())
	n := node("command", &graph.Verification{Kind: "command", Command: []string{"definitely-not-a-real-binary-xyz"}})
	res, err := eng.Verify(context.Background(), n, "", 1, msgsWithDiff())
	if err == nil {
		t.Fatalf("gate returned no error but the command never ran: %+v", res)
	}
	if res.Pass {
		t.Fatal("gate passed even though the verification command could not be started")
	}
}
