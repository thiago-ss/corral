package ocxreviewer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/livetest"
	"corral/internal/ocx"
	"corral/internal/spike"
	"corral/internal/verify"
)

// fakeLLM is a scripted OpenCode server: it records review prompts and
// serves a fixed sequence of transcripts, mirroring the fake-reviewer
// script surface. The last transcript repeats, so a reviewer that keeps
// polling settles on it deterministically.
type fakeLLM struct {
	mu       sync.Mutex
	steps    [][]ocx.Message
	step     int
	prompts  []string
	sessions int
}

func newFakeLLM(steps ...[]ocx.Message) *fakeLLM {
	return &fakeLLM{steps: steps}
}

func (f *fakeLLM) serve() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			f.sessions++
			_ = json.NewEncoder(w).Encode(ocx.Session{ID: "ses_1", Directory: "proj", Title: "review"})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/prompt_async"):
			var body struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			for _, p := range body.Parts {
				f.prompts = append(f.prompts, p.Text)
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/message"):
			// Serve the next scripted transcript; the last one repeats so
			// a reviewer that keeps polling settles on it deterministically.
			if f.step < len(f.steps)-1 {
				f.step++
			}
			_ = json.NewEncoder(w).Encode(f.steps[f.step])
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func llmBusy() []ocx.Message {
	return []ocx.Message{{Info: ocx.MessageInfo{Role: "assistant"}}}
}

func llmText(text string) ocx.Message {
	finish := "stop"
	part, _ := json.Marshal(map[string]string{"type": "text", "text": text})
	return ocx.Message{
		Info:  ocx.MessageInfo{Role: "assistant", Finish: &finish},
		Parts: []json.RawMessage{part},
	}
}

func llmError(name string) []ocx.Message {
	raw, _ := json.Marshal(map[string]string{"name": name})
	rm := json.RawMessage(raw)
	return []ocx.Message{{Info: ocx.MessageInfo{Role: "assistant", Error: &rm}}}
}

func reviewReq(worktree string) verify.ReviewRequest {
	return verify.ReviewRequest{
		Attempt: adapter.Attempt{
			ID:        "run_1/w1/1",
			NodeID:    "w1",
			Objective: "create manifest.json with a name field",
			Role:      "worker",
			Cwd:       worktree,
		},
		Worktree: worktree,
		Feedback: "manifest.json is missing the name field",
		Messages: []adapter.Message{
			{Role: "user", Text: "create manifest.json", Diffs: []adapter.Diff{
				{File: "manifest.json", Patch: "+name: x", Additions: 1, Status: "added"},
			}},
			{Role: "assistant", Finish: "stop", Text: "created manifest.json", Meta: map[string]string{
				"exit": "0", "stdout": "ok",
			}},
		},
	}
}

func TestReviewerApproved(t *testing.T) {
	llm := newFakeLLM(
		llmBusy(), // session starts streaming: not terminal yet
		[]ocx.Message{llmText("APPROVED\nNote: manifest.json now has a name field and matches the schema.")},
	)
	srv := llm.serve()
	defer srv.Close()

	drv := New(ocx.New(srv.URL, t.TempDir()), Options{PollInterval: time.Millisecond, Timeout: 5 * time.Second})
	approved, note, err := drv.Review(context.Background(), reviewReq(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if !approved {
		t.Fatal("approved = false, want true")
	}
	if !strings.Contains(note, "name field") {
		t.Fatalf("note = %q, want the reviewer's justification", note)
	}

	if len(llm.prompts) != 1 {
		t.Fatalf("review prompts = %d, want 1", len(llm.prompts))
	}
	for _, want := range []string{"OBJECTIVE", "create manifest.json", "PRIOR FEEDBACK", "DIFF ARTIFACT", "manifest.json", "CHECK RESULTS", "exit=0"} {
		if !strings.Contains(llm.prompts[0], want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}

func TestReviewerNotApproved(t *testing.T) {
	llm := newFakeLLM([]ocx.Message{llmText("NOT_APPROVED\nNote: the manifest is still missing the required count field.")})
	srv := llm.serve()
	defer srv.Close()

	drv := New(ocx.New(srv.URL, t.TempDir()), Options{PollInterval: time.Millisecond, Timeout: 5 * time.Second})
	approved, note, err := drv.Review(context.Background(), reviewReq(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if approved {
		t.Fatal("approved = true, want false")
	}
	if !strings.Contains(note, "count field") {
		t.Fatalf("note = %q, want rejection feedback", note)
	}
}

func TestReviewerSessionError(t *testing.T) {
	llm := newFakeLLM(llmError("MessageAbortedError"))
	srv := llm.serve()
	defer srv.Close()

	drv := New(ocx.New(srv.URL, t.TempDir()), Options{PollInterval: time.Millisecond, Timeout: 5 * time.Second})
	if _, _, err := drv.Review(context.Background(), reviewReq(t.TempDir())); err == nil {
		t.Fatal("session error not surfaced")
	} else if !strings.Contains(err.Error(), "MessageAbortedError") {
		t.Fatalf("error = %q, want session error name", err)
	}
}

func TestReviewerNoVerdict(t *testing.T) {
	llm := newFakeLLM([]ocx.Message{llmText("The changes look fine to me; ship it.")})
	srv := llm.serve()
	defer srv.Close()

	drv := New(ocx.New(srv.URL, t.TempDir()), Options{PollInterval: time.Millisecond, Timeout: 5 * time.Second})
	if _, _, err := drv.Review(context.Background(), reviewReq(t.TempDir())); err == nil {
		t.Fatal("missing verdict not surfaced")
	} else if !strings.Contains(err.Error(), "no explicit verdict") {
		t.Fatalf("error = %q, want missing-verdict error", err)
	}
}

func TestReviewerTimeout(t *testing.T) {
	llm := newFakeLLM(llmBusy()) // never reaches a terminal message
	srv := llm.serve()
	defer srv.Close()

	drv := New(ocx.New(srv.URL, t.TempDir()), Options{PollInterval: time.Millisecond, Timeout: 50 * time.Millisecond})
	if _, _, err := drv.Review(context.Background(), reviewReq(t.TempDir())); err == nil {
		t.Fatal("timeout not surfaced")
	} else if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %q, want timeout error", err)
	}
}

func TestParseVerdict(t *testing.T) {
	cases := []struct {
		text     string
		approved bool
		note     string
		ok       bool
	}{
		{"APPROVED\nNote: good work", true, "good work", true},
		{"NOT_APPROVED\nNote: missing tests", false, "missing tests", true},
		{"NOT APPROVED.\nNote: wrong order.", false, "wrong order.", true},
		{"We approve.\nAPPROVED\nNote: verified by hand.", true, "verified by hand.", true},
		{"no verdict anywhere", false, "", false},
		{"NOT_APPROVED without a note", false, "", true},
	}
	for _, c := range cases {
		approved, note, ok := parseVerdict(c.text)
		if ok != c.ok || approved != c.approved || note != c.note {
			t.Errorf("parseVerdict(%q) = (%v, %q, %v), want (%v, %q, %v)",
				c.text, approved, note, ok, c.approved, c.note, c.ok)
		}
	}
}

func TestPromptForIncludesEvidence(t *testing.T) {
	req := reviewReq("/tmp/worktree")
	p := promptFor(req)
	for _, want := range []string{
		"OBJECTIVE",
		"ATTEMPT ROLE: worker",
		"WORKTREE: /tmp/worktree",
		"PRIOR FEEDBACK",
		"DIFF ARTIFACT",
		"manifest.json",
		"CHECK RESULTS",
		"exit=0",
		"TRANSCRIPT",
		"VERDICT",
		"NOT_APPROVED",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}

func TestOpenCodeReviewerLive(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-review-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(proj, "manifest.json"), []byte(`{"name":"x"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	drv := New(ocx.New(srv.Base, proj), Options{PollInterval: 400 * time.Millisecond, Timeout: 8 * time.Minute})
	approved, note, err := drv.Review(ctx, reviewReq(proj))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(note) == "" {
		t.Error("reviewer returned an empty note")
	}
	t.Logf("verdict: approved=%v note=%q", approved, note)
}
