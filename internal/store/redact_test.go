package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/graph"
)

func TestRedactPreservesJSON(t *testing.T) {
	in := `{"apiKey":"abcdef","nested":{"access_token":"tokensecret1"},"items":[{"client_secret":"hunter2hunter2"}],"authorization":"Bearer abcdefgh","aws_secret_access_key":"aws-secret-value","private_key":"private-key-value","accessKeySecret":"access-key-value","large":9007199254740993,"maxTokens":123,"tokenCount":4,"promptTokens":5,"completionTokens":6,"totalTokens":11,"cachedTokens":2,"cacheCreationTokens":1,"cacheReadTokens":2}`
	got := Redact(in)
	if !json.Valid([]byte(got)) {
		t.Fatalf("Redact returned invalid JSON: %q", got)
	}
	for _, secret := range []string{"abcdef", "tokensecret1", "hunter2hunter2", "abcdefgh", "aws-secret-value", "private-key-value", "access-key-value"} {
		if strings.Contains(got, secret) {
			t.Fatalf("Redact(%q) still contains %q: %q", in, secret, got)
		}
	}
	if !strings.Contains(got, "9007199254740993") {
		t.Fatalf("Redact changed exact JSON number: %q", got)
	}
	for _, accounting := range []string{`"maxTokens":123`, `"tokenCount":4`, `"promptTokens":5`, `"completionTokens":6`, `"totalTokens":11`, `"cachedTokens":2`, `"cacheCreationTokens":1`, `"cacheReadTokens":2`} {
		if !strings.Contains(got, accounting) {
			t.Fatalf("Redact changed non-secret accounting key %s: %q", accounting, got)
		}
	}
}

func TestRedactJSONScalarString(t *testing.T) {
	got := Redact(`"Bearer sk-verysecretkey1234567890"`)
	if strings.Contains(got, "verysecretkey") || !json.Valid([]byte(got)) {
		t.Fatalf("Redact leaked scalar JSON secret or broke JSON: %q", got)
	}
}

func TestRedact(t *testing.T) {
	cases := []struct{ in, wantAbsent string }{
		{"Authorization: Bearer sk-secret1234567890abc", "sk-secret1234567890abc"},
		{"api_key = 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822c", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822c"},
		{"password: hunter2hunter2hunter2", "hunter2hunter2hunter2"},
		{"plain text with no secrets", ""},
		{"token = 1234567890abcdef1234567890", "1234567890abcdef1234567890"},
	}
	for _, c := range cases {
		got := Redact(c.in)
		if c.wantAbsent != "" && strings.Contains(got, c.wantAbsent) {
			t.Errorf("Redact(%q) = %q still contains %q", c.in, got, c.wantAbsent)
		}
		if c.wantAbsent == "" && got != c.in {
			t.Errorf("Redact(%q) = %q, want unchanged", c.in, got)
		}
	}
}

func TestSecretsNeverPersisted(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	g := &graph.Graph{Nodes: []*graph.Node{{
		ID: "w1", Type: graph.NodeAgent, Objective: "o", AcceptanceCriteria: []string{"c"},
	}}}
	if err := st.CreateRun(ctx, "r1", g, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	secret := "Bearer sk-verysecretkey1234567890abcdef"
	ts := time.Now().UnixMilli()
	if err := st.RecordAttempt(ctx, Attempt{
		ID: "r1/w1/1", RunID: "r1", NodeID: "w1", No: 1, Status: "done",
		Evidence: "verifier saw " + secret, FinishedAt: &ts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordArtifact(ctx, Artifact{
		RunID: "r1", AttemptID: "r1/w1/1", NodeID: "w1", Name: "diff",
		Hash: "h", Content: "output with " + secret,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendEvent(ctx, "r1", "w1", EventVerdict, "", "", "r1/w1/1", `{"note":"`+secret+`"}`, time.Now()); err != nil {
		t.Fatal(err)
	}

	atts, _ := st.Attempts(ctx, "r1", "w1")
	if strings.Contains(atts[0].Evidence, "sk-verysecretkey") {
		t.Fatalf("evidence leaked secret: %q", atts[0].Evidence)
	}
	arts, _ := st.Artifacts(ctx, "r1", "r1/w1/1")
	if strings.Contains(arts[0].Content, "sk-verysecretkey") {
		t.Fatalf("artifact leaked secret: %q", arts[0].Content)
	}
	evs, _ := st.Events(ctx, "r1")
	if strings.Contains(string(evs[len(evs)-1].Payload), "sk-verysecretkey") {
		t.Fatalf("event leaked secret: %q", evs[len(evs)-1].Payload)
	}
}
