package daemon_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"

	"corral/internal/clock"
	"corral/internal/daemon"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
)

func getJSON(base, path string, out any) error {
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	req.Header.Set("X-Corral-Role", "operator")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSON(base, path string, body, out any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Corral-Role", "operator")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return &httpError{code: resp.StatusCode, body: string(data)}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type httpError struct {
	code int
	body string
}

func (e *httpError) Error() string { return e.body }

// TestOpenAPIContract verifies the daemon API against its OpenAPI
// document: every registered route is documented, and live responses
// validate against the document's schemas.
func TestOpenAPIContract(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	s := sched.New(st, sched.NewFakeDriver(clock.Real{}, nil), &sched.EngineVerifier{Eng: verify.New(t.TempDir())}, clock.Real{}, sched.Options{})
	d := daemon.New(st, s, nil, t.TempDir(), "")
	t.Cleanup(d.Close)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)

	// 1. The document is served and valid.
	var doc map[string]any
	if err := getJSON(srv.URL, "/doc", &doc); err != nil {
		t.Fatalf("get doc: %v", err)
	}
	paths, _ := doc["paths"].(map[string]any)
	registered := []string{
		"/api/health", "/api/plan", "/api/runs", "/api/runs/{id}",
		"/api/runs/{id}/watch", "/api/runs/{id}/events",
		"/api/runs/{id}/approve", "/api/runs/{id}/reject", "/api/runs/{id}/cancel",
		"/api/runs/{id}/retry", "/api/runs/{id}/steer", "/api/runs/{id}/permission",
		"/api/runs/{id}/export", "/doc",
	}
	for _, p := range registered {
		if _, ok := paths[p]; !ok {
			t.Errorf("route %s missing from OpenAPI document", p)
		}
	}

	// 2. Live responses validate against the document schemas.
	compiler := jsonschema.NewCompiler()
	for name, schema := range doc["components"].(map[string]any)["schemas"].(map[string]any) {
		raw, _ := json.Marshal(schema)
		if err := compiler.AddResource("schemas/"+name, strings.NewReader(string(raw))); err != nil {
			t.Fatalf("compile schema %s: %v", name, err)
		}
	}
	validate := func(schemaName string, body []byte) error {
		sch, err := compiler.Compile("schemas/" + schemaName)
		if err != nil {
			return err
		}
		var v any
		if err := json.Unmarshal(body, &v); err != nil {
			return err
		}
		return sch.Validate(v)
	}

	resp, err := http.Get(srv.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	health, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := validate("Health", health); err != nil {
		t.Errorf("health response invalid: %v", err)
	}
	resp, err = http.Get(srv.URL + "/api/runs")
	if err != nil {
		t.Fatal(err)
	}
	runs, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var runsArr any
	json.Unmarshal(runs, &runsArr)
	sch, _ := compiler.Compile("schemas/RunSummary")
	items, _ := runsArr.([]any)
	for _, it := range items {
		if err := sch.Validate(it); err != nil {
			t.Errorf("run summary invalid: %v", err)
		}
	}
}

// TestAuditExport covers the full audit trail: graph, states, events,
// attempts with sessions/worktrees and content-addressed artifacts.
func TestAuditExport(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "e.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	workdir := t.TempDir()
	drv := sched.NewFakeDriver(clock.Real{}, nil)
	eng := verify.New(workdir)
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clock.Real{}, sched.Options{Concurrency: 2})
	d := daemon.New(st, s, nil, workdir, "")
	t.Cleanup(d.Close)
	ctx := context.Background()
	d.SetContext(ctx)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)

	g := &graph.Graph{Nodes: []*graph.Node{{
		ID: "w1", Type: graph.NodeAgent, Role: "worker",
		Objective: "write a.txt", AcceptanceCriteria: []string{"a.txt"},
		Priority: graph.PriorityNormal, WriteScope: []string{"a.txt"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}},
		Meta:         map[string]string{"cwd": workdir},
	}}}
	drv.SetScript("w1", sched.Script{Delay: 100 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})
	var created struct{ RunID string }
	if err := postJSON(srv.URL, "/api/runs", map[string]any{"graph": g}, &created); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var detail struct {
		Status string `json:"status"`
		Done   bool   `json:"done"`
	}
	for time.Now().Before(deadline) {
		if err := getJSON(srv.URL, "/api/runs/"+created.RunID, &detail); err == nil && detail.Done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !detail.Done {
		t.Fatal("run did not complete")
	}

	// Export contains everything needed to reconstruct provenance.
	var export struct {
		RunID     string                       `json:"runID"`
		Status    string                       `json:"status"`
		Exported  string                       `json:"exportedAt"`
		Events    []json.RawMessage            `json:"events"`
		Attempts  map[string][]json.RawMessage `json:"attempts"`
		Artifacts map[string][]json.RawMessage `json:"artifacts"`
		States    map[string]string            `json:"states"`
		Graph     json.RawMessage              `json:"graph"`
	}
	if err := getJSON(srv.URL, "/api/runs/"+created.RunID+"/export", &export); err != nil {
		t.Fatal(err)
	}
	if export.RunID != created.RunID || export.Status != "completed" || export.Exported == "" {
		t.Fatalf("export header wrong: %+v", export)
	}
	if len(export.Events) < 5 {
		t.Fatalf("export events too few: %d", len(export.Events))
	}
	if len(export.Attempts["w1"]) != 1 {
		t.Fatalf("export attempts wrong: %+v", export.Attempts)
	}
	if len(export.Artifacts) == 0 {
		t.Fatal("export has no artifacts")
	}
	for _, arts := range export.Artifacts {
		for _, a := range arts {
			var art struct {
				Name  string `json:"name"`
				Hash  string `json:"hash"`
				Path  string `json:"path"`
				Patch string `json:"content"`
			}
			json.Unmarshal(a, &art)
			if art.Name != "diff" || art.Hash == "" || art.Patch == "" {
				t.Fatalf("artifact malformed: %+v", art)
			}
		}
	}
	if export.States["w1"] != "done" {
		t.Fatalf("export states wrong: %v", export.States)
	}
	if !strings.Contains(string(export.Graph), `"w1"`) {
		t.Fatal("export graph missing nodes")
	}
}

var _ = daemon.RoleOperator
