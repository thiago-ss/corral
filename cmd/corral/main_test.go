package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/livetest"
	"corral/internal/tui"
	"corral/internal/worktree"
)

// captureOutput runs fn with stdout redirected and returns its output.
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	fn()
	w.Close()
	data, _ := io.ReadAll(r)
	return string(data)
}

func gitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	return dir
}

func TestInitCmd(t *testing.T) {
	dir := gitRepo(t)
	out := captureOutput(t, func() {
		if err := initCmd(dir); err != nil {
			t.Errorf("init: %v", err)
		}
	})
	keyFile := filepath.Join(dir, ".corral", "api.key")
	data, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("api key missing: %v", err)
	}
	key := strings.TrimSpace(string(data))
	if len(key) != 32 {
		t.Fatalf("api key length = %d, want 32 hex chars", len(key))
	}
	if fi, _ := os.Stat(keyFile); fi.Mode().Perm() != 0o600 {
		t.Fatalf("api key perms = %v, want 0600", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(dir, ".corral", "config.json")); err != nil {
		t.Fatalf("config missing: %v", err)
	}
	if !strings.Contains(out, "initialized corral") {
		t.Fatalf("init output wrong: %s", out)
	}
	// The plugin and agent config are installed automatically.
	if data, err := os.ReadFile(filepath.Join(dir, ".opencode", "tools", "corral.ts")); err != nil || !strings.Contains(string(data), "corral_plan") {
		t.Fatalf("plugin not installed: %v", err)
	}
	cfgData, err := os.ReadFile(filepath.Join(dir, "opencode.json"))
	if err != nil || !strings.Contains(string(cfgData), `"corral-orchestrator"`) {
		t.Fatalf("agent config not installed: %v", err)
	}
	// Idempotent: second init keeps the same key.
	if err := initCmd(dir); err != nil {
		t.Fatal(err)
	}
	data2, _ := os.ReadFile(keyFile)
	if string(data2) != string(data) {
		t.Fatal("init regenerated the api key")
	}
}

func TestInitFailsOutsideGit(t *testing.T) {
	if err := initCmd(t.TempDir()); err == nil {
		t.Fatal("init succeeded outside a git repository")
	}
}

func TestDoctorReportsProblems(t *testing.T) {
	dir := gitRepo(t)
	// No daemon running: doctor must fail and print the check lines.
	var err error
	out := captureOutput(t, func() { err = doctorCmd(dir, "") })
	if err == nil {
		t.Fatal("doctor should fail when daemon is down")
	}
	s := out
	for _, want := range []string{"corral doctor", "opencode", "git repository", "daemon", "plugin", "config"} {
		if !strings.Contains(s, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, s)
		}
	}
}

func TestDoctorPassesWithDaemonUp(t *testing.T) {
	dir := gitRepo(t)
	// Provide a stub opencode so the version check passes on CI runners
	// that do not have it installed.
	stub := filepath.Join(t.TempDir(), "opencode")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\necho 1.18.15\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	origPath := os.Getenv("PATH")
	t.Setenv("PATH", filepath.Dir(stub)+string(os.PathListSeparator)+origPath)
	// Simulate a healthy daemon.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"healthy":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CORRAL_DAEMON_URL", srv.URL)
	// Write the plugin + config so the remaining checks pass.
	os.MkdirAll(filepath.Join(dir, ".opencode", "tools"), 0o755)
	os.WriteFile(filepath.Join(dir, ".opencode", "tools", "corral.ts"), []byte("// x"), 0o644)
	os.MkdirAll(filepath.Join(dir, ".corral"), 0o755)
	os.WriteFile(filepath.Join(dir, ".corral", "config.json"), []byte("{}"), 0o644)

	var err error
	out := captureOutput(t, func() { err = doctorCmd(dir, "") })
	if err != nil {
		t.Fatalf("doctor should pass: %v\n%s", err, out)
	}
	if strings.Contains(out, "✗") {
		t.Fatalf("doctor reported failures:\n%s", out)
	}
}

func TestExportCommand(t *testing.T) {
	// Fake daemon serving a minimal export and recording the bearer key.
	payload := `{"runID":"run_1","status":"completed","events":[],"attempts":{},"artifacts":{}}`
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		w.Write([]byte(payload))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CORRAL_DAEMON_URL", srv.URL)
	t.Setenv("CORRAL_DAEMON_KEY", "")

	// Serve the key from the repository, as status/tui/doctor do.
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".corral"), 0o755)
	if err := os.WriteFile(filepath.Join(dir, ".corral", "api.key"), []byte("repo-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	outFile := filepath.Join(dir, "audit.json")
	if err := exportCmd("run_1", outFile); err != nil {
		t.Fatalf("export: %v", err)
	}
	if gotKey != "repo-key" {
		t.Fatalf("export used key %q, want repo key from .corral/api.key", gotKey)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"runID": "run_1"`) {
		t.Fatalf("export file wrong: %s", data)
	}
}

func TestExportCommandEnvOverride(t *testing.T) {
	// The env var must still override the repository key.
	payload := `{"runID":"run_1","status":"completed","events":[],"attempts":{},"artifacts":{}}`
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		w.Write([]byte(payload))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CORRAL_DAEMON_URL", srv.URL)
	t.Setenv("CORRAL_DAEMON_KEY", "env-key")

	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".corral"), 0o755)
	os.WriteFile(filepath.Join(dir, ".corral", "api.key"), []byte("repo-key\n"), 0o600)

	outFile := filepath.Join(dir, "audit.json")
	if err := exportCmd("run_1", outFile); err != nil {
		t.Fatalf("export: %v", err)
	}
	if gotKey != "env-key" {
		t.Fatalf("export used key %q, want env override", gotKey)
	}
}

func TestInitMergesExistingOpenCodeConfig(t *testing.T) {
	dir := gitRepo(t)
	// Pre-existing opencode.json with custom settings must be preserved.
	existing := `{
		"theme": "dark",
		"agent": {"build": {"model": "custom/model"}}
	}`
	if err := os.WriteFile(filepath.Join(dir, "opencode.json"), []byte(existing), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := initCmd(dir); err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	data, _ := os.ReadFile(filepath.Join(dir, "opencode.json"))
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["theme"] != "dark" {
		t.Fatalf("existing config clobbered: %s", data)
	}
	agents := cfg["agent"].(map[string]any)
	if agents["build"] == nil {
		t.Fatalf("existing agent lost: %s", data)
	}
	for _, name := range []string{"corral-orchestrator", "corral-planner", "corral-worker", "corral-reviewer", "corral-merger"} {
		if agents[name] == nil {
			t.Fatalf("corral agent %s missing: %s", name, data)
		}
	}
}

func TestInitBacksUpInvalidConfig(t *testing.T) {
	dir := gitRepo(t)
	os.WriteFile(filepath.Join(dir, "opencode.json"), []byte("{not json"), 0o644)
	if err := initCmd(dir); err != nil {
		t.Fatalf("init with invalid config: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "opencode.json.bak")); err != nil {
		t.Fatal("invalid config not backed up")
	}
}

func TestUpCmdSpawnsHealthyDaemon(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode not found")
	}
	dir := gitRepo(t)
	// Build the real binary for the detached spawn.
	bin := filepath.Join(t.TempDir(), "corral")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, out)
	}
	t.Setenv("CORRAL_BIN", bin)

	// Use a dedicated port to avoid clashing with any live daemon.
	port := freePort()
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	t.Setenv("CORRAL_DAEMON_URL", url)
	out := captureOutput(t, func() {
		if err := upCmdWithPort(dir, port); err != nil {
			t.Errorf("up: %v", err)
		}
	})
	t.Logf("up output:\n%s", out)
	// Daemon must be healthy.
	deadline := time.Now().Add(15 * time.Second)
	key, _ := readKey(dir)
	client := tui.NewClient(url, key)
	var health struct {
		Healthy bool `json:"healthy"`
	}
	up := false
	for time.Now().Before(deadline) {
		if client.Do(context.Background(), "GET", "/api/health", nil, &health) == nil && health.Healthy {
			up = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !up {
		t.Fatal("daemon spawned by up is not healthy")
	}
	// Kill it.
	_ = exec.Command("pkill", "-f", "corral daemon --port "+fmt.Sprint(port)).Run()
}

func TestSchedOptsDefaultsAndOverrides(t *testing.T) {
	for _, name := range []string{
		"CORRAL_BREAKER_MAX_FAILURES", "CORRAL_BREAKER_WINDOW",
		"CORRAL_RUN_MAX_TOKENS", "CORRAL_RUN_MAX_COST",
	} {
		t.Setenv(name, "")
	}
	o := schedOpts(nil)
	if o.Concurrency != 4 {
		t.Fatalf("Concurrency = %d, want 4 (fixed wiring)", o.Concurrency)
	}
	if o.Worktrees != nil {
		t.Fatalf("Worktrees = %v, want nil (as passed in)", o.Worktrees)
	}
	if o.BreakerMaxFailures != 5 {
		t.Fatalf("BreakerMaxFailures = %d, want default 5", o.BreakerMaxFailures)
	}
	if o.BreakerWindow != 15*time.Minute {
		t.Fatalf("BreakerWindow = %s, want default 15m", o.BreakerWindow)
	}
	if o.RunMaxTokens != 1_000_000 {
		t.Fatalf("RunMaxTokens = %d, want default 1000000", o.RunMaxTokens)
	}
	if o.RunMaxCost != 100 {
		t.Fatalf("RunMaxCost = %v, want default 100", o.RunMaxCost)
	}

	// Overrides apply.
	t.Setenv("CORRAL_BREAKER_MAX_FAILURES", "3")
	t.Setenv("CORRAL_BREAKER_WINDOW", "60")
	t.Setenv("CORRAL_RUN_MAX_TOKENS", "5000")
	t.Setenv("CORRAL_RUN_MAX_COST", "0.25")
	o = schedOpts(nil)
	if o.BreakerMaxFailures != 3 {
		t.Fatalf("BreakerMaxFailures = %d, want 3", o.BreakerMaxFailures)
	}
	if o.BreakerWindow != time.Minute {
		t.Fatalf("BreakerWindow = %s, want 1m", o.BreakerWindow)
	}
	if o.RunMaxTokens != 5000 {
		t.Fatalf("RunMaxTokens = %d, want 5000", o.RunMaxTokens)
	}
	if o.RunMaxCost != 0.25 {
		t.Fatalf("RunMaxCost = %v, want 0.25", o.RunMaxCost)
	}

	// 0 disables a safeguard rather than falling back to the default.
	t.Setenv("CORRAL_RUN_MAX_TOKENS", "0")
	t.Setenv("CORRAL_BREAKER_MAX_FAILURES", "0")
	o = schedOpts(nil)
	if o.RunMaxTokens != 0 || o.BreakerMaxFailures != 0 {
		t.Fatalf("zero should disable safeguards: %+v", o)
	}

	// Unparsable values fall back to the defaults.
	t.Setenv("CORRAL_RUN_MAX_TOKENS", "banana")
	t.Setenv("CORRAL_RUN_MAX_COST", "not-a-number")
	t.Setenv("CORRAL_BREAKER_WINDOW", "soon")
	o = schedOpts(nil)
	if o.RunMaxTokens != 1_000_000 || o.RunMaxCost != 100 || o.BreakerWindow != 15*time.Minute {
		t.Fatalf("unparsable env should fall back to defaults: %+v", o)
	}
}

func TestWorktreesCommand(t *testing.T) {
	repo := gitRepo(t)
	ctx := context.Background()
	wtm := worktree.NewManager(repo)
	path, err := wtm.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureOutput(t, func() { err = worktreesCmdWithDir(repo, false, 0) })
	if err != nil {
		t.Fatalf("worktrees: %v", err)
	}
	if !strings.Contains(out, "corral/r1/w1/1") {
		t.Fatalf("listing missing branch:\n%s", out)
	}
	if strings.Contains(out, "pruned") {
		t.Fatalf("listing pruned unexpectedly:\n%s", out)
	}

	// Nothing is merged or stale, so --prune removes nothing.
	out = captureOutput(t, func() { err = worktreesCmdWithDir(repo, true, 0) })
	if err != nil {
		t.Fatalf("worktrees --prune: %v", err)
	}
	if !strings.Contains(out, "nothing to prune") {
		t.Fatalf("prune output wrong:\n%s", out)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("worktree dir removed: %v", err)
	}
}

func TestWorktreesCommandPrunesMerged(t *testing.T) {
	repo := gitRepo(t)
	ctx := context.Background()
	wtm := worktree.NewManager(repo)
	path, err := wtm.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := wtm.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := wtm.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatal(err)
	}

	out := captureOutput(t, func() { err = worktreesCmdWithDir(repo, true, 0) })
	if err != nil {
		t.Fatalf("worktrees --prune: %v", err)
	}
	if !strings.Contains(out, "pruned") || !strings.Contains(out, "corral/r1/w1/1") {
		t.Fatalf("prune output wrong:\n%s", out)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("merged worktree dir not removed: %v", err)
	}
	// Main checkout untouched.
	if data, err := os.ReadFile(filepath.Join(repo, "a.txt")); err != nil || string(data) != "hello" {
		t.Fatalf("main checkout wrong: %v %q", err, data)
	}
}

func TestWorktreesCommandEmpty(t *testing.T) {
	repo := gitRepo(t)
	out := captureOutput(t, func() { _ = worktreesCmdWithDir(repo, false, 0) })
	if !strings.Contains(out, "no attempt worktrees") {
		t.Fatalf("empty listing output wrong:\n%s", out)
	}
}

func TestVersionAtLeast(t *testing.T) {
	cases := []struct {
		v, min string
		want   bool
	}{
		{"1.18.14", "1.18.0", true},
		{"1.18.0", "1.18.0", true},
		{"1.17.9", "1.18.0", false},
		{"2.0.0", "1.18.0", true},
		{"dev", "1.18.0", false},
		{"1.18.14 (dev)", "1.18.0", true},
	}
	for _, c := range cases {
		if got := versionAtLeast(c.v, c.min); got != c.want {
			t.Errorf("versionAtLeast(%q, %q) = %v, want %v", c.v, c.min, got, c.want)
		}
	}
}

func TestStatusCommand(t *testing.T) {
	// Fake daemon serving one run.
	payload := `[{"id":"run_1","status":"active","states":{"w1":"running","gate":"pending"},"done":false}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			w.Write([]byte(`{"healthy":true}`))
			return
		}
		w.Write([]byte(payload))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CORRAL_DAEMON_URL", srv.URL)
	dir := gitRepo(t)
	os.MkdirAll(filepath.Join(dir, ".corral"), 0o755)
	os.WriteFile(filepath.Join(dir, ".corral", "api.key"), []byte("k\n"), 0o600)

	out := captureOutput(t, func() { _ = statusCmdWithDir(dir) })
	if !strings.Contains(out, "run_1") || !strings.Contains(out, "w1:running") {
		t.Fatalf("status output wrong:\n%s", out)
	}
}
