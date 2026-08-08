// Command corral runs the corral scheduler daemon and its companion tools.
//
//	corral daemon            start the scheduler control API (embedded OpenCode server)
//	corral tui               companion dashboard over the daemon
//	corral init              one-command local initialization
//	corral doctor            environment and daemon checks
//	corral export <runID>    full audit export of a run
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"corral/internal/assets"
	"corral/internal/clock"
	"corral/internal/daemon"
	"corral/internal/ocx"
	"corral/internal/ocxadapter"
	"corral/internal/sched"
	"corral/internal/spike"
	"corral/internal/store"
	"corral/internal/tui"
	"corral/internal/verify"
	"corral/internal/worktree"

	tea "github.com/charmbracelet/bubbletea"
)

// version is injected at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Println("corral " + version)
		return
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "corral:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: corral <daemon|tui|init|doctor|export> [flags]")
	}
	switch args[0] {
	case "update":
		return updateCmd()
	case "daemon":
		fs := flag.NewFlagSet("daemon", flag.ExitOnError)
		port := fs.Int("port", 4519, "daemon HTTP port")
		apiKey := fs.String("key", "", "Bearer token for the daemon API (empty = no auth)")
		fs.Parse(args[1:])
		if *apiKey == "" {
			*apiKey = os.Getenv("CORRAL_DAEMON_KEY")
		}
		return daemonCmd(*port, *apiKey)
	case "tui":
		return tuiCmd()
	case "init":
		return initCmd(dirOf(""))
	case "up":
		return upCmd()
	case "doctor":
		return doctorCmd(dirOf(""), "")
	case "export":
		if len(args) < 2 {
			return fmt.Errorf("usage: corral export <runID> [--out file.json]")
		}
		out := ""
		if len(args) >= 4 && args[2] == "--out" {
			out = args[3]
		}
		return exportCmd(args[1], out)
	default:
		return fmt.Errorf("unknown command %q (try: daemon, tui, up, init, doctor, export, update)", args[0])
	}
}

func daemonCmd(port int, apiKey string) error {
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	// Capture the signal that triggers shutdown so the log explains why
	// the daemon exited.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		sig := <-sigCh
		log.Printf("received %v; shutting down gracefully", sig)
	}()
	defer func() {
		if err != nil && err != http.ErrServerClosed {
			log.Printf("daemon exiting: %v", err)
		}
	}()

	// Embedded OpenCode server on a fixed port so the watchdog can
	// restart it on the same URL without breaking the adapter clients.
	servePort := freePort()
	var ocMu sync.Mutex
	ocServer, err := spike.StartServer(ctx, dir, servePort, os.Stderr)
	if err != nil {
		return fmt.Errorf("start opencode server: %w", err)
	}
	log.Printf("opencode server: %s", ocServer.Base)
	defer func() { stopServer(ocServer) }()

	// Watchdog: if the embedded server dies, restart it on the same port.
	go func() {
		for {
			if err := ocServer.Cmd.Wait(); err == nil {
				log.Printf("opencode server exited cleanly")
			} else {
				log.Printf("opencode server exited unexpectedly: %v; restarting", err)
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(2 * time.Second)
			ns, err := spike.StartServer(ctx, dir, servePort, os.Stderr)
			if err != nil {
				log.Printf("opencode server restart failed: %v", err)
				return
			}
			ocMu.Lock()
			ocServer = ns
			ocMu.Unlock()
			log.Printf("opencode server restarted: %s", ns.Base)
		}
	}()

	if err := os.MkdirAll(filepath.Join(dir, ".corral"), 0o755); err != nil {
		return err
	}
	st, err := store.Open(filepath.Join(dir, ".corral", "corral.db"))
	if err != nil {
		return err
	}
	defer st.Close()

	oc := ocx.New(ocServer.Base, dir)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: time.Second})
	defer drv.Close()
	wtm := worktree.NewManager(dir)
	eng := verify.New(dir)
	eng.Runner = verify.ExecRunner{}
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clock.Real{}, sched.Options{
		Concurrency: 4, Worktrees: wtm,
	})
	d := daemon.New(st, s, daemon.NewOpenCodePlanner(oc, "", planTimeout()), dir, apiKey)
	if err := d.Resume(ctx); err != nil {
		log.Printf("resume: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	server := &http.Server{Addr: addr, Handler: d.Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("corral daemon: http://%s (api key %v)", addr, apiKey != "")
	return server.ListenAndServe()
}

// stopServer kills an embedded opencode server process.
func stopServer(s *spike.Server) {
	if s != nil && s.Cmd != nil && s.Cmd.Process != nil {
		_ = s.Cmd.Process.Kill()
		_, _ = s.Cmd.Process.Wait()
	}
}

// freePort reserves a free TCP port and returns it (released immediately;
// fine for local single-process use).
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func tuiCmd() error {
	base := os.Getenv("CORRAL_DAEMON_URL")
	if base == "" {
		base = "http://127.0.0.1:4519"
	}
	key, _ := readKey(dirOf(""))
	client := tui.NewClient(base, key)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p := tea.NewProgram(tui.New(client, ctx), tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func daemonURL() string {
	if base := os.Getenv("CORRAL_DAEMON_URL"); base != "" {
		return base
	}
	return "http://127.0.0.1:4519"
}

// planTimeout is the planner session budget; override with
// CORRAL_PLAN_TIMEOUT (seconds).
func planTimeout() time.Duration {
	if v := os.Getenv("CORRAL_PLAN_TIMEOUT"); v != "" {
		if s, err := strconv.Atoi(v); err == nil && s > 0 {
			return time.Duration(s) * time.Second
		}
	}
	return 5 * time.Minute
}

// upCmd is the one-command start: initialize if needed, launch the daemon
// detached, wait for health, and report.
func upCmd() error {
	return upCmdWithPort(dirOf(""), 4519)
}

func upCmdWithPort(dir string, port int) error {
	// Ensure initialized (idempotent).
	if err := initCmd(dir); err != nil {
		return err
	}
	key, err := readKey(dir)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	// Already running?
	check := tui.NewClient(url, key)
	var health struct {
		Healthy bool `json:"healthy"`
	}
	if check.Do(context.Background(), http.MethodGet, "/api/health", nil, &health) == nil && health.Healthy {
		fmt.Println("daemon already running at", url)
		return doctorCmd(dir, url)
	}

	bin := os.Getenv("CORRAL_BIN")
	if bin == "" {
		bin, err = os.Executable()
		if err != nil {
			return err
		}
	}
	logDir := filepath.Join(dir, ".corral")
	logFile, err := os.OpenFile(filepath.Join(logDir, "daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// nohup keeps the daemon alive after the shell exits; Setsid gives it
	// its own session and process group so terminal signals (Ctrl+C,
	// SIGHUP on tab close) can never reach it.
	cmd := exec.Command("nohup", bin, "daemon", "--port", fmt.Sprint(port))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CORRAL_DAEMON_KEY="+key, "CORRAL_DAEMON_URL="+url)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := cmd.Process.Release(); err != nil {
		return err
	}
	// Wait for health.
	deadline := time.Now().Add(30 * time.Second)
	healthy := false
	for time.Now().Before(deadline) {
		if check.Do(context.Background(), http.MethodGet, "/api/health", nil, &health) == nil && health.Healthy {
			healthy = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !healthy {
		return fmt.Errorf("daemon did not become healthy; see %s", filepath.Join(logDir, "daemon.log"))
	}
	fmt.Println("daemon started at", url)
	fmt.Println()
	return doctorCmd(dir, url)
}

// readKey returns the API key written by init (env overrides).
func readKey(dir string) (string, error) {
	if k := os.Getenv("CORRAL_DAEMON_KEY"); k != "" {
		return k, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, ".corral", "api.key"))
	if err != nil {
		return "", fmt.Errorf("no api key; run corral init first: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// dirOf returns dir, or the current working directory when empty.
func dirOf(dir string) string {
	if dir != "" {
		return dir
	}
	d, err := os.Getwd()
	if err != nil {
		return "."
	}
	return d
}

// initCmd performs one-command local initialization: verify the git repo,
// create .corral/, generate an API key, and print next steps.
func initCmd(wantDir string) error {
	dir := dirOf(wantDir)
	out := exec.Command("git", "rev-parse", "--is-inside-work-tree")
	out.Dir = dir
	if err := out.Run(); err != nil {
		return fmt.Errorf("not inside a git repository: %w", err)
	}
	corralDir := filepath.Join(dir, ".corral")
	if err := os.MkdirAll(corralDir, 0o755); err != nil {
		return err
	}
	keyFile := filepath.Join(corralDir, "api.key")
	var key string
	if data, err := os.ReadFile(keyFile); err == nil && strings.TrimSpace(string(data)) != "" {
		key = strings.TrimSpace(string(data))
		fmt.Println("existing api key kept:", keyFile)
	} else {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		key = hex.EncodeToString(b)
		if err := os.WriteFile(keyFile, []byte(key+"\n"), 0o600); err != nil {
			return err
		}
		fmt.Println("api key written:", keyFile)
	}
	cfg := map[string]any{"dir": dir, "apiKey": key, "daemonURL": daemonURL()}
	if err := writeJSONFile(filepath.Join(corralDir, "config.json"), cfg); err != nil {
		return err
	}
	if err := installPlugin(dir); err != nil {
		return err
	}
	if err := installAgentConfig(dir); err != nil {
		return err
	}
	fmt.Println("initialized corral in", dir)
	fmt.Println()
	fmt.Println("ready. next steps:")
	fmt.Println("  corral up        start the daemon in the background")
	fmt.Println("  corral tui       follow runs")
	fmt.Println("  restart opencode — then ask the corral-planner agent to plan your goal")
	return nil
}

// installPlugin writes the corral OpenCode tools into .opencode/tools/.
func installPlugin(dir string) error {
	pluginDir := filepath.Join(dir, ".opencode", "tools")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "corral.ts"), []byte(assets.CorralPluginTS), 0o644); err != nil {
		return err
	}
	return nil
}

// installAgentConfig merges the corral agents (planner, orchestrator,
// worker, reviewer, merger) into the project's opencode.json, preserving
// any existing configuration. Existing agent entries are left untouched.
func installAgentConfig(dir string) error {
	cfgPath := filepath.Join(dir, "opencode.json")
	existing := map[string]any{}
	if data, err := os.ReadFile(cfgPath); err == nil {
		if err := json.Unmarshal(data, &existing); err != nil {
			// Back up the unreadable file and start fresh.
			_ = os.Rename(cfgPath, cfgPath+".bak")
			existing = map[string]any{}
		}
	}
	agents := map[string]any{}
	var embedded map[string]any
	if err := json.Unmarshal([]byte(assets.OpenCodeConfigJSON), &embedded); err != nil {
		return fmt.Errorf("embedded agent config corrupt: %w", err)
	}
	if em, ok := embedded["agent"].(map[string]any); ok {
		agents = em
	}
	existingAgents, _ := existing["agent"].(map[string]any)
	if existingAgents == nil {
		existingAgents = map[string]any{}
	}
	for name, def := range agents {
		if _, ok := existingAgents[name]; !ok {
			existingAgents[name] = def
		}
	}
	existing["agent"] = existingAgents
	return writeJSONFile(cfgPath, existing)
}

func doctorCmd(wantDir, url string) error {
	if url == "" {
		url = daemonURL()
	}
	return doctorWithURL(wantDir, url)
}

func doctorWithURL(wantDir, url string) error {
	ok := true
	check := func(name string, pass bool, detail string) {
		mark := "✓"
		if !pass {
			mark = "✗"
			ok = false
		}
		fmt.Printf("  %s %-28s %s\n", mark, name, detail)
	}

	fmt.Println("corral doctor")
	// OpenCode version.
	if out, err := exec.Command("opencode", "--version").Output(); err == nil {
		v := strings.TrimSpace(string(out))
		pass := versionAtLeast(v, "1.18.0")
		check("opencode", pass, v)
	} else {
		check("opencode", false, "not found on PATH")
	}
	// Git repository.
	dir := dirOf(wantDir)
	gitCmd := exec.Command("git", "rev-parse", "--is-inside-work-tree")
	gitCmd.Dir = dir
	check("git repository", gitCmd.Run() == nil, dir)
	// Daemon reachable (with the api key when configured).
	key, _ := readKey(dir)
	client := tui.NewClient(url, key)
	var health struct {
		Healthy bool `json:"healthy"`
	}
	daemonUp := client.Do(context.Background(), http.MethodGet, "/api/health", nil, &health) == nil && health.Healthy
	check("daemon", daemonUp, url)
	// Plugin present.
	plugin := filepath.Join(dir, ".opencode", "tools", "corral.ts")
	check("plugin", fileExists(plugin), plugin)
	// Config.
	cfg := filepath.Join(dir, ".corral", "config.json")
	check("config", fileExists(cfg), cfg)
	if !ok {
		return fmt.Errorf("doctor found problems")
	}
	return nil
}

func exportCmd(runID, outFile string) error {
	client := tui.NewClient(daemonURL(), os.Getenv("CORRAL_DAEMON_KEY"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var payload json.RawMessage
	if err := client.Do(ctx, http.MethodGet, "/api/runs/"+runID+"/export", nil, &payload); err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, payload, "", "  "); err != nil {
		return err
	}
	if outFile != "" && outFile != "-" {
		if err := os.WriteFile(outFile, pretty.Bytes(), 0o644); err != nil {
			return err
		}
		fmt.Printf("audit export written to %s\n", outFile)
		return nil
	}
	_, err := os.Stdout.Write(pretty.Bytes())
	return err
}

func versionAtLeast(v, min string) bool {
	re := regexp.MustCompile(`\d+\.\d+\.\d+`)
	m := re.FindString(v)
	if m == "" {
		return false
	}
	var a, b [3]int
	fmt.Sscanf(m, "%d.%d.%d", &a[0], &a[1], &a[2])
	fmt.Sscanf(min, "%d.%d.%d", &b[0], &b[1], &b[2])
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return true
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
