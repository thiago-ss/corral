package claudeadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/livetest"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
)

// TestMain forwards the --corral-claude-permission-tool flag to the package's
// MCP permission helper so the live test's real claude can call back through
// the driver's broker (the same contract an embedding binary must honour).
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == helperFlag {
		RunPermissionHelper()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeProc simulates a claude process: it emits stream-json events written by
// the test and records signals for abort assertions.
type fakeProc struct {
	r       *io.PipeReader
	w       *io.PipeWriter
	doneCh  chan struct{}
	sigCh   chan os.Signal
	spec    spawnSpec
	mu      sync.Mutex
	exited  bool
	waitErr error
}

func newFakeProc() (*fakeProc, *io.PipeReader) {
	r, w := io.Pipe()
	return &fakeProc{
		r:      r,
		w:      w,
		doneCh: make(chan struct{}),
		sigCh:  make(chan os.Signal, 8),
	}, r
}

func (f *fakeProc) stdout() io.Reader { return f.r }
func (f *fakeProc) done() <-chan struct{} {
	return f.doneCh
}
func (f *fakeProc) wait() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.waitErr
}
func (f *fakeProc) signal(sig os.Signal) error {
	f.sigCh <- sig
	return nil
}

// finish ends the fake process with a Wait result, closing the stream.
func (f *fakeProc) finish(err error) {
	f.mu.Lock()
	if !f.exited {
		f.exited = true
		f.waitErr = err
		f.w.Close()
		close(f.doneCh)
	}
	f.mu.Unlock()
}

// write emits one stream-json event line.
func (f *fakeProc) write(ev map[string]any) {
	b, err := json.Marshal(ev)
	if err != nil {
		panic(err)
	}
	f.w.Write(append(b, '\n'))
}

// fakeSpawn returns a spawn func bound to a fake process, recording the
// spawn spec for assertions.
func fakeSpawn(fp *fakeProc) spawnFunc {
	return func(_ context.Context, spec spawnSpec) (process, error) {
		fp.spec = spec
		return fp, nil
	}
}

func attemptFor(id, objective string) adapter.Attempt {
	return adapter.Attempt{
		ID:                 id,
		NodeID:             id,
		Objective:          objective,
		Role:               "worker",
		Model:              "claude-sonnet-5",
		Cwd:                "/tmp/work",
		WriteScope:         []string{"src"},
		MaxDurationSeconds: 600,
	}
}

// drainSteps pulls one completion from the driver via Step.
func drainSteps(d *Driver, want int) []adapter.Completion {
	deadline := time.Now().Add(10 * time.Second)
	var out []adapter.Completion
	for len(out) < want && time.Now().Before(deadline) {
		cs := d.Step(context.Background(), time.Now())
		out = append(out, cs...)
		if len(cs) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return out
}

// drainFor polls Step for dur, returning as soon as any completion arrives.
// It is used to assert that no duplicate completion is ever emitted.
func drainFor(d *Driver, dur time.Duration) []adapter.Completion {
	deadline := time.Now().Add(dur)
	var out []adapter.Completion
	for time.Now().Before(deadline) {
		out = append(out, d.Step(context.Background(), time.Now())...)
		if len(out) > 0 {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	return out
}

// waitPending polls PendingPermission until the request with the wanted id
// is pending (the broker and the event watcher populate it asynchronously).
func waitPending(t *testing.T, ps adapter.PermissionSession, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pid, ok, err := ps.PendingPermission(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if ok && pid == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	pid, ok, _ := ps.PendingPermission(context.Background())
	t.Fatalf("pending permission = %q, %v; want %q, true", pid, ok, want)
}

func TestDriverImplementsAdapterInterfaces(t *testing.T) {
	var _ adapter.Driver = (*Driver)(nil)
	var _ adapter.Stepper = (*Driver)(nil)
	var _ adapter.Session = (*session)(nil)
	var _ adapter.PermissionSession = (*session)(nil)
}

func TestSystemInitUsesClaudeStreamShapeAndRemapsSession(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	sess, err := drv.Start(context.Background(), attemptFor("w1/1", "task"))
	if err != nil {
		t.Fatal(err)
	}
	oldID := sess.ID()
	actualID := newUUID()
	sess.(*session).at.handleEvent(streamEvent{
		Type:      "system",
		Subtype:   "init",
		SessionID: actualID,
	})
	if got := sess.ID(); got != actualID {
		t.Fatalf("session id = %q, want init event id %q", got, actualID)
	}
	if got := drv.attemptBySession(actualID); got != sess.(*session).at {
		t.Fatal("actual session id was not mapped to its attempt")
	}
	if got := drv.attemptBySession(oldID); got != nil {
		t.Fatal("generated session id remained mapped after init remap")
	}
	fp.finish(nil)
}

// TestStartCompletion drives the happy path: system subtype init, an assistant turn,
// tool callbacks, and a terminal result event. The completion must be emitted
// exactly once with the accumulated transcript, and the spawned claude must
// have received the expected headless flags.
func TestStartCompletion(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true, PollInterval: 50 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	a := attemptFor("w1/1", "create src/alpha.txt")
	sess, err := drv.Start(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}

	// The session id is the UUID the driver generated and passed to claude.
	sid := sess.ID()
	if len(sid) != 36 || strings.Count(sid, "-") != 4 {
		t.Fatalf("session id %q is not a UUID", sid)
	}

	ev := map[string]any{"type": "system", "subtype": "init", "session_id": sid}
	fp.write(ev)
	fp.write(map[string]any{"type": "assistant", "message": map[string]any{
		"role": "assistant", "stop_reason": "end_turn",
		"usage":   map[string]any{"input_tokens": 10, "output_tokens": 20},
		"content": []map[string]any{{"type": "text", "text": "I will create the file."}},
	}})
	fp.write(map[string]any{"type": "user", "message": map[string]any{
		"role": "user",
		"content": []map[string]any{{
			"type": "tool_result", "tool_use_id": "tu1",
			"content": []map[string]any{{"type": "text", "text": "wrote src/alpha.txt"}},
		}},
	}})
	fp.write(map[string]any{"type": "assistant", "message": map[string]any{
		"role": "assistant", "stop_reason": "end_turn",
		"usage":   map[string]any{"input_tokens": 5, "output_tokens": 3},
		"content": []map[string]any{{"type": "text", "text": "Done. src/alpha.txt exists."}},
	}})
	fp.write(map[string]any{"type": "result", "subtype": "success", "session_id": sid, "result": "Done."})
	fp.finish(nil)

	cs := drainSteps(drv, 1)
	if len(cs) != 1 {
		t.Fatalf("got %d completions, want 1", len(cs))
	}
	c := cs[0]
	if c.AttemptID != "w1/1" {
		t.Errorf("attempt id = %q, want w1/1", c.AttemptID)
	}
	if c.SessionID != sid {
		t.Errorf("session id = %q, want %q", c.SessionID, sid)
	}
	if c.Status != adapter.StatusIdle {
		t.Errorf("status = %q, want idle", c.Status)
	}
	msgs := c.Messages
	if len(msgs) != 3 {
		t.Fatalf("messages = %d, want 3", len(msgs))
	}
	if msgs[0].Role != "assistant" || !strings.Contains(msgs[0].Text, "I will create the file.") {
		t.Errorf("msg[0] = %+v", msgs[0])
	}
	if msgs[0].Tokens != 30 {
		t.Errorf("msg[0] tokens = %d, want 30", msgs[0].Tokens)
	}
	if msgs[1].Role != "user" || !strings.Contains(msgs[1].Text, "wrote src/alpha.txt") {
		t.Errorf("msg[1] = %+v", msgs[1])
	}
	if !strings.Contains(msgs[2].Text, "Done.") {
		t.Errorf("msg[2] = %+v", msgs[2])
	}

	// The session view exposes the same transcript.
	got, err := sess.Messages(context.Background())
	if err != nil || len(got) != 3 {
		t.Fatalf("session messages: %v, %d", err, len(got))
	}

	// No duplicate completion when the process exits after the result event.
	if extra := drainFor(drv, 300*time.Millisecond); len(extra) != 0 {
		t.Fatalf("duplicate completions: %+v", extra)
	}

	// The claude invocation carries the expected headless arguments.
	args := strings.Join(fp.spec.args, " ")
	for _, want := range []string{"-p", "--output-format", "stream-json", "--verbose",
		"--include-partial-messages", "--session-id", sid, "--model", "claude-sonnet-5",
		"--allowedTools"} {
		if !strings.Contains(args, want) {
			t.Errorf("args %q missing %q", args, want)
		}
	}
	hasEdit := false
	hasWrite := false
	for _, arg := range fp.spec.args {
		if strings.HasPrefix(arg, "Edit(") {
			hasEdit = true
		}
		if strings.HasPrefix(arg, "Write(") {
			hasWrite = true
		}
	}
	if !hasEdit {
		t.Errorf("write scope not mapped to an Edit rule: %v", fp.spec.args)
	}
	if !hasWrite {
		t.Errorf("write scope not mapped to a Write rule: %v", fp.spec.args)
	}
	if fp.spec.dir != "/tmp/work" {
		t.Errorf("cwd = %q, want /tmp/work", fp.spec.dir)
	}
	if !strings.Contains(fp.spec.args[1], "(role: worker)") {
		t.Errorf("prompt missing role header: %q", fp.spec.args[1])
	}
	if !strings.Contains(fp.spec.args[1], a.Objective) {
		t.Errorf("prompt missing objective: %q", fp.spec.args[1])
	}
}

// TestExitFallbackCompletion verifies the reconciliation path: when the
// terminal result event is missed the process exit decides the completion,
// mapping exit 0 to idle and a non-zero exit to error.
func TestExitFallbackCompletion(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want adapter.Status
	}{
		{"exit zero", nil, adapter.StatusIdle},
		{"exit nonzero", fmt.Errorf("exit status 1"), adapter.StatusError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fp, _ := newFakeProc()
			drv := New(Options{DisablePermissions: true, PollInterval: 20 * time.Millisecond})
			drv.spawn = fakeSpawn(fp)
			defer drv.Close()

			sess, err := drv.Start(context.Background(), attemptFor("w1/1", "write a file"))
			if err != nil {
				t.Fatal(err)
			}
			sid := sess.ID()
			fp.write(map[string]any{"type": "system", "subtype": "init", "session_id": sid})
			fp.write(map[string]any{"type": "assistant", "message": map[string]any{
				"role":    "assistant",
				"content": []map[string]any{{"type": "text", "text": "working..."}},
			}})
			// No result event: the completion must still arrive via exit.
			fp.finish(tc.err)

			cs := drainSteps(drv, 1)
			if len(cs) != 1 {
				t.Fatalf("got %d completions, want 1", len(cs))
			}
			if cs[0].Status != tc.want {
				t.Errorf("status = %q, want %q", cs[0].Status, tc.want)
			}
			if len(cs[0].Messages) != 1 {
				t.Errorf("messages = %d, want 1", len(cs[0].Messages))
			}
			if extra := drainFor(drv, 300*time.Millisecond); len(extra) != 0 {
				t.Fatalf("duplicate completions: %+v", extra)
			}
		})
	}
}

// TestAbort verifies that aborting a running attempt yields exactly one
// completion with StatusAborted, and that the claude process receives a
// terminate signal.
func TestAbort(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true, PollInterval: 20 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	sess, err := drv.Start(context.Background(), attemptFor("w1/1", "long task"))
	if err != nil {
		t.Fatal(err)
	}
	fp.write(map[string]any{"type": "system", "subtype": "init", "session_id": sess.ID()})
	fp.write(map[string]any{"type": "assistant", "message": map[string]any{
		"role":    "assistant",
		"content": []map[string]any{{"type": "text", "text": "starting..."}},
	}})

	// Give the watcher time to record the events, then abort mid-run.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := sess.Status(context.Background())
		if st == adapter.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("attempt never reached running")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := sess.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The fake claude observes the SIGTERM and exits.
	select {
	case sig := <-fp.sigCh:
		if sig != syscall.SIGTERM {
			t.Errorf("signal = %v, want SIGTERM", sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake process never received the abort signal")
	}
	fp.finish(fmt.Errorf("signal: terminated"))

	cs := drainSteps(drv, 1)
	if len(cs) != 1 {
		t.Fatalf("got %d completions, want 1", len(cs))
	}
	if cs[0].Status != adapter.StatusAborted {
		t.Errorf("status = %q, want aborted", cs[0].Status)
	}
	if got, _ := sess.Status(context.Background()); got != adapter.StatusAborted {
		t.Errorf("session status = %q, want aborted", got)
	}
}

// TestPermissionBroker exercises the real permission transport end to end: a
// helper goroutine acting as claude's MCP permission tool connects to the
// broker socket, parks a request, and waits; the scheduler-side session sees
// the pending permission and RespondPermission delivers the decision.
func TestPermissionBroker(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: false, PollInterval: 20 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	sess, err := drv.Start(context.Background(), attemptFor("w1/1", "write src/alpha.txt"))
	if err != nil {
		t.Fatal(err)
	}
	ps, ok := sess.(adapter.PermissionSession)
	if !ok {
		t.Fatal("session does not implement PermissionSession")
	}
	sid := sess.ID()
	if drv.broker == nil {
		t.Fatal("permission broker not started")
	}
	// The spawned claude got the permission tool wiring.
	args := strings.Join(fp.spec.args, " ")
	for _, want := range []string{"--mcp-config", "--permission-prompt-tool", "mcp__corral__handle_permission_prompt"} {
		if !strings.Contains(args, want) {
			t.Errorf("args missing %q: %s", want, args)
		}
	}
	if strings.Count(args, permissionToolName) < 2 {
		t.Errorf("permission MCP tool not explicitly allowed and configured: %s", args)
	}
	configIndex := -1
	for i, arg := range fp.spec.args {
		if arg == "--mcp-config" {
			configIndex = i + 1
			break
		}
	}
	if configIndex < 0 || configIndex >= len(fp.spec.args) {
		t.Fatal("missing MCP config argument")
	}
	var config mcpConfig
	if err := json.Unmarshal([]byte(fp.spec.args[configIndex]), &config); err != nil {
		t.Fatalf("decode MCP config: %v", err)
	}
	helperEnv := config.Servers[permissionServerName].Env
	if helperEnv["CORRAL_CLAUDE_ATTEMPT_ID"] != "w1/1" || helperEnv["CORRAL_CLAUDE_SESSION_ID"] != sid {
		t.Fatalf("helper identity env = %v", helperEnv)
	}

	fp.write(map[string]any{"type": "system", "subtype": "init", "session_id": sid})

	// A helper (what claude spawns as --corral-claude-permission-tool)
	// connects to the broker and parks a permission request.
	helperDone := make(chan decision, 1)
	helperErr := make(chan error, 1)
	go func() {
		conn, err := net.Dial("unix", drv.broker.path)
		if err != nil {
			helperErr <- err
			return
		}
		defer conn.Close()
		req := map[string]any{
			"attempt_id": "w1/1", "session_id": sid, "request_id": "req-1", "tool_name": "Write",
			"prompt": "write file", "tool_input": map[string]any{"file_path": "src/alpha.txt"},
		}
		if err := json.NewEncoder(conn).Encode(req); err != nil {
			helperErr <- err
			return
		}
		var reply decision
		if err := json.NewDecoder(conn).Decode(&reply); err != nil {
			helperErr <- err
			return
		}
		helperDone <- reply
	}()

	// The scheduler's view: the permission is pending and the session is
	// permission-capable.
	waitPending(t, ps, "req-1")

	// Claude may issue tool calls concurrently. The adapter exposes one
	// permission at a time, so a second request is denied immediately instead
	// of replacing the first and leaving its helper stranded.
	conn, err := net.Dial("unix", drv.broker.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(conn).Encode(map[string]any{
		"attempt_id": "w1/1", "session_id": sid, "request_id": "req-concurrent", "tool_name": "Bash",
	}); err != nil {
		t.Fatal(err)
	}
	var concurrent decision
	if err := json.NewDecoder(conn).Decode(&concurrent); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if concurrent.Allow || !strings.Contains(concurrent.Message, "already pending") {
		t.Fatalf("concurrent permission decision = %+v", concurrent)
	}
	if pid, ok, _ := ps.PendingPermission(context.Background()); !ok || pid != "req-1" {
		t.Fatalf("concurrent request replaced pending permission: %q, %v", pid, ok)
	}

	// Approve it; the parked helper must receive the allow decision.
	if err := ps.RespondPermission(context.Background(), "req-1", true); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-helperDone:
		if !reply.Allow {
			t.Errorf("helper got deny, want allow")
		}
	case err := <-helperErr:
		t.Fatalf("helper error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("helper never received the permission decision")
	}
	if pid, ok, _ := ps.PendingPermission(context.Background()); ok {
		t.Errorf("permission still pending after response: %q", pid)
	}
	if err := ps.RespondPermission(context.Background(), "req-1", true); err == nil {
		t.Fatal("responding twice to permission succeeded")
	}

	// A denied request clears the pending state and returns deny.
	go func() {
		conn, err := net.Dial("unix", drv.broker.path)
		if err != nil {
			helperErr <- err
			return
		}
		defer conn.Close()
		json.NewEncoder(conn).Encode(map[string]any{
			"attempt_id": "w1/1", "session_id": sid, "request_id": "req-2", "tool_name": "Bash",
		})
		var reply decision
		if err := json.NewDecoder(conn).Decode(&reply); err != nil {
			helperErr <- err
			return
		}
		helperDone <- reply
	}()
	waitPending(t, ps, "req-2")
	if err := ps.RespondPermission(context.Background(), "req-2", false); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-helperDone:
		if reply.Allow {
			t.Errorf("helper got allow, want deny")
		}
	case err := <-helperErr:
		t.Fatalf("helper error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("helper never received the denied decision")
	}

	// The attempt still completes normally afterwards.
	fp.write(map[string]any{"type": "assistant", "message": map[string]any{
		"role":    "assistant",
		"content": []map[string]any{{"type": "text", "text": "done writing"}},
	}})
	fp.write(map[string]any{"type": "result", "subtype": "success", "session_id": sid})
	fp.finish(nil)
	cs := drainSteps(drv, 1)
	if len(cs) != 1 || cs[0].Status != adapter.StatusIdle {
		t.Fatalf("completion = %+v, want a single idle", cs)
	}
}

// TestPermissionHelperClaude221226InputShape crosses the full MCP helper and
// broker boundary with Claude Code 2.1.226's permission-tool arguments:
// {tool_name,input,tool_use_id}. It spawns the actual exported helper path so
// attempt/session ownership must be obtained from the child environment.
func TestPermissionHelperClaude221226InputShape(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{PollInterval: 20 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	a := attemptFor("w1/1", "write src/alpha.txt")
	sess, err := drv.Start(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	ps := sess.(adapter.PermissionSession)

	helper := exec.Command(os.Args[0], helperFlag)
	helper.Env = append(os.Environ(),
		"CORRAL_CLAUDE_BROKER="+drv.broker.path,
		"CORRAL_CLAUDE_ATTEMPT_ID="+a.ID,
		"CORRAL_CLAUDE_SESSION_ID="+sess.ID(),
	)
	helperIn, err := helper.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	helperOut, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var helperStderr strings.Builder
	helper.Stderr = &helperStderr
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if waited {
			return
		}
		_ = helperIn.Close()
		_ = helper.Process.Kill()
		_ = helper.Wait()
	})
	enc := json.NewEncoder(helperIn)
	dec := json.NewDecoder(helperOut)

	call := func(rpcID, toolUseID string, allow bool) string {
		t.Helper()
		arguments := map[string]any{
			"tool_name": "Write",
			"input":     map[string]any{"file_path": "src/alpha.txt", "content": "ok\n"},
		}
		if toolUseID != "" {
			arguments["tool_use_id"] = toolUseID
		}
		params, err := json.Marshal(map[string]any{
			"name":      "handle_permission_prompt",
			"arguments": arguments,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.Encode(rpcRequest{
			ID:     json.RawMessage(rpcID),
			Method: "tools/call",
			Params: params,
		}); err != nil {
			t.Fatal(err)
		}

		var requestID string
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if id, ok, err := ps.PendingPermission(context.Background()); err != nil {
				t.Fatal(err)
			} else if ok {
				requestID = id
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if requestID == "" {
			t.Fatal("helper request never became pending")
		}
		if toolUseID != "" && requestID != toolUseID {
			t.Fatalf("permission id = %q, want tool_use_id %q", requestID, toolUseID)
		}
		if toolUseID == "" && (len(requestID) != 36 || strings.Count(requestID, "-") != 4) {
			t.Fatalf("generated permission id = %q, want UUID", requestID)
		}
		if err := ps.RespondPermission(context.Background(), requestID, allow); err != nil {
			t.Fatal(err)
		}

		var response struct {
			Result struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
				IsError bool `json:"isError"`
			} `json:"result"`
		}
		if err := dec.Decode(&response); err != nil {
			t.Fatal(err)
		}
		if response.Result.IsError || len(response.Result.Content) != 1 || response.Result.Content[0].Type != "text" {
			t.Fatalf("MCP response = %+v", response)
		}
		return response.Result.Content[0].Text
	}

	allowText := call("1", "toolu_01ABC", true)
	var allowed struct {
		Behavior     string         `json:"behavior"`
		UpdatedInput map[string]any `json:"updatedInput"`
	}
	if err := json.Unmarshal([]byte(allowText), &allowed); err != nil {
		t.Fatalf("decode allow decision %q: %v", allowText, err)
	}
	if allowed.Behavior != "allow" || allowed.UpdatedInput["file_path"] != "src/alpha.txt" {
		t.Fatalf("allow decision = %+v", allowed)
	}

	denyText := call("2", "", false)
	var denied struct {
		Behavior string `json:"behavior"`
		Message  string `json:"message"`
	}
	if err := json.Unmarshal([]byte(denyText), &denied); err != nil {
		t.Fatalf("decode deny decision %q: %v", denyText, err)
	}
	if denied.Behavior != "deny" || denied.Message == "" {
		t.Fatalf("deny decision = %+v", denied)
	}

	if err := helperIn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := helper.Wait(); err != nil {
		t.Fatalf("helper exit: %v; stderr: %s", err, helperStderr.String())
	}
	waited = true
	fp.finish(nil)
}

// TestStreamPermissionTracking covers the fallback path where the pending
// permission is observed from the event stream itself (no broker helper), as
// ocxadapter does with its permission.updated events.
func TestStreamPermissionTracking(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true, PollInterval: 20 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	sess, err := drv.Start(context.Background(), attemptFor("w1/1", "task"))
	if err != nil {
		t.Fatal(err)
	}
	ps, ok := sess.(adapter.PermissionSession)
	if !ok {
		t.Fatal("session does not implement PermissionSession")
	}
	fp.write(map[string]any{"type": "system", "subtype": "init", "session_id": sess.ID()})
	fp.write(map[string]any{"type": "user", "message": map[string]any{
		"role": "user",
		"content": []map[string]any{{
			"type": "permission_request", "state": "needs_response",
			"request_id": "stream-req", "tool_name": "Write",
		}},
	}})

	waitPending(t, ps, "stream-req")
	if err := ps.RespondPermission(context.Background(), "wrong-id", false); err == nil {
		t.Fatal("responding to wrong permission id succeeded")
	}
	if pid, ok, _ := ps.PendingPermission(context.Background()); !ok || pid != "stream-req" {
		t.Fatalf("wrong response changed pending permission: %q, %v", pid, ok)
	}
	// With permissions disabled there is no broker; responding still clears
	// the pending state.
	if err := ps.RespondPermission(context.Background(), "stream-req", false); err != nil {
		t.Fatal(err)
	}
	if pid, ok, _ := ps.PendingPermission(context.Background()); ok {
		t.Errorf("permission still pending: %q", pid)
	}
	if err := ps.RespondPermission(context.Background(), "stream-req", false); err == nil {
		t.Fatal("responding to a resolved permission succeeded")
	}
	fp.finish(nil)
}

func TestCLIProcessWaitBlocksUntilExitResultIsCached(t *testing.T) {
	p := &cliProcess{doneCh: make(chan struct{})}
	want := fmt.Errorf("exit status 7")
	got := make(chan error, 1)
	go func() { got <- p.wait() }()

	select {
	case err := <-got:
		t.Fatalf("wait returned before process exit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	p.waitMu.Lock()
	p.waitErr = want
	p.waitMu.Unlock()
	close(p.doneCh)
	select {
	case err := <-got:
		if err != want {
			t.Fatalf("wait error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after process exit")
	}
}

// TestStartError covers the failure path: when the claude binary cannot be
// spawned the driver returns an error so the scheduler fails the node.
func TestStartError(t *testing.T) {
	drv := New(Options{DisablePermissions: true})
	defer drv.Close()
	drv.spawn = func(context.Context, spawnSpec) (process, error) {
		return nil, fmt.Errorf("claude: exec: not found")
	}
	if _, err := drv.Start(context.Background(), attemptFor("w1/1", "task")); err == nil {
		t.Fatal("expected start error")
	}
}

func TestStartRejectsDuplicateAttemptID(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true})
	drv.spawn = fakeSpawn(fp)
	defer drv.Close()

	a := attemptFor("w1/1", "task")
	if _, err := drv.Start(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := drv.Start(context.Background(), a); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("duplicate Start error = %v", err)
	}
	fp.finish(nil)
}

func TestStartRejectsClosedDriverWithoutSpawning(t *testing.T) {
	drv := New(Options{DisablePermissions: true})
	drv.Close()
	spawned := false
	drv.spawn = func(context.Context, spawnSpec) (process, error) {
		spawned = true
		return nil, fmt.Errorf("unexpected spawn")
	}
	if _, err := drv.Start(context.Background(), attemptFor("w1/1", "task")); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Start after Close error = %v", err)
	}
	if spawned {
		t.Fatal("closed driver spawned a process")
	}
}

// TestCloseKillsSessions verifies the driver tears down live sessions.
func TestCloseKillsSessions(t *testing.T) {
	fp, _ := newFakeProc()
	drv := New(Options{DisablePermissions: true, PollInterval: 20 * time.Millisecond})
	drv.spawn = fakeSpawn(fp)

	sess, err := drv.Start(context.Background(), attemptFor("w1/1", "task"))
	if err != nil {
		t.Fatal(err)
	}
	fp.write(map[string]any{"type": "system", "subtype": "init", "session_id": sess.ID()})
	fp.finish(nil)

	drv.Close()
	// A second Close is a no-op.
	drv.Close()

	// No completion may be emitted after the driver is closed (the scan
	// path is suppressed by terminate).
	if cs := drv.Step(context.Background(), time.Now()); len(cs) != 0 {
		t.Fatalf("completions after close: %+v", cs)
	}
}

// skipLive gates tests that drive the real claude CLI.
func skipLive(t *testing.T) {
	t.Helper()
	livetest.SkipIfDisabled(t)
	if os.Getenv("CORRAL_CLAUDE_LIVE") != "1" {
		t.Skip("live claude test disabled (CORRAL_CLAUDE_LIVE=1 to run)")
	}
	if _, err := exec.LookPath("claude"); err != nil {
		t.Skip("claude binary not found")
	}
}

// TestLiveClaudeSingleNode drives the real claude CLI through the scheduler:
// one worker writes a marker file in its own worktree, passes a command
// verification gate, and the run records exactly one attempt.
func TestLiveClaudeSingleNode(t *testing.T) {
	skipLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-claude-")
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

	st, err := store.Open(filepath.Join(t.TempDir(), "claude.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	drv := New(Options{PollInterval: 500 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })

	n := &graph.Node{
		ID: "w1", Type: graph.NodeAgent, Role: "worker",
		Objective:          "Create a file named claude-marker.txt containing exactly one line: CORRAL-CLAUDE-OK. Do not run any other commands.",
		AcceptanceCriteria: []string{"marker file produced"},
		WriteScope:         []string{"claude-marker.txt"},
		Verification:       &graph.Verification{Kind: "command", Command: []string{"grep", "-q", "CORRAL-CLAUDE-OK", "claude-marker.txt"}},
		Priority:           graph.PriorityNormal,
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
		Budget:             graph.Budget{MaxDuration: 10 * time.Minute},
	}

	s := sched.New(st, drv, &sched.EngineVerifier{Eng: verify.New(proj)}, clock.Real{}, sched.Options{
		Concurrency: 1,
	})
	h, err := s.Create(ctx, "run-claude", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx, 500*time.Millisecond); err != nil {
		for _, id := range []string{"w1"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		t.Fatalf("run: %v", err)
	}

	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 state = %s, want done", st1)
	}
	atts, err := st.Attempts(ctx, "run-claude", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if len(atts) != 1 {
		t.Fatalf("w1 attempts = %d, want exactly 1", len(atts))
	}
	if atts[0].Status != "done" {
		t.Errorf("w1 attempt status = %s, want done", atts[0].Status)
	}
	if len(atts[0].SessionID) != 36 {
		t.Errorf("w1 session id = %q, want a UUID (claude --session-id)", atts[0].SessionID)
	}
	data, err := os.ReadFile(filepath.Join(proj, "claude-marker.txt"))
	if err != nil {
		t.Fatalf("claude-marker.txt missing: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "CORRAL-CLAUDE-OK" {
		t.Errorf("claude-marker.txt = %q, want CORRAL-CLAUDE-OK", got)
	}
}
