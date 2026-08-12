package claudeadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Claude Code headless sessions surface permission prompts by invoking the
// --permission-prompt-tool MCP tool; the driver answers them through a local
// unix socket broker.
//
//	claude --permission-prompt-tool mcp__corral__handle_permission_prompt
//	                       │
//	              (permission prompt)
//	                       ▼
//	   MCP helper child (--corral-claude-permission-tool, stdio JSON-RPC)
//	                       │  net.Dial(unix)
//	                       ▼
//	              driver permission broker socket
//	                       ▲
//	        scheduler -> PermissionSession.RespondPermission
//
// The helper supplies its attempt and launch-session identity from the MCP
// process environment. The broker validates both, keeps the connection open
// until a decision arrives, and unblocks the helper (and Claude) exactly once.

const (
	helperFlag           = "--corral-claude-permission-tool"
	permissionServerName = "corral"
	permissionToolName   = "mcp__corral__handle_permission_prompt"
)

// mcpConfig is the --mcp-config document that wires the permission helper
// into claude as an MCP stdio server.
type mcpConfig struct {
	Servers map[string]mcpServerConfig `json:"mcpServers"`
}

type mcpServerConfig struct {
	Command string            `json:"command"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
}

// helperExecutable is the binary claude spawns as the MCP permission helper:
// the driver's own executable. The embedding binary must forward
// --corral-claude-permission-tool to RunPermissionHelper (see the live test
// for a TestMain example).
func helperExecutable() string {
	if exe, err := os.Executable(); err == nil {
		return exe
	}
	return os.Args[0]
}

// brokerRequest is the wire message sent by the helper to the broker.
type brokerRequest struct {
	AttemptID string          `json:"attempt_id"`
	SessionID string          `json:"session_id"`
	RequestID string          `json:"request_id"`
	ToolName  string          `json:"tool_name"`
	ToolInput json.RawMessage `json:"tool_input"`
}

// decision is the broker's reply to a permission request.
type decision struct {
	Allow   bool   `json:"allow"`
	Message string `json:"message,omitempty"`
}

func denyDecision(msg string) decision {
	return decision{Allow: false, Message: msg}
}

// permissionBroker answers permission requests from claude's MCP helper over
// a unix socket, one connection per pending request.
type permissionBroker struct {
	d       *Driver
	ln      net.Listener
	path    string
	dir     string // auto-created private directory; empty for an override path
	done    chan struct{}
	mu      sync.Mutex
	pending map[permissionKey]chan decision
	closed  bool
}

type permissionKey struct {
	attemptID string
	requestID string
}

func (d *Driver) startBroker() error {
	d.brokerOnce.Do(func() {
		path := d.opts.PermissionSocket
		dir := ""
		if path == "" {
			var err error
			dir, err = os.MkdirTemp("", "corral-claude-")
			if err != nil {
				d.brokerErr = err
				return
			}
			path = filepath.Join(dir, "broker.sock")
		}
		if info, err := os.Lstat(path); err == nil {
			if info.Mode()&os.ModeSocket == 0 {
				if dir != "" {
					_ = os.Remove(dir)
				}
				d.brokerErr = fmt.Errorf("refusing to replace non-socket permission path %q", path)
				return
			}
			if err := os.Remove(path); err != nil {
				if dir != "" {
					_ = os.Remove(dir)
				}
				d.brokerErr = fmt.Errorf("remove stale socket: %w", err)
				return
			}
		} else if !os.IsNotExist(err) {
			if dir != "" {
				_ = os.Remove(dir)
			}
			d.brokerErr = fmt.Errorf("inspect permission socket: %w", err)
			return
		}
		ln, err := net.Listen("unix", path)
		if err != nil {
			if dir != "" {
				_ = os.Remove(dir)
			}
			d.brokerErr = err
			return
		}
		if err := os.Chmod(path, 0o600); err != nil {
			_ = ln.Close()
			_ = os.Remove(path)
			if dir != "" {
				_ = os.Remove(dir)
			}
			d.brokerErr = fmt.Errorf("secure socket mode: %w", err)
			return
		}
		b := &permissionBroker{
			d:       d,
			ln:      ln,
			path:    path,
			dir:     dir,
			done:    make(chan struct{}),
			pending: map[permissionKey]chan decision{},
		}
		d.broker = b
		go b.serve()
	})
	return d.brokerErr
}

func (b *permissionBroker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			select {
			case <-b.done:
				return
			default:
			}
			continue
		}
		go b.handle(conn)
	}
}

// handle serves one helper connection: receive the request, park it as the
// attempt's pending permission, and block until a decision is delivered.
func (b *permissionBroker) handle(conn net.Conn) {
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	var req brokerRequest
	if err := dec.Decode(&req); err != nil || req.AttemptID == "" || req.SessionID == "" || req.RequestID == "" {
		return
	}
	at := b.d.attemptByID(req.AttemptID)
	if at == nil {
		_ = enc.Encode(denyDecision("unknown attempt"))
		return
	}
	at.mu.Lock()
	validSession := req.SessionID == at.launchID || req.SessionID == at.sessionID
	at.mu.Unlock()
	if !validSession {
		_ = enc.Encode(denyDecision("session does not belong to attempt"))
		return
	}

	ch := make(chan decision, 1)
	key := permissionKey{attemptID: req.AttemptID, requestID: req.RequestID}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = enc.Encode(denyDecision("driver closed"))
		return
	}
	at.mu.Lock()
	if at.permission != "" && at.permission != req.RequestID {
		at.mu.Unlock()
		b.mu.Unlock()
		_ = enc.Encode(denyDecision("another permission request is already pending"))
		return
	}
	if _, exists := b.pending[key]; exists {
		at.mu.Unlock()
		b.mu.Unlock()
		_ = enc.Encode(denyDecision("duplicate permission request"))
		return
	}
	b.pending[key] = ch
	at.permission = req.RequestID
	at.mu.Unlock()
	b.mu.Unlock()

	var reply decision
	select {
	case reply = <-ch:
	case <-b.done:
		reply = denyDecision("driver closed")
	case <-time.After(10 * time.Minute):
		reply = denyDecision("permission request timed out")
	}
	b.mu.Lock()
	if b.pending[key] == ch {
		delete(b.pending, key)
	}
	b.mu.Unlock()
	at.mu.Lock()
	if at.permission == req.RequestID {
		at.permission = ""
	}
	at.mu.Unlock()
	_ = enc.Encode(reply)
}

// respond claims and resolves the helper waiting on requestID. Missing and
// duplicate responses are rejected instead of being silently accepted.
func (b *permissionBroker) respond(ctx context.Context, at *attempt, requestID string, allow bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := permissionKey{attemptID: at.attemptID, requestID: requestID}
	b.mu.Lock()
	ch := b.pending[key]
	if ch != nil {
		delete(b.pending, key) // claim the request; duplicate responses now fail
	}
	b.mu.Unlock()
	if ch == nil {
		return fmt.Errorf("claudeadapter: permission %q has no waiting helper", requestID)
	}
	reply := decision{Allow: allow}
	if !allow {
		reply.Message = "denied by operator"
	}
	ch <- reply // buffered channel; cannot block
	return nil
}

func (b *permissionBroker) close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()
	close(b.done)
	_ = b.ln.Close()
	_ = os.Remove(b.path)
	if b.dir != "" {
		_ = os.Remove(b.dir)
	}
}

// ---------------------------------------------------------------------------
// MCP stdio server (the --corral-claude-permission-tool helper). The helper
// is spawned by claude as an MCP server; each tools/call for the permission
// tool is forwarded to the driver's broker socket and the decision is
// returned as the tool result.

// RunPermissionHelper serves the MCP permission-prompt tool over stdio. The
// embedding binary must invoke it when os.Args[1] ==
// "--corral-claude-permission-tool" (the helper spawns with that flag). It
// returns only on error; callers exit afterwards.
func RunPermissionHelper() {
	broker := os.Getenv("CORRAL_CLAUDE_BROKER")
	if broker == "" {
		fmt.Fprintln(os.Stderr, "corral claude permission helper: CORRAL_CLAUDE_BROKER not set")
		os.Exit(1)
	}
	attemptID := os.Getenv("CORRAL_CLAUDE_ATTEMPT_ID")
	sessionID := os.Getenv("CORRAL_CLAUDE_SESSION_ID")
	if attemptID == "" || sessionID == "" {
		fmt.Fprintln(os.Stderr, "corral claude permission helper: attempt/session identity not set")
		os.Exit(1)
	}
	s := &mcpServer{broker: broker, attemptID: attemptID, sessionID: sessionID}
	if err := s.serve(os.Stdin, os.Stdout); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "corral claude permission helper: %v\n", err)
		os.Exit(1)
	}
}

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpServer struct {
	broker    string
	attemptID string
	sessionID string
	mu        sync.Mutex
}

func (s *mcpServer) serve(in io.Reader, out io.Writer) error {
	enc := json.NewEncoder(out)
	dec := json.NewDecoder(in)
	for {
		var req rpcRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		switch req.Method {
		case "initialize":
			s.write(enc, req.ID, map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]string{"name": "corral", "version": "0.0.1"},
			})
		case "notifications/initialized":
			// no response expected
		case "tools/list":
			s.write(enc, req.ID, map[string]any{
				"tools": []map[string]any{{
					"name":        "handle_permission_prompt",
					"description": "Corral handles Claude Code permission prompts on behalf of the scheduler.",
					"inputSchema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"tool_name":   map[string]string{"type": "string"},
							"input":       map[string]string{"type": "object"},
							"tool_use_id": map[string]string{"type": "string"},
						},
						"required":             []string{"tool_name", "input"},
						"additionalProperties": true,
					},
				}},
			})
		case "tools/call":
			go s.handleToolCall(enc, req.ID, req.Params)
		case "ping":
			s.write(enc, req.ID, map[string]any{})
		default:
			s.writeErr(enc, req.ID, -32601, "unknown method "+req.Method)
		}
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// handleToolCall blocks until the driver answers the permission prompt, so
// it runs in its own goroutine; writes are serialized by the encoder mutex.
func (s *mcpServer) handleToolCall(enc *json.Encoder, id json.RawMessage, params json.RawMessage) {
	var call toolCallParams
	var args struct {
		ToolName  string          `json:"tool_name"`
		ToolInput json.RawMessage `json:"input"`
		ToolUseID string          `json:"tool_use_id"`
	}
	if json.Unmarshal(params, &call) != nil || call.Name != "handle_permission_prompt" {
		s.writeToolError(enc, id, "unexpected tool call")
		return
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil || args.ToolName == "" || len(args.ToolInput) == 0 || string(args.ToolInput) == "null" {
		s.writeToolError(enc, id, "invalid permission prompt arguments")
		return
	}
	requestID := args.ToolUseID
	if requestID == "" {
		requestID = newUUID()
	}

	reply, err := s.askBroker(brokerRequest{
		AttemptID: s.attemptID,
		SessionID: s.sessionID,
		RequestID: requestID,
		ToolName:  args.ToolName,
		ToolInput: args.ToolInput,
	})
	if err != nil {
		s.writeToolError(enc, id, err.Error())
		return
	}
	text, err := json.Marshal(decisionJSON(reply, args.ToolInput))
	if err != nil {
		s.writeToolError(enc, id, err.Error())
		return
	}
	s.write(enc, id, map[string]any{
		"content": []map[string]string{{"type": "text", "text": string(text)}},
		"isError": false,
	})
}

// decisionJSON renders a broker decision as the permission tool result
// claude expects: an allow must echo the tool input back.
func decisionJSON(d decision, toolInput json.RawMessage) map[string]any {
	if d.Allow {
		return map[string]any{"behavior": "allow", "updatedInput": rawOrEmpty(toolInput)}
	}
	return map[string]any{"behavior": "deny", "message": d.Message}
}

func rawOrEmpty(raw json.RawMessage) any {
	if len(raw) == 0 || strings.EqualFold(string(raw), "null") {
		return map[string]any{}
	}
	return raw
}

// askBroker forwards a permission request to the driver's broker socket and
// waits for the decision.
func (s *mcpServer) askBroker(req brokerRequest) (decision, error) {
	conn, err := net.DialTimeout("unix", s.broker, 10*time.Second)
	if err != nil {
		return denyDecision(err.Error()), err
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return denyDecision(err.Error()), err
	}
	var reply decision
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&reply); err != nil {
		return denyDecision(err.Error()), err
	}
	return reply, nil
}

func (s *mcpServer) write(enc *json.Encoder, id json.RawMessage, result any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = enc.Encode(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *mcpServer) writeErr(enc *json.Encoder, id json.RawMessage, code int, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = enc.Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}})
}

func (s *mcpServer) writeToolError(enc *json.Encoder, id json.RawMessage, msg string) {
	s.write(enc, id, map[string]any{
		"content": []map[string]string{{"type": "text", "text": msg}},
		"isError": true,
	})
}
