package spike

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"time"
)

type Server struct {
	Cmd    *exec.Cmd
	Base   string
	Stderr io.Writer
}

// StartServer starts an opencode serve process. port 0 picks a free port;
// a fixed port allows restarts on the same URL (daemon watchdog).
func StartServer(ctx context.Context, workdir string, port int, stderr io.Writer) (*Server, error) {
	bin := "opencode"
	var cmd *exec.Cmd
	var base string
	for attempt := 0; attempt < 5; attempt++ {
		if port == 0 {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			port = listener.Addr().(*net.TCPAddr).Port
			listener.Close()
		}

		cmd = exec.CommandContext(ctx, bin, "serve", "--port", fmt.Sprint(port), "--hostname", "127.0.0.1")
		cmd.Dir = workdir
		if stderr != nil {
			cmd.Stderr = stderr
		}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start opencode serve: %w", err)
		}
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		s := &Server{Cmd: cmd, Base: base, Stderr: stderr}

		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				s.Stop()
				return nil, ctx.Err()
			default:
			}
			resp, err := http.Get(s.Base + "/global/health")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return s, nil
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
		s.Stop()
		cmd = nil
		base = ""
	}
	return nil, fmt.Errorf("opencode serve did not become healthy (5 attempts)")
}

func (s *Server) Stop() {
	if s.Cmd != nil && s.Cmd.Process != nil {
		_ = s.Cmd.Process.Kill()
		_, _ = s.Cmd.Process.Wait()
	}
}
