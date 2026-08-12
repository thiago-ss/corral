package ocxadapter

import (
	"encoding/json"
	"strings"
	"testing"

	"corral/internal/adapter"
	"corral/internal/ocx"
)

func TestTerminalStatusClassifiesFinishReasons(t *testing.T) {
	tests := []struct {
		name     string
		finish   string
		terminal bool
		status   adapter.Status
	}{
		{name: "stop", finish: "stop", terminal: true, status: adapter.StatusIdle},
		{name: "tool calls", finish: "tool-calls", terminal: false},
		{name: "length", finish: "length", terminal: true, status: adapter.StatusError},
		{name: "content filter", finish: "content-filter", terminal: true, status: adapter.StatusError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			finish := test.finish
			messages := []ocx.Message{{Info: ocx.MessageInfo{
				Role: "assistant", Finish: &finish,
			}}}
			terminal, status := terminalStatus(messages, false)
			if terminal != test.terminal || status != test.status {
				t.Fatalf("terminalStatus(%q) = (%v, %q), want (%v, %q)",
					test.finish, terminal, status, test.terminal, test.status)
			}
		})
	}
}

func TestTerminalErrorDescribesProviderFailure(t *testing.T) {
	finish := "length"
	if got := terminalError([]ocx.Message{{Info: ocx.MessageInfo{Role: "assistant", Finish: &finish}}}); got == nil || !strings.Contains(got.Error(), "length") {
		t.Fatalf("finish error = %v", got)
	}
	raw := json.RawMessage(`{"name":"ProviderAuthError"}`)
	if got := terminalError([]ocx.Message{{Info: ocx.MessageInfo{Role: "assistant", Error: &raw}}}); got == nil || !strings.Contains(got.Error(), "ProviderAuthError") {
		t.Fatalf("assistant error = %v", got)
	}
}
