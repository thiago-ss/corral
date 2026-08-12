package tui

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDesktopNotificationDoesNotEmbedInputInAppleScript(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	injection := `" & do shell script "touch /tmp/pwned" & "`
	cmd := desktopNotify(ctx, injection, injection)
	if cmd == nil {
		// Unsupported host. Construction is platform-dependent.
		return
	}
	if runtime.GOOS == "darwin" {
		if len(cmd.Args) < 5 {
			t.Fatalf("notification command args: %q", cmd.Args)
		}
		script := cmd.Args[2]
		if strings.Contains(script, injection) || strings.Contains(script, "touch /tmp/pwned") {
			t.Fatalf("untrusted input embedded in program source: %q", script)
		}
	} else if runtime.GOOS == "linux" {
		if len(cmd.Args) != 4 || cmd.Args[1] != "--" {
			t.Fatalf("unsafe notify-send argv: %q", cmd.Args)
		}
	}
	if cmd.Args[len(cmd.Args)-2] != injection || cmd.Args[len(cmd.Args)-1] != injection {
		t.Fatalf("notification text not passed as data argv: %q", cmd.Args)
	}
}

func TestBoundNotificationIsBoundedAndValidUTF8(t *testing.T) {
	got := boundNotification(strings.Repeat("🙂", notificationLimit))
	if len(got) > notificationLimit {
		t.Fatalf("notification = %d bytes, limit %d", len(got), notificationLimit)
	}
	if !utf8.ValidString(got) {
		t.Fatal("notification truncation broke UTF-8")
	}
}
