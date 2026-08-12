package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"
)

const notificationLimit = 512

// NotifyAttention raises terminal attention when the run needs a human:
// a terminal bell, plus a desktop notification on macOS (osascript) and
// Linux (notify-send). Best-effort and non-blocking; failures are ignored.
func NotifyAttention(title, body string) {
	ringBell()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	cmd := desktopNotify(ctx, boundNotification(title), boundNotification(body))
	if cmd != nil {
		if err := cmd.Start(); err != nil {
			cancel()
			return
		}
		// Reap the child and release the timeout asynchronously. NotifyAttention
		// itself is already run as a Bubble Tea command, never from Update.
		go func() {
			_ = cmd.Wait()
			cancel()
		}()
		return
	}
	cancel()
}

// ringBell writes a terminal bell (BEL) to stdout. This is safe inside a
// bubbletea program: BEL is a control character that does not disturb the
// rendered frame.
func ringBell() {
	_, _ = fmt.Fprint(os.Stdout, "\a")
}

// desktopNotify returns the platform notification command, or nil when the
// platform has no supported notifier.
func desktopNotify(ctx context.Context, title, body string) *exec.Cmd {
	switch runtime.GOOS {
	case "darwin":
		// Pass untrusted text as argv, outside the AppleScript source. No
		// quoting or escaping can turn it into executable AppleScript.
		return exec.CommandContext(ctx, "osascript", "-e",
			`on run argv
display notification (item 2 of argv) with title (item 1 of argv)
end run`, title, body)
	case "linux":
		return exec.CommandContext(ctx, "notify-send", "--", title, body)
	}
	return nil
}

func boundNotification(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == 0 || (r < 32 && r != '\n' && r != '\t') {
			return -1
		}
		return r
	}, s)
	if len(s) > notificationLimit {
		s = s[:notificationLimit]
		// Avoid returning malformed UTF-8 after byte bounding.
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
