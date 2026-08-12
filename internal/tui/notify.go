package tui

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

// NotifyAttention raises terminal attention when the run needs a human:
// a terminal bell, plus a desktop notification on macOS (osascript) and
// Linux (notify-send). Best-effort and non-blocking; failures are ignored.
func NotifyAttention(title, body string) {
	ringBell()
	cmd := desktopNotify(title, body)
	if cmd != nil {
		_ = cmd.Start()
	}
}

// ringBell writes a terminal bell (BEL) to stdout. This is safe inside a
// bubbletea program: BEL is a control character that does not disturb the
// rendered frame.
func ringBell() {
	_, _ = fmt.Fprint(os.Stdout, "\a")
}

// desktopNotify returns the platform notification command, or nil when the
// platform has no supported notifier.
func desktopNotify(title, body string) *exec.Cmd {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("osascript", "-e",
			fmt.Sprintf(`display notification %q with title %q`, body, title))
	case "linux":
		return exec.Command("notify-send", title, body)
	}
	return nil
}
