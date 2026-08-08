// Package livetest gates integration tests that need a live OpenCode
// server and model provider. Set CORRAL_LIVE=0 to skip them (fast,
// deterministic runs); unset or CORRAL_LIVE=1 runs everything.
package livetest

import (
	"os"
	"testing"
)

func SkipIfDisabled(t *testing.T) {
	t.Helper()
	if os.Getenv("CORRAL_LIVE") == "0" {
		t.Skip("live test disabled (CORRAL_LIVE=0)")
	}
}
