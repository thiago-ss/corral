//go:build !windows

package main

import "syscall"

// detachedSysProcAttr puts the daemon in its own session/process group
// so terminal signals (Ctrl+C, SIGHUP on tab close) cannot reach it.
func detachedSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
