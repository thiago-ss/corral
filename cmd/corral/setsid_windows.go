//go:build windows

package main

import "syscall"

func detachedSysProcAttr() *syscall.SysProcAttr { return nil }
