package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Update endpoints; overridable for tests via env.
const (
	defaultUpdateAPI = "https://api.github.com/repos/thiago-ss/corral"
	defaultUpdateDL  = "https://github.com/thiago-ss/corral/releases/download"
)

// updateCmd self-updates the corral binary from GitHub releases:
//   - no-op when already on the latest release
//   - downloads corral-<os>-<arch> for the current platform
//   - sanity-checks the download (it must run and report the tag)
//   - atomically replaces the running binary
func updateCmd() error {
	apiBase := getenvDefault("CORRAL_UPDATE_API", defaultUpdateAPI)
	dlBase := getenvDefault("CORRAL_UPDATE_DL", defaultUpdateDL)
	target := os.Getenv("CORRAL_UPDATE_TARGET")
	if target == "" {
		exe, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate binary: %w", err)
		}
		target = exe
	}

	tag, err := latestReleaseTag(apiBase)
	if err != nil {
		return fmt.Errorf("check for updates: %w", err)
	}
	current := strings.TrimPrefix(version, "v")
	tagV := strings.TrimPrefix(tag, "v")
	if current != "dev" {
		if current == tagV {
			fmt.Printf("corral is already up to date (%s)\n", tag)
			return nil
		}
		if versionAtLeast(current, tagV) {
			return fmt.Errorf("you are on %s, which is newer than the latest release %s — nothing to update", version, tag)
		}
	}

	asset := "corral-" + runtime.GOOS + "-" + goArch()
	url := dlBase + "/" + tag + "/" + asset
	fmt.Printf("downloading %s\n", url)
	tmp, err := os.CreateTemp("", "corral-update-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := downloadBinary(url, tmp); err != nil {
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		return err
	}

	// Sanity check: the download must be a real corral binary reporting
	// the release tag.
	out, err := exec.Command(tmp.Name(), "version").Output()
	if err != nil {
		return fmt.Errorf("downloaded binary failed the sanity check: %w", err)
	}
	if !strings.Contains(string(out), strings.TrimPrefix(tag, "v")) {
		return fmt.Errorf("downloaded binary version mismatch: got %q, want %q", strings.TrimSpace(string(out)), tag)
	}

	if err := replaceBinary(tmp.Name(), target); err != nil {
		return err
	}
	fmt.Printf("updated corral %s -> %s\n", currentOrDev(), tag)
	return nil
}

func currentOrDev() string {
	if version == "dev" {
		return "(dev build)"
	}
	return version
}

func goArch() string {
	switch runtime.GOARCH {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	}
	return runtime.GOARCH
}

// latestReleaseTag queries the GitHub releases API for the newest tag.
func latestReleaseTag(apiBase string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"/releases/latest", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("release lookup: %s", resp.Status)
	}
	var rel struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return "", err
	}
	if rel.TagName == "" {
		return "", fmt.Errorf("release lookup returned no tag")
	}
	return rel.TagName, nil
}

func downloadBinary(url string, out *os.File) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", url, resp.Status)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	return nil
}

// replaceBinary atomically swaps the target with the downloaded binary.
// On Windows the running executable is locked, so we report where the
// new binary was placed instead.
func replaceBinary(tmp, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		dest := target + ".new"
		if err := os.Rename(tmp, dest); err != nil {
			return fmt.Errorf("replace binary: %w", err)
		}
		fmt.Printf("downloaded to %s — replace corral.exe manually (running executables are locked on Windows)\n", dest)
		return nil
	}
	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("replace binary: %w", err)
	}
	return nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
