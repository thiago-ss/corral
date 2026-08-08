package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeReleaseServer serves a releases/latest API and a downloadable
// "binary" (a shell script that reports its version, like the sanity
// check expects).
func fakeReleaseServer(t *testing.T, tag, asset, binaryScript string) *httptest.Server {
	t.Helper()
	if asset == "" {
		asset = "corral-" + runtime.GOOS + "-" + goArch()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"tag_name":%q}`, tag)
	})
	mux.HandleFunc("/download/", func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.URL.Path, "/download/"+tag+"/")
		if got != asset {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(binaryScript))
	})
	return httptest.NewServer(mux)
}

func fakeBinary(t *testing.T, tag string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "corral-fake")
	script := fmt.Sprintf("#!/bin/sh\necho \"corral %s\"\n", tag)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func TestUpdateInstallsNewerRelease(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows replace path is manual")
	}
	// Current build is "dev"; the fake release is v9.9.9.
	srv := fakeReleaseServer(t, "v9.9.9", "", "#!/bin/sh\necho \"corral v9.9.9\"\n")
	t.Cleanup(srv.Close)
	target := filepath.Join(t.TempDir(), "bin", "corral")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CORRAL_UPDATE_API", srv.URL)
	t.Setenv("CORRAL_UPDATE_DL", srv.URL+"/download")
	t.Setenv("CORRAL_UPDATE_TARGET", target)

	var out strings.Builder
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err := updateCmd()
	w.Close()
	os.Stdout = orig
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	out.Write(buf[:n])
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !strings.Contains(out.String(), "updated corral") {
		t.Fatalf("output: %s", out.String())
	}
	// The target must now be the new binary.
	got, err := exec.Command(target, "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "v9.9.9") {
		t.Fatalf("target version = %q", got)
	}
}

func TestUpdateAlreadyUpToDate(t *testing.T) {
	// Simulate being on the latest release by testing the comparison
	// through a release server and a fake current version.
	srv := fakeReleaseServer(t, "v9.9.9", "", "#!/bin/sh\necho corral v9.9.9\n")
	t.Cleanup(srv.Close)
	target := fakeBinary(t, "v9.9.9")
	t.Setenv("CORRAL_UPDATE_API", srv.URL)
	t.Setenv("CORRAL_UPDATE_DL", srv.URL+"/download")
	t.Setenv("CORRAL_UPDATE_TARGET", target)
	oldVersion := version
	version = "v9.9.9"
	t.Cleanup(func() { version = oldVersion })

	var out strings.Builder
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err := updateCmd()
	w.Close()
	os.Stdout = orig
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	out.Write(buf[:n])
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !strings.Contains(out.String(), "already up to date") {
		t.Fatalf("output: %s", out.String())
	}
}

func TestUpdateRefusesDowngrade(t *testing.T) {
	srv := fakeReleaseServer(t, "v0.0.1", "", "#!/bin/sh\necho corral v0.0.1\n")
	t.Cleanup(srv.Close)
	target := fakeBinary(t, "v9.9.9")
	t.Setenv("CORRAL_UPDATE_API", srv.URL)
	t.Setenv("CORRAL_UPDATE_DL", srv.URL+"/download")
	t.Setenv("CORRAL_UPDATE_TARGET", target)
	oldVersion := version
	version = "v9.9.9"
	t.Cleanup(func() { version = oldVersion })
	if err := updateCmd(); err == nil {
		t.Fatal("downgrade should be refused")
	}
}

func TestUpdateSanityCheckRejectsGarbage(t *testing.T) {
	srv := fakeReleaseServer(t, "v9.9.9", "", "this is not a binary")
	t.Cleanup(srv.Close)
	target := fakeBinary(t, "v9.9.9")
	t.Setenv("CORRAL_UPDATE_API", srv.URL)
	t.Setenv("CORRAL_UPDATE_DL", srv.URL+"/download")
	t.Setenv("CORRAL_UPDATE_TARGET", target)
	if err := updateCmd(); err == nil {
		t.Fatal("garbage download should fail the sanity check")
	}
}

func TestUpdateDownloadError(t *testing.T) {
	// 404 for the asset: download must fail with a clear error.
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	target := fakeBinary(t, "v9.9.9")
	t.Setenv("CORRAL_UPDATE_API", srv.URL)
	t.Setenv("CORRAL_UPDATE_DL", srv.URL+"/download")
	t.Setenv("CORRAL_UPDATE_TARGET", target)
	if err := updateCmd(); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("expected 404 error, got %v", err)
	}
}
