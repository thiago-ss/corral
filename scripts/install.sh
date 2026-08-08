#!/bin/sh
# corral installer — downloads the latest release binary from GitHub.
#
#   curl -fsSL https://raw.githubusercontent.com/thiago-ss/corral/main/scripts/install.sh | sh
#
# Installs to ~/.local/bin (override with PREFIX=/usr/local/bin) and
# verifies the binary runs.
set -eu

REPO="thiago-ss/corral"
PREFIX="${PREFIX:-$HOME/.local/bin}"
VERSION="${VERSION:-latest}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac
case "$os" in
  linux|darwin) ;;
  *) echo "unsupported os: $os" >&2; exit 1 ;;
esac

if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
fi
[ -n "$VERSION" ] || { echo "could not determine latest version" >&2; exit 1; }

asset="corral-$os-$arch"
url="https://github.com/$REPO/releases/download/$VERSION/$asset"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "downloading $url"
curl -fsSL "$url" -o "$tmp/corral"
chmod +x "$tmp/corral"

mkdir -p "$PREFIX"
install -m 0755 "$tmp/corral" "$PREFIX/corral"

echo "installed corral $VERSION to $PREFIX/corral"
"$PREFIX/corral" version
echo
echo "next: cd into a git repo and run: corral up"
