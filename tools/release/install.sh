#!/bin/sh
# Installs the ferrika CLI from a GitHub release (Linux and macOS).
#
#   curl -fsSL https://github.com/Jangmyun/ferrika/releases/latest/download/install.sh | sh
#
# Environment:
#   FERRIKA_VERSION       release tag to install, for example v0.1.1 (default: latest)
#   FERRIKA_INSTALL_DIR   target folder (default: ~/.local/bin)
#
# The archive is checked against its published sha256 before anything is
# installed. No root rights are needed unless FERRIKA_INSTALL_DIR asks for them.
set -eu

REPO="Jangmyun/ferrika"

say() { printf 'ferrika-install: %s\n' "$*"; }
fail() {
  printf 'ferrika-install: error: %s\n' "$*" >&2
  exit 1
}

# Everything runs inside main, so a download cut off half way through
# `curl | sh` runs nothing.
main() {
  command -v curl >/dev/null 2>&1 || fail "curl is required"
  command -v tar >/dev/null 2>&1 || fail "tar is required"

  case "$(uname -s)" in
    Darwin) os="apple-darwin" ;;
    Linux)
      # The Linux build links glibc; musl systems (Alpine) cannot run it.
      if ldd --version 2>&1 | grep -qi musl; then
        fail "musl libc is not supported; build from source with 'cargo install ferrika-cli'"
      fi
      os="unknown-linux-gnu"
      ;;
    *) fail "unsupported OS $(uname -s); on Windows use install.ps1" ;;
  esac

  case "$(uname -m)" in
    x86_64 | amd64) arch="x86_64" ;;
    arm64 | aarch64) arch="aarch64" ;;
    *) fail "unsupported CPU $(uname -m)" ;;
  esac
  # A shell under Rosetta reports x86_64 on Apple silicon; use the native build.
  if [ "$os" = "apple-darwin" ] && [ "$arch" = "x86_64" ] &&
    [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
    arch="aarch64"
  fi
  target="${arch}-${os}"

  version="${FERRIKA_VERSION:-}"
  if [ -z "$version" ]; then
    # /releases/latest redirects to /releases/tag/<tag>; this avoids the API
    # and its rate limit.
    latest=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest") ||
      fail "could not look up the latest release"
    version="${latest##*/}"
  fi
  case "$version" in
    v*) ;;
    *) version="v${version}" ;;
  esac

  dir="${FERRIKA_INSTALL_DIR:-${HOME}/.local/bin}"
  archive="ferrika-${version}-${target}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${version}/${archive}"

  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT INT TERM

  say "downloading ferrika ${version} for ${target}"
  curl -fsSL -o "${tmp}/${archive}" "$url" || fail "download failed: ${url}"
  curl -fsSL -o "${tmp}/${archive}.sha256" "${url}.sha256" || fail "download failed: ${url}.sha256"

  want=$(cut -d ' ' -f 1 "${tmp}/${archive}.sha256")
  if command -v sha256sum >/dev/null 2>&1; then
    got=$(sha256sum "${tmp}/${archive}" | cut -d ' ' -f 1)
  elif command -v shasum >/dev/null 2>&1; then
    got=$(shasum -a 256 "${tmp}/${archive}" | cut -d ' ' -f 1)
  else
    fail "sha256sum or shasum is required to check the download"
  fi
  [ "$got" = "$want" ] || fail "checksum mismatch for ${archive} (expected ${want}, got ${got})"

  tar -xzf "${tmp}/${archive}" -C "$tmp" ferrika
  mkdir -p "$dir"
  # Write to a temporary name and rename, so a running ferrika is not
  # overwritten in place.
  cp "${tmp}/ferrika" "${dir}/.ferrika.new"
  chmod 755 "${dir}/.ferrika.new"
  mv -f "${dir}/.ferrika.new" "${dir}/ferrika"

  say "installed ${dir}/ferrika"
  "${dir}/ferrika" --version

  case ":${PATH}:" in
    *":${dir}:"*) ;;
    *) say "${dir} is not on your PATH; add it, for example: echo 'export PATH=\"${dir}:\$PATH\"' >> ~/.profile" ;;
  esac
}

main "$@"
