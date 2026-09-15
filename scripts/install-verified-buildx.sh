#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
    echo "The verified Buildx artifact requires Linux amd64." >&2
    exit 1
fi

buildx_version=v0.36.1
buildx_sha256=48af8a397ebd60178778bf63611dbcebe5f5e7a9be90eb9147b24b9587455778
buildx_download_dir=$(mktemp -d)
trap 'rm -rf "$buildx_download_dir"' EXIT

curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
    --retry 3 \
    "https://github.com/docker/buildx/releases/download/${buildx_version}/buildx-${buildx_version}.linux-amd64" \
    --output "$buildx_download_dir/docker-buildx"
# Verify the release artifact before Docker or setup-buildx can execute it.
printf '%s  %s\n' "$buildx_sha256" "$buildx_download_dir/docker-buildx" | sha256sum --check --strict

buildx_plugin_dir="${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins"
mkdir -p "$buildx_plugin_dir"
install -m 0755 "$buildx_download_dir/docker-buildx" "$buildx_plugin_dir/docker-buildx"
