#!/usr/bin/env bash
set -euo pipefail

# Regression for Docker 28.0.4's per-address allocator: ten Docker-owned
# loopback ports must not exhaust wildcard allocation in the same range.
loopback="relayer-port-loopback-$$"
wildcard="relayer-port-wildcard-$$"
trap 'docker rm --force "$wildcard" "$loopback" >/dev/null' EXIT

bindings=()
for port in {80..89}; do
    bindings+=(--publish "127.0.0.1:20000-29999:$port")
done
docker run --detach --name "$loopback" "${bindings[@]}" alpine:3.21 sleep 300
docker run --detach --name "$wildcard" --publish 20000-29999:9092 --publish 9093 alpine:3.21 sleep 300
docker inspect --format '{{.Name}} {{json .NetworkSettings.Ports}}' "$loopback" "$wildcard"
