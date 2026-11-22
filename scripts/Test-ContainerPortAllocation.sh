#!/usr/bin/env bash
set -euo pipefail

# Regression for Docker 28.0.4's per-address allocator: ten Docker-owned
# loopback ports must not exhaust wildcard allocation in the same range.
loopback="relayer-port-loopback-$$"
wildcard="relayer-port-wildcard-$$"
image=busybox:1.37.0 # Its default image includes the httpd applet.
trap 'docker rm --force "$wildcard" "$loopback" >/dev/null' EXIT

bindings=()
for port in {80..89}; do
    bindings+=(--publish "127.0.0.1:20000-29999:$port")
done
docker run --detach --name "$loopback" "${bindings[@]}" "$image" sh -ec '
    mkdir /www
    echo relayer-port-ok > /www/index.html
    exec httpd -f -p 80 -h /www
'
docker run --detach --name "$wildcard" --publish 20000-29999:9092 --publish 9093 "$image" sh -ec '
    mkdir /www
    echo relayer-port-ok > /www/index.html
    httpd -p 9092 -h /www
    exec httpd -f -p 9093 -h /www
'
docker inspect --format '{{.Name}} {{json .NetworkSettings.Ports}}' "$loopback" "$wildcard"

# Exercise host-to-container IPv4 forwarding for explicit and automatic ports.
check_forwarding() {
    local container=$1 container_port=$2 host_ip=$3 host_port response
    host_port=$(docker port "$container" "$container_port/tcp" | awk -F: -v ip="$host_ip" '$1 == ip { print $2 }')
    if [[ -z $host_port ]]; then
        echo "$container has no $host_ip publication for $container_port/tcp" >&2
        return 1
    fi
    response=$(curl --noproxy '*' --fail --silent --show-error --retry 4 \
        --retry-connrefused --max-time 2 --retry-max-time 10 "http://127.0.0.1:$host_port/")
    if [[ $response != relayer-port-ok ]]; then
        echo "$container returned an unexpected response on host port $host_port" >&2
        return 1
    fi
}

check_forwarding "$loopback" 80 127.0.0.1
check_forwarding "$wildcard" 9092 0.0.0.0
check_forwarding "$wildcard" 9093 0.0.0.0
