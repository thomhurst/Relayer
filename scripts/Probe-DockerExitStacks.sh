#!/usr/bin/env bash
set -euo pipefail

out="$1"
socket=${DOCKER_HOST#unix://}
daemon_dir=$(dirname "$socket")
pid=$(sudo cat "$daemon_dir/docker.pid")
sudo test -r "$daemon_dir/dockerd.log"
mkdir -p "$out"

# Capture Docker API event order for matching the dump to a container ID.
timeout 540 docker events --filter type=container \
    --format '{{.TimeNano}} {{.Action}} {{.Actor.ID}} {{index .Actor.Attributes "image"}} {{index .Actor.Attributes "signal"}}' \
    > "$out/events.log" &
events=$!
trap 'kill "$events" 2>/dev/null || true' EXIT

# Request a native daemon stack dump while blocked handlers are still present.
sudo timeout 540 tail -n +1 -F "$daemon_dir/dockerd.log" | while IFS= read -r line; do
    if [[ "$line" == *"failed to exit within 10s of kill"* ]]; then
        printf '%s\n' "$line" | tee -a "$out/fallbacks.log"
        sudo kill -USR1 "$pid"
        # A burst of fallback warnings needs only one stack snapshot.
        sleep 3
    fi
done
