#!/usr/bin/env bash
set -euo pipefail

out=$1
daemon_dir=$(dirname "${DOCKER_HOST#unix://}")
pid=$(sudo cat "$daemon_dir/docker.pid")
sudo test -r "$daemon_dir/dockerd.log"

# Capture only container IDs, images, and lifecycle timing.
docker events --filter type=container \
    --format '{{.TimeNano}} {{.Action}} {{.Actor.ID}} {{index .Actor.Attributes "image"}} {{index .Actor.Attributes "signal"}}' \
    > "$out/events.log" &
events=$!
trap 'kill "$events" 2>/dev/null || true; wait "$events" || true' EXIT
trap 'exit 0' TERM

# Poll the dedicated daemon log quickly enough to capture blocked handlers before
# the test's independent 15-second disposal deadline. Tail exits with this shell.
coproc DAEMON_LOG { sudo tail --pid="$$" --sleep-interval=0.1 -n 0 -F "$daemon_dir/dockerd.log"; }
while IFS= read -r line <&"${DAEMON_LOG[0]}"; do
    if [[ $line == *"failed to exit within 10s of kill"* ]]; then
        printf '%s\n' "$line" >> "$out/fallbacks.log"
        sudo kill -USR1 "$pid"
        # One snapshot covers a burst of simultaneous warnings.
        sleep 1
    fi
done
