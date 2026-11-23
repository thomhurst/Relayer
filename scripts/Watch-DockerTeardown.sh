#!/usr/bin/env bash
set -euo pipefail

out=$1
# The Docker setup action places the socket, PID file and log in one runner-local directory.
daemon_dir=$(dirname "${DOCKER_HOST#unix://}")
pid=$(sudo -n cat "$daemon_dir/docker.pid")
sudo -n test -r "$daemon_dir/dockerd.log"

# Keep only lifecycle timing, container IDs, images and signals.
docker events --since "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" --filter type=container \
    --format '{{.TimeNano}} {{.Action}} {{.Actor.ID}} {{index .Actor.Attributes "image"}} {{index .Actor.Attributes "signal"}}' \
    > "$out/events.log" &
events=$!
stop_events() {
    kill "$events" 2>/dev/null || true
    wait "$events" || true
}
trap stop_events EXIT
trap 'exit 0' TERM

# Request a live stack while the failing operation is still blocked.
coproc DAEMON_LOG { exec sudo -n tail --pid="$$" --sleep-interval=0.1 -n 0 -F "$daemon_dir/dockerd.log"; }
log_fd=${DAEMON_LOG[0]}
next_dump=0
while IFS= read -r line <&"$log_fd"; do
    if [[ $line == *'of kill - trying direct SIGKILL'* ]]; then
        printf '%s\n' "$line" >> "$out/fallbacks.log"
        # One stack dump covers a burst of concurrent fallbacks.
        if (( SECONDS >= next_dump )); then
            sudo -n kill -USR1 "$pid"
            next_dump=$((SECONDS + 1))
        fi
    fi
done

echo "Lost the Docker daemon log stream." >&2
exit 1
