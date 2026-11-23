#!/usr/bin/env bash
set -euo pipefail

image=alpine:3.21
work=$(mktemp -d)
label="relayer.broker-removal=$work"
cleanup() {
    docker ps -aq --filter "label=$label" | xargs -r timeout 40 docker rm -fv >/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

docker pull "$image" >/dev/null
docker info --format 'engine={{.ServerVersion}} driver={{.Driver}} kernel={{.KernelVersion}} cgroup={{.CgroupDriver}}'
failed=0
for mode in published none; do
    args=(--network none)
    if [[ $mode == published ]]; then
        args=(-p 20000-29999:9092 -p 20000-29999:9093)
    fi
    for round in {1..12}; do
        pids=()
        for i in {1..12}; do
            docker run -d --label "$label" "${args[@]}" "$image" sleep 300 > "$work/id-$i" &
            pids+=("$!")
        done
        for pid in "${pids[@]}"; do wait "$pid"; done

        pids=()
        for i in {1..12}; do
            (
                read -r id < "$work/id-$i"
                start=$(date +%s%3N)
                code=0
                timeout 35 docker rm -fv "$id" >/dev/null || code=$?
                elapsed=$(($(date +%s%3N) - start))
                printf '%s %s %s %s ms (exit %s)\n' "$mode" "$round" "${id:0:12}" "$elapsed" "$code"
                (( code == 0 && elapsed < 15000 ))
            ) &
            pids+=("$!")
        done
        for pid in "${pids[@]}"; do
            wait "$pid" || failed=1
        done
    done
done
exit "$failed"
