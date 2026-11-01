#!/usr/bin/env bash
set -euo pipefail

# Run only on a dedicated Linux test host, before starting/restarting Docker.
# Docker snapshots ip_local_port_range when its allocator initializes. Reserve the
# low portion from kernel-assigned outbound sockets, while Docker still selects
# and atomically binds published ports. This also covers Testcontainers' Ryuk,
# whose builder does not expose a host-port range (moby/moby#54079).
reserved=$(sysctl -n net.ipv4.ip_local_reserved_ports)
sysctl -w "net.ipv4.ip_local_reserved_ports=${reserved:+$reserved,}20000-29999"
sysctl -w 'net.ipv4.ip_local_port_range=20000 60999'
