#!/bin/sh
# Starts a small Redis Cluster (masters only by default) inside one container.
# Nodes announce 127.0.0.1 so that clients on the Docker host can follow
# MOVED/ASK redirects through published ports. Intended for development and
# integration tests only.
set -eu
PORTS="${CLUSTER_PORTS:-7001 7002 7003}"
ANNOUNCE_IP="${CLUSTER_ANNOUNCE_IP:-127.0.0.1}"
for p in $PORTS; do
  # Ephemeral test cluster: always start from a clean node state, so a
  # restarted container does not trip over stale nodes.conf files.
  rm -rf "/data/$p"
  mkdir -p "/data/$p"
  redis-server --port "$p" --cluster-enabled yes \
    --cluster-config-file "/data/$p/nodes.conf" --dir "/data/$p" \
    --cluster-announce-ip "$ANNOUNCE_IP" --cluster-node-timeout 2000 \
    --appendonly no --save "" --protected-mode no --daemonize yes \
    --logfile "/data/$p/redis.log"
done
for p in $PORTS; do
  until redis-cli -p "$p" ping >/dev/null 2>&1; do sleep 0.1; done
done
FIRST=$(echo "$PORTS" | awk '{print $1}')
if ! redis-cli -p "$FIRST" cluster info | grep -q 'cluster_state:ok'; then
  ADDRS=""
  for p in $PORTS; do ADDRS="$ADDRS 127.0.0.1:$p"; done
  # shellcheck disable=SC2086
  yes yes | redis-cli --cluster create $ADDRS --cluster-replicas 0 >/dev/null
fi
until redis-cli -p "$FIRST" cluster info | grep -q 'cluster_state:ok'; do sleep 0.2; done
echo "cluster ready on ports: $PORTS"
# Keep the container alive while the nodes run.
tail -f /dev/null
