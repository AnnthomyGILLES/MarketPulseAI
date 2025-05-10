#!/bin/bash
# wait-for-cassandra.sh - Wait for Cassandra to be ready before running the main application

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for Cassandra at $host:$port to be ready..."

# Keep checking until Cassandra is ready
until cqlsh $host $port -e "DESCRIBE KEYSPACES" --connect-timeout=15 >/dev/null 2>&1; do
  >&2 echo "Cassandra is unavailable - sleeping for 10 seconds"
  sleep 10
done

>&2 echo "Cassandra is up - executing command"
exec $cmd 