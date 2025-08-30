#!/usr/bin/env bash
set -euo pipefail

echo "[init] Aguardando cluster do Cassandra ficar pronto..."
for i in {1..120}; do
  if cqlsh cassandra1 9042 -e "DESCRIBE CLUSTER;" >/dev/null 2>&1; then
    echo "[init] Cluster OK."
    break
  fi
  sleep 2
done

echo "[init] Aplicando migrations em ordem..."
shopt -s nullglob
for f in /migrations/*.cql; do
  echo "[init] Aplicando ${f}"
  cqlsh cassandra1 9042 -f "${f}"
done
echo "[init] Migrations aplicadas com sucesso."
