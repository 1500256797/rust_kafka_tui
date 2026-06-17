#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
elif command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
else
  echo "未找到 podman 或 docker，请先安装 Podman Desktop / Docker。" >&2
  exit 1
fi

if ${RUNTIME} compose version >/dev/null 2>&1; then
  COMPOSE="${RUNTIME} compose"
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE="podman-compose"
else
  echo "未找到 compose 插件，请安装 podman-compose 或启用 ${RUNTIME} compose。" >&2
  exit 1
fi

echo "使用: ${COMPOSE}"
${COMPOSE} up -d

echo
echo "Kafka:  localhost:9092"
echo "AKHQ:   http://localhost:8080"
echo
echo "测试 kafka-tui:"
echo "  cargo run -- --config ../config.example.yaml"
