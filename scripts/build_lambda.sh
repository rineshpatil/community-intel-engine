#!/usr/bin/env bash
set -euo pipefail

# Packages the Lambda bundle without Docker. uv's --python-platform fetches
# manylinux wheels for compiled dependencies (pydantic-core) so the bundle runs
# on Lambda regardless of the machine that built it.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/build/lambda"

rm -rf "$OUT"
mkdir -p "$OUT"

uv pip install \
  --target "$OUT" \
  --python-platform x86_64-manylinux2014 \
  --python-version 3.12 \
  --only-binary :all: \
  pydantic pydantic-settings praw

cp -R "$ROOT/src/community_intel" "$OUT/community_intel"

# boto3 is provided by the Lambda runtime; shipping it wastes ~10MB.
rm -rf "$OUT"/boto3 "$OUT"/botocore "$OUT"/*.dist-info

echo "built $OUT"
