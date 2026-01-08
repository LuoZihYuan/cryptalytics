#!/bin/bash
set -e

PROTO_DIR="proto"
OUT_DIR="services/ingestion/ingestion/pb"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

cd services/ingestion
uv run python -m grpc_tools.protoc \
    -I="../../$PROTO_DIR" \
    --python_out="../../$OUT_DIR" \
    --grpc_python_out="../../$OUT_DIR" \
    "../../$PROTO_DIR/ingestion.proto"
cd ../..

sed -i '' 's/import ingestion_pb2/from . import ingestion_pb2/' "$OUT_DIR/ingestion_pb2_grpc.py"

echo "✓ Proto files generated in $OUT_DIR"