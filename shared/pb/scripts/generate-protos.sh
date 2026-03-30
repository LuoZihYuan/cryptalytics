#!/bin/bash
set -e

cd "$(dirname "$0")/.."

PROTO_DIR="proto"
PB_OUT="src/pb"

uv run python -m grpc_tools.protoc \
    -I="$PROTO_DIR" \
    --python_out="$PB_OUT" \
    --grpc_python_out="$PB_OUT" \
    "$PROTO_DIR/ingestion.proto"

# Fix relative import inside generated gRPC stub
sed -i '' 's/import ingestion_pb2/from . import ingestion_pb2/' "$PB_OUT/ingestion_pb2_grpc.py"

echo "✓ Proto files generated in $PB_OUT"