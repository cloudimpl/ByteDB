#!/bin/bash

# CI Test Script for ByteDB
# This script runs comprehensive tests to ensure the SQL query engine works correctly

set -e  # Exit on any error

echo "🔧 Building ByteDB..."
go build

echo "📊 Running unit tests..."
go test -v

echo "🧪 Running integration tests..."
go test -v -run TestEndToEnd

echo "🎯 Running accuracy tests..."
go test -v -run TestQueryResultAccuracy

echo ""
echo "🎉 ALL TESTS PASSED!"
echo "✅ ByteDB SQL query engine is working correctly"
echo "✅ Parquet file reading is functional"
echo "✅ WHERE clause filtering is accurate"
echo "✅ Type coercion is working"
echo "✅ Error handling is proper"
echo "✅ JSON output is correct"
echo "✅ Schema inspection works"
echo ""
echo "🚀 ByteDB is ready for production use!"