#!/bin/bash

# CASE Expression Test Runner for ByteDB
# This script runs all CASE expression tests and captures results

echo "=========================================="
echo "ByteDB CASE Expression Test Suite"
echo "=========================================="
echo "Date: $(date)"
echo ""

# Build ByteDB
echo "Building ByteDB..."
go build -o bytedb main.go parser.go query_engine.go parquet_reader.go cache.go

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful"
echo ""

# Run tests
echo "Running CASE expression tests..."
echo ""

# Run the test file
./bytedb ./data < test_case_expressions.sql > case_test_results.txt 2>&1

# Check if tests ran successfully
if [ $? -eq 0 ]; then
    echo "✅ Tests completed successfully"
    echo ""
    echo "Test results saved to: case_test_results.txt"
    echo ""
    echo "Summary of tests run:"
    grep "Test [0-9]*:" case_test_results.txt | head -20
    echo ""
    echo "Final message:"
    grep "All CASE expression tests completed" case_test_results.txt
else
    echo "❌ Tests failed with errors"
    echo "Error details:"
    tail -20 case_test_results.txt
fi

echo ""
echo "=========================================="
echo "Test run completed. Check case_test_results.txt for full output."
echo "=========================================="