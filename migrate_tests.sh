#!/bin/bash

# Script to help migrate tests to use fixed test data

echo "Test Migration Helper"
echo "===================="
echo ""
echo "This script will help identify tests that need migration to the new test data system."
echo ""

# Find all test files
echo "Finding test files..."
TEST_FILES=$(find . -name "*_test.go" -not -path "./vendor/*" | sort)

echo "Found $(echo "$TEST_FILES" | wc -l | xargs) test files"
echo ""

# Check each file for old patterns
echo "Checking for tests that need migration..."
echo ""

for file in $TEST_FILES; do
    # Skip our new test files
    if [[ "$file" == "./test_base.go" || "$file" == "./test_helpers.go" || "$file" == "./test_data.go" ]]; then
        continue
    fi
    
    needs_migration=false
    reasons=""
    
    # Check for generateSampleData calls
    if grep -q "generateSampleData()" "$file" 2>/dev/null; then
        needs_migration=true
        reasons="${reasons}- Contains generateSampleData() calls\n"
    fi
    
    # Check for NewQueryEngine("./data")
    if grep -q 'NewQueryEngine("./data")' "$file" 2>/dev/null; then
        needs_migration=true
        reasons="${reasons}- Uses NewQueryEngine(\"./data\")\n"
    fi
    
    # Check for hardcoded test expectations without constants
    if grep -E "Expected [0-9]+ (rows|employees|products)" "$file" 2>/dev/null | grep -v "Test" >/dev/null; then
        needs_migration=true
        reasons="${reasons}- Contains hardcoded numeric expectations\n"
    fi
    
    if [ "$needs_migration" = true ]; then
        echo "❌ $file needs migration:"
        echo -e "$reasons"
    else
        echo "✅ $file appears to be migrated or doesn't need migration"
    fi
done

echo ""
echo "Migration Guide:"
echo "==============="
echo "1. Replace generateSampleData() calls - they're no longer needed"
echo "2. Replace NewQueryEngine(\"./data\") with NewTestQueryEngine()"
echo "3. Use test data constants instead of magic numbers:"
echo "   - TestEmployeesInEngineering instead of 4"
echo "   - TestEmployeesOver70k instead of 7"
echo "   - TestProductsOver100 instead of 3"
echo "   etc."
echo ""
echo "See TEST_DATA_MIGRATION.md for complete guide"