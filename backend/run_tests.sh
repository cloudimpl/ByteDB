#!/bin/bash

echo "🚀 Running ByteDB Test Suite"
echo "============================="

echo ""
echo "📝 Running unit tests..."
go test -v

echo ""
echo "🔍 Running integration tests..."
echo ""

echo "Testing basic SELECT queries..."
echo "SELECT * FROM employees LIMIT 3;" | ./bytedb ./data | grep -q "John Doe" && echo "✅ Basic SELECT works" || echo "❌ Basic SELECT failed"

echo "Testing WHERE clause with numeric comparison..."
echo "SELECT name, price FROM products WHERE price > 100;" | ./bytedb ./data | grep -q "Laptop" && echo "✅ Numeric WHERE works" || echo "❌ Numeric WHERE failed"

echo "Testing WHERE clause with string comparison..."
echo "SELECT name FROM employees WHERE department = 'Engineering';" | ./bytedb ./data | grep -q "John Doe" && echo "✅ String WHERE works" || echo "❌ String WHERE failed"

echo "Testing JSON output..."
echo "\\json SELECT * FROM employees LIMIT 1;" | ./bytedb ./data | grep -q '"columns"' && echo "✅ JSON output works" || echo "❌ JSON output failed"

echo "Testing schema inspection..."
echo "\\d employees" | ./bytedb ./data | grep -q "Columns:" && echo "✅ Schema inspection works" || echo "❌ Schema inspection failed"

echo ""
echo "🎉 Test run complete!"
echo ""
echo "To run manual tests, use:"
echo "  ./bytedb ./data"
echo ""
echo "Example queries:"
echo "  SELECT * FROM employees LIMIT 5;"
echo "  SELECT name, salary FROM employees WHERE salary > 75000;"
echo "  SELECT * FROM products WHERE price < 50;"
echo "  \\d employees"
echo "  \\json SELECT * FROM products LIMIT 3;"