# ByteDB Makefile
# Provides automated testing and build commands

.PHONY: build test test-case test-joins test-window test-subqueries bench clean data help

# Default target
all: build

# Build ByteDB
build:
	@echo "Building ByteDB..."
	@go build -o bytedb main.go parser.go query_engine.go parquet_reader.go cache.go
	@echo "✅ Build completed successfully"

# Run all tests
test: build
	@echo "Running all tests..."
	@go test -v ./...
	@echo "✅ All tests completed"

# Run CASE expression tests specifically
test-case: build
	@echo "Running CASE expression tests..."
	@go test -v -run "TestBasicCaseExpressions|TestCaseExpressionDataTypes|TestCaseExpressionErrors"

# Run JOIN tests
test-joins: build
	@echo "Running JOIN tests..."
	@go test -v -run "TestJoin"

# Run window function tests
test-window: build
	@echo "Running window function tests..."
	@go test -v -run "TestWindow"

# Run subquery tests
test-subqueries: build
	@echo "Running subquery tests..."
	@go test -v -run "TestSubquery"

# Run benchmarks
bench: build
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem

# Generate sample data
data:
	@echo "Generating sample data..."
	@go run gen_data.go
	@echo "✅ Sample data generated in ./data/"

# Run quick smoke test
smoke-test: build data
	@echo "Running smoke test..."
	@echo "SELECT name, department FROM employees LIMIT 1" | ./bytedb ./data
	@echo "✅ Smoke test passed - ByteDB is working!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f bytedb
	@rm -rf ./data/*.parquet
	@echo "✅ Clean completed"

# Show help
help:
	@echo "ByteDB Build & Test System"
	@echo ""
	@echo "Available commands:"
	@echo "  make build        - Build ByteDB binary"
	@echo "  make test         - Run all Go tests"
	@echo "  make test-case    - Run CASE expression tests"
	@echo "  make test-joins   - Run JOIN operation tests"
	@echo "  make test-window  - Run window function tests"
	@echo "  make test-subqueries - Run subquery tests"
	@echo "  make bench        - Run performance benchmarks"
	@echo "  make data         - Generate sample Parquet data"
	@echo "  make smoke-test   - Run quick functionality test"
	@echo "  make clean        - Clean build artifacts and data"
	@echo "  make help         - Show this help"
	@echo ""
	@echo "Example usage:"
	@echo "  make build && make data && make smoke-test"