# ByteDB Makefile
# Provides automated testing and build commands

.PHONY: build test test-case clean help

# Default target
all: build

# Build ByteDB
build:
	@echo "Building ByteDB..."
	@go build -o bytedb main.go parser.go query_engine.go parquet_reader.go cache.go
	@echo "✅ Build completed successfully"

# Run all tests
test: test-case

# Run CASE expression tests specifically
test-case: build
	@echo "Running CASE expression tests..."
	@./run_case_tests.sh
	@echo ""
	@echo "Validating results..."
	@python3 validate_case_tests.py

# Run quick smoke test
smoke-test: build
	@echo "Running smoke test..."
	@echo "SELECT 'Smoke test passed' as result" | ./bytedb ./data

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f bytedb case_test_results.txt
	@echo "✅ Clean completed"

# Show help
help:
	@echo "ByteDB Test Framework"
	@echo ""
	@echo "Available commands:"
	@echo "  make build      - Build ByteDB"
	@echo "  make test       - Run all tests"
	@echo "  make test-case  - Run CASE expression tests"
	@echo "  make smoke-test - Run quick smoke test"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make help       - Show this help"