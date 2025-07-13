# ByteDB Makefile

.PHONY: build clean test test-sql test-unit test-all help run-basic run-case run-error run-performance build-test-runner

# Build the main ByteDB binary
build:
	go build -o bytedb main.go

# Build the SQL test runner
build-test-runner:
	go build -o sql_test_runner cmd/sql_test_runner.go

# Clean build artifacts
clean:
	rm -f bytedb sql_test_runner
	go clean

# Generate sample data
gen-data:
	go run demos/gen_data.go

# Run all tests (unit + SQL)
test-all: test-unit test-sql

# Run Go unit tests
test-unit:
	go test -v ./...

# Run SQL tests
test-sql: build-test-runner
	./sql_test_runner -dir tests/ -verbose

# Run basic SQL tests only
test-basic: build-test-runner
	./sql_test_runner -file tests/basic_queries.sql -verbose

# Run CASE expression tests (regression tests)
test-case: build-test-runner
	./sql_test_runner -file tests/case_expressions.sql -verbose -trace-level DEBUG

# Run error handling tests
test-error: build-test-runner
	./sql_test_runner -file tests/error_handling.sql -verbose

# Run performance tests
test-performance: build-test-runner
	./sql_test_runner -file tests/performance_tests.json -verbose

# Run SQL tests with tracing enabled
test-trace: build-test-runner
	./sql_test_runner -dir tests/ -verbose -trace-level DEBUG -trace-components ALL

# Run only tests with specific tags
test-tags-%: build-test-runner
	./sql_test_runner -dir tests/ -verbose -tags $*

# Quick test for basic functionality
test-quick: build-test-runner
	./sql_test_runner -dir tests/ -tags basic

# Development workflow - build, generate data, and test
dev: build gen-data test-basic

# Show available targets
help:
	@echo "ByteDB Makefile Targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  build              - Build the main ByteDB binary"
	@echo "  build-test-runner  - Build the SQL test runner"
	@echo "  clean              - Clean build artifacts"
	@echo ""
	@echo "Data targets:"
	@echo "  gen-data           - Generate sample data files"
	@echo ""
	@echo "Test targets:"
	@echo "  test-all           - Run all tests (unit + SQL)"
	@echo "  test-unit          - Run Go unit tests"
	@echo "  test-sql           - Run all SQL tests"
	@echo "  test-basic         - Run basic SQL tests only"
	@echo "  test-case          - Run CASE expression tests with DEBUG tracing"
	@echo "  test-error         - Run error handling tests"
	@echo "  test-performance   - Run performance tests"
	@echo "  test-trace         - Run SQL tests with full tracing"
	@echo "  test-quick         - Quick test for basic functionality"
	@echo "  test-tags-TAG      - Run tests with specific tags (e.g., make test-tags-basic)"
	@echo ""
	@echo "Development targets:"
	@echo "  dev                - Build, generate data, and run basic tests"
	@echo "  help               - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make dev                    # Quick development setup"
	@echo "  make test-case              # Test CASE expressions with tracing"
	@echo "  make test-tags-performance  # Run only performance tests"
	@echo "  make test-trace             # Run all tests with full tracing"

# Default target
all: build build-test-runner