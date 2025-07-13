# ByteDB SQL Testing Framework

This directory contains comprehensive SQL tests for ByteDB using the integrated testing framework with traceability support.

## Overview

The ByteDB SQL Testing Framework provides:

- **SQL File Testing**: Write tests directly in SQL files with special comment annotations
- **JSON Test Suites**: Define complex test scenarios in JSON format
- **Traceability Integration**: Built-in tracing support for debugging query execution
- **Performance Testing**: Validate query performance and optimization
- **Error Testing**: Verify proper error handling and validation
- **Assertion System**: Rich validation of results, performance, and traces

## Quick Start

### Running Tests

```bash
# Build the test runner
go build -o sql_test_runner cmd/sql_test_runner.go

# Run a single SQL test file
./sql_test_runner -file tests/basic_queries.sql

# Run all tests in the directory
./sql_test_runner -dir tests/ -verbose

# Run with tracing enabled
./sql_test_runner -file tests/case_expressions.sql -trace-level DEBUG

# Filter tests by tags
./sql_test_runner -dir tests/ -tags basic,case
```

### Example Output

```
=== Processing: tests/basic_queries.sql ===
Running test suite: basic_queries

[1/10] Running test: simple_select
  ✓ PASS - simple_select (15.2ms)

[2/10] Running test: filtered_select
  ✓ PASS - filtered_select (8.1ms)

[3/10] Running test: count_all
  ✓ PASS - count_all (12.3ms)

=== Test Summary ===
Total: 10 tests
Passed: 10
Failed: 0
Duration: 156.7ms
```

## Test File Formats

### SQL Test Files

SQL test files use special comment annotations to define test cases:

```sql
-- @test name=my_test
-- @description Test description
-- @expect_rows 5
-- @expect_columns id,name,department
-- @tags basic,select
-- @trace_level DEBUG
-- @trace_components QUERY,EXECUTION
-- @timeout 5s
SELECT * FROM employees WHERE department = 'Engineering';

-- @test name=another_test
-- @description Another test
-- @expect_error table not found
-- @tags error,negative
SELECT * FROM non_existent_table;
```

#### Available Annotations

| Annotation | Description | Example |
|------------|-------------|---------|
| `@test name=<name>` | Test case name (required) | `@test name=basic_select` |
| `@description <text>` | Test description | `@description Test basic SELECT` |
| `@expect_rows <number>` | Expected row count | `@expect_rows 10` |
| `@expect_columns <list>` | Expected column names | `@expect_columns id,name,salary` |
| `@expect_error <text>` | Expected error message | `@expect_error table not found` |
| `@tags <list>` | Test tags for filtering | `@tags basic,select,performance` |
| `@trace_level <level>` | Tracing level | `@trace_level DEBUG` |
| `@trace_components <list>` | Trace components | `@trace_components QUERY,CASE` |
| `@timeout <duration>` | Test timeout | `@timeout 5s` |

#### Trace Levels

- `OFF` - No tracing
- `ERROR` - Critical errors only
- `WARN` - Warning conditions
- `INFO` - General information
- `DEBUG` - Detailed execution information
- `VERBOSE` - Maximum detail

#### Trace Components

- `QUERY` - Overall query lifecycle
- `PARSER` - SQL parsing operations
- `OPTIMIZER` - Query optimization decisions
- `EXECUTION` - Query execution phases
- `CASE` - CASE expression evaluation
- `SORT` - Sorting operations
- `JOIN` - Join operations
- `FILTER` - WHERE clause processing
- `AGGREGATE` - GROUP BY and aggregation functions
- `CACHE` - Query result caching

### JSON Test Files

JSON test files provide more structured test definitions:

```json
{
  "name": "my_test_suite",
  "description": "Test suite description",
  "test_cases": [
    {
      "name": "test_name",
      "sql": "SELECT * FROM employees",
      "description": "Test description",
      "expected": {
        "row_count": 10,
        "columns": ["id", "name", "department"],
        "performance": {
          "max_duration": "1s"
        }
      },
      "trace": {
        "level": "DEBUG",
        "components": ["QUERY", "EXECUTION"]
      },
      "tags": ["basic", "select"],
      "timeout": "5s"
    }
  ]
}
```

## Test Files in This Directory

### basic_queries.sql
Core SQL functionality tests including:
- Simple SELECT queries
- WHERE clause filtering
- JOIN operations
- Aggregate functions
- String functions
- Arithmetic expressions

### case_expressions.sql
Comprehensive CASE expression tests including:
- Basic CASE expressions
- CASE with ORDER BY (regression test)
- Nested CASE expressions
- CASE in WHERE clauses
- CASE with aggregation

### error_handling.sql
Error condition tests including:
- Invalid table names
- Invalid column names
- Syntax errors
- Division by zero
- Type mismatches

### performance_tests.json
Performance validation tests including:
- Large table scans
- Aggregation performance
- JOIN performance
- Complex query performance

## Writing New Tests

### Best Practices

1. **Use Descriptive Names**: Test names should clearly describe what is being tested
2. **Add Descriptions**: Include meaningful descriptions for complex tests
3. **Use Appropriate Tags**: Tag tests for easy filtering and organization
4. **Set Realistic Timeouts**: Don't make timeouts too strict or too loose
5. **Use Tracing Wisely**: Enable tracing for debugging but be mindful of performance
6. **Test Error Conditions**: Include negative tests for error handling
7. **Validate Results**: Use assertions to verify expected outcomes

### Example: Adding a New Test

```sql
-- @test name=window_functions_basic
-- @description Test basic window function functionality
-- @expect_rows 10
-- @expect_columns name,salary,department,rank
-- @tags window,advanced
-- @trace_level DEBUG
-- @trace_components QUERY,EXECUTION
-- @timeout 10s
SELECT name, 
       salary, 
       department,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees
ORDER BY department, rank;
```

### Debugging Failed Tests

When tests fail, use tracing to debug:

```bash
# Enable verbose tracing for failed test
./sql_test_runner -file tests/case_expressions.sql \
                  -trace-level VERBOSE \
                  -trace-components ALL \
                  -verbose

# Run specific test by tag
./sql_test_runner -dir tests/ -tags regression -verbose
```

The trace output will show detailed execution steps:

```
[15:04:05.126] DEBUG/CASE: Evaluating CASE expression | alias=salary_grade when_clauses=2
[15:04:05.126] DEBUG/CASE: WHEN clause matched | clause_index=1 result=Medium
[15:04:05.127] INFO/OPTIMIZER: Skipping optimization due to CASE expression ORDER BY
[15:04:05.128] DEBUG/SORT: Sorting rows | column=salary_grade order=DESC count=10
```

## Command Line Options

### sql_test_runner Options

```bash
Usage: sql_test_runner [options]

Options:
  -file string
        Path to the SQL test file
  -dir string
        Path to directory containing SQL test files
  -data string
        Path to the data directory (default "./data")
  -verbose
        Enable verbose output
  -tags string
        Comma-separated list of tags to filter tests
  -trace-level string
        Override trace level for all tests (OFF,ERROR,WARN,INFO,DEBUG,VERBOSE)
  -trace-components string
        Override trace components for all tests (comma-separated)
  -timeout string
        Override timeout for all tests
  -format string
        Output format: console, json (default "console")
```

### Examples

```bash
# Basic usage
./sql_test_runner -file tests/basic_queries.sql

# With debugging
./sql_test_runner -file tests/case_expressions.sql \
                  -trace-level DEBUG \
                  -trace-components CASE,SORT,OPTIMIZER \
                  -verbose

# Filter by tags
./sql_test_runner -dir tests/ -tags "basic,performance" -verbose

# Performance testing with timeout override
./sql_test_runner -file tests/performance_tests.json \
                  -timeout 30s \
                  -verbose
```

## Integration with CI/CD

The test runner exits with appropriate exit codes for CI/CD integration:

- Exit code 0: All tests passed
- Exit code 1: One or more tests failed

Example GitHub Actions workflow:

```yaml
- name: Run SQL Tests
  run: |
    go build -o sql_test_runner cmd/sql_test_runner.go
    ./sql_test_runner -dir tests/ -verbose
```

## Contributing

When adding new tests:

1. Choose the appropriate file or create a new one for your test category
2. Use meaningful test names and descriptions
3. Add appropriate tags for organization
4. Include both positive and negative test cases
5. Use tracing to verify internal behavior when needed
6. Update this README if adding new test categories

For more information about the tracing system, see [docs/TRACING.md](../docs/TRACING.md).