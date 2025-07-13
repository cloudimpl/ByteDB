# Distributed SQL Testing with Data Validation

The distributed test framework now includes comprehensive data validation capabilities, allowing tests to verify not just row counts and column names, but exact data content.

## Data Validation Features

### SQL Annotation Format

Use the `@expect_data` annotation with a JSON array to specify expected data:

```sql
-- @test name=verify_exact_results
-- @description Validate exact data content
-- @expect_rows 3
-- @expect_columns department,count
-- @expect_data [{"department": "Engineering", "count": 4}, {"department": "Sales", "count": 3}, {"department": "HR", "count": 2}]
SELECT department, COUNT(*) as count
FROM employees
GROUP BY department
ORDER BY department;
```

### JSON Test Format

In JSON test files, use the `data` field within `expected`:

```json
{
  "name": "test_exact_data",
  "description": "Test with exact data validation",
  "sql": "SELECT department, COUNT(*) as count FROM employees GROUP BY department ORDER BY department",
  "expected": {
    "row_count": 3,
    "columns": ["department", "count"],
    "data": [
      {"department": "Engineering", "count": 4},
      {"department": "Sales", "count": 3},
      {"department": "HR", "count": 2}
    ]
  }
}
```

## Validation Behavior

### Type Flexibility

The validator handles type conversions intelligently:
- Numeric types are compared by value (int vs float64)
- String representations are used as fallback
- NULL values are properly handled

### Error Reporting

Data validation errors provide detailed information:
- Row-level mismatches
- Column-specific differences
- Expected vs actual values

Example error:
```
row 0, column 'count': expected 5, got 4
row 2: missing column 'department'
expected 5 data rows, got 3
```

## Test Examples

### Basic Data Validation

```sql
-- @test name=exact_employee_count
-- @expect_rows 1
-- @expect_columns total
-- @expect_data [{"total": 10}]
SELECT COUNT(*) as total FROM employees;
```

### Complex Aggregation

```sql
-- @test name=department_statistics
-- @expect_rows 5
-- @expect_columns department,emp_count,avg_salary
-- @expect_data [{"department": "Engineering", "emp_count": 4, "avg_salary": 72500}, {"department": "Finance", "emp_count": 1, "avg_salary": 55000}]
SELECT 
    department,
    COUNT(*) as emp_count,
    AVG(salary) as avg_salary
FROM employees
GROUP BY department
ORDER BY department;
```

### Empty Result Validation

```sql
-- @test name=no_results
-- @expect_rows 0
-- @expect_columns name,salary
-- @expect_data []
SELECT name, salary FROM employees WHERE salary > 1000000;
```

## Running Data Validation Tests

```bash
# Run the SQL data validation tests
./distributed_sql_test_runner -file tests/distributed_data_validation.sql -verbose

# Run the JSON data validation tests
./distributed_sql_test_runner -file tests/distributed_data_validation.json -verbose

# Run with specific worker count
./distributed_sql_test_runner -file tests/distributed_data_validation.sql -workers 5
```

## Benefits

1. **Comprehensive Testing**: Verify not just structure but actual query results
2. **Regression Prevention**: Detect when query results change unexpectedly
3. **Integration Testing**: Validate end-to-end distributed query execution
4. **Documentation**: Tests serve as examples of expected query behavior

## Future Enhancements

- Order-agnostic validation for unordered result sets
- Partial data validation (subset matching)
- Pattern-based validation for dynamic data
- Column-level transformations for comparison