# Distributed SQL Testing with Data Validation and Tracing

The distributed test framework includes comprehensive data validation capabilities and advanced tracing integration, allowing tests to verify not just row counts and column names, but exact data content while providing deep observability into distributed query execution.

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

## Distributed Tracing in Tests

### Trace Configuration in Tests

The distributed test framework supports comprehensive tracing configuration through test annotations:

```sql
-- @test name=traced_aggregation_test
-- @description Test COUNT with detailed tracing
-- @expect_rows 1
-- @expect_columns total_count
-- @workers 3
-- @trace_level DEBUG
-- @trace_components COORDINATOR,WORKER,FRAGMENT,AGGREGATION
SELECT COUNT(*) as total_count FROM employees;
```

### Available Trace Components for Testing

| Component | Purpose | Use in Tests |
|-----------|---------|--------------|
| `COORDINATOR` | Query orchestration | Track query distribution and result aggregation |
| `WORKER` | Worker operations | Monitor worker initialization and fragment assignment |
| `FRAGMENT` | Fragment execution | Detailed fragment performance and execution metrics |
| `PLANNING` | Query planning | Analyze distributed plan generation and optimization |
| `NETWORK` | Network operations | Monitor data transfer and optimization effectiveness |
| `AGGREGATION` | Distributed aggregation | Verify partial aggregation and result combination |
| `PARTITIONING` | Data partitioning | Track partition strategies and data distribution |
| `MONITORING` | System monitoring | Health checks and resource utilization |

### Command-Line Trace Configuration

Override trace settings for specific test runs:

```bash
# Enable comprehensive tracing for all tests
./distributed_sql_test_runner \
  -file tests/distributed_basic_queries.sql \
  -trace-level DEBUG \
  -trace-components ALL \
  -verbose

# Focus on performance analysis
./distributed_sql_test_runner \
  -dir tests \
  -trace-level INFO \
  -trace-components FRAGMENT,AGGREGATION,NETWORK \
  -workers 3

# Debug worker issues
./distributed_sql_test_runner \
  -file tests/distributed_data_validation.json \
  -trace-level VERBOSE \
  -trace-components WORKER,FRAGMENT,MONITORING
```

### Trace Output in Test Results

When tracing is enabled, test output includes structured trace messages:

```
   🧪 Test: distributed_count_aggregation
      Description: Test COUNT(*) with network optimization ✅ PASSED (1 rows in 1.851916ms)
      Workers Used: 3
      Fragments: 3
      ✨ Network Optimization Applied
      📍 Trace Messages:
         [18:24:37.066] INFO/COORDINATOR: Executing distributed query | requestID=test-123 sql=SELECT COUNT(*) FROM employees
         [18:24:37.089] INFO/FRAGMENT: Fragment execution completed | fragmentID=scan_fragment_0_agg duration=3.422ms rowsReturned=1
         [18:24:37.094] INFO/AGGREGATION: Partial aggregation applied | function=COUNT partialResults=3 finalResult=10
```

### Test-Specific Tracing Scenarios

#### Performance Testing
```sql
-- @test name=performance_benchmark
-- @description Benchmark distributed query performance
-- @expect_rows 5
-- @workers 3
-- @trace_level INFO
-- @trace_components FRAGMENT,AGGREGATION,NETWORK
-- @performance max_duration=100ms
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department;
```

#### Network Optimization Verification
```sql
-- @test name=network_optimization_test
-- @description Verify network optimization effectiveness
-- @expect_rows 1
-- @workers 3
-- @network_optimization
-- @verify_partial_aggs
-- @trace_level DEBUG
-- @trace_components NETWORK,AGGREGATION
SELECT COUNT(*) as total FROM employees;
```

#### Worker Health Monitoring
```sql
-- @test name=worker_health_test
-- @description Monitor worker health during execution
-- @expect_rows 10
-- @workers 3
-- @trace_level DEBUG
-- @trace_components WORKER,MONITORING,FRAGMENT
SELECT * FROM employees ORDER BY id;
```

### Analyzing Test Traces

#### Extract Performance Metrics
```bash
# Get fragment execution times
grep "Fragment execution completed" test_output.log | \
  awk -F'duration=' '{print $2}' | \
  awk -F'ms' '{print $1}' | \
  sort -n

# Check network optimization effectiveness
grep "Data transfer optimized" test_output.log | \
  awk -F'reduction=' '{print $2}' | \
  awk -F'%' '{print $1}'
```

#### Validate Distributed Behavior
```bash
# Verify all workers are used
grep "Fragment execution completed" test_output.log | \
  awk -F'workerID=' '{print $2}' | \
  awk -F' ' '{print $1}' | \
  sort | uniq -c

# Check aggregation effectiveness
grep "Partial aggregation applied" test_output.log | \
  awk -F'partialResults=' '{print $2}' | \
  awk -F' ' '{print $1}'
```

### Integration with CI/CD

#### Automated Performance Testing
```bash
#!/bin/bash
# ci_distributed_tests.sh

# Run distributed tests with performance tracing
./distributed_sql_test_runner \
  -dir tests \
  -trace-level INFO \
  -trace-components FRAGMENT,AGGREGATION,NETWORK \
  -verbose \
  2>&1 | tee distributed_test_results.log

# Extract and validate performance metrics
avg_duration=$(grep "Fragment execution completed" distributed_test_results.log | \
  awk -F'duration=' '{print $2}' | \
  awk -F'ms' '{sum+=$1; count++} END {print sum/count}')

echo "Average fragment execution time: ${avg_duration}ms"

# Fail if performance degrades
if (( $(echo "$avg_duration > 50" | bc -l) )); then
  echo "Performance regression detected!"
  exit 1
fi
```

#### Test Coverage Verification
```bash
# Verify all distributed components are tested
components_tested=$(grep -E "trace_components|TRACE_COMPONENTS" tests/*.sql tests/*.json | \
  grep -oE "(COORDINATOR|WORKER|FRAGMENT|PLANNING|NETWORK|AGGREGATION|PARTITIONING|MONITORING)" | \
  sort | uniq)

echo "Tested components: $components_tested"
```

### Debugging Failed Tests

#### Common Debugging Patterns

1. **Enable Maximum Tracing**:
```bash
./distributed_sql_test_runner \
  -file tests/failing_test.sql \
  -trace-level VERBOSE \
  -trace-components ALL \
  -verbose
```

2. **Focus on Specific Component**:
```bash
# If aggregation is failing
./distributed_sql_test_runner \
  -file tests/failing_test.sql \
  -trace-level DEBUG \
  -trace-components AGGREGATION,FRAGMENT
```

3. **Analyze Worker Distribution**:
```bash
# Check if workers are balanced
grep "Fragment execution" test_output.log | \
  awk -F'workerID=' '{print $2}' | \
  awk -F' ' '{print $1}' | \
  sort | uniq -c
```

#### Test Environment Debugging

```bash
# Check test data distribution
for worker in 1 2 3; do
  echo "Worker $worker data:"
  ls -la data/worker-$worker/
done

# Verify table mappings
cat data/worker-1/table_mappings.json
```

## Future Enhancements

### Testing Framework
- Order-agnostic validation for unordered result sets
- Partial data validation (subset matching)
- Pattern-based validation for dynamic data
- Column-level transformations for comparison

### Tracing Integration
- Automated performance regression detection
- Trace-based test assertions (verify optimization applied)
- Integration with monitoring dashboards
- Custom trace analysis plugins for test framework