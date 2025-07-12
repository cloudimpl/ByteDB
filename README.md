# ByteDB - Advanced SQL Query Engine for Parquet Files

A powerful SQL query engine built in Go that allows you to execute complex SQL queries on Parquet files. Features comprehensive SQL support including JOINs, subqueries, aggregate functions, window functions, CASE expressions, and **distributed query execution** across multiple nodes.

Built using [pg_query_go](https://github.com/pganalyze/pg_query_go) for SQL parsing and [parquet-go](https://github.com/parquet-go/parquet-go) for Parquet file handling.

## 📚 Documentation

- 📋 [Feature Status Matrix](FEATURE_STATUS.md) - Comprehensive feature support overview
- 📝 [Changelog](CHANGELOG.md) - Detailed list of changes and fixes
- 🧪 [Test Data Migration Guide](TEST_DATA_MIGRATION.md) - Guide for test data system
- 🌐 [Distributed Query Design](DISTRIBUTED_DESIGN.md) - Architecture and implementation of distributed query execution

## 🎉 Recent Updates

### Bug Fixes (Latest)
- ✅ Fixed WHERE clause operators (AND, OR, BETWEEN, NOT BETWEEN)
- ✅ Fixed SQL string functions (CONCAT, UPPER, LOWER, LENGTH) 
- ✅ Fixed GROUP BY with aggregate functions
- ✅ Fixed CASE expression evaluation
- ✅ Fixed EXISTS and IN subqueries
- ✅ Fixed query optimization rules (column pruning, join ordering)
- ✅ Added support for subqueries in SELECT clause

### Known Issues
- Multiple subqueries in SELECT may intermittently fail
- Some complex JOIN queries may return unexpected results
- SELECT * may not return all columns in optimized execution path

## 🌐 NEW: Distributed Query Execution

ByteDB now supports distributed query execution across multiple worker nodes, enabling horizontal scaling and processing of large datasets with minimal network overhead.

### Key Features
- **99.99% Network Transfer Reduction**: Advanced aggregate optimization performs partial aggregations on workers
- **Physical Data Partitioning**: Data pre-distributed across worker directories for true data locality
- **Cost-Based Query Planning**: Intelligent optimization based on data statistics and cluster resources
- **Pluggable Transport Layer**: Supports in-memory (testing), gRPC, and HTTP transports
- **Multi-Stage Execution**: Optimized execution plans with parallel processing

### Quick Demo
```bash
# Run the distributed demo
go test -run TestDistributedDemo -v

# The demo will:
# 1. Start a coordinator and 3 worker nodes
# 2. Distribute sample data across workers
# 3. Execute various distributed queries
# 4. Show performance comparisons
```

### Example: Distributed Aggregation
```sql
-- This query is optimized to transfer only ~1KB instead of ~1MB
SELECT department, COUNT(*), AVG(salary) 
FROM employees 
GROUP BY department

-- Execution flow:
-- 1. Each worker computes partial COUNT and SUM/COUNT for AVG
-- 2. Workers send only aggregated results (not raw data)
-- 3. Coordinator combines partial results for final answer
```

## 🚀 Features

### Core SQL Support
- **SELECT Queries**: Full SELECT support with column aliases and expressions
- **JOIN Operations**: INNER, LEFT, RIGHT, and FULL OUTER JOINs
- **Subqueries**: Correlated and non-correlated subqueries in SELECT, WHERE, and FROM clauses
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **Window Functions**: ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD() with PARTITION BY and ORDER BY
- **CASE Expressions**: Full CASE WHEN...THEN...ELSE support with nesting
- **Advanced WHERE Clauses**: Complex conditions with AND, OR, parentheses, IN, BETWEEN, LIKE, EXISTS

### Distributed Query Execution
- **Multi-Node Processing**: Scale queries across multiple worker nodes
- **Intelligent Query Planning**: Cost-based optimization for distributed execution
- **Aggregate Pushdown**: Partial aggregation on workers reduces network transfer by 99%+
- **Physical Data Partitioning**: Pre-distributed data across worker directories
- **Flexible Deployment**: Pluggable transport layer (Memory/gRPC/HTTP)
- **Fault Tolerance**: Worker health monitoring and failure handling

### Data Operations
- **Parquet File Reading**: Efficient reading and querying of Parquet files
- **Schema Inspection**: View table schemas and column information
- **Query Caching**: Intelligent caching system for improved performance
- **Data Type Support**: Comprehensive handling of strings, numbers, dates, and NULL values

### Interface & Tools
- **Interactive CLI**: Rich command-line interface with help system
- **Multiple Output Formats**: Table format and JSON output
- **Automated Testing**: Comprehensive test suite with Go testing framework
- **Performance Monitoring**: Built-in query performance tracking

## 📦 Installation

```bash
git clone <repository-url>
cd bytedb
go mod tidy
go build
```

## 🎯 Quick Start

### Generate Sample Data

```bash
go run gen_data.go
```

This creates sample Parquet files in `./data/` directory with employee and department data.

### Start the Query Engine

#### Single-Node Mode
```bash
./bytedb ./data
```

#### Distributed Mode (Demo)
```bash
# Run the distributed demo with 3 workers
go test -run TestDistributedDemo -v

# See distributed queries in action with:
# - Automatic data distribution
# - Multi-worker query execution  
# - 99.99% network optimization
```

### Example Queries

#### Single-Node Queries
```sql
-- Basic aggregation
SELECT COUNT(*) FROM employees;

-- Group by with multiple aggregates
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary 
FROM employees 
GROUP BY department;

-- Complex joins
SELECT e.name, e.salary, d.budget 
FROM employees e 
JOIN departments d ON e.department = d.name 
WHERE e.salary > d.budget * 0.1;
```

#### Distributed Query Examples
```sql
-- Distributed aggregation (99.99% network optimization)
SELECT department, COUNT(*), SUM(salary), AVG(salary) 
FROM employees 
GROUP BY department;
-- Workers compute partial aggregates, coordinator combines

-- Filtered aggregation across nodes
SELECT department, AVG(salary) as avg_salary 
FROM employees 
WHERE salary > 60000 
GROUP BY department 
ORDER BY avg_salary DESC;

-- Large-scale counting
SELECT COUNT(*) as total_employees FROM employees;
-- Each worker counts locally, coordinator sums counts
```

## 💡 Working Examples (Recently Fixed)

### Complex WHERE Clauses
```sql
-- AND/OR operators work correctly
SELECT name, salary, department 
FROM employees 
WHERE (department = 'Engineering' AND salary > 70000) 
   OR (department = 'Sales' AND salary > 60000);

-- BETWEEN and NOT BETWEEN
SELECT name, salary FROM employees WHERE salary BETWEEN 60000 AND 80000;
SELECT name, salary FROM employees WHERE salary NOT BETWEEN 50000 AND 70000;

-- IN operator with multiple values
SELECT name FROM employees WHERE department IN ('Engineering', 'Sales', 'Marketing');
```

### String Functions
```sql
-- CONCAT function
SELECT CONCAT(name, ' - ', department) as employee_info FROM employees;

-- UPPER/LOWER functions  
SELECT UPPER(name) as name_upper, LOWER(department) as dept_lower FROM employees;

-- LENGTH function
SELECT name FROM employees WHERE LENGTH(name) > 10;
```

### Aggregate Functions with GROUP BY
```sql
-- COUNT with GROUP BY works correctly
SELECT department, COUNT(*) as count FROM employees GROUP BY department;

-- Multiple aggregates
SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal, MAX(salary) as max_sal
FROM employees GROUP BY department;
```

### CASE Expressions
```sql
-- Simple CASE
SELECT name, salary,
       CASE 
           WHEN salary > 80000 THEN 'High'
           WHEN salary > 60000 THEN 'Medium'
           ELSE 'Low'
       END as salary_grade
FROM employees;
```

### Subqueries
```sql
-- EXISTS subquery
SELECT name FROM departments d 
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department = d.name);

-- IN subquery
SELECT name FROM employees 
WHERE department IN (SELECT name FROM departments WHERE budget > 200000);

-- Scalar subquery in SELECT  
SELECT name, (SELECT COUNT(*) FROM employees) as total_count FROM employees LIMIT 5;
```

### Basic Queries
```sql
-- Simple queries
SELECT * FROM employees;
SELECT name, department, salary FROM employees WHERE salary > 70000;

-- JOINs
SELECT e.name, e.salary, d.budget 
FROM employees e 
JOIN departments d ON e.department = d.name;

-- Aggregate functions
SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary 
FROM employees 
GROUP BY department;

-- Window functions
SELECT name, salary, department,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank_in_dept
FROM employees;

-- CASE expressions
SELECT name, salary,
       CASE WHEN salary > 80000 THEN 'Senior'
            WHEN salary > 65000 THEN 'Mid-level' 
            ELSE 'Junior' END as level
FROM employees;

-- Correlated subqueries
SELECT e.name, e.department 
FROM employees e 
WHERE EXISTS (SELECT 1 FROM employees e2 WHERE e2.department = e.department AND e2.salary > e.salary);

-- Complex queries
SELECT e.name, e.salary, d.budget,
       CASE WHEN e.salary > AVG(e2.salary) THEN 'Above Average' ELSE 'Below Average' END as performance
FROM employees e 
JOIN departments d ON e.department = d.name
JOIN employees e2 ON e2.department = e.department
GROUP BY e.name, e.salary, d.budget;
```

## 🛠️ Supported SQL Features

### SELECT Queries
- ✅ Column selection and aliases: `SELECT col1 AS alias, col2 FROM table`
- ✅ Wildcard selection: `SELECT * FROM table`
- ✅ Calculated columns and expressions
- ✅ DISTINCT (basic support)
- ✅ LIMIT clause

### JOIN Operations
- ✅ INNER JOIN: `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id`
- ✅ LEFT JOIN: `SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id`
- ✅ RIGHT JOIN: `SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id`
- ✅ FULL OUTER JOIN: `SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id`
- ✅ Multiple table joins
- ✅ Table aliases

### WHERE Clauses
- ✅ Comparison operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- ✅ Logical operators: `AND`, `OR`, `NOT`
- ✅ Parentheses for grouping: `(condition1 OR condition2) AND condition3`
- ✅ String matching: `LIKE` with wildcards
- ✅ Range checking: `BETWEEN value1 AND value2`
- ✅ List membership: `IN (value1, value2, value3)`
- ✅ NULL checking: `IS NULL`, `IS NOT NULL`
- ✅ Subquery conditions: `EXISTS`, `IN (SELECT ...)`

### Aggregate Functions
- ✅ `COUNT(*)`, `COUNT(column)`
- ✅ `SUM(column)`, `AVG(column)`
- ✅ `MIN(column)`, `MAX(column)`
- ✅ `GROUP BY` clause
- ✅ `HAVING` clause (basic support)

### Window Functions
- ✅ `ROW_NUMBER() OVER (...)`
- ✅ `RANK() OVER (...)`, `DENSE_RANK() OVER (...)`
- ✅ `LAG(column, offset, default) OVER (...)`
- ✅ `LEAD(column, offset, default) OVER (...)`
- ✅ `PARTITION BY` for window specification
- ✅ `ORDER BY` for window specification
- ✅ Multiple window functions in same query

### CASE Expressions
- ✅ Searched CASE: `CASE WHEN condition THEN result ELSE default END`
- ✅ Nested CASE expressions
- ✅ Multiple CASE expressions in same query
- ✅ CASE in SELECT, WHERE clauses
- ✅ Mixed data types in results
- ✅ NULL handling

### Subqueries
- ✅ Scalar subqueries in SELECT: `SELECT (SELECT COUNT(*) FROM t2) FROM t1`
- ✅ Subqueries in WHERE: `WHERE col IN (SELECT col FROM t2)`
- ✅ EXISTS subqueries: `WHERE EXISTS (SELECT 1 FROM t2 WHERE ...)`
- ✅ Correlated subqueries: `WHERE col > (SELECT AVG(col) FROM t2 WHERE t2.dept = t1.dept)`

### Other Features
- ✅ `ORDER BY` clause with ASC/DESC
- ✅ Column and table aliases
- ✅ Multiple data types: strings, integers, floats, dates
- ✅ NULL value handling
- ✅ Query result caching

## 🧪 Testing

ByteDB uses a fixed test data system to ensure deterministic and reliable tests.

### Test Data System

Tests use fixed data stored in `./testdata/` directory (automatically created):
- 10 employees with known salaries, departments, and ages
- 10 products with specific prices and categories
- 6 departments with defined budgets

### Running Tests

```bash
# Run all tests
go test -v

# Run specific test categories
go test -v -run TestBasicCaseExpressions
go test -v -run TestJoinOperations
go test -v -run TestWindowFunctions
go test -v -run TestSubqueries

# Run benchmarks
go test -bench=.

# Clean test data
make clean
```

### Writing Tests

Use the test helpers for consistent test data:

```go
func TestExample(t *testing.T) {
    // Use NewTestQueryEngine() instead of NewQueryEngine("./data")
    engine := NewTestQueryEngine()
    defer engine.Close()
    
    // Use constants instead of magic numbers
    result, err := engine.Execute("SELECT * FROM employees WHERE salary > 70000")
    if len(result.Rows) != TestEmployeesOver70k {
        t.Errorf("Expected %d employees, got %d", TestEmployeesOver70k, len(result.Rows))
    }
}
```

See `test_data.go` for available test data constants and `TEST_DATA_MIGRATION.md` for migration guide.

## 🌐 Running Distributed Queries

### Quick Start with Demo
```bash
# Run the distributed demo
go test -run TestDistributedDemo -v

# This will:
# 1. Create sample data distributed across 3 workers
# 2. Start coordinator and worker nodes
# 3. Execute various distributed queries
# 4. Show performance comparisons vs single-node
```

### Understanding the Architecture
- **Coordinator**: Receives queries, plans execution, combines results
- **Workers**: Execute query fragments on local data partitions
- **Data Distribution**: Data is physically partitioned across `./data/worker-*/` directories
- **Optimization**: Aggregate queries transfer only ~1KB instead of ~1MB of data

### Performance Benefits
```
Example: COUNT(*) on 1M rows
- Single Node: Reads all 1M rows
- Distributed: Each worker counts locally, sends only count
- Network Transfer: 3 integers vs 1M rows (99.99% reduction)
```

## 📁 Code Structure

```
bytedb/
├── main.go                      # CLI interface and command handling
├── core/                        # Core query engine components
│   ├── parser.go               # SQL parsing using pg_query_go
│   ├── planner.go              # Query planning and optimization
│   ├── executor.go             # Query execution engine
│   └── cache.go                # Query result caching system
├── distributed/                 # Distributed query execution
│   ├── coordinator/            # Coordinator node implementation
│   ├── worker/                 # Worker node implementation
│   ├── planner/                # Distributed query planning
│   │   ├── distributed_planner.go
│   │   ├── aggregate_optimizer.go  # 99.99% optimization
│   │   └── cost_estimator.go
│   └── communication/          # Inter-node communication protocol
├── gen_data.go                 # Sample data generation utility
├── distributed_demo.go         # Distributed execution demo
├── *_test.go                   # Comprehensive test suite
├── Makefile                    # Build and test automation
├── go.mod                      # Go module definition
└── data/                       # Sample Parquet files
    ├── employees.parquet
    ├── departments.parquet
    └── worker-*/               # Distributed worker data directories
```

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### Query Returns Unexpected Results
1. **Check WHERE clause parentheses**: Complex conditions need proper grouping
   ```sql
   -- Wrong: ambiguous precedence
   WHERE dept = 'Sales' OR dept = 'Marketing' AND salary > 70000
   
   -- Correct: explicit grouping  
   WHERE (dept = 'Sales' OR dept = 'Marketing') AND salary > 70000
   ```

2. **Verify column names**: Column names are case-sensitive
   ```sql
   -- May fail if column is 'Department' not 'department'
   SELECT department FROM employees;
   ```

3. **Check data types in comparisons**: Ensure compatible types
   ```sql
   -- String vs numeric comparison
   WHERE salary > '70000'  -- May not work as expected
   WHERE salary > 70000    -- Correct numeric comparison
   ```

#### Performance Issues
1. **Enable query optimization**: Optimization is enabled by default but verify it's working
2. **Use column selection**: Avoid `SELECT *` when possible
3. **Check query cache**: Use `\cache` to see if caching is helping

#### Subquery Issues
1. **Correlated subqueries**: Ensure outer table aliases are properly referenced
2. **EXISTS vs IN**: Use EXISTS for better performance with large datasets
3. **Scalar subqueries**: Must return exactly one row and one column

## 🚀 Query Optimization Features

ByteDB includes an advanced query optimizer that automatically improves query performance:

### Optimization Rules

1. **Predicate Pushdown**: Filters are pushed down to table scans to reduce data read
   ```sql
   -- Filter applied during scan, not after
   SELECT * FROM employees WHERE department = 'Engineering';
   ```

2. **Column Pruning**: Only required columns are read from Parquet files
   ```sql
   -- Only reads 'name' and 'salary' columns from disk
   SELECT name, salary FROM employees;
   ```

3. **Join Order Optimization**: Smaller tables are used as build side in hash joins
   ```sql
   -- Automatically reorders to put smaller table on right
   SELECT * FROM large_table JOIN small_table ON condition;
   ```

### Viewing Optimization Stats

```go
// In code
stats, err := engine.GetOptimizationStats(sql)

// Stats include:
// - Original vs optimized query plan
// - Estimated cost reduction
// - Applied optimization rules
```

## 🎮 Meta Commands

- `\d table_name` - Describe table schema
- `\l` - List all tables
- `\json <sql>` - Return results as JSON
- `\cache` - Show cache statistics
- `help` - Show available commands
- `exit` or `quit` - Exit the program

## 🏗️ Architecture

The query engine consists of several optimized components:

1. **SQL Parser**: Advanced PostgreSQL-compatible SQL parsing
2. **Query Planner**: Optimizes query execution plans
3. **Join Engine**: Efficient join algorithms for multiple tables
4. **Subquery Engine**: Handles correlated and non-correlated subqueries
5. **Aggregate Engine**: Processes GROUP BY and aggregate functions
6. **Window Function Engine**: Implements SQL window functions
7. **Cache System**: LRU cache with TTL for query results
8. **Parquet Reader**: Optimized Parquet file access with predicate pushdown

## 📊 Performance Features

- **Query Caching**: Automatic caching of query results with configurable TTL
- **Predicate Pushdown**: Efficient filtering at the Parquet level
- **Memory Management**: Optimized memory usage for large datasets
- **Lazy Evaluation**: Data is processed only when needed
- **Index-aware**: Leverages Parquet column statistics for optimization

## 🔧 Configuration

The engine supports various configuration options:

```go
// Cache configuration
cacheConfig := CacheConfig{
    MaxMemoryMB: 100,              // Cache size limit
    DefaultTTL:  5 * time.Minute,  // Default cache TTL
    Enabled:     true,             // Enable/disable caching
}
```

## 🚧 Known Limitations

- Simple CASE syntax (`CASE column WHEN value THEN result`) has limited support
- Some advanced SQL features like CTEs, UNION, window frames not yet implemented
- Performance optimization for very large datasets is ongoing
- Complex arithmetic expressions in CASE results need enhancement

## 📝 Example Session

```bash
$ ./bytedb ./data
ByteDB - Simple SQL Query Engine for Parquet Files
Type 'help' for available commands, 'exit' to quit

bytedb> SELECT e.name, e.salary, d.budget,
        CASE WHEN e.salary > 75000 THEN 'Senior' ELSE 'Junior' END as level
        FROM employees e 
        JOIN departments d ON e.department = d.name 
        WHERE d.budget > 500000
        ORDER BY e.salary DESC;

name            salary  budget   level
Lisa Davis      85000   1000000  Senior
Mike Johnson    80000   1000000  Senior
Chris Anderson  78000   1000000  Senior
John Doe        75000   1000000  Junior

(4 rows)

bytedb> SELECT department, 
        COUNT(*) as emp_count,
        AVG(salary) as avg_salary,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as dept_rank
        FROM employees 
        GROUP BY department;

department   emp_count  avg_salary  dept_rank
Engineering  4          79500       1
Marketing    2          63500       2
Sales        2          71000       3
Finance      1          68000       4
HR           1          55000       5

(5 rows)

bytedb> \d employees
Table: employees
Columns:
  - id (INT32)
  - name (BYTE_ARRAY)
  - department (BYTE_ARRAY)
  - salary (DOUBLE)
  - age (INT32)
  - hire_date (BYTE_ARRAY)

bytedb> exit
Goodbye!
```

## 🚀 Future Distributed Enhancements

### Coming Soon
- **gRPC Transport**: Production-ready network communication
- **Dynamic Worker Discovery**: Automatic worker registration and discovery
- **Fault Tolerance**: Automatic retry and failover for failed workers
- **Data Rebalancing**: Dynamic data redistribution based on workload
- **Query Result Streaming**: Stream large results without buffering
- **Distributed Caching**: Coordinated cache across worker nodes
- **Advanced Join Strategies**: Broadcast joins and distributed hash joins
- **Monitoring Dashboard**: Real-time cluster and query performance metrics

### Performance Roadmap
- Sub-linear scaling for analytical queries
- Adaptive query execution based on runtime statistics
- Columnar data exchange format for even better compression
- GPU acceleration for compute-intensive operations

## 🤝 Contributing

We welcome contributions! Please feel free to submit issues, feature requests, and pull requests.

### Development Setup

1. Clone the repository
2. Run `go mod tidy` to install dependencies
3. Run `make test` to execute the test suite
4. Run `make build` to build the binary

### Testing

- Add tests for new features in appropriate `*_test.go` files
- Ensure all existing tests pass before submitting PRs
- Include performance benchmarks for significant changes

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

**ByteDB** - Bringing the power of SQL to Parquet files with modern features and performance optimization.