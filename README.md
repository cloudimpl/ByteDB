# ByteDB - Advanced SQL Query Engine for Parquet Files

A powerful SQL query engine built in Go that allows you to execute complex SQL queries on Parquet files. Features comprehensive SQL support including JOINs, subqueries, aggregate functions, window functions, and CASE expressions.

Built using [pg_query_go](https://github.com/pganalyze/pg_query_go) for SQL parsing and [parquet-go](https://github.com/parquet-go/parquet-go) for Parquet file handling.

## 🚀 Features

### Core SQL Support
- **SELECT Queries**: Full SELECT support with column aliases and expressions
- **JOIN Operations**: INNER, LEFT, RIGHT, and FULL OUTER JOINs
- **Subqueries**: Correlated and non-correlated subqueries in SELECT, WHERE, and FROM clauses
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **Window Functions**: ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD() with PARTITION BY and ORDER BY
- **CASE Expressions**: Full CASE WHEN...THEN...ELSE support with nesting
- **Advanced WHERE Clauses**: Complex conditions with AND, OR, parentheses, IN, BETWEEN, LIKE, EXISTS

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

```bash
./bytedb ./data
```

### Example Queries

```sql
-- Basic queries
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

Run the comprehensive test suite:

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
```

## 📁 Code Structure

```
bytedb/
├── main.go                      # CLI interface and command handling
├── parser.go                    # SQL parsing using pg_query_go
├── parquet_reader.go            # Parquet file reading and filtering
├── query_engine.go              # Query execution engine and optimizations
├── cache.go                     # Query result caching system
├── gen_data.go                  # Sample data generation utility
├── *_test.go                    # Comprehensive test suite
├── Makefile                     # Build and test automation
├── go.mod                       # Go module definition
└── data/                        # Sample Parquet files
    ├── employees.parquet
    └── departments.parquet
```

## 🎮 Meta Commands

- `\\d table_name` - Describe table schema
- `\\l` - List all tables
- `\\json <sql>` - Return results as JSON
- `\\cache` - Show cache statistics
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