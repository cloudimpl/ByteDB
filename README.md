# ByteDB - Simple SQL Query Engine for Parquet Files

A lightweight SQL query engine built in Go that allows you to query Parquet files using SQL syntax. Built using [pg_query_go](https://github.com/pganalyze/pg_query_go) for SQL parsing and [parquet-go](https://github.com/parquet-go/parquet-go) for Parquet file handling.

## Features

- **SQL Query Support**: Execute SELECT queries with WHERE clauses, column selection, and LIMIT
- **Parquet File Reading**: Efficiently read and query Parquet files
- **Interactive CLI**: Command-line interface for running queries
- **Multiple Output Formats**: Table format and JSON output
- **Schema Inspection**: View table schemas and column information

## Installation

```bash
git clone <repository-url>
cd bytedb
go mod tidy
go build
```

## Usage

### Generate Sample Data

First, generate some sample Parquet files:

```bash
go run sample_data_generator.go
```

This creates `./data/employees.parquet` and `./data/products.parquet` with sample data.

### Start the Query Engine

```bash
./bytedb ./data
```

### Example Queries

```sql
-- Select all employees
SELECT * FROM employees;

-- Select specific columns
SELECT name, department, salary FROM employees;

-- Filter by department
SELECT * FROM employees WHERE department = 'Engineering';

-- Limit results
SELECT * FROM employees LIMIT 5;

-- Complex query with multiple conditions
SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 75000;
```

### Meta Commands

- `\\d table_name` - Describe table schema
- `\\l` - List all tables (not implemented yet)
- `\\json <sql>` - Return results as JSON
- `help` - Show available commands
- `exit` or `quit` - Exit the program

## Architecture

The query engine consists of several components:

1. **SQL Parser** (`parser.go`): Uses pg_query_go to parse SQL into AST
2. **Parquet Reader** (`parquet_reader.go`): Handles reading and filtering Parquet files
3. **Query Engine** (`query_engine.go`): Coordinates parsing and execution
4. **Main CLI** (`main.go`): Interactive command-line interface

## Supported SQL Features

### SELECT Queries
- Column selection: `SELECT col1, col2 FROM table`
- Wildcard selection: `SELECT * FROM table`
- Column aliases: `SELECT col1 AS alias FROM table`

### WHERE Clauses
- Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- String matching: `LIKE` (basic pattern matching)
- Multiple conditions with AND (implicit)

### Other Features
- `LIMIT` clause for result limiting
- Table names correspond to `.parquet` files in the data directory

## Code Structure

```
bytedb/
├── main.go                    # CLI interface
├── parser.go                  # SQL parsing using pg_query_go
├── parquet_reader.go          # Parquet file reading and filtering
├── query_engine.go            # Query execution engine
├── sample_data_generator.go   # Sample data generation
├── go.mod                     # Go module definition
└── data/                      # Sample Parquet files
    ├── employees.parquet
    └── products.parquet
```

## Dependencies

- [github.com/pganalyze/pg_query_go/v6](https://github.com/pganalyze/pg_query_go) - PostgreSQL query parsing
- [github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go) - Parquet file handling

## Limitations

- Currently supports only SELECT queries
- No JOIN operations
- No aggregate functions (COUNT, SUM, etc.)
- No ORDER BY clause
- No GROUP BY clause
- Basic WHERE clause support (AND logic only)
- No subqueries

## Example Session

```bash
$ ./bytedb ./data
ByteDB - Simple SQL Query Engine for Parquet Files
Type 'help' for available commands, 'exit' to quit

bytedb> SELECT * FROM employees LIMIT 3;
id	name	department	salary	age	hire_date
1	John Doe	Engineering	75000	30	2020-01-15
2	Jane Smith	Marketing	65000	28	2019-03-22
3	Mike Johnson	Engineering	80000	35	2018-07-10

(3 rows)

bytedb> SELECT name, salary FROM employees WHERE department = 'Engineering';
name	salary
John Doe	75000
Mike Johnson	80000
Lisa Davis	85000
Chris Anderson	78000

(4 rows)

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

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is open source and available under the [MIT License](LICENSE).