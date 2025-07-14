# Breaking Changes

## Command Line Usage Change (Latest)

### Previous Behavior
- ByteDB required a data directory as the first argument: `bytedb ./data`
- All table queries were relative to this data directory

### New Behavior
- ByteDB now uses an in-memory catalog by default
- Optional persistent catalog with `--persist` flag: `bytedb --persist [catalog_path]`
- File paths in queries are relative to current working directory
- No required command line arguments

### Migration Guide

#### Old way:
```bash
./bytedb ./data
bytedb> SELECT * FROM employees;  # Looks for ./data/employees.parquet
```

#### New way:
```bash
# For quick exploration (in-memory catalog)
./bytedb
bytedb> SELECT * FROM read_parquet('data/employees.parquet');

# For persistent tables
./bytedb --persist
bytedb> CREATE TABLE employees AS SELECT * FROM read_parquet('data/employees.parquet');
bytedb> SELECT * FROM employees;
```

### Benefits
- More flexible file access - can query files from any location
- No need to copy files to a specific data directory
- Direct querying without registration using `read_parquet()`
- Better alignment with DuckDB usage patterns

## Table Name Resolution (Previous)

### Previous Behavior
- If a table was not explicitly registered, ByteDB would automatically look for a file named `<table_name>.parquet` in the data directory
- This allowed queries like `SELECT * FROM users` to work without registration if a `users.parquet` file existed

### New Behavior
- Tables MUST be explicitly registered before use
- Queries against unregistered tables will fail with error: `table not registered: <table_name>`
- This applies to both the legacy table registry and the new catalog system

### Migration Guide

#### Option 1: Register tables explicitly
```go
// Using table registry
engine.RegisterTable("users", "/path/to/users.parquet")

// Using catalog system  
catalogManager.RegisterTable(ctx, "users", "/path/to/users.parquet", "parquet")
```

#### Option 2: Use table mappings file
```json
{
  "tables": [
    {
      "table_name": "users",
      "file_path": "/path/to/users.parquet"
    }
  ]
}
```

### Rationale
- Explicit registration provides better control and security
- Prevents accidental access to arbitrary files
- Aligns with standard database behavior where tables must be created before use
- Supports multi-file tables where filename-based resolution wouldn't work