# Breaking Changes

## Table Name Resolution (Latest)

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