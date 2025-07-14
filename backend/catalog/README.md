# ByteDB Catalog System

The ByteDB catalog system provides metadata management for tables, schemas, and catalogs. It supports multi-file tables, automatic schema inference, and statistics calculation.

## Table Management

### Creating Tables

There are two ways to create tables:

1. **Create an empty table** (then add files later):
```go
// Create an empty table with a predefined schema
columns := []catalog.ColumnMetadata{
    {Name: "id", Type: "INT64", Nullable: false},
    {Name: "name", Type: "STRING", Nullable: true},
    {Name: "salary", Type: "DOUBLE", Nullable: true},
}
err := manager.CreateTable(ctx, "employees", "parquet", columns)

// Add files to the table later
err = manager.AddFileToTable(ctx, "employees", "/data/employees_2023.parquet", true)
err = manager.AddFileToTable(ctx, "employees", "/data/employees_2024.parquet", true)
```

2. **Register a table with an initial file** (schema inferred from file):
```go
// Creates table and infers schema from the parquet file
err := manager.RegisterTable(ctx, "employees", "/data/employees.parquet", "parquet")

// Add more files later
err = manager.AddFileToTable(ctx, "employees", "/data/employees_archive.parquet", true)
```

### File Operations

#### Adding Files to Tables
```go
// Add with schema validation (recommended)
err := manager.AddFileToTable(ctx, "employees", "/data/new_employees.parquet", true)

// Add without schema validation (faster but risky)
err := manager.AddFileToTable(ctx, "employees", "/data/trusted_file.parquet", false)
```

Features:
- Automatic schema validation against existing table schema
- Prevents duplicate files
- Updates table statistics automatically
- Sets primary location if table was empty

#### Removing Files from Tables
```go
// Remove a specific file
err := manager.RemoveFileFromTable(ctx, "employees", "/data/old_employees.parquet")
```

Features:
- Prevents removing the last file (use DropTable instead)
- Updates primary location if removed
- Recalculates statistics automatically

### Table Statistics

Statistics are automatically calculated and maintained:

1. **Automatic calculation**:
   - When registering a table with a file
   - When adding files to a table
   - When removing files from a table

2. **Manual refresh**:
```go
// Recalculate statistics for all files in the table
err := manager.RefreshTableStatistics(ctx, "employees")
```

3. **Statistics include**:
   - Row count (estimated from file size)
   - Total size in bytes
   - Last analyzed timestamp
   - Last modified timestamp

### Querying Table Information

```go
// Get table metadata
table, err := manager.GetTableMetadata(ctx, "employees")

// Get all file paths for a table
files, err := manager.GetTableFiles(ctx, "employees")

// Get table location (primary file)
location, err := manager.GetTableLocation(ctx, "employees")

// Get all file paths with resolution
paths, err := manager.ResolveTablePaths(ctx, "employees", "/data")
```

### Schema Management

The catalog supports automatic schema inference and validation:

1. **Schema inference**: When creating a table from a parquet file, the schema is automatically read
2. **Schema validation**: When adding files, schemas are compared for compatibility
3. **Schema evolution**: New columns are allowed, but existing columns must match

### Multi-File Tables

Tables can consist of multiple parquet files:

```go
// Create a partitioned table
err := manager.CreateTable(ctx, "sales", "parquet", salesColumns)

// Add monthly partition files
err = manager.AddFileToTable(ctx, "sales", "/data/sales_2024_01.parquet", true)
err = manager.AddFileToTable(ctx, "sales", "/data/sales_2024_02.parquet", true)
err = manager.AddFileToTable(ctx, "sales", "/data/sales_2024_03.parquet", true)

// Query will scan all files
files, _ := manager.GetTableFiles(ctx, "sales")
// Returns: ["/data/sales_2024_01.parquet", "/data/sales_2024_02.parquet", "/data/sales_2024_03.parquet"]
```

### Best Practices

1. **Always use schema validation** when adding files unless you're certain about compatibility
2. **Create tables with explicit schemas** when you know the structure upfront
3. **Use RefreshTableStatistics** after bulk file operations
4. **Monitor statistics** to track table growth and optimize query planning

## Implementation Details

### Storage Backends

The catalog system supports pluggable storage backends:
- **FileMetadataStore**: JSON-based file storage (default)
- **MemoryMetadataStore**: In-memory storage for testing
- Custom implementations can be added via the MetadataStore interface

### Concurrency

The catalog manager is thread-safe and supports concurrent operations. File operations are atomic to prevent corruption.

### Error Handling

Common errors:
- `ErrTableNotFound`: Table doesn't exist
- `ErrSchemaIncompatible`: File schema doesn't match table schema  
- `ErrTableExists`: Table already exists
- File operation errors (file not found, permission denied, etc.)