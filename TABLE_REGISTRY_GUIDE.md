# ByteDB Table Registry Guide

## Overview

The Table Registry feature in ByteDB allows you to map logical table names to physical parquet files, providing flexibility in data management and schema evolution.

## Key Features

1. **External Table Name Mapping** - Map any table name to any parquet file
2. **Schema Management** - Store and validate table schemas
3. **Partitioned Tables** - Support for tables split across multiple files
4. **Configuration Files** - JSON-based table mapping configuration
5. **Backward Compatibility** - Falls back to default `tablename.parquet` behavior

## Usage

### 1. Programmatic Registration

```go
engine := core.NewQueryEngine("./data")

// Simple table mapping
engine.RegisterTable("customers", "customer_data_v2.parquet")

// With schema information
mapping := &core.TableMapping{
    TableName: "users",
    FilePath:  "user_data_2024.parquet",
    Schema: &core.TableSchema{
        Columns: []core.ColumnSchema{
            {Name: "user_id", Type: "int64", Nullable: false},
            {Name: "username", Type: "string", Nullable: false},
        },
    },
}
engine.RegisterTableWithSchema(mapping)

// Partitioned table
engine.RegisterPartitionedTable("sales", []string{
    "sales/2024/01/data.parquet",
    "sales/2024/02/data.parquet",
    "sales/2024/03/data.parquet",
})
```

### 2. Configuration File

Create a `table_mappings.json` file:

```json
{
  "tables": [
    {
      "table_name": "users",
      "file_path": "user_data_2024.parquet",
      "schema": {
        "columns": [
          {"name": "user_id", "type": "int64", "nullable": false},
          {"name": "username", "type": "string", "nullable": false}
        ]
      }
    },
    {
      "table_name": "events",
      "partitions": [
        "events/2024/01/events.parquet",
        "events/2024/02/events.parquet"
      ]
    }
  ]
}
```

Load the configuration:

```go
engine.LoadTableMappings("table_mappings.json")
```

### 3. Distributed Workers

Workers automatically load table mappings from their data directory:

```
data/
├── worker-1/
│   ├── table_mappings.json
│   └── *.parquet files
├── worker-2/
│   ├── table_mappings.json
│   └── *.parquet files
```

## Use Cases

### 1. Version Management
```go
// Switch between data versions without changing queries
engine.RegisterTable("products", "products_v3_optimized.parquet")
```

### 2. A/B Testing
```go
// Test different data processing pipelines
if testGroup == "A" {
    engine.RegisterTable("features", "features_pipeline_a.parquet")
} else {
    engine.RegisterTable("features", "features_pipeline_b.parquet")
}
```

### 3. Time-based Data
```go
// Point to current month's data
currentMonth := time.Now().Format("2006_01")
engine.RegisterTable("transactions", 
    fmt.Sprintf("transactions_%s.parquet", currentMonth))
```

### 4. Multi-tenant Systems
```go
// Different files per tenant
engine.RegisterTable("orders", 
    fmt.Sprintf("tenant_%s/orders.parquet", tenantID))
```

## API Reference

### QueryEngine Methods

- `RegisterTable(tableName, filePath string) error`
- `RegisterTableWithSchema(mapping *TableMapping) error`
- `RegisterPartitionedTable(tableName string, partitions []string) error`
- `LoadTableMappings(configPath string) error`
- `SaveTableMappings(configPath string) error`
- `GetTableRegistry() *TableRegistry`

### TableRegistry Methods

- `GetTablePath(tableName string) (string, error)`
- `GetTableMapping(tableName string) (*TableMapping, error)`
- `GetAllPartitions(tableName string) ([]string, error)`
- `ListTables() []string`
- `Clear()`

## Migration Guide

Existing queries continue to work without changes. The system falls back to the default behavior (`tablename.parquet`) if no mapping is registered.

To migrate existing tables:

1. Create a table mappings configuration file
2. Map your current table names to their files
3. Load the configuration at startup
4. Gradually migrate to new file naming schemes

## Best Practices

1. **Use descriptive file names** - Include version, date, or pipeline information
2. **Document schemas** - Always include schema information for validation
3. **Version your mappings** - Keep configuration files in version control
4. **Test thoroughly** - Verify queries work with new mappings before production
5. **Monitor performance** - Some mappings may affect query optimization

## Example: Complete Setup

```go
func setupTableRegistry(engine *core.QueryEngine) error {
    // Load base configuration
    if err := engine.LoadTableMappings("config/tables.json"); err != nil {
        return err
    }
    
    // Override for environment
    env := os.Getenv("ENVIRONMENT")
    if env == "staging" {
        engine.RegisterTable("users", "staging/users_latest.parquet")
    }
    
    // Dynamic monthly data
    month := time.Now().Format("2006_01")
    engine.RegisterTable("logs", fmt.Sprintf("logs/logs_%s.parquet", month))
    
    return nil
}
```