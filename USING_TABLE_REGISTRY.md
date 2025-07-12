# Using the Table Registry in ByteDB

## Current Default Behavior

By default, ByteDB still uses the traditional approach:
- `SELECT * FROM employees` → looks for `employees.parquet`
- `SELECT * FROM products` → looks for `products.parquet`

The table registry is implemented but **not active by default**.

## How to Enable Table Registry

### Option 1: Configuration File (Recommended)

1. Create a `table_mappings.json` file in your data directory:

```json
{
  "tables": [
    {
      "table_name": "users",
      "file_path": "employees.parquet"
    },
    {
      "table_name": "staff",
      "file_path": "employees.parquet"
    }
  ]
}
```

2. ByteDB will automatically load it when you run:
```bash
bytedb ./data
```

3. Now you can use the mapped names:
```sql
SELECT * FROM users;    -- reads from employees.parquet
SELECT * FROM staff;    -- also reads from employees.parquet
SELECT * FROM employees; -- still works (fallback)
```

### Option 2: Programmatic Registration

```go
engine := core.NewQueryEngine("./data")

// Register individual tables
engine.RegisterTable("archived_sales", "sales_2023.parquet")
engine.RegisterTable("current_sales", "sales_2024.parquet")

// Or load from a config file
engine.LoadTableMappings("my_tables.json")
```

## Example Use Cases

### 1. Table Aliases
```json
{
  "table_name": "emp",
  "file_path": "employees.parquet"
}
```
Now `SELECT * FROM emp` works as an alias.

### 2. Version Management
```json
{
  "table_name": "customers",
  "file_path": "customers_v3_cleaned.parquet"
}
```
Switch between data versions without changing queries.

### 3. Environment-Specific Mappings
Create different configs for dev/staging/prod:
- `dev/table_mappings.json` → test data
- `prod/table_mappings.json` → production data

### 4. Data Migration
```json
{
  "table_name": "orders",
  "file_path": "legacy/old_orders_format.parquet"
}
```
Gradually migrate to new formats while keeping queries unchanged.

## Checking Current Behavior

To verify if table registry is being used:

1. Create a simple test mapping:
```bash
echo '{
  "tables": [{
    "table_name": "test",
    "file_path": "employees.parquet"
  }]
}' > data/table_mappings.json
```

2. Run ByteDB and query:
```sql
SELECT COUNT(*) FROM test;
```

If it returns data from employees.parquet, the registry is working!

## Important Notes

- The registry provides **backward compatibility** - unmapped tables still use the default behavior
- File paths in the registry are relative to the data directory
- If a table is registered, it takes precedence over the default file name mapping
- HTTP tables (registered with URLs) take precedence over both registry and default mappings