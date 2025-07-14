# ByteDB Catalog System

## Overview

ByteDB now includes a comprehensive catalog system that provides a three-level hierarchy for organizing tables:
- **Catalog** → **Schema** → **Table**

This system is similar to traditional database management systems and provides better organization, metadata management, and multi-tenancy support.

## Architecture

### Three-Level Hierarchy

1. **Catalog**: The top-level container (e.g., `production`, `analytics`, `staging`)
2. **Schema**: Logical grouping within a catalog (e.g., `sales`, `finance`, `hr`)
3. **Table**: Individual data tables within a schema

Example: `analytics.finance.revenue_2024`

### Pluggable Metadata Stores

The catalog system supports different storage backends:

- **Memory Store**: In-memory metadata storage (default, not persistent)
- **File Store**: JSON-based file storage (persistent)
- **Future**: PostgreSQL, MySQL, S3, etc.

## Enabling the Catalog System

### Method 1: Configuration File

Create a `catalog.json` file in your data directory:

```json
{
  "enabled": true,
  "store_type": "file",
  "store_config": {
    "base_path": ".catalog"
  }
}
```

### Method 2: CLI Commands

```sql
-- Enable with memory store
\catalog enable memory

-- Enable with file store
\catalog enable file base_path=.catalog
```

### Method 3: Programmatic

```go
config := map[string]interface{}{
    "base_path": ".catalog",
    "migrate_registry": true,  // Migrate existing table registry
}
engine.EnableCatalog("file", config)
```

## Using the Catalog System

### Registering Tables

```sql
-- Register in default catalog and schema
\catalog register employees employees.parquet

-- Register with explicit schema
\catalog register sales.orders orders_2024.parquet

-- Register with full path
\catalog register analytics.finance.revenue revenue.parquet
```

### Querying Tables

```sql
-- Query using simple name (uses default catalog.schema)
SELECT * FROM employees;

-- Query with schema qualification
SELECT * FROM sales.orders;

-- Query with full qualification
SELECT * FROM analytics.finance.revenue;

-- Note: 'default' is a reserved keyword, so it must be quoted
SELECT * FROM "default"."default".employees;
```

### Catalog Commands

```sql
-- List all catalogs
\dc

-- List schemas in a catalog
\dn                    -- Default catalog
\dn analytics         -- Specific catalog

-- List tables
\dt                    -- All tables
\dt employees         -- Specific table
\dt sales.*          -- All tables in sales schema
\dt *.finance.*      -- All tables in finance schema across catalogs

-- Drop a table from catalog
\catalog drop employees
\catalog drop sales.orders
```

## Migration from Table Registry

The catalog system can automatically migrate your existing table registry:

```go
config := map[string]interface{}{
    "migrate_registry": true,
}
engine.EnableCatalog("file", config)
```

This will:
1. Read all mappings from the table registry
2. Create them in the default catalog and schema
3. Preserve all properties and metadata

## File Store Structure

When using the file-based metadata store, the structure looks like:

```
.catalog/
├── catalogs.json
├── default/
│   ├── schemas.json
│   └── default/
│       └── tables.json
└── analytics/
    ├── schemas.json
    ├── finance/
    │   └── tables.json
    └── sales/
        └── tables.json
```

## Backward Compatibility

The catalog system maintains full backward compatibility:

1. If catalog is not enabled, ByteDB uses the traditional approach
2. Table registry continues to work alongside the catalog
3. Unmapped tables fall back to `tablename.parquet` convention

### Resolution Order

1. HTTP tables (if registered)
2. Catalog system (if enabled)
3. Table registry
4. Default file mapping (`tablename.parquet`)

## Best Practices

### Naming Conventions

- Use lowercase for catalog and schema names
- Use underscores for multi-word names
- Avoid SQL reserved keywords or quote them

### Organization Strategies

1. **By Environment**:
   ```
   dev.application.users
   staging.application.users
   production.application.users
   ```

2. **By Department**:
   ```
   company.sales.orders
   company.finance.invoices
   company.hr.employees
   ```

3. **By Data Type**:
   ```
   raw.clickstream.events
   processed.clickstream.sessions
   analytics.clickstream.metrics
   ```

## Examples

### Setting Up a Multi-Tenant System

```sql
-- Enable catalog with file store
\catalog enable file base_path=.catalog

-- Create tenant-specific tables
\catalog register tenant1.app.users tenant1/users.parquet
\catalog register tenant1.app.orders tenant1/orders.parquet
\catalog register tenant2.app.users tenant2/users.parquet
\catalog register tenant2.app.orders tenant2/orders.parquet

-- Query tenant-specific data
SELECT * FROM tenant1.app.users WHERE active = true;
SELECT COUNT(*) FROM tenant2.app.orders;
```

### Data Lake Organization

```sql
-- Raw data
\catalog register raw.events.clicks raw/2024/01/clicks.parquet
\catalog register raw.events.impressions raw/2024/01/impressions.parquet

-- Processed data
\catalog register processed.events.daily_clicks processed/clicks_daily.parquet
\catalog register processed.events.user_sessions processed/sessions.parquet

-- Analytics
\catalog register analytics.metrics.conversion_rate analytics/conversion.parquet
```

## Troubleshooting

### Common Issues

1. **"default" keyword error**:
   ```sql
   -- Wrong
   SELECT * FROM default.employees;
   
   -- Correct
   SELECT * FROM "default".employees;
   ```

2. **Catalog not found**:
   - Ensure the catalog system is enabled
   - Check if the catalog exists with `\dc`

3. **Persistence issues**:
   - Use file store instead of memory store
   - Check write permissions on the catalog directory

## Future Enhancements

1. **Additional Metadata Stores**:
   - PostgreSQL/MySQL for centralized metadata
   - S3/Cloud storage for distributed systems
   - Etcd/Consul for high availability

2. **Advanced Features**:
   - Table versioning
   - Schema evolution tracking
   - Access control per catalog/schema
   - Automatic schema discovery from Parquet files

3. **Integration**:
   - Apache Hive metastore compatibility
   - AWS Glue catalog integration
   - Data lineage tracking