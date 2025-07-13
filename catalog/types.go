package catalog

import (
	"fmt"
	"strings"
	"time"
)

// CatalogMetadata represents a catalog in the metadata hierarchy
type CatalogMetadata struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Properties  map[string]string `json:"properties"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// SchemaMetadata represents a schema within a catalog
type SchemaMetadata struct {
	Name        string            `json:"name"`
	CatalogName string            `json:"catalog_name"`
	Description string            `json:"description"`
	Owner       string            `json:"owner"`
	Properties  map[string]string `json:"properties"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// TableMetadata represents a table within a schema
type TableMetadata struct {
	Name         string            `json:"name"`
	SchemaName   string            `json:"schema_name"`
	CatalogName  string            `json:"catalog_name"`
	Description  string            `json:"description"`
	Owner        string            `json:"owner"`
	Location     string            `json:"location"`      // File path or URI
	Format       string            `json:"format"`        // parquet, csv, json, etc.
	Columns      []ColumnMetadata  `json:"columns"`
	PartitionKeys []string         `json:"partition_keys"`
	Properties   map[string]string `json:"properties"`
	Statistics   *TableStatistics  `json:"statistics,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// ColumnMetadata represents a column in a table
type ColumnMetadata struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Comment     string            `json:"comment"`
	Nullable    bool              `json:"nullable"`
	Properties  map[string]string `json:"properties"`
}

// TableStatistics contains statistics about a table
type TableStatistics struct {
	RowCount      int64     `json:"row_count"`
	SizeBytes     int64     `json:"size_bytes"`
	LastAnalyzed  time.Time `json:"last_analyzed"`
	LastModified  time.Time `json:"last_modified"`
}

// TableIdentifier uniquely identifies a table
type TableIdentifier struct {
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
}

// ParseTableIdentifier parses a table identifier string
// Supports formats: table, schema.table, catalog.schema.table
func ParseTableIdentifier(identifier string, defaultCatalog, defaultSchema string) TableIdentifier {
	parts := strings.Split(identifier, ".")
	
	switch len(parts) {
	case 1:
		// Just table name
		return TableIdentifier{
			Catalog: defaultCatalog,
			Schema:  defaultSchema,
			Table:   parts[0],
		}
	case 2:
		// schema.table
		return TableIdentifier{
			Catalog: defaultCatalog,
			Schema:  parts[0],
			Table:   parts[1],
		}
	case 3:
		// catalog.schema.table
		return TableIdentifier{
			Catalog: parts[0],
			Schema:  parts[1],
			Table:   parts[2],
		}
	default:
		// Invalid format, return as table name
		return TableIdentifier{
			Catalog: defaultCatalog,
			Schema:  defaultSchema,
			Table:   identifier,
		}
	}
}

// String returns the fully qualified table name
func (ti TableIdentifier) String() string {
	return fmt.Sprintf("%s.%s.%s", ti.Catalog, ti.Schema, ti.Table)
}

// QualifiedName returns schema.table format
func (ti TableIdentifier) QualifiedName() string {
	return fmt.Sprintf("%s.%s", ti.Schema, ti.Table)
}

// Errors
var (
	ErrCatalogNotFound = fmt.Errorf("catalog not found")
	ErrSchemaNotFound  = fmt.Errorf("schema not found")
	ErrTableNotFound   = fmt.Errorf("table not found")
	ErrCatalogExists   = fmt.Errorf("catalog already exists")
	ErrSchemaExists    = fmt.Errorf("schema already exists")
	ErrTableExists     = fmt.Errorf("table already exists")
)