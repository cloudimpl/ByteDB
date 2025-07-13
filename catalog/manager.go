package catalog

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
)

// SchemaReader is an interface for reading parquet schemas
type SchemaReader interface {
	ReadParquetSchema(filePath string) ([]ColumnMetadata, error)
}

// Manager manages catalog metadata and provides high-level operations
type Manager struct {
	store          MetadataStore
	defaultCatalog string
	defaultSchema  string
	mu             sync.RWMutex
	schemaReader   SchemaReader
}

// NewManager creates a new catalog manager
func NewManager(store MetadataStore, defaultCatalog, defaultSchema string) *Manager {
	return &Manager{
		store:          store,
		defaultCatalog: defaultCatalog,
		defaultSchema:  defaultSchema,
	}
}

// SetSchemaReader sets the schema reader for reading parquet schemas
func (m *Manager) SetSchemaReader(reader SchemaReader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemaReader = reader
}

// Initialize initializes the catalog manager
func (m *Manager) Initialize(ctx context.Context) error {
	return m.store.Initialize(ctx)
}

// Close closes the catalog manager
func (m *Manager) Close() error {
	return m.store.Close()
}

// SetDefaults sets the default catalog and schema
func (m *Manager) SetDefaults(catalog, schema string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultCatalog = catalog
	m.defaultSchema = schema
}

// GetDefaults returns the default catalog and schema
func (m *Manager) GetDefaults() (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultCatalog, m.defaultSchema
}

// GetMetadataStore returns the underlying metadata store
func (m *Manager) GetMetadataStore() MetadataStore {
	return m.store
}

// RegisterTable registers a table in the catalog
func (m *Manager) RegisterTable(ctx context.Context, identifier string, location string, format string) error {
	// Parse the identifier
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Ensure catalog exists
	_, err := m.store.GetCatalog(ctx, tableID.Catalog)
	if err == ErrCatalogNotFound {
		// Create default catalog if it doesn't exist
		catalog := &CatalogMetadata{
			Name:        tableID.Catalog,
			Description: fmt.Sprintf("Auto-created catalog for %s", tableID.Catalog),
			Properties:  make(map[string]string),
		}
		if err := m.store.CreateCatalog(ctx, catalog); err != nil {
			return fmt.Errorf("failed to create catalog: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check catalog: %w", err)
	}
	
	// Ensure schema exists
	_, err = m.store.GetSchema(ctx, tableID.Catalog, tableID.Schema)
	if err == ErrSchemaNotFound {
		// Create default schema if it doesn't exist
		schema := &SchemaMetadata{
			Name:        tableID.Schema,
			CatalogName: tableID.Catalog,
			Description: fmt.Sprintf("Auto-created schema for %s.%s", tableID.Catalog, tableID.Schema),
			Properties:  make(map[string]string),
		}
		if err := m.store.CreateSchema(ctx, schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check schema: %w", err)
	}
	
	// Create table metadata
	table := &TableMetadata{
		Name:        tableID.Table,
		SchemaName:  tableID.Schema,
		CatalogName: tableID.Catalog,
		Location:    location,
		Locations:   []string{location}, // Initialize with first file
		Format:      format,
		Columns:     []ColumnMetadata{}, // Will be populated when table is analyzed
		Properties:  make(map[string]string),
	}
	
	// Try to create the table
	err = m.store.CreateTable(ctx, table)
	if err == ErrTableExists {
		// Update existing table
		existingTable, _ := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
		existingTable.Location = location
		existingTable.Locations = []string{location}
		existingTable.Format = format
		return m.store.UpdateTable(ctx, existingTable)
	}
	
	return err
}

// GetTableLocation resolves a table identifier to its physical location
func (m *Manager) GetTableLocation(ctx context.Context, identifier string) (string, error) {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return "", fmt.Errorf("table %s not found: %w", tableID.String(), err)
	}
	
	return table.Location, nil
}

// ResolveTablePath resolves a table name to its file path
// This method provides backward compatibility with the existing table registry
func (m *Manager) ResolveTablePath(ctx context.Context, tableName string, dataPath string) (string, error) {
	// First try to get from catalog
	location, err := m.GetTableLocation(ctx, tableName)
	if err == nil {
		// If location is absolute, return as is
		if filepath.IsAbs(location) {
			return location, nil
		}
		// Otherwise, join with data path
		return filepath.Join(dataPath, location), nil
	}
	
	// Fallback to default behavior (tableName.parquet)
	if err == ErrTableNotFound {
		return "", err
	}
	
	return "", fmt.Errorf("failed to resolve table path: %w", err)
}

// ResolveTablePaths resolves a table name to all its file paths
// This returns all files associated with a table for multi-file tables
func (m *Manager) ResolveTablePaths(ctx context.Context, tableName string, dataPath string) ([]string, error) {
	tableID := ParseTableIdentifier(tableName, m.defaultCatalog, m.defaultSchema)
	
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return nil, fmt.Errorf("table %s not found: %w", tableID.String(), err)
	}
	
	// If no locations array, use the single location
	if len(table.Locations) == 0 && table.Location != "" {
		table.Locations = []string{table.Location}
	}
	
	// Resolve all paths
	paths := make([]string, 0, len(table.Locations))
	for _, location := range table.Locations {
		if filepath.IsAbs(location) {
			paths = append(paths, location)
		} else {
			paths = append(paths, filepath.Join(dataPath, location))
		}
	}
	
	return paths, nil
}

// ListTables lists all tables matching the pattern
func (m *Manager) ListTables(ctx context.Context, pattern string) ([]*TableMetadata, error) {
	if pattern == "" || pattern == "*" {
		// List all tables
		catalogs, err := m.store.ListCatalogs(ctx)
		if err != nil {
			return nil, err
		}
		
		var allTables []*TableMetadata
		for _, catalog := range catalogs {
			schemas, err := m.store.ListSchemas(ctx, catalog.Name)
			if err != nil {
				continue
			}
			
			for _, schema := range schemas {
				tables, err := m.store.ListTables(ctx, catalog.Name, schema.Name)
				if err != nil {
					continue
				}
				allTables = append(allTables, tables...)
			}
		}
		return allTables, nil
	}
	
	// Parse pattern as table identifier
	tableID := ParseTableIdentifier(pattern, m.defaultCatalog, m.defaultSchema)
	
	// If specific table requested
	if tableID.Table != "*" && tableID.Table != "" {
		table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
		if err != nil {
			return nil, err
		}
		return []*TableMetadata{table}, nil
	}
	
	// List tables in specific schema
	return m.store.ListTables(ctx, tableID.Catalog, tableID.Schema)
}

// CreateCatalog creates a new catalog
func (m *Manager) CreateCatalog(ctx context.Context, name, description string) error {
	catalog := &CatalogMetadata{
		Name:        name,
		Description: description,
		Properties:  make(map[string]string),
	}
	return m.store.CreateCatalog(ctx, catalog)
}

// CreateSchema creates a new schema
func (m *Manager) CreateSchema(ctx context.Context, catalogName, schemaName, description string) error {
	schema := &SchemaMetadata{
		Name:        schemaName,
		CatalogName: catalogName,
		Description: description,
		Properties:  make(map[string]string),
	}
	return m.store.CreateSchema(ctx, schema)
}

// DropTable drops a table from the catalog
func (m *Manager) DropTable(ctx context.Context, identifier string) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	return m.store.DeleteTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
}

// DropSchema drops a schema and all its tables
func (m *Manager) DropSchema(ctx context.Context, catalogName, schemaName string) error {
	if catalogName == "" {
		catalogName = m.defaultCatalog
	}
	return m.store.DeleteSchema(ctx, catalogName, schemaName)
}

// DropCatalog drops a catalog and all its schemas
func (m *Manager) DropCatalog(ctx context.Context, catalogName string) error {
	return m.store.DeleteCatalog(ctx, catalogName)
}

// GetTableMetadata returns metadata for a specific table
func (m *Manager) GetTableMetadata(ctx context.Context, identifier string) (*TableMetadata, error) {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	return m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
}

// UpdateTableStatistics updates statistics for a table
func (m *Manager) UpdateTableStatistics(ctx context.Context, identifier string, stats *TableStatistics) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	return m.store.UpdateTableStatistics(ctx, tableID.Catalog, tableID.Schema, tableID.Table, stats)
}

// AddFileToTable adds a parquet file to an existing table with schema validation
func (m *Manager) AddFileToTable(ctx context.Context, identifier string, filePath string, validateSchema bool) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Get existing table
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("failed to get table: %w", err)
	}
	
	// If schema validation is requested and table has schema
	if validateSchema && len(table.Columns) > 0 {
		// Read schema from the new parquet file
		newSchema, err := m.readParquetSchema(filePath)
		if err != nil {
			return fmt.Errorf("failed to read parquet schema: %w", err)
		}
		
		// Compare schemas
		compatible, differences := CompareSchemas(table.Columns, newSchema)
		if !compatible {
			return fmt.Errorf("%w: %v", ErrSchemaIncompatible, differences)
		}
		
		// Log any differences (like new columns) even if compatible
		// In a real system, you might want to log these differences
	} else if len(table.Columns) == 0 {
		// If table has no schema yet, read it from the first file
		newSchema, err := m.readParquetSchema(filePath)
		if err == nil {
			table.Columns = newSchema
			// Update the table with the schema
			if err := m.store.UpdateTable(ctx, table); err != nil {
				return fmt.Errorf("failed to update table schema: %w", err)
			}
		}
	}
	
	// Add the file to the table
	return m.store.AddFileToTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table, filePath)
}

// RemoveFileFromTable removes a file from a table
func (m *Manager) RemoveFileFromTable(ctx context.Context, identifier string, filePath string) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	err := m.store.RemoveFileFromTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table, filePath)
	if err != nil {
		return err
	}
	
	// Notify that table metadata has changed (for cache invalidation)
	// This would be handled by the query engine if it has a reference to the manager
	return nil
}

// readParquetSchema reads the schema from a parquet file
func (m *Manager) readParquetSchema(filePath string) ([]ColumnMetadata, error) {
	if m.schemaReader == nil {
		// If no schema reader is set, just return empty schema
		return nil, nil
	}
	
	return m.schemaReader.ReadParquetSchema(filePath)
}

// LoadFromTableRegistry loads table mappings from the existing table registry format
// This provides backward compatibility
func (m *Manager) LoadFromTableRegistry(ctx context.Context, mappings []TableMapping) error {
	for _, mapping := range mappings {
		if err := m.RegisterTable(ctx, mapping.TableName, mapping.FilePath, "parquet"); err != nil {
			return fmt.Errorf("failed to register table %s: %w", mapping.TableName, err)
		}
	}
	return nil
}

// TableMapping represents a table mapping from the legacy format
type TableMapping struct {
	TableName  string            `json:"table_name"`
	FilePath   string            `json:"file_path"`
	Properties map[string]string `json:"properties,omitempty"`
}