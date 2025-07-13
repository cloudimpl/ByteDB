package catalog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
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

// CreateTable creates an empty table in the catalog
func (m *Manager) CreateTable(ctx context.Context, identifier string, format string, columns []ColumnMetadata) error {
	// Parse the identifier
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Ensure catalog and schema exist
	if err := m.ensureCatalogAndSchema(ctx, tableID.Catalog, tableID.Schema); err != nil {
		return err
	}
	
	// Create table metadata
	table := &TableMetadata{
		Name:        tableID.Table,
		SchemaName:  tableID.Schema,
		CatalogName: tableID.Catalog,
		Location:    "", // No primary location for empty table
		Locations:   []string{}, // Empty table has no files
		Format:      format,
		Columns:     columns,
		Properties:  make(map[string]string),
		Statistics: &TableStatistics{
			RowCount:     0,
			SizeBytes:    0,
			LastAnalyzed: time.Now(),
			LastModified: time.Now(),
		},
	}
	
	return m.store.CreateTable(ctx, table)
}

// RegisterTable registers a table with an initial file in the catalog
// This is a convenience method that creates a table and adds the first file
func (m *Manager) RegisterTable(ctx context.Context, identifier string, location string, format string) error {
	// Parse the identifier
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Check if table already exists
	_, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err == nil {
		// Table exists, add the file to it
		return m.AddFileToTable(ctx, identifier, location, true)
	} else if err != ErrTableNotFound {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	
	// Table doesn't exist, create it with schema from the file
	var columns []ColumnMetadata
	if m.schemaReader != nil {
		columns, err = m.schemaReader.ReadParquetSchema(location)
		if err != nil {
			// Log warning but continue with empty schema
			fmt.Printf("Warning: Failed to read schema from %s: %v\n", location, err)
			columns = []ColumnMetadata{}
		}
	}
	
	// Create the table
	if err := m.CreateTable(ctx, identifier, format, columns); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	// Add the file to the newly created table
	return m.AddFileToTable(ctx, identifier, location, false) // Skip validation since we just created with this schema
}

// ensureCatalogAndSchema ensures catalog and schema exist, creating them if necessary
func (m *Manager) ensureCatalogAndSchema(ctx context.Context, catalogName, schemaName string) error {
	// Ensure catalog exists
	_, err := m.store.GetCatalog(ctx, catalogName)
	if err == ErrCatalogNotFound {
		// Create default catalog if it doesn't exist
		catalog := &CatalogMetadata{
			Name:        catalogName,
			Description: fmt.Sprintf("Auto-created catalog for %s", catalogName),
			Properties:  make(map[string]string),
		}
		if err := m.store.CreateCatalog(ctx, catalog); err != nil && err != ErrCatalogExists {
			return fmt.Errorf("failed to create catalog: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check catalog: %w", err)
	}
	
	// Ensure schema exists
	_, err = m.store.GetSchema(ctx, catalogName, schemaName)
	if err == ErrSchemaNotFound {
		// Create default schema if it doesn't exist
		schema := &SchemaMetadata{
			Name:        schemaName,
			CatalogName: catalogName,
			Description: fmt.Sprintf("Auto-created schema for %s.%s", catalogName, schemaName),
			Properties:  make(map[string]string),
		}
		if err := m.store.CreateSchema(ctx, schema); err != nil && err != ErrSchemaExists {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check schema: %w", err)
	}
	
	return nil
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

// GetTableFiles returns all file paths for a table
func (m *Manager) GetTableFiles(ctx context.Context, identifier string) ([]string, error) {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return nil, fmt.Errorf("table %s not found: %w", tableID.String(), err)
	}
	
	return table.Locations, nil
}

// RefreshTableStatistics recalculates statistics for a table from its files
func (m *Manager) RefreshTableStatistics(ctx context.Context, identifier string) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", tableID.String(), err)
	}
	
	// Calculate aggregated statistics for all files in the table
	if stats, err := m.calculateTableStatistics(table.Locations); err == nil {
		return m.UpdateTableStatistics(ctx, identifier, stats)
	} else {
		return fmt.Errorf("failed to calculate statistics for table %s: %w", identifier, err)
	}
}

// AddFileToTable adds a parquet file to an existing table with schema validation
func (m *Manager) AddFileToTable(ctx context.Context, identifier string, filePath string, validateSchema bool) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Get existing table
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("failed to get table: %w", err)
	}
	
	// Check if file already exists in the table
	for _, existingPath := range table.Locations {
		if existingPath == filePath {
			return fmt.Errorf("file %s already exists in table %s", filePath, identifier)
		}
	}
	
	// If schema validation is requested and table has schema
	if validateSchema && len(table.Columns) > 0 && m.schemaReader != nil {
		// Read schema from the new parquet file
		newSchema, err := m.schemaReader.ReadParquetSchema(filePath)
		if err != nil {
			return fmt.Errorf("failed to read parquet schema: %w", err)
		}
		
		// Compare schemas
		compatible, differences := CompareSchemas(table.Columns, newSchema)
		if !compatible {
			return fmt.Errorf("%w: %v", ErrSchemaIncompatible, differences)
		}
		
		// Log any differences (like new columns) even if compatible
		if len(differences) > 0 {
			fmt.Printf("Info: Schema differences detected for file %s: %v\n", filePath, differences)
		}
	} else if len(table.Columns) == 0 && m.schemaReader != nil {
		// If table has no schema yet, read it from the first file
		newSchema, err := m.schemaReader.ReadParquetSchema(filePath)
		if err == nil {
			table.Columns = newSchema
			// Update the table with the schema
			if err := m.store.UpdateTable(ctx, table); err != nil {
				return fmt.Errorf("failed to update table schema: %w", err)
			}
		} else {
			fmt.Printf("Warning: Failed to read schema from %s: %v\n", filePath, err)
		}
	}
	
	// Add the file to the table
	err = m.store.AddFileToTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table, filePath)
	if err != nil {
		return fmt.Errorf("failed to add file to table: %w", err)
	}
	
	// Recalculate table statistics after adding the file
	updatedTable, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("failed to get updated table: %w", err)
	}
	
	// Update the primary location if it was empty
	if updatedTable.Location == "" {
		updatedTable.Location = filePath
		if err := m.store.UpdateTable(ctx, updatedTable); err != nil {
			fmt.Printf("Warning: Failed to update primary location: %v\n", err)
		}
	}
	
	// Calculate aggregated statistics for all files in the table
	if stats, err := m.calculateTableStatistics(updatedTable.Locations); err == nil {
		return m.UpdateTableStatistics(ctx, identifier, stats)
	} else {
		fmt.Printf("Warning: Failed to recalculate statistics for table %s: %v\n", identifier, err)
		return nil // Don't fail the operation if statistics calculation fails
	}
}

// RemoveFileFromTable removes a file from a table
func (m *Manager) RemoveFileFromTable(ctx context.Context, identifier string, filePath string) error {
	tableID := ParseTableIdentifier(identifier, m.defaultCatalog, m.defaultSchema)
	
	// Get table before removal to check primary location
	table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("failed to get table: %w", err)
	}
	
	// Check if we're removing the last file
	if len(table.Locations) <= 1 {
		return fmt.Errorf("cannot remove last file from table %s; use DropTable instead", identifier)
	}
	
	err = m.store.RemoveFileFromTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table, filePath)
	if err != nil {
		return err
	}
	
	// If we removed the primary location, update it
	if table.Location == filePath {
		// Get updated table to find a new primary location
		updatedTable, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
		if err != nil {
			return fmt.Errorf("failed to get updated table: %w", err)
		}
		
		if len(updatedTable.Locations) > 0 {
			updatedTable.Location = updatedTable.Locations[0]
			if err := m.store.UpdateTable(ctx, updatedTable); err != nil {
				fmt.Printf("Warning: Failed to update primary location: %v\n", err)
			}
		}
	}
	
	// Recalculate table statistics after removing the file
	updatedTable, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
	if err != nil {
		return fmt.Errorf("failed to get updated table after file removal: %w", err)
	}
	
	// Calculate aggregated statistics for remaining files in the table
	if stats, err := m.calculateTableStatistics(updatedTable.Locations); err == nil {
		return m.UpdateTableStatistics(ctx, identifier, stats)
	} else {
		fmt.Printf("Warning: Failed to recalculate statistics for table %s after file removal: %v\n", identifier, err)
		return nil // Don't fail the operation if statistics calculation fails
	}
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

// calculateFileStatistics calculates statistics for a single parquet file
func (m *Manager) calculateFileStatistics(filePath string) (*TableStatistics, error) {
	// Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	
	// For now, estimate row count based on file size
	// In production, this would read actual parquet metadata using a parquet library
	estimatedRowCount := fileInfo.Size() / 100 // Rough estimate: 100 bytes per row
	if estimatedRowCount < 1 {
		estimatedRowCount = 1 // Minimum of 1 row
	}
	
	return &TableStatistics{
		RowCount:     estimatedRowCount,
		SizeBytes:    fileInfo.Size(),
		LastAnalyzed: time.Now(),
		LastModified: fileInfo.ModTime(),
	}, nil
}

// calculateTableStatistics calculates aggregated statistics for all files in a table
func (m *Manager) calculateTableStatistics(filePaths []string) (*TableStatistics, error) {
	if len(filePaths) == 0 {
		return &TableStatistics{
			RowCount:     0,
			SizeBytes:    0,
			LastAnalyzed: time.Now(),
			LastModified: time.Now(),
		}, nil
	}
	
	var totalRowCount int64 = 0
	var totalSizeBytes int64 = 0
	var latestModTime time.Time
	
	for _, filePath := range filePaths {
		fileStats, err := m.calculateFileStatistics(filePath)
		if err != nil {
			fmt.Printf("Warning: Failed to calculate statistics for file %s: %v\n", filePath, err)
			continue
		}
		
		totalRowCount += fileStats.RowCount
		totalSizeBytes += fileStats.SizeBytes
		
		if fileStats.LastModified.After(latestModTime) {
			latestModTime = fileStats.LastModified
		}
	}
	
	return &TableStatistics{
		RowCount:     totalRowCount,
		SizeBytes:    totalSizeBytes,
		LastAnalyzed: time.Now(),
		LastModified: latestModTime,
	}, nil
}