package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
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
		Location:    "",         // No primary location for empty table
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

	// Read actual parquet metadata to get precise row count
	actualRowCount, err := m.getParquetRowCount(filePath)
	if err != nil {
		// Fallback to file size estimation if parquet reading fails
		fmt.Printf("Warning: Failed to read parquet metadata from %s, using size estimation: %v\n", filePath, err)
		estimatedRowCount := fileInfo.Size() / 100 // Rough estimate: 100 bytes per row
		if estimatedRowCount < 1 {
			estimatedRowCount = 1 // Minimum of 1 row
		}
		actualRowCount = estimatedRowCount
	}

	// Read column statistics from parquet metadata
	columnStats, err := m.getParquetColumnStats(filePath)
	if err != nil {
		// Just log warning, column stats are optional
		fmt.Printf("Warning: Failed to read column statistics from %s: %v\n", filePath, err)
		columnStats = nil
	}

	return &TableStatistics{
		RowCount:     actualRowCount,
		SizeBytes:    fileInfo.Size(),
		LastAnalyzed: time.Now(),
		LastModified: fileInfo.ModTime(),
		ColumnStats:  columnStats,
	}, nil
}

// getParquetRowCount reads the actual row count from parquet file metadata
func (m *Manager) getParquetRowCount(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file stats: %w", err)
	}

	reader, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Calculate total rows across all row groups
	totalRows := int64(0)
	for _, rg := range reader.RowGroups() {
		totalRows += rg.NumRows()
	}

	return totalRows, nil
}

// getParquetColumnStats reads column statistics from parquet file metadata
// This is efficient as it only reads metadata, not actual data
func (m *Manager) getParquetColumnStats(filePath string) (map[string]*ColumnStatistics, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	reader, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := reader.Schema()
	
	// Initialize column statistics map
	columnStats := make(map[string]*ColumnStatistics)

	// Aggregate statistics across all row groups
	for _, rowGroup := range reader.RowGroups() {
		for idx, columnChunk := range rowGroup.ColumnChunks() {
			// Type assert to FileColumnChunk to access statistics
			fileColumnChunk, ok := columnChunk.(*parquet.FileColumnChunk)
			if !ok {
				continue
			}
			
			// Get column name
			columnName := getColumnName(schema, idx)
			
			// Initialize stats for this column if not exists
			if _, exists := columnStats[columnName]; !exists {
				columnStats[columnName] = &ColumnStatistics{
					NullCount:     0,
					DistinctCount: -1, // Not available from metadata
				}
			}
			
			stats := columnStats[columnName]
			
			// Aggregate null count
			stats.NullCount += fileColumnChunk.NullCount()
			
			// Get min/max bounds
			min, max, hasMinMax := fileColumnChunk.Bounds()
			if hasMinMax {
				// Update min value
				minVal := parquetValueToInterface(min)
				if stats.MinValue == nil || compareValues(minVal, stats.MinValue) < 0 {
					stats.MinValue = minVal
				}
				
				// Update max value
				maxVal := parquetValueToInterface(max)
				if stats.MaxValue == nil || compareValues(maxVal, stats.MaxValue) > 0 {
					stats.MaxValue = maxVal
				}
				
				// Track string sizes
				if strVal, ok := minVal.(string); ok {
					size := int64(len(strVal))
					if size > stats.MaxSize {
						stats.MaxSize = size
					}
				}
				if strVal, ok := maxVal.(string); ok {
					size := int64(len(strVal))
					if size > stats.MaxSize {
						stats.MaxSize = size
					}
				}
			}
		}
	}

	return columnStats, nil
}

// getColumnName retrieves the column name from schema by index
func getColumnName(schema *parquet.Schema, columnIndex int) string {
	fields := schema.Fields()
	if columnIndex < len(fields) {
		return fields[columnIndex].Name()
	}
	return fmt.Sprintf("column_%d", columnIndex)
}

// parquetValueToInterface converts a parquet.Value to a Go interface{}
func parquetValueToInterface(v parquet.Value) interface{} {
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return v.Int32()
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return v.Float()
	case parquet.Double:
		return v.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return v.String()
	default:
		return v.String()
	}
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
	aggregatedColumnStats := make(map[string]*ColumnStatistics)

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

		// Aggregate column statistics across files
		if fileStats.ColumnStats != nil {
			for colName, colStats := range fileStats.ColumnStats {
				if aggStats, exists := aggregatedColumnStats[colName]; exists {
					// Aggregate existing stats
					aggStats.NullCount += colStats.NullCount
					if colStats.MinValue != nil && (aggStats.MinValue == nil || compareValues(colStats.MinValue, aggStats.MinValue) < 0) {
						aggStats.MinValue = colStats.MinValue
					}
					if colStats.MaxValue != nil && (aggStats.MaxValue == nil || compareValues(colStats.MaxValue, aggStats.MaxValue) > 0) {
						aggStats.MaxValue = colStats.MaxValue
					}
					if colStats.MaxSize > aggStats.MaxSize {
						aggStats.MaxSize = colStats.MaxSize
					}
				} else {
					// First time seeing this column
					aggStats = &ColumnStatistics{
						NullCount:     colStats.NullCount,
						DistinctCount: colStats.DistinctCount,
						MinValue:      colStats.MinValue,
						MaxValue:      colStats.MaxValue,
						AvgSize:       colStats.AvgSize,
						MaxSize:       colStats.MaxSize,
					}
					aggregatedColumnStats[colName] = aggStats
				}
			}
		}
	}

	return &TableStatistics{
		RowCount:     totalRowCount,
		SizeBytes:    totalSizeBytes,
		LastAnalyzed: time.Now(),
		LastModified: latestModTime,
		ColumnStats:  aggregatedColumnStats,
	}, nil
}

// GetTableStoragePath returns the appropriate storage path for a new table file with UUID
func (m *Manager) GetTableStoragePath(tableName string, fallbackDataPath string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Generate UUID for unique filename
	uuid, err := generateUUID()
	if err != nil {
		return "", fmt.Errorf("failed to generate UUID: %w", err)
	}

	filename := fmt.Sprintf("%s/%s.parquet", tableName, uuid)

	// Check if this is a file-based catalog store
	if fileStore, ok := m.store.(*FileMetadataStore); ok {
		// For file-based catalogs, store data in catalog/data/ folder
		catalogDataDir := filepath.Join(fileStore.basePath, "data")
		if err := os.MkdirAll(catalogDataDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create catalog data directory: %w", err)
		}
		return filepath.Join(catalogDataDir, filename), nil
	}

	// For in-memory catalogs, use a temporary directory
	tempDir := filepath.Join(os.TempDir(), "bytedb-"+generateSessionID())
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temporary data directory: %w", err)
	}
	return filepath.Join(tempDir, filename), nil
}

// WriteTableData writes query results to a parquet file and registers the table
func (m *Manager) WriteTableData(ctx context.Context, tableName string, columns []string, rows [][]interface{}, fallbackDataPath string) error {
	// Get the storage path for this table
	filePath, err := m.GetTableStoragePath(tableName, fallbackDataPath)
	if err != nil {
		return fmt.Errorf("failed to determine storage path: %w", err)
	}

	// Write data to parquet file and collect column statistics
	columnStats, err := writeParquetFileWithStats(filePath, columns, rows)
	if err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	// Register the table in the catalog
	if err := m.RegisterTable(ctx, tableName, filePath, "parquet"); err != nil {
		return fmt.Errorf("failed to register table: %w", err)
	}

	// Update table statistics with column stats
	if columnStats != nil {
		tableID := ParseTableIdentifier(tableName, m.defaultCatalog, m.defaultSchema)
		table, err := m.store.GetTable(ctx, tableID.Catalog, tableID.Schema, tableID.Table)
		if err == nil && table.Statistics != nil {
			table.Statistics.ColumnStats = columnStats
			if err := m.UpdateTableStatistics(ctx, tableName, table.Statistics); err != nil {
				fmt.Printf("Warning: Failed to update column statistics: %v\n", err)
			}
		}
	}

	return nil
}

// writeParquetFileWithStats writes data to a parquet file and collects column statistics
func writeParquetFileWithStats(filePath string, columns []string, rows [][]interface{}) (map[string]*ColumnStatistics, error) {
	if len(rows) == 0 || len(columns) == 0 {
		return nil, fmt.Errorf("no data to write")
	}
	
	// Initialize column statistics collectors
	columnStats := make(map[string]*ColumnStatistics)
	for _, col := range columns {
		columnStats[col] = &ColumnStatistics{
			NullCount:     0,
			DistinctCount: -1, // We won't track distinct count for now (too expensive)
		}
	}
	
	// Collect statistics while preparing data
	for _, row := range rows {
		for i, col := range columns {
			value := row[i]
			stats := columnStats[col]
			
			if value == nil {
				stats.NullCount++
				continue
			}
			
			// Update min/max values
			if stats.MinValue == nil || compareValues(value, stats.MinValue) < 0 {
				stats.MinValue = value
			}
			if stats.MaxValue == nil || compareValues(value, stats.MaxValue) > 0 {
				stats.MaxValue = value
			}
			
			// Track size for string types
			if strVal, ok := value.(string); ok {
				size := int64(len(strVal))
				if size > stats.MaxSize {
					stats.MaxSize = size
				}
			}
		}
	}
	
	// Write the parquet file
	if err := writeParquetFile(filePath, columns, rows); err != nil {
		return nil, err
	}
	
	return columnStats, nil
}

// compareValues compares two values for min/max tracking
func compareValues(a, b interface{}) int {
	// Handle same types
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case int, int32, int64:
		aInt := toInt64(av)
		bInt := toInt64(b)
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case float32, float64:
		aFloat := toFloat64(av)
		bFloat := toFloat64(b)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case bool:
		if bv, ok := b.(bool); ok {
			if !av && bv {
				return -1
			} else if av && !bv {
				return 1
			}
			return 0
		}
	}
	
	// Default: convert to string and compare
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// Helper functions for type conversion
func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}

// writeParquetFile writes data to a parquet file with dynamic schema
func writeParquetFile(filePath string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 || len(columns) == 0 {
		return fmt.Errorf("no data to write")
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create dynamic schema based on the data
	schema, err := createDynamicSchema(columns, rows)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create parquet writer with dynamic schema
	writer := parquet.NewGenericWriter[map[string]interface{}](file, &parquet.WriterConfig{Schema: schema})
	defer writer.Close()

	// Convert rows to map format and write
	records := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("row length %d doesn't match column count %d", len(row), len(columns))
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = row[i]
		}
		records = append(records, record)
	}

	// Write all records at once
	_, err = writer.Write(records)
	if err != nil {
		return fmt.Errorf("failed to write records: %w", err)
	}

	return nil
}

// createDynamicSchema creates a parquet schema based on column names and sample data
func createDynamicSchema(columns []string, rows [][]interface{}) (*parquet.Schema, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no sample data to infer schema")
	}

	// Use first row to infer types
	sampleRow := rows[0]
	if len(sampleRow) != len(columns) {
		return nil, fmt.Errorf("sample row length doesn't match column count")
	}

	// Create schema group map
	group := make(parquet.Group)
	for i, col := range columns {
		value := sampleRow[i]
		field, err := createFieldFromValue(col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to create field for column %s: %w", col, err)
		}
		group[col] = field
	}

	// Create and return the schema
	return parquet.NewSchema("DynamicRecord", group), nil
}

// createFieldFromValue creates a parquet field based on the Go value type
func createFieldFromValue(name string, value interface{}) (parquet.Node, error) {
	if value == nil {
		// Default to optional string for nil values
		return parquet.Optional(parquet.String()), nil
	}

	switch value.(type) {
	case string:
		return parquet.String(), nil
	case int, int32:
		return parquet.Leaf(parquet.Int32Type), nil
	case int64:
		return parquet.Leaf(parquet.Int64Type), nil
	case float32, float64:
		return parquet.Leaf(parquet.DoubleType), nil
	case bool:
		return parquet.Leaf(parquet.BooleanType), nil
	default:
		// Try to handle other numeric types using reflection
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			return parquet.Leaf(parquet.Int32Type), nil
		case reflect.Int64:
			return parquet.Leaf(parquet.Int64Type), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			return parquet.Leaf(parquet.Int32Type), nil
		case reflect.Uint64:
			return parquet.Leaf(parquet.Int64Type), nil
		case reflect.Float32, reflect.Float64:
			return parquet.Leaf(parquet.DoubleType), nil
		case reflect.Bool:
			return parquet.Leaf(parquet.BooleanType), nil
		case reflect.String:
			return parquet.String(), nil
		default:
			// Fall back to string representation
			return parquet.String(), nil
		}
	}
}

// generateUUID generates a UUID v4 for unique filenames
func generateUUID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	// Set version (4) and variant bits according to RFC 4122
	bytes[6] = (bytes[6] & 0x0f) | 0x40 // Version 4
	bytes[8] = (bytes[8] & 0x3f) | 0x80 // Variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		bytes[0:4],
		bytes[4:6],
		bytes[6:8],
		bytes[8:10],
		bytes[10:16],
	), nil
}

// generateSessionID generates a random session ID for temporary directories
func generateSessionID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}
