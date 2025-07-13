package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileMetadataStore is a file-based implementation of MetadataStore
type FileMetadataStore struct {
	basePath string
	mu       sync.RWMutex

	// In-memory cache
	catalogs map[string]*CatalogMetadata
	schemas  map[string]map[string]*SchemaMetadata
	tables   map[string]map[string]map[string]*TableMetadata
}

// NewFileMetadataStore creates a new file-based metadata store
func NewFileMetadataStore(basePath string) *FileMetadataStore {
	return &FileMetadataStore{
		basePath: basePath,
		catalogs: make(map[string]*CatalogMetadata),
		schemas:  make(map[string]map[string]*SchemaMetadata),
		tables:   make(map[string]map[string]map[string]*TableMetadata),
	}
}

// Initialize initializes the store
func (f *FileMetadataStore) Initialize(ctx context.Context) error {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(f.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Load existing metadata
	if err := f.loadMetadata(); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Create default catalog and schema if they don't exist
	if _, exists := f.catalogs["default"]; !exists {
		defaultCatalog := &CatalogMetadata{
			Name:        "default",
			Description: "Default catalog",
			Properties:  make(map[string]string),
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		if err := f.CreateCatalog(ctx, defaultCatalog); err != nil {
			return err
		}

		defaultSchema := &SchemaMetadata{
			Name:        "default",
			CatalogName: "default",
			Description: "Default schema",
			Properties:  make(map[string]string),
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		if err := f.CreateSchema(ctx, defaultSchema); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the store
func (f *FileMetadataStore) Close() error {
	// Save any pending changes
	return f.saveMetadata()
}

// loadMetadata loads all metadata from disk
func (f *FileMetadataStore) loadMetadata() error {
	catalogsPath := filepath.Join(f.basePath, "catalogs.json")
	if _, err := os.Stat(catalogsPath); os.IsNotExist(err) {
		// No existing metadata
		return nil
	}

	// Load catalogs
	data, err := os.ReadFile(catalogsPath)
	if err != nil {
		return err
	}

	var catalogList []*CatalogMetadata
	if err := json.Unmarshal(data, &catalogList); err != nil {
		return err
	}

	for _, catalog := range catalogList {
		f.catalogs[catalog.Name] = catalog
		f.schemas[catalog.Name] = make(map[string]*SchemaMetadata)
		f.tables[catalog.Name] = make(map[string]map[string]*TableMetadata)

		// Load schemas for this catalog
		schemasPath := filepath.Join(f.basePath, catalog.Name, "schemas.json")
		if schemaData, err := os.ReadFile(schemasPath); err == nil {
			var schemaList []*SchemaMetadata
			if err := json.Unmarshal(schemaData, &schemaList); err == nil {
				for _, schema := range schemaList {
					f.schemas[catalog.Name][schema.Name] = schema
					f.tables[catalog.Name][schema.Name] = make(map[string]*TableMetadata)

					// Load tables for this schema
					tablesPath := filepath.Join(f.basePath, catalog.Name, schema.Name, "tables.json")
					if tableData, err := os.ReadFile(tablesPath); err == nil {
						var tableList []*TableMetadata
						if err := json.Unmarshal(tableData, &tableList); err == nil {
							for _, table := range tableList {
								f.tables[catalog.Name][schema.Name][table.Name] = table
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// saveMetadata saves all metadata to disk
func (f *FileMetadataStore) saveMetadata() error {
	// Save catalogs
	var catalogList []*CatalogMetadata
	for _, catalog := range f.catalogs {
		catalogList = append(catalogList, catalog)
	}

	catalogsPath := filepath.Join(f.basePath, "catalogs.json")
	if err := f.writeJSON(catalogsPath, catalogList); err != nil {
		return err
	}

	// Save schemas and tables for each catalog
	for catalogName, schemas := range f.schemas {
		catalogPath := filepath.Join(f.basePath, catalogName)
		if err := os.MkdirAll(catalogPath, 0755); err != nil {
			return err
		}

		var schemaList []*SchemaMetadata
		for _, schema := range schemas {
			schemaList = append(schemaList, schema)
		}

		schemasPath := filepath.Join(catalogPath, "schemas.json")
		if err := f.writeJSON(schemasPath, schemaList); err != nil {
			return err
		}

		// Save tables for each schema
		for schemaName, tables := range f.tables[catalogName] {
			schemaPath := filepath.Join(catalogPath, schemaName)
			if err := os.MkdirAll(schemaPath, 0755); err != nil {
				return err
			}

			var tableList []*TableMetadata
			for _, table := range tables {
				tableList = append(tableList, table)
			}

			tablesPath := filepath.Join(schemaPath, "tables.json")
			if err := f.writeJSON(tablesPath, tableList); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeJSON writes data to a JSON file atomically
func (f *FileMetadataStore) writeJSON(path string, data interface{}) error {
	// Write to temp file first
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(data); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Atomic rename
	return os.Rename(tempPath, path)
}

// Catalog operations

func (f *FileMetadataStore) CreateCatalog(ctx context.Context, catalog *CatalogMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.catalogs[catalog.Name]; exists {
		return ErrCatalogExists
	}

	catalogCopy := *catalog
	catalogCopy.CreatedAt = time.Now()
	catalogCopy.UpdatedAt = time.Now()

	f.catalogs[catalog.Name] = &catalogCopy
	f.schemas[catalog.Name] = make(map[string]*SchemaMetadata)
	f.tables[catalog.Name] = make(map[string]map[string]*TableMetadata)

	return f.saveMetadata()
}

func (f *FileMetadataStore) GetCatalog(ctx context.Context, name string) (*CatalogMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	catalog, exists := f.catalogs[name]
	if !exists {
		return nil, ErrCatalogNotFound
	}

	catalogCopy := *catalog
	return &catalogCopy, nil
}

func (f *FileMetadataStore) ListCatalogs(ctx context.Context) ([]*CatalogMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var catalogs []*CatalogMetadata
	for _, catalog := range f.catalogs {
		catalogCopy := *catalog
		catalogs = append(catalogs, &catalogCopy)
	}

	return catalogs, nil
}

func (f *FileMetadataStore) UpdateCatalog(ctx context.Context, catalog *CatalogMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.catalogs[catalog.Name]; !exists {
		return ErrCatalogNotFound
	}

	catalogCopy := *catalog
	catalogCopy.UpdatedAt = time.Now()
	f.catalogs[catalog.Name] = &catalogCopy

	return f.saveMetadata()
}

func (f *FileMetadataStore) DeleteCatalog(ctx context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.catalogs[name]; !exists {
		return ErrCatalogNotFound
	}

	if len(f.schemas[name]) > 0 {
		return fmt.Errorf("cannot delete catalog %s: has schemas", name)
	}

	delete(f.catalogs, name)
	delete(f.schemas, name)
	delete(f.tables, name)

	// Remove catalog directory
	catalogPath := filepath.Join(f.basePath, name)
	os.RemoveAll(catalogPath)

	return f.saveMetadata()
}

// Schema operations

func (f *FileMetadataStore) CreateSchema(ctx context.Context, schema *SchemaMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.catalogs[schema.CatalogName]; !exists {
		return ErrCatalogNotFound
	}

	if schemas, exists := f.schemas[schema.CatalogName]; exists {
		if _, schemaExists := schemas[schema.Name]; schemaExists {
			return ErrSchemaExists
		}
	}

	schemaCopy := *schema
	schemaCopy.CreatedAt = time.Now()
	schemaCopy.UpdatedAt = time.Now()

	f.schemas[schema.CatalogName][schema.Name] = &schemaCopy

	if f.tables[schema.CatalogName] == nil {
		f.tables[schema.CatalogName] = make(map[string]map[string]*TableMetadata)
	}
	f.tables[schema.CatalogName][schema.Name] = make(map[string]*TableMetadata)

	return f.saveMetadata()
}

func (f *FileMetadataStore) GetSchema(ctx context.Context, catalogName, schemaName string) (*SchemaMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	schemas, exists := f.schemas[catalogName]
	if !exists {
		return nil, ErrCatalogNotFound
	}

	schema, exists := schemas[schemaName]
	if !exists {
		return nil, ErrSchemaNotFound
	}

	schemaCopy := *schema
	return &schemaCopy, nil
}

func (f *FileMetadataStore) ListSchemas(ctx context.Context, catalogName string) ([]*SchemaMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	schemas, exists := f.schemas[catalogName]
	if !exists {
		return nil, ErrCatalogNotFound
	}

	var result []*SchemaMetadata
	for _, schema := range schemas {
		schemaCopy := *schema
		result = append(result, &schemaCopy)
	}

	return result, nil
}

func (f *FileMetadataStore) UpdateSchema(ctx context.Context, schema *SchemaMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	schemas, exists := f.schemas[schema.CatalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	if _, exists := schemas[schema.Name]; !exists {
		return ErrSchemaNotFound
	}

	schemaCopy := *schema
	schemaCopy.UpdatedAt = time.Now()
	f.schemas[schema.CatalogName][schema.Name] = &schemaCopy

	return f.saveMetadata()
}

func (f *FileMetadataStore) DeleteSchema(ctx context.Context, catalogName, schemaName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if tables, exists := f.tables[catalogName]; exists {
		if schemaTables, exists := tables[schemaName]; exists && len(schemaTables) > 0 {
			return fmt.Errorf("cannot delete schema %s.%s: has tables", catalogName, schemaName)
		}
	}

	delete(f.schemas[catalogName], schemaName)
	delete(f.tables[catalogName], schemaName)

	// Remove schema directory
	schemaPath := filepath.Join(f.basePath, catalogName, schemaName)
	os.RemoveAll(schemaPath)

	return f.saveMetadata()
}

// Table operations

func (f *FileMetadataStore) CreateTable(ctx context.Context, table *TableMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	schemas, exists := f.schemas[table.CatalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	if _, exists := schemas[table.SchemaName]; !exists {
		return ErrSchemaNotFound
	}

	if tables, exists := f.tables[table.CatalogName][table.SchemaName]; exists {
		if _, tableExists := tables[table.Name]; tableExists {
			return ErrTableExists
		}
	}

	tableCopy := *table
	tableCopy.CreatedAt = time.Now()
	tableCopy.UpdatedAt = time.Now()

	tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
	copy(tableCopy.Columns, table.Columns)

	// Initialize Locations if empty
	if len(tableCopy.Locations) == 0 && tableCopy.Location != "" {
		tableCopy.Locations = []string{tableCopy.Location}
	}

	f.tables[table.CatalogName][table.SchemaName][table.Name] = &tableCopy

	return f.saveMetadata()
}

func (f *FileMetadataStore) GetTable(ctx context.Context, catalogName, schemaName, tableName string) (*TableMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	catalogTables, exists := f.tables[catalogName]
	if !exists {
		return nil, ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[schemaName]
	if !exists {
		return nil, ErrSchemaNotFound
	}

	table, exists := schemaTables[tableName]
	if !exists {
		return nil, ErrTableNotFound
	}

	tableCopy := *table
	tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
	copy(tableCopy.Columns, table.Columns)

	return &tableCopy, nil
}

func (f *FileMetadataStore) GetTableByIdentifier(ctx context.Context, identifier TableIdentifier) (*TableMetadata, error) {
	return f.GetTable(ctx, identifier.Catalog, identifier.Schema, identifier.Table)
}

func (f *FileMetadataStore) ListTables(ctx context.Context, catalogName, schemaName string) ([]*TableMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	catalogTables, exists := f.tables[catalogName]
	if !exists {
		return nil, ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[schemaName]
	if !exists {
		return nil, ErrSchemaNotFound
	}

	var result []*TableMetadata
	for _, table := range schemaTables {
		tableCopy := *table
		tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
		copy(tableCopy.Columns, table.Columns)
		result = append(result, &tableCopy)
	}

	return result, nil
}

func (f *FileMetadataStore) UpdateTable(ctx context.Context, table *TableMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	catalogTables, exists := f.tables[table.CatalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[table.SchemaName]
	if !exists {
		return ErrSchemaNotFound
	}

	if _, exists := schemaTables[table.Name]; !exists {
		return ErrTableNotFound
	}

	tableCopy := *table
	tableCopy.UpdatedAt = time.Now()
	tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
	copy(tableCopy.Columns, table.Columns)

	f.tables[table.CatalogName][table.SchemaName][table.Name] = &tableCopy

	return f.saveMetadata()
}

func (f *FileMetadataStore) DeleteTable(ctx context.Context, catalogName, schemaName, tableName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	catalogTables, exists := f.tables[catalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[schemaName]
	if !exists {
		return ErrSchemaNotFound
	}

	if _, exists := schemaTables[tableName]; !exists {
		return ErrTableNotFound
	}

	delete(f.tables[catalogName][schemaName], tableName)

	return f.saveMetadata()
}

func (f *FileMetadataStore) UpdateTableStatistics(ctx context.Context, catalogName, schemaName, tableName string, stats *TableStatistics) error {
	table, err := f.GetTable(ctx, catalogName, schemaName, tableName)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	table.Statistics = stats
	table.UpdatedAt = time.Now()

	f.tables[catalogName][schemaName][tableName] = table

	return f.saveMetadata()
}

func (f *FileMetadataStore) SearchTables(ctx context.Context, searchPattern string) ([]*TableMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	pattern := searchPattern // Case-sensitive search
	var results []*TableMetadata

	for _, catalogTables := range f.tables {
		for _, schemaTables := range catalogTables {
			for _, table := range schemaTables {
				if contains(table.Name, pattern) ||
					contains(table.SchemaName, pattern) ||
					contains(table.Description, pattern) {

					tableCopy := *table
					tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
					copy(tableCopy.Columns, table.Columns)
					results = append(results, &tableCopy)
				}
			}
		}
	}

	return results, nil
}

func contains(s, substr string) bool {
	return len(substr) > 0 && len(s) >= len(substr) && hasSubstring(s, substr)
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (f *FileMetadataStore) AddFileToTable(ctx context.Context, catalogName, schemaName, tableName string, filePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	catalogTables, exists := f.tables[catalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[schemaName]
	if !exists {
		return ErrSchemaNotFound
	}

	table, exists := schemaTables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Check if file already exists
	for _, loc := range table.Locations {
		if loc == filePath {
			return fmt.Errorf("file already registered: %s", filePath)
		}
	}

	// Add the file
	table.Locations = append(table.Locations, filePath)
	table.UpdatedAt = time.Now()

	return f.saveMetadata()
}

func (f *FileMetadataStore) RemoveFileFromTable(ctx context.Context, catalogName, schemaName, tableName string, filePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	catalogTables, exists := f.tables[catalogName]
	if !exists {
		return ErrCatalogNotFound
	}

	schemaTables, exists := catalogTables[schemaName]
	if !exists {
		return ErrSchemaNotFound
	}

	table, exists := schemaTables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Find and remove the file
	found := false
	newLocations := make([]string, 0, len(table.Locations))
	for _, loc := range table.Locations {
		if loc != filePath {
			newLocations = append(newLocations, loc)
		} else {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("file not found in table: %s", filePath)
	}

	// Don't allow removing the last file
	if len(newLocations) == 0 {
		return fmt.Errorf("cannot remove last file from table")
	}

	table.Locations = newLocations
	table.UpdatedAt = time.Now()

	// Update primary location if it was removed
	if table.Location == filePath && len(newLocations) > 0 {
		table.Location = newLocations[0]
	}

	return f.saveMetadata()
}

// Transaction support

func (f *FileMetadataStore) BeginTransaction(ctx context.Context) (Transaction, error) {
	// File store doesn't support transactions
	return &noOpTransaction{}, nil
}

// Factory implementation

type FileMetadataStoreFactory struct{}

func (f *FileMetadataStoreFactory) CreateStore(config map[string]interface{}) (MetadataStore, error) {
	basePath, ok := config["base_path"].(string)
	if !ok {
		return nil, fmt.Errorf("base_path is required for file metadata store")
	}

	return NewFileMetadataStore(basePath), nil
}

func init() {
	RegisterMetadataStore("file", &FileMetadataStoreFactory{})
}
