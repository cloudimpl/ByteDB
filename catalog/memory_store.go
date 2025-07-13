package catalog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MemoryMetadataStore is an in-memory implementation of MetadataStore
type MemoryMetadataStore struct {
	mu       sync.RWMutex
	catalogs map[string]*CatalogMetadata
	schemas  map[string]map[string]*SchemaMetadata        // catalogName -> schemaName -> schema
	tables   map[string]map[string]map[string]*TableMetadata // catalogName -> schemaName -> tableName -> table
}

// NewMemoryMetadataStore creates a new in-memory metadata store
func NewMemoryMetadataStore() *MemoryMetadataStore {
	return &MemoryMetadataStore{
		catalogs: make(map[string]*CatalogMetadata),
		schemas:  make(map[string]map[string]*SchemaMetadata),
		tables:   make(map[string]map[string]map[string]*TableMetadata),
	}
}

// Initialize initializes the store
func (m *MemoryMetadataStore) Initialize(ctx context.Context) error {
	// Create default catalog and schema
	defaultCatalog := &CatalogMetadata{
		Name:        "default",
		Description: "Default catalog",
		Properties:  make(map[string]string),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	
	if err := m.CreateCatalog(ctx, defaultCatalog); err != nil {
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
	
	return m.CreateSchema(ctx, defaultSchema)
}

// Close closes the store
func (m *MemoryMetadataStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Clear all data
	m.catalogs = make(map[string]*CatalogMetadata)
	m.schemas = make(map[string]map[string]*SchemaMetadata)
	m.tables = make(map[string]map[string]map[string]*TableMetadata)
	
	return nil
}

// Catalog operations

func (m *MemoryMetadataStore) CreateCatalog(ctx context.Context, catalog *CatalogMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.catalogs[catalog.Name]; exists {
		return ErrCatalogExists
	}
	
	// Deep copy to avoid external modifications
	catalogCopy := *catalog
	catalogCopy.CreatedAt = time.Now()
	catalogCopy.UpdatedAt = time.Now()
	
	m.catalogs[catalog.Name] = &catalogCopy
	m.schemas[catalog.Name] = make(map[string]*SchemaMetadata)
	m.tables[catalog.Name] = make(map[string]map[string]*TableMetadata)
	
	return nil
}

func (m *MemoryMetadataStore) GetCatalog(ctx context.Context, name string) (*CatalogMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	catalog, exists := m.catalogs[name]
	if !exists {
		return nil, ErrCatalogNotFound
	}
	
	// Return a copy
	catalogCopy := *catalog
	return &catalogCopy, nil
}

func (m *MemoryMetadataStore) ListCatalogs(ctx context.Context) ([]*CatalogMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var catalogs []*CatalogMetadata
	for _, catalog := range m.catalogs {
		catalogCopy := *catalog
		catalogs = append(catalogs, &catalogCopy)
	}
	
	return catalogs, nil
}

func (m *MemoryMetadataStore) UpdateCatalog(ctx context.Context, catalog *CatalogMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.catalogs[catalog.Name]; !exists {
		return ErrCatalogNotFound
	}
	
	catalogCopy := *catalog
	catalogCopy.UpdatedAt = time.Now()
	m.catalogs[catalog.Name] = &catalogCopy
	
	return nil
}

func (m *MemoryMetadataStore) DeleteCatalog(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.catalogs[name]; !exists {
		return ErrCatalogNotFound
	}
	
	// Check if catalog has schemas
	if len(m.schemas[name]) > 0 {
		return fmt.Errorf("cannot delete catalog %s: has schemas", name)
	}
	
	delete(m.catalogs, name)
	delete(m.schemas, name)
	delete(m.tables, name)
	
	return nil
}

// Schema operations

func (m *MemoryMetadataStore) CreateSchema(ctx context.Context, schema *SchemaMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check if catalog exists
	if _, exists := m.catalogs[schema.CatalogName]; !exists {
		return ErrCatalogNotFound
	}
	
	// Check if schema already exists
	if schemas, exists := m.schemas[schema.CatalogName]; exists {
		if _, schemaExists := schemas[schema.Name]; schemaExists {
			return ErrSchemaExists
		}
	}
	
	// Deep copy
	schemaCopy := *schema
	schemaCopy.CreatedAt = time.Now()
	schemaCopy.UpdatedAt = time.Now()
	
	m.schemas[schema.CatalogName][schema.Name] = &schemaCopy
	
	// Initialize tables map for this schema
	if m.tables[schema.CatalogName] == nil {
		m.tables[schema.CatalogName] = make(map[string]map[string]*TableMetadata)
	}
	m.tables[schema.CatalogName][schema.Name] = make(map[string]*TableMetadata)
	
	return nil
}

func (m *MemoryMetadataStore) GetSchema(ctx context.Context, catalogName, schemaName string) (*SchemaMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	schemas, exists := m.schemas[catalogName]
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

func (m *MemoryMetadataStore) ListSchemas(ctx context.Context, catalogName string) ([]*SchemaMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	schemas, exists := m.schemas[catalogName]
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

func (m *MemoryMetadataStore) UpdateSchema(ctx context.Context, schema *SchemaMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	schemas, exists := m.schemas[schema.CatalogName]
	if !exists {
		return ErrCatalogNotFound
	}
	
	if _, exists := schemas[schema.Name]; !exists {
		return ErrSchemaNotFound
	}
	
	schemaCopy := *schema
	schemaCopy.UpdatedAt = time.Now()
	m.schemas[schema.CatalogName][schema.Name] = &schemaCopy
	
	return nil
}

func (m *MemoryMetadataStore) DeleteSchema(ctx context.Context, catalogName, schemaName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check if schema has tables
	if tables, exists := m.tables[catalogName]; exists {
		if schemaTables, exists := tables[schemaName]; exists && len(schemaTables) > 0 {
			return fmt.Errorf("cannot delete schema %s.%s: has tables", catalogName, schemaName)
		}
	}
	
	delete(m.schemas[catalogName], schemaName)
	delete(m.tables[catalogName], schemaName)
	
	return nil
}

// Table operations

func (m *MemoryMetadataStore) CreateTable(ctx context.Context, table *TableMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate catalog and schema exist
	schemas, exists := m.schemas[table.CatalogName]
	if !exists {
		return ErrCatalogNotFound
	}
	
	if _, exists := schemas[table.SchemaName]; !exists {
		return ErrSchemaNotFound
	}
	
	// Check if table already exists
	if tables, exists := m.tables[table.CatalogName][table.SchemaName]; exists {
		if _, tableExists := tables[table.Name]; tableExists {
			return ErrTableExists
		}
	}
	
	// Deep copy
	tableCopy := *table
	tableCopy.CreatedAt = time.Now()
	tableCopy.UpdatedAt = time.Now()
	
	// Copy columns
	tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
	copy(tableCopy.Columns, table.Columns)
	
	m.tables[table.CatalogName][table.SchemaName][table.Name] = &tableCopy
	
	return nil
}

func (m *MemoryMetadataStore) GetTable(ctx context.Context, catalogName, schemaName, tableName string) (*TableMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	catalogTables, exists := m.tables[catalogName]
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
	
	// Deep copy
	tableCopy := *table
	tableCopy.Columns = make([]ColumnMetadata, len(table.Columns))
	copy(tableCopy.Columns, table.Columns)
	
	return &tableCopy, nil
}

func (m *MemoryMetadataStore) GetTableByIdentifier(ctx context.Context, identifier TableIdentifier) (*TableMetadata, error) {
	return m.GetTable(ctx, identifier.Catalog, identifier.Schema, identifier.Table)
}

func (m *MemoryMetadataStore) ListTables(ctx context.Context, catalogName, schemaName string) ([]*TableMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	catalogTables, exists := m.tables[catalogName]
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

func (m *MemoryMetadataStore) UpdateTable(ctx context.Context, table *TableMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	catalogTables, exists := m.tables[table.CatalogName]
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
	
	m.tables[table.CatalogName][table.SchemaName][table.Name] = &tableCopy
	
	return nil
}

func (m *MemoryMetadataStore) DeleteTable(ctx context.Context, catalogName, schemaName, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	catalogTables, exists := m.tables[catalogName]
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
	
	delete(m.tables[catalogName][schemaName], tableName)
	
	return nil
}

func (m *MemoryMetadataStore) UpdateTableStatistics(ctx context.Context, catalogName, schemaName, tableName string, stats *TableStatistics) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	table, err := m.GetTable(ctx, catalogName, schemaName, tableName)
	if err != nil {
		return err
	}
	
	table.Statistics = stats
	table.UpdatedAt = time.Now()
	
	m.tables[catalogName][schemaName][tableName] = table
	
	return nil
}

func (m *MemoryMetadataStore) SearchTables(ctx context.Context, searchPattern string) ([]*TableMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	pattern := strings.ToLower(searchPattern)
	var results []*TableMetadata
	
	for _, catalogTables := range m.tables {
		for _, schemaTables := range catalogTables {
			for _, table := range schemaTables {
				// Search in table name, schema name, and description
				if strings.Contains(strings.ToLower(table.Name), pattern) ||
					strings.Contains(strings.ToLower(table.SchemaName), pattern) ||
					strings.Contains(strings.ToLower(table.Description), pattern) {
					
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

// Transaction support

func (m *MemoryMetadataStore) BeginTransaction(ctx context.Context) (Transaction, error) {
	// Memory store doesn't support transactions
	return &noOpTransaction{}, nil
}

type noOpTransaction struct{}

func (t *noOpTransaction) Commit() error   { return nil }
func (t *noOpTransaction) Rollback() error { return nil }

// Factory implementation

type MemoryMetadataStoreFactory struct{}

func (f *MemoryMetadataStoreFactory) CreateStore(config map[string]interface{}) (MetadataStore, error) {
	return NewMemoryMetadataStore(), nil
}

func init() {
	RegisterMetadataStore("memory", &MemoryMetadataStoreFactory{})
}