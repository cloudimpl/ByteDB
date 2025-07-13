package catalog

import (
	"context"
	"fmt"
)

// MetadataStore is the interface for pluggable metadata storage
type MetadataStore interface {
	// Catalog operations
	CreateCatalog(ctx context.Context, catalog *CatalogMetadata) error
	GetCatalog(ctx context.Context, name string) (*CatalogMetadata, error)
	ListCatalogs(ctx context.Context) ([]*CatalogMetadata, error)
	UpdateCatalog(ctx context.Context, catalog *CatalogMetadata) error
	DeleteCatalog(ctx context.Context, name string) error

	// Schema operations
	CreateSchema(ctx context.Context, schema *SchemaMetadata) error
	GetSchema(ctx context.Context, catalogName, schemaName string) (*SchemaMetadata, error)
	ListSchemas(ctx context.Context, catalogName string) ([]*SchemaMetadata, error)
	UpdateSchema(ctx context.Context, schema *SchemaMetadata) error
	DeleteSchema(ctx context.Context, catalogName, schemaName string) error

	// Table operations
	CreateTable(ctx context.Context, table *TableMetadata) error
	GetTable(ctx context.Context, catalogName, schemaName, tableName string) (*TableMetadata, error)
	GetTableByIdentifier(ctx context.Context, identifier TableIdentifier) (*TableMetadata, error)
	ListTables(ctx context.Context, catalogName, schemaName string) ([]*TableMetadata, error)
	UpdateTable(ctx context.Context, table *TableMetadata) error
	DeleteTable(ctx context.Context, catalogName, schemaName, tableName string) error

	// Table statistics
	UpdateTableStatistics(ctx context.Context, catalogName, schemaName, tableName string, stats *TableStatistics) error

	// Search operations
	SearchTables(ctx context.Context, searchPattern string) ([]*TableMetadata, error)
	
	// Transaction support (optional)
	BeginTransaction(ctx context.Context) (Transaction, error)
	
	// Lifecycle
	Initialize(ctx context.Context) error
	Close() error
}

// Transaction represents a metadata transaction
type Transaction interface {
	Commit() error
	Rollback() error
}

// MetadataStoreFactory creates metadata store instances
type MetadataStoreFactory interface {
	CreateStore(config map[string]interface{}) (MetadataStore, error)
}

// Registry of available metadata store implementations
var metadataStoreFactories = make(map[string]MetadataStoreFactory)

// RegisterMetadataStore registers a new metadata store implementation
func RegisterMetadataStore(name string, factory MetadataStoreFactory) {
	metadataStoreFactories[name] = factory
}

// CreateMetadataStore creates a metadata store instance
func CreateMetadataStore(storeType string, config map[string]interface{}) (MetadataStore, error) {
	factory, exists := metadataStoreFactories[storeType]
	if !exists {
		return nil, fmt.Errorf("unknown metadata store type: %s", storeType)
	}
	return factory.CreateStore(config)
}