package catalog

import (
	"context"
	"testing"
)

func TestMemoryMetadataStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetadataStore()

	// Initialize store
	err := store.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	// Test catalog operations
	t.Run("Catalog Operations", func(t *testing.T) {
		// Create catalog
		catalog := &CatalogMetadata{
			Name:        "test_catalog",
			Description: "Test catalog",
			Properties:  make(map[string]string),
		}
		
		err := store.CreateCatalog(ctx, catalog)
		if err != nil {
			t.Errorf("Failed to create catalog: %v", err)
		}

		// Get catalog
		retrieved, err := store.GetCatalog(ctx, "test_catalog")
		if err != nil {
			t.Errorf("Failed to get catalog: %v", err)
		}
		if retrieved.Name != "test_catalog" {
			t.Errorf("Expected catalog name 'test_catalog', got '%s'", retrieved.Name)
		}

		// List catalogs
		catalogs, err := store.ListCatalogs(ctx)
		if err != nil {
			t.Errorf("Failed to list catalogs: %v", err)
		}
		// Should have default + test_catalog
		if len(catalogs) < 2 {
			t.Errorf("Expected at least 2 catalogs, got %d", len(catalogs))
		}

		// Try to create duplicate
		err = store.CreateCatalog(ctx, catalog)
		if err != ErrCatalogExists {
			t.Errorf("Expected ErrCatalogExists, got %v", err)
		}
	})

	// Test schema operations
	t.Run("Schema Operations", func(t *testing.T) {
		schema := &SchemaMetadata{
			Name:        "test_schema",
			CatalogName: "test_catalog",
			Description: "Test schema",
			Properties:  make(map[string]string),
		}

		err := store.CreateSchema(ctx, schema)
		if err != nil {
			t.Errorf("Failed to create schema: %v", err)
		}

		// Get schema
		retrieved, err := store.GetSchema(ctx, "test_catalog", "test_schema")
		if err != nil {
			t.Errorf("Failed to get schema: %v", err)
		}
		if retrieved.Name != "test_schema" {
			t.Errorf("Expected schema name 'test_schema', got '%s'", retrieved.Name)
		}

		// List schemas
		schemas, err := store.ListSchemas(ctx, "test_catalog")
		if err != nil {
			t.Errorf("Failed to list schemas: %v", err)
		}
		if len(schemas) != 1 {
			t.Errorf("Expected 1 schema, got %d", len(schemas))
		}
	})

	// Test table operations
	t.Run("Table Operations", func(t *testing.T) {
		table := &TableMetadata{
			Name:        "test_table",
			SchemaName:  "test_schema",
			CatalogName: "test_catalog",
			Location:    "/path/to/file.parquet",
			Format:      "parquet",
			Columns: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
			Properties: make(map[string]string),
		}

		err := store.CreateTable(ctx, table)
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
		}

		// Get table
		retrieved, err := store.GetTable(ctx, "test_catalog", "test_schema", "test_table")
		if err != nil {
			t.Errorf("Failed to get table: %v", err)
		}
		if retrieved.Name != "test_table" {
			t.Errorf("Expected table name 'test_table', got '%s'", retrieved.Name)
		}
		if len(retrieved.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(retrieved.Columns))
		}

		// List tables
		tables, err := store.ListTables(ctx, "test_catalog", "test_schema")
		if err != nil {
			t.Errorf("Failed to list tables: %v", err)
		}
		if len(tables) != 1 {
			t.Errorf("Expected 1 table, got %d", len(tables))
		}
	})

	// Test multi-file operations
	t.Run("Multi-File Operations", func(t *testing.T) {
		// Add file to table
		err := store.AddFileToTable(ctx, "test_catalog", "test_schema", "test_table", "/path/to/file2.parquet")
		if err != nil {
			t.Errorf("Failed to add file to table: %v", err)
		}

		// Check that file was added
		table, err := store.GetTable(ctx, "test_catalog", "test_schema", "test_table")
		if err != nil {
			t.Errorf("Failed to get table: %v", err)
		}
		if len(table.Locations) != 2 {
			t.Errorf("Expected 2 files, got %d", len(table.Locations))
		}

		// Try to add duplicate file
		err = store.AddFileToTable(ctx, "test_catalog", "test_schema", "test_table", "/path/to/file2.parquet")
		if err == nil {
			t.Errorf("Expected error when adding duplicate file")
		}

		// Remove file
		err = store.RemoveFileFromTable(ctx, "test_catalog", "test_schema", "test_table", "/path/to/file2.parquet")
		if err != nil {
			t.Errorf("Failed to remove file from table: %v", err)
		}

		// Check that file was removed
		table, err = store.GetTable(ctx, "test_catalog", "test_schema", "test_table")
		if err != nil {
			t.Errorf("Failed to get table: %v", err)
		}
		if len(table.Locations) != 1 {
			t.Errorf("Expected 1 file after removal, got %d", len(table.Locations))
		}

		// Try to remove last file
		err = store.RemoveFileFromTable(ctx, "test_catalog", "test_schema", "test_table", table.Locations[0])
		if err == nil {
			t.Errorf("Expected error when removing last file")
		}
	})

	// Clean up
	err = store.Close()
	if err != nil {
		t.Errorf("Failed to close store: %v", err)
	}
}

func TestTableIdentifierParsing(t *testing.T) {
	tests := []struct {
		input           string
		defaultCatalog  string
		defaultSchema   string
		expectedCatalog string
		expectedSchema  string
		expectedTable   string
	}{
		{
			input:           "mytable",
			defaultCatalog:  "default",
			defaultSchema:   "public",
			expectedCatalog: "default",
			expectedSchema:  "public",
			expectedTable:   "mytable",
		},
		{
			input:           "myschema.mytable",
			defaultCatalog:  "default",
			defaultSchema:   "public",
			expectedCatalog: "default",
			expectedSchema:  "myschema",
			expectedTable:   "mytable",
		},
		{
			input:           "mycatalog.myschema.mytable",
			defaultCatalog:  "default",
			defaultSchema:   "public",
			expectedCatalog: "mycatalog",
			expectedSchema:  "myschema",
			expectedTable:   "mytable",
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := ParseTableIdentifier(test.input, test.defaultCatalog, test.defaultSchema)
			if result.Catalog != test.expectedCatalog {
				t.Errorf("Expected catalog '%s', got '%s'", test.expectedCatalog, result.Catalog)
			}
			if result.Schema != test.expectedSchema {
				t.Errorf("Expected schema '%s', got '%s'", test.expectedSchema, result.Schema)
			}
			if result.Table != test.expectedTable {
				t.Errorf("Expected table '%s', got '%s'", test.expectedTable, result.Table)
			}
		})
	}
}

func TestSchemaComparison(t *testing.T) {
	tests := []struct {
		name         string
		existing     []ColumnMetadata
		new          []ColumnMetadata
		compatible   bool
		differences  int
	}{
		{
			name: "Identical schemas",
			existing: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
			new: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
			compatible:  true,
			differences: 0,
		},
		{
			name: "New column added",
			existing: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
			},
			new: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
			compatible:  true,
			differences: 1, // new column
		},
		{
			name: "Missing required column",
			existing: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
			new: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
			},
			compatible:  false,
			differences: 1, // missing column
		},
		{
			name: "Type mismatch",
			existing: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
			},
			new: []ColumnMetadata{
				{Name: "id", Type: "string", Nullable: false},
			},
			compatible:  false,
			differences: 1, // type mismatch
		},
		{
			name: "Nullability change (compatible)",
			existing: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: false},
			},
			new: []ColumnMetadata{
				{Name: "id", Type: "int32", Nullable: true},
			},
			compatible:  true,
			differences: 1, // nullability change (but compatible)
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compatible, differences := CompareSchemas(test.existing, test.new)
			if compatible != test.compatible {
				t.Errorf("Expected compatible=%v, got %v", test.compatible, compatible)
			}
			if len(differences) != test.differences {
				t.Errorf("Expected %d differences, got %d: %v", test.differences, len(differences), differences)
			}
		})
	}
}