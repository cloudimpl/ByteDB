package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// MockSchemaReader implements SchemaReader for testing
type MockSchemaReader struct {
	schemas map[string][]ColumnMetadata
}

func NewMockSchemaReader() *MockSchemaReader {
	return &MockSchemaReader{
		schemas: map[string][]ColumnMetadata{
			"employees.parquet": {
				{Name: "id", Type: "INT64", Nullable: false},
				{Name: "name", Type: "STRING", Nullable: true},
				{Name: "department", Type: "STRING", Nullable: true},
				{Name: "salary", Type: "DOUBLE", Nullable: true},
			},
			"employees_2023.parquet": {
				{Name: "id", Type: "INT64", Nullable: false},
				{Name: "name", Type: "STRING", Nullable: true},
				{Name: "department", Type: "STRING", Nullable: true},
				{Name: "salary", Type: "DOUBLE", Nullable: true},
			},
			"employees_incompatible.parquet": {
				{Name: "id", Type: "STRING", Nullable: false}, // Type mismatch
				{Name: "name", Type: "STRING", Nullable: true},
				{Name: "department", Type: "STRING", Nullable: true},
				{Name: "salary", Type: "DOUBLE", Nullable: true},
			},
			"employees_extended.parquet": {
				{Name: "id", Type: "INT64", Nullable: false},
				{Name: "name", Type: "STRING", Nullable: true},
				{Name: "department", Type: "STRING", Nullable: true},
				{Name: "salary", Type: "DOUBLE", Nullable: true},
				{Name: "bonus", Type: "DOUBLE", Nullable: true}, // New column
			},
		},
	}
}

func (m *MockSchemaReader) ReadParquetSchema(filePath string) ([]ColumnMetadata, error) {
	filename := filepath.Base(filePath)
	if schema, ok := m.schemas[filename]; ok {
		return schema, nil
	}
	return nil, os.ErrNotExist
}

func TestCatalogManager_TableCreation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetadataStore()
	manager := NewManager(store, "default", "default") // Use default.default to match memory store
	manager.SetSchemaReader(NewMockSchemaReader())
	
	t.Logf("Created manager with defaults: catalog=%s, schema=%s", "default", "default")
	
	// Initialize
	if err := manager.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	t.Log("Manager initialized successfully")
	
	t.Run("CreateEmptyTable", func(t *testing.T) {
		columns := []ColumnMetadata{
			{Name: "id", Type: "INT64", Nullable: false},
			{Name: "name", Type: "STRING", Nullable: true},
		}
		
		t.Log("Creating empty table...")
		err := manager.CreateTable(ctx, "test_table", "parquet", columns)
		if err != nil {
			t.Errorf("Failed to create empty table: %v", err)
		}
		t.Log("Table created successfully")
		
		// Verify table exists
		table, err := manager.GetTableMetadata(ctx, "test_table")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		if len(table.Locations) != 0 {
			t.Errorf("Expected empty locations, got %d files", len(table.Locations))
		}
		
		if table.Statistics.RowCount != 0 {
			t.Errorf("Expected 0 rows, got %d", table.Statistics.RowCount)
		}
	})
	
	t.Run("RegisterTableWithFile", func(t *testing.T) {
		// Create a dummy file
		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "employees.parquet")
		if err := os.WriteFile(filePath, []byte("dummy"), 0644); err != nil {
			t.Fatal(err)
		}
		
		err := manager.RegisterTable(ctx, "employees", filePath, "parquet")
		if err != nil {
			t.Errorf("Failed to register table: %v", err)
		}
		t.Logf("RegisterTable completed")
		
		// Verify table has schema from file
		table, err := manager.GetTableMetadata(ctx, "employees")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		if len(table.Columns) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(table.Columns))
		}
		
		if len(table.Locations) != 1 {
			t.Errorf("Expected 1 file, got %d", len(table.Locations))
			t.Logf("Table locations: %v", table.Locations)
			t.Logf("Table primary location: %s", table.Location)
		}
	})
}

func TestCatalogManager_FileOperations(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetadataStore()
	manager := NewManager(store, "default", "default")
	manager.SetSchemaReader(NewMockSchemaReader())
	
	// Initialize
	if err := manager.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create test files
	tempDir := t.TempDir()
	file1 := filepath.Join(tempDir, "employees.parquet")
	file2 := filepath.Join(tempDir, "employees_2023.parquet")
	file3 := filepath.Join(tempDir, "employees_incompatible.parquet")
	file4 := filepath.Join(tempDir, "employees_extended.parquet")
	
	for _, f := range []string{file1, file2, file3, file4} {
		if err := os.WriteFile(f, []byte("dummy"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	
	// Register initial table
	if err := manager.RegisterTable(ctx, "employees", file1, "parquet"); err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}
	
	t.Run("AddCompatibleFile", func(t *testing.T) {
		err := manager.AddFileToTable(ctx, "employees", file2, true)
		if err != nil {
			t.Errorf("Failed to add compatible file: %v", err)
		}
		
		files, err := manager.GetTableFiles(ctx, "employees")
		if err != nil {
			t.Errorf("Failed to get table files: %v", err)
		}
		
		if len(files) != 2 {
			t.Errorf("Expected 2 files, got %d", len(files))
		}
	})
	
	t.Run("AddIncompatibleFile", func(t *testing.T) {
		err := manager.AddFileToTable(ctx, "employees", file3, true)
		if err == nil {
			t.Errorf("Expected error adding incompatible file")
		}
	})
	
	t.Run("AddFileWithNewColumns", func(t *testing.T) {
		err := manager.AddFileToTable(ctx, "employees", file4, true)
		if err != nil {
			t.Errorf("Failed to add file with new columns: %v", err)
		}
		
		files, err := manager.GetTableFiles(ctx, "employees")
		if err != nil {
			t.Errorf("Failed to get table files: %v", err)
		}
		
		if len(files) != 3 {
			t.Errorf("Expected 3 files, got %d", len(files))
		}
	})
	
	t.Run("RemoveFile", func(t *testing.T) {
		err := manager.RemoveFileFromTable(ctx, "employees", file4)
		if err != nil {
			t.Errorf("Failed to remove file: %v", err)
		}
		
		files, err := manager.GetTableFiles(ctx, "employees")
		if err != nil {
			t.Errorf("Failed to get table files: %v", err)
		}
		
		if len(files) != 2 {
			t.Errorf("Expected 2 files after removal, got %d", len(files))
		}
	})
	
	t.Run("PreventRemovingLastFile", func(t *testing.T) {
		// Remove all but one file
		manager.RemoveFileFromTable(ctx, "employees", file2)
		
		// Try to remove last file
		err := manager.RemoveFileFromTable(ctx, "employees", file1)
		if err == nil {
			t.Errorf("Expected error when removing last file")
		}
	})
}

func TestCatalogManager_Statistics(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetadataStore()
	manager := NewManager(store, "default", "default")
	
	// Initialize
	if err := manager.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create test files with different sizes
	tempDir := t.TempDir()
	file1 := filepath.Join(tempDir, "small.parquet")
	file2 := filepath.Join(tempDir, "large.parquet")
	
	// Create files with different sizes
	if err := os.WriteFile(file1, make([]byte, 1000), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, make([]byte, 5000), 0644); err != nil {
		t.Fatal(err)
	}
	
	// Register table with first file
	if err := manager.RegisterTable(ctx, "data", file1, "parquet"); err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}
	
	t.Run("InitialStatistics", func(t *testing.T) {
		table, err := manager.GetTableMetadata(ctx, "data")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		if table.Statistics.SizeBytes != 1000 {
			t.Errorf("Expected size 1000, got %d", table.Statistics.SizeBytes)
		}
		
		// Row count is estimated
		if table.Statistics.RowCount == 0 {
			t.Errorf("Expected non-zero row count")
		}
	})
	
	t.Run("AggregatedStatistics", func(t *testing.T) {
		// Add second file
		err := manager.AddFileToTable(ctx, "data", file2, false)
		if err != nil {
			t.Errorf("Failed to add file: %v", err)
		}
		
		table, err := manager.GetTableMetadata(ctx, "data")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		if table.Statistics.SizeBytes != 6000 {
			t.Errorf("Expected total size 6000, got %d", table.Statistics.SizeBytes)
		}
	})
	
	t.Run("RefreshStatistics", func(t *testing.T) {
		// Manually refresh
		err := manager.RefreshTableStatistics(ctx, "data")
		if err != nil {
			t.Errorf("Failed to refresh statistics: %v", err)
		}
		
		table, err := manager.GetTableMetadata(ctx, "data")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		// Check that LastAnalyzed was updated
		if time.Since(table.Statistics.LastAnalyzed) > time.Second {
			t.Errorf("LastAnalyzed not updated")
		}
	})
}

func TestCatalogManager_EmptyTableWorkflow(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetadataStore()
	manager := NewManager(store, "default", "default")
	manager.SetSchemaReader(NewMockSchemaReader())
	
	// Initialize
	if err := manager.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create empty table
	columns := []ColumnMetadata{
		{Name: "id", Type: "INT64", Nullable: false},
		{Name: "name", Type: "STRING", Nullable: true},
		{Name: "department", Type: "STRING", Nullable: true},
		{Name: "salary", Type: "DOUBLE", Nullable: true},
	}
	
	err := manager.CreateTable(ctx, "staged_data", "parquet", columns)
	if err != nil {
		t.Fatalf("Failed to create empty table: %v", err)
	}
	
	// Create test files
	tempDir := t.TempDir()
	file1 := filepath.Join(tempDir, "employees.parquet")
	file2 := filepath.Join(tempDir, "employees_2023.parquet")
	
	for _, f := range []string{file1, file2} {
		if err := os.WriteFile(f, []byte("dummy"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	
	t.Run("AddFilesToEmptyTable", func(t *testing.T) {
		// Add first file
		err := manager.AddFileToTable(ctx, "staged_data", file1, true)
		if err != nil {
			t.Errorf("Failed to add first file: %v", err)
		}
		
		table, err := manager.GetTableMetadata(ctx, "staged_data")
		if err != nil {
			t.Errorf("Failed to get table metadata: %v", err)
		}
		
		// Primary location should be set
		if table.Location != file1 {
			t.Errorf("Expected primary location to be set")
		}
		
		// Add second file
		err = manager.AddFileToTable(ctx, "staged_data", file2, true)
		if err != nil {
			t.Errorf("Failed to add second file: %v", err)
		}
		
		files, err := manager.GetTableFiles(ctx, "staged_data")
		if err != nil {
			t.Errorf("Failed to get files: %v", err)
		}
		
		if len(files) != 2 {
			t.Errorf("Expected 2 files, got %d", len(files))
		}
	})
}