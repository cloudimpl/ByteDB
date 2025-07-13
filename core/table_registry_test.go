package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTableRegistry_NoFallbackToFilename(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()
	
	// Create a parquet file that would match the fallback pattern
	testFile := filepath.Join(tempDir, "test_table.parquet")
	if err := os.WriteFile(testFile, []byte("dummy"), 0644); err != nil {
		t.Fatal(err)
	}
	
	// Create table registry
	registry := NewTableRegistry(tempDir)
	
	// Try to get path for unregistered table (should fail even though file exists)
	_, err := registry.GetTablePath("test_table")
	if err == nil {
		t.Errorf("Expected error for unregistered table, but got none")
	}
	
	expectedError := "table not registered: test_table"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestTableRegistry_RegisteredTableWorks(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()
	
	// Create a parquet file
	testFile := filepath.Join(tempDir, "my_data.parquet")
	if err := os.WriteFile(testFile, []byte("dummy"), 0644); err != nil {
		t.Fatal(err)
	}
	
	// Create table registry
	registry := NewTableRegistry(tempDir)
	
	// Register the table
	err := registry.RegisterTable("test_table", testFile)
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}
	
	// Get path should now work
	path, err := registry.GetTablePath("test_table")
	if err != nil {
		t.Errorf("Failed to get path for registered table: %v", err)
	}
	
	if path != testFile {
		t.Errorf("Expected path '%s', got '%s'", testFile, path)
	}
}

func TestTableRegistry_ErrorForNonExistentTable(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewTableRegistry(tempDir)
	
	// Try to get a completely non-existent table
	_, err := registry.GetTablePath("non_existent_table")
	if err == nil {
		t.Errorf("Expected error for non-existent table")
	}
	
	expectedError := "table not registered: non_existent_table"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}