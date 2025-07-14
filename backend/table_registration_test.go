package main

import (
	"os"
	"path/filepath"
	"testing"
	
	"bytedb/core"
)

func TestTableRegistrationRequired(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()
	
	// Create a parquet file that would match the old fallback pattern
	testFile := filepath.Join(tempDir, "employees.parquet")
	// Just create an empty file for testing
	if err := os.WriteFile(testFile, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	
	// Create query engine
	engine := core.NewQueryEngine(tempDir)
	defer engine.Close()
	
	// Try to query without registering the table
	query := "SELECT * FROM employees"
	result, err := engine.Execute(query)
	
	// Should get an error about table not being registered
	if err == nil && result != nil && result.Error == "" {
		t.Errorf("Expected error for unregistered table, but query succeeded")
	}
	
	// The error should mention that the table is not registered
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	} else if result != nil && result.Error != "" {
		errorMsg = result.Error
	}
	
	if errorMsg != "" && !contains(errorMsg, "not registered") && !contains(errorMsg, "not found") {
		t.Errorf("Expected 'not registered' or 'not found' error, got: %s", errorMsg)
	}
	
	// Now register the table
	// Note: This will fail because the file is empty, but that's OK for this test
	// We're just testing the registration requirement
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[0:len(substr)] == substr || contains(s[1:], substr)))
}