package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bytedb/core"
	"github.com/parquet-go/parquet-go"
)

// TestMultiFileComplete tests the complete multi-file functionality in sequence
func TestMultiFileComplete(t *testing.T) {
	testDir := "./test_multi_file_complete"
	defer os.RemoveAll(testDir)

	// Define test data
	type Employee struct {
		ID         int32   `parquet:"id"`
		Name       string  `parquet:"name"`
		Department string  `parquet:"department"`
		Salary     float64 `parquet:"salary"`
	}

	// Helper to create parquet files
	createFile := func(path string, employees []Employee) error {
		os.MkdirAll(filepath.Dir(path), 0755)
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()

		writer := parquet.NewGenericWriter[Employee](file)
		_, err = writer.Write(employees)
		if err != nil {
			return err
		}
		return writer.Close()
	}

	// Create test files
	file1 := filepath.Join(testDir, "q1.parquet")
	file2 := filepath.Join(testDir, "q2.parquet")
	file3 := filepath.Join(testDir, "q3.parquet")

	createFile(file1, []Employee{
		{ID: 1, Name: "Alice", Department: "Eng", Salary: 100000},
		{ID: 2, Name: "Bob", Department: "Eng", Salary: 95000},
	})
	createFile(file2, []Employee{
		{ID: 3, Name: "Charlie", Department: "Sales", Salary: 80000},
		{ID: 4, Name: "David", Department: "Sales", Salary: 85000},
	})
	createFile(file3, []Employee{
		{ID: 5, Name: "Eve", Department: "HR", Salary: 70000},
		{ID: 6, Name: "Frank", Department: "HR", Salary: 75000},
	})

	// Initialize engine
	engine := core.NewQueryEngine(testDir)
	defer engine.Close()

	// Enable catalog
	config := map[string]interface{}{
		"base_path": filepath.Join(testDir, ".catalog"),
	}
	engine.EnableCatalog("file", config)

	manager := engine.GetCatalogManager()
	ctx := context.Background()

	// Use absolute paths
	absFile1, _ := filepath.Abs(file1)
	absFile2, _ := filepath.Abs(file2)
	absFile3, _ := filepath.Abs(file3)

	// Step 1: Register table with first file
	t.Log("Step 1: Registering table with first file")
	err := manager.RegisterTable(ctx, "emp", absFile1, "parquet")
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}

	// Verify initial count
	result, _ := engine.Execute("SELECT COUNT(*) as total FROM emp")
	count := getIntFromResult(result.Rows[0]["total"])
	if count != 2 {
		t.Errorf("Step 1: Expected 2 records, got %d", count)
	}

	// Step 2: Add second file
	t.Log("Step 2: Adding second file")
	err = manager.AddFileToTable(ctx, "emp", absFile2, true)
	if err != nil {
		t.Fatalf("Failed to add second file: %v", err)
	}
	engine.InvalidateTableReader("emp")
	
	// Debug: Check table metadata
	table, _ := manager.GetTableMetadata(ctx, "emp")
	t.Logf("Table locations after adding file2: %v", table.Locations)

	// Verify count after adding
	result, _ = engine.Execute("SELECT COUNT(*) as total FROM emp")
	count = getIntFromResult(result.Rows[0]["total"])
	if count != 4 {
		t.Errorf("Step 2: Expected 4 records, got %d", count)
	}

	// Step 3: Add third file
	t.Log("Step 3: Adding third file")
	err = manager.AddFileToTable(ctx, "emp", absFile3, true)
	if err != nil {
		t.Fatalf("Failed to add third file: %v", err)
	}
	engine.InvalidateTableReader("emp")

	// Verify total count
	result, _ = engine.Execute("SELECT COUNT(*) as total FROM emp")
	count = getIntFromResult(result.Rows[0]["total"])
	if count != 6 {
		t.Errorf("Step 3: Expected 6 records, got %d", count)
	}

	// Step 4: Test aggregation across files
	t.Log("Step 4: Testing aggregation")
	result, _ = engine.Execute("SELECT department, COUNT(*) as cnt FROM emp GROUP BY department ORDER BY department")
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 departments, got %d", len(result.Rows))
	}

	// Step 5: Remove middle file
	t.Log("Step 5: Removing middle file")
	err = manager.RemoveFileFromTable(ctx, "emp", absFile2)
	if err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}
	engine.InvalidateTableReader("emp")

	// Verify count after removal
	result, _ = engine.Execute("SELECT COUNT(*) as total FROM emp")
	count = getIntFromResult(result.Rows[0]["total"])
	if count != 4 {
		t.Errorf("Step 5: Expected 4 records after removal, got %d", count)
	}

	// Verify Sales department is gone
	result, _ = engine.Execute("SELECT COUNT(*) as cnt FROM emp WHERE department = 'Sales'")
	salesCount := getIntFromResult(result.Rows[0]["cnt"])
	if salesCount != 0 {
		t.Errorf("Expected 0 Sales employees after removal, got %d", salesCount)
	}

	// Step 6: Test table info
	t.Log("Step 6: Testing table info")
	info, err := engine.GetTableInfo("emp")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	
	if !strings.Contains(info, "Files: 2") {
		t.Errorf("Expected 'Files: 2' in table info")
	}
	if strings.Contains(info, "q2.parquet") {
		t.Errorf("Removed file should not appear in table info")
	}

	// Step 7: Test schema validation
	t.Log("Step 7: Testing schema validation")
	
	// Create incompatible file
	type BadEmployee struct {
		ID   string `parquet:"id"` // Wrong type
		Name string `parquet:"name"`
	}
	
	badFile := filepath.Join(testDir, "bad.parquet")
	file, _ := os.Create(badFile)
	writer := parquet.NewGenericWriter[BadEmployee](file)
	writer.Write([]BadEmployee{{ID: "X", Name: "Bad"}})
	writer.Close()
	file.Close()
	
	absBadFile, _ := filepath.Abs(badFile)
	err = manager.AddFileToTable(ctx, "emp", absBadFile, true)
	if err == nil {
		t.Errorf("Expected error when adding incompatible schema")
	}
	
	t.Log("All tests completed successfully!")
}

func getIntFromResult(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	default:
		return 0
	}
}