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

// TestMultiFileTableOperations tests the complete multi-file table functionality
func TestMultiFileTableOperations(t *testing.T) {
	testDir := "./test_multi_file_ops"
	defer os.RemoveAll(testDir)

	// Define test data structure
	type Employee struct {
		ID         int32   `parquet:"id"`
		Name       string  `parquet:"name"`
		Department string  `parquet:"department"`
		Salary     float64 `parquet:"salary"`
	}

	// Helper to create parquet files
	createParquetFile := func(path string, employees []Employee) error {
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

	// Create test data files
	file1 := filepath.Join(testDir, "emp_2023_q1.parquet")
	file2 := filepath.Join(testDir, "emp_2023_q2.parquet")
	file3 := filepath.Join(testDir, "emp_2023_q3.parquet")

	// Q1 data
	if err := createParquetFile(file1, []Employee{
		{ID: 1, Name: "Alice", Department: "Engineering", Salary: 100000},
		{ID: 2, Name: "Bob", Department: "Engineering", Salary: 95000},
		{ID: 3, Name: "Charlie", Department: "Sales", Salary: 80000},
	}); err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	// Q2 data
	if err := createParquetFile(file2, []Employee{
		{ID: 4, Name: "David", Department: "Engineering", Salary: 105000},
		{ID: 5, Name: "Eve", Department: "HR", Salary: 70000},
		{ID: 6, Name: "Frank", Department: "Sales", Salary: 85000},
	}); err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Q3 data
	if err := createParquetFile(file3, []Employee{
		{ID: 7, Name: "Grace", Department: "HR", Salary: 75000},
		{ID: 8, Name: "Henry", Department: "Engineering", Salary: 110000},
		{ID: 9, Name: "Iris", Department: "Sales", Salary: 90000},
	}); err != nil {
		t.Fatalf("Failed to create file3: %v", err)
	}

	// Initialize query engine
	engine := core.NewQueryEngine(testDir)
	defer engine.Close()

	// Enable catalog system
	config := map[string]interface{}{
		"base_path": filepath.Join(testDir, ".catalog"),
	}
	if err := engine.EnableCatalog("file", config); err != nil {
		t.Fatalf("Failed to enable catalog: %v", err)
	}

	manager := engine.GetCatalogManager()
	ctx := context.Background()

	// Use absolute paths
	absFile1, _ := filepath.Abs(file1)
	absFile2, _ := filepath.Abs(file2)
	absFile3, _ := filepath.Abs(file3)

	t.Run("RegisterAndAddFiles", func(t *testing.T) {
		// Register table with first file
		if err := manager.RegisterTable(ctx, "employees_2023", absFile1, "parquet"); err != nil {
			t.Fatalf("Failed to register table: %v", err)
		}

		// Verify initial registration
		table, err := manager.GetTableMetadata(ctx, "employees_2023")
		if err != nil {
			t.Fatalf("Failed to get table metadata: %v", err)
		}
		if len(table.Locations) != 1 {
			t.Errorf("Expected 1 location, got %d", len(table.Locations))
		}

		// Add second file
		if err := manager.AddFileToTable(ctx, "employees_2023", absFile2, true); err != nil {
			t.Fatalf("Failed to add second file: %v", err)
		}
		
		// Invalidate cached reader after adding file
		engine.InvalidateTableReader("employees_2023")

		// Add third file
		if err := manager.AddFileToTable(ctx, "employees_2023", absFile3, true); err != nil {
			t.Fatalf("Failed to add third file: %v", err)
		}
		
		// Invalidate again
		engine.InvalidateTableReader("employees_2023")

		// Verify all files added
		table, err = manager.GetTableMetadata(ctx, "employees_2023")
		if err != nil {
			t.Fatalf("Failed to get table metadata: %v", err)
		}
		if len(table.Locations) != 3 {
			t.Errorf("Expected 3 locations, got %d", len(table.Locations))
		}
	})

	t.Run("QueryMultiFileTable", func(t *testing.T) {
		// Test COUNT across all files
		result, err := engine.Execute("SELECT COUNT(*) as total FROM employees_2023")
		if err != nil {
			t.Fatalf("Failed to execute COUNT: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		total := getCount(result.Rows[0]["total"])
		if total != 9 {
			t.Errorf("Expected 9 total employees, got %d", total)
		}

		// Test aggregation by department
		result, err = engine.Execute(`
			SELECT department, 
			       COUNT(*) as emp_count,
			       AVG(salary) as avg_salary,
			       MIN(salary) as min_salary,
			       MAX(salary) as max_salary
			FROM employees_2023 
			GROUP BY department 
			ORDER BY department
		`)
		if err != nil || result.Error != "" {
			t.Fatalf("Failed aggregation query: %v, %s", err, result.Error)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 departments, got %d", len(result.Rows))
		}

		// Verify Engineering department (4 employees across 3 files)
		for _, row := range result.Rows {
			if row["department"] == "Engineering" {
				count := getCount(row["emp_count"])
				if count != 4 {
					t.Errorf("Expected 4 Engineering employees, got %d", count)
				}
				
				maxSalary := row["max_salary"].(float64)
				if maxSalary != 110000 {
					t.Errorf("Expected max Engineering salary 110000, got %f", maxSalary)
				}
			}
		}

		// Test filtering across files
		result, err = engine.Execute("SELECT name FROM employees_2023 WHERE salary > 100000 ORDER BY id")
		if err != nil || result.Error != "" {
			t.Fatalf("Failed filter query: %v, %s", err, result.Error)
		}
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 high earners, got %d", len(result.Rows))
		}
	})

	t.Run("TableInfoDisplay", func(t *testing.T) {
		info, err := engine.GetTableInfo("employees_2023")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		// Verify info contains file count and paths
		if !strings.Contains(info, "Files: 3") {
			t.Errorf("Expected 'Files: 3' in table info")
		}
		if !strings.Contains(info, "emp_2023_q1.parquet") {
			t.Errorf("Expected Q1 file in table info")
		}
		if !strings.Contains(info, "Row Count: 9") {
			t.Errorf("Expected 'Row Count: 9' in table info")
		}
	})

	t.Run("RemoveFile", func(t *testing.T) {
		// Remove Q2 file
		if err := manager.RemoveFileFromTable(ctx, "employees_2023", absFile2); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
		
		// Invalidate the cached reader
		engine.InvalidateTableReader("employees_2023")

		// Verify file removed
		table, err := manager.GetTableMetadata(ctx, "employees_2023")
		if err != nil {
			t.Fatalf("Failed to get table metadata: %v", err)
		}
		if len(table.Locations) != 2 {
			t.Errorf("Expected 2 locations after removal, got %d", len(table.Locations))
		}

		// Debug: Check what files the table has
		paths, err := manager.ResolveTablePaths(ctx, "employees_2023", testDir)
		if err != nil {
			t.Fatalf("Failed to resolve table paths: %v", err)
		}
		t.Logf("Table paths after removal: %v", paths)
		
		// Query should reflect removed data
		result, err := engine.Execute("SELECT COUNT(*) as total FROM employees_2023")
		if err != nil || result.Error != "" {
			t.Fatalf("Failed query after removal: %v, %s", err, result.Error)
		}
		
		total := getCount(result.Rows[0]["total"])
		if total != 6 {
			t.Errorf("Expected 6 employees after removal, got %d", total)
		}

		// Verify specific IDs from Q2 are gone
		result, err = engine.Execute("SELECT id FROM employees_2023 WHERE id IN (4, 5, 6)")
		if err != nil || result.Error != "" {
			t.Fatalf("Failed to check removed IDs: %v, %s", err, result.Error)
		}
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows for removed Q2 data, got %d", len(result.Rows))
		}
	})

	t.Run("SchemaValidation", func(t *testing.T) {
		// Create a file with incompatible schema
		type BadEmployee struct {
			ID     string  `parquet:"id"`      // Wrong type
			Name   string  `parquet:"name"`
			Salary float64 `parquet:"salary"`
			// Missing department field
		}

		badFile := filepath.Join(testDir, "bad_schema.parquet")
		file, _ := os.Create(badFile)
		writer := parquet.NewGenericWriter[BadEmployee](file)
		writer.Write([]BadEmployee{{ID: "X", Name: "Bad", Salary: 50000}})
		writer.Close()
		file.Close()

		absBadFile, _ := filepath.Abs(badFile)
		
		// Try to add with validation - should fail
		err := manager.AddFileToTable(ctx, "employees_2023", absBadFile, true)
		if err == nil {
			t.Errorf("Expected error when adding incompatible schema")
		}
		if err != nil && !strings.Contains(err.Error(), "schema") {
			t.Errorf("Expected schema error, got: %v", err)
		}

		// Verify table still has 2 files
		table, _ := manager.GetTableMetadata(ctx, "employees_2023")
		if len(table.Locations) != 2 {
			t.Errorf("Table should still have 2 files after failed add")
		}
	})
}

// Helper function to handle int/float64 conversion
func getCount(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	default:
		return 0
	}
}