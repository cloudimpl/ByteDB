package core

import (
	"os"
	"path/filepath"
	"testing"

	"bytedb/common"
	"github.com/parquet-go/parquet-go"
)

type TestEmployee struct {
	ID     int32  `parquet:"id"`
	Name   string `parquet:"name"`
	Salary int32  `parquet:"salary"`
}

func createTestFile(t *testing.T, path string, employees []TestEmployee) {
	dir := filepath.Dir(path)
	os.MkdirAll(dir, 0755)

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[TestEmployee](file)
	_, err = writer.Write(employees)
	if err != nil {
		t.Fatalf("Failed to write records: %v", err)
	}
	writer.Close()
}

func TestNewMultiFileParquetReader(t *testing.T) {
	testDir := "./test_multi_reader"
	defer os.RemoveAll(testDir)

	// Create test files
	file1 := filepath.Join(testDir, "emp1.parquet")
	file2 := filepath.Join(testDir, "emp2.parquet")
	file3 := filepath.Join(testDir, "emp3.parquet")

	employees1 := []TestEmployee{
		{ID: 1, Name: "Alice", Salary: 100000},
		{ID: 2, Name: "Bob", Salary: 80000},
	}
	employees2 := []TestEmployee{
		{ID: 3, Name: "Charlie", Salary: 90000},
		{ID: 4, Name: "David", Salary: 85000},
	}
	employees3 := []TestEmployee{
		{ID: 5, Name: "Eve", Salary: 95000},
	}

	createTestFile(t, file1, employees1)
	createTestFile(t, file2, employees2)
	createTestFile(t, file3, employees3)

	t.Run("Create multi-file reader", func(t *testing.T) {
		files := []string{file1, file2, file3}
		reader, err := NewMultiFileParquetReader(files)
		if err != nil {
			t.Fatalf("Failed to create multi-file reader: %v", err)
		}
		defer reader.Close()

		// Check that multiFileReader is set
		if reader.multiFileReader == nil {
			t.Errorf("Expected multiFileReader to be set")
		}

		// Check schema
		schema := reader.GetSchema()
		if len(schema) != 3 {
			t.Errorf("Expected 3 fields in schema, got %d", len(schema))
		}

		// Verify field names
		expectedFields := []string{"id", "name", "salary"}
		for i, field := range schema {
			if field.Name != expectedFields[i] {
				t.Errorf("Expected field %s, got %s", expectedFields[i], field.Name)
			}
		}
	})

	t.Run("Read all records", func(t *testing.T) {
		files := []string{file1, file2, file3}
		reader, err := NewMultiFileParquetReader(files)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		rows, err := reader.ReadAll()
		if err != nil {
			t.Fatalf("Failed to read all: %v", err)
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 rows total, got %d", len(rows))
		}

		// Verify we have all IDs
		ids := make(map[int32]bool)
		for _, row := range rows {
			id := row["id"].(int32)
			ids[id] = true
		}

		for i := int32(1); i <= 5; i++ {
			if !ids[i] {
				t.Errorf("Missing ID %d", i)
			}
		}
	})

	t.Run("Read specific columns", func(t *testing.T) {
		files := []string{file1, file2, file3}
		reader, err := NewMultiFileParquetReader(files)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		// Read only id and name columns
		rows, err := reader.ReadAllWithColumns([]string{"id", "name"})
		if err != nil {
			t.Fatalf("Failed to read columns: %v", err)
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}

		// Verify only requested columns are present
		for _, row := range rows {
			if _, exists := row["id"]; !exists {
				t.Errorf("Missing id column")
			}
			if _, exists := row["name"]; !exists {
				t.Errorf("Missing name column")
			}
			if _, exists := row["salary"]; exists {
				t.Errorf("Salary column should not be present")
			}
		}
	})

	t.Run("Get row count", func(t *testing.T) {
		files := []string{file1, file2, file3}
		reader, err := NewMultiFileParquetReader(files)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		count := reader.GetRowCount()
		if count != 5 {
			t.Errorf("Expected row count 5, got %d", count)
		}
	})

	t.Run("Empty file list", func(t *testing.T) {
		_, err := NewMultiFileParquetReader([]string{})
		if err == nil {
			t.Errorf("Expected error for empty file list")
		}
	})

	t.Run("Schema mismatch", func(t *testing.T) {
		// Create a file with different schema
		type DifferentRecord struct {
			ID    string `parquet:"id"` // Different type
			Name  string `parquet:"name"`
			Email string `parquet:"email"` // Different field
		}

		badFile := filepath.Join(testDir, "bad.parquet")
		file, _ := os.Create(badFile)
		writer := parquet.NewGenericWriter[DifferentRecord](file)
		writer.Write([]DifferentRecord{{ID: "6", Name: "Frank", Email: "frank@example.com"}})
		writer.Close()
		file.Close()

		// Try to create reader with mismatched schemas
		files := []string{file1, badFile}
		_, err := NewMultiFileParquetReader(files)
		if err == nil {
			t.Errorf("Expected error for schema mismatch")
		}
	})
}

func TestSchemaCompatibility(t *testing.T) {
	tests := []struct {
		name       string
		schema1    []common.Field
		schema2    []common.Field
		compatible bool
	}{
		{
			name: "Identical schemas",
			schema1: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			schema2: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			compatible: true,
		},
		{
			name: "Different number of fields",
			schema1: []common.Field{
				{Name: "id", Type: "int32", Required: true},
			},
			schema2: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			compatible: false,
		},
		{
			name: "Different field names",
			schema1: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			schema2: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "email", Type: "string", Required: false},
			},
			compatible: false,
		},
		{
			name: "Different field types",
			schema1: []common.Field{
				{Name: "id", Type: "int32", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			schema2: []common.Field{
				{Name: "id", Type: "string", Required: true},
				{Name: "name", Type: "string", Required: false},
			},
			compatible: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := schemasCompatible(test.schema1, test.schema2)
			if result != test.compatible {
				t.Errorf("Expected compatible=%v, got %v", test.compatible, result)
			}
		})
	}
}