package columnar

import (
	"os"
	"testing"
)

func TestReadOnlyMode(t *testing.T) {
	filename := "test_readonly.bytedb"
	defer os.Remove(filename)
	
	// Step 1: Create a file with some data
	t.Run("CreateFile", func(t *testing.T) {
		cf, err := CreateFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		
		// Add columns
		cf.AddColumn("id", DataTypeInt64, false)
		cf.AddColumn("name", DataTypeString, false)
		
		// Load data
		intData := []IntData{
			{Value: 1, RowNum: 0},
			{Value: 2, RowNum: 1},
			{Value: 3, RowNum: 2},
		}
		stringData := []StringData{
			{Value: "Alice", RowNum: 0},
			{Value: "Bob", RowNum: 1},
			{Value: "Charlie", RowNum: 2},
		}
		
		if err := cf.LoadIntColumn("id", intData); err != nil {
			t.Fatal(err)
		}
		if err := cf.LoadStringColumn("name", stringData); err != nil {
			t.Fatal(err)
		}
		
		if err := cf.Close(); err != nil {
			t.Fatal(err)
		}
	})
	
	// Step 2: Open in read-only mode and verify queries work
	t.Run("ReadOnlyQueries", func(t *testing.T) {
		cf, err := OpenFileReadOnly(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf.Close()
		
		// Verify read-only flag is set
		if !cf.readOnly {
			t.Error("Expected readOnly flag to be true")
		}
		
		// Test queries work
		bitmap, err := cf.QueryInt("id", 2)
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Error("Query failed")
		}
		
		// Test row lookups work
		val, found, err := cf.LookupIntByRow("id", 1)
		if err != nil || !found || val != 2 {
			t.Error("Row lookup failed")
		}
		
		// Test string queries
		bitmap, err = cf.QueryString("name", "Bob")
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Error("String query failed")
		}
	})
	
	// Step 3: Verify write operations fail in read-only mode
	t.Run("ReadOnlyWriteProtection", func(t *testing.T) {
		cf, err := OpenFileReadOnly(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf.Close()
		
		// Try to add column - should fail
		err = cf.AddColumn("new_col", DataTypeInt64, false)
		if err == nil {
			t.Error("Expected error when adding column in read-only mode")
		}
		if err.Error() != "cannot perform write operation: file is opened in read-only mode" {
			t.Errorf("Unexpected error message: %v", err)
		}
		
		// Try to load data - should fail
		err = cf.LoadIntColumn("id", []IntData{{Value: 4, RowNum: 3}})
		if err == nil {
			t.Error("Expected error when loading data in read-only mode")
		}
		if err.Error() != "cannot perform write operation: file is opened in read-only mode" {
			t.Errorf("Unexpected error message: %v", err)
		}
	})
	
	// Step 4: Verify read-write mode still works
	t.Run("ReadWriteMode", func(t *testing.T) {
		cf, err := OpenFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf.Close()
		
		// Verify read-only flag is false
		if cf.readOnly {
			t.Error("Expected readOnly flag to be false")
		}
		
		// Should be able to add new column
		err = cf.AddColumn("score", DataTypeInt64, false)
		if err != nil {
			t.Errorf("Failed to add column in read-write mode: %v", err)
		}
		
		// Should be able to load data
		err = cf.LoadIntColumn("score", []IntData{
			{Value: 100, RowNum: 0},
			{Value: 200, RowNum: 1},
			{Value: 300, RowNum: 2},
		})
		if err != nil {
			t.Errorf("Failed to load data in read-write mode: %v", err)
		}
	})
}

func TestReadOnlyPerformance(t *testing.T) {
	filename := "test_readonly_perf.bytedb"
	defer os.Remove(filename)
	
	// Create a file with substantial data
	numRows := 10000
	
	// Create file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	
	// Generate data
	intData := make([]IntData, numRows)
	stringData := make([]StringData, numRows)
	categories := []string{"A", "B", "C", "D", "E"}
	
	for i := 0; i < numRows; i++ {
		intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		stringData[i] = StringData{Value: categories[i%5], RowNum: uint64(i)}
	}
	
	cf.LoadIntColumn("id", intData)
	cf.LoadStringColumn("category", stringData)
	cf.Close()
	
	// Compare file opening times
	t.Run("OpeningPerformance", func(t *testing.T) {
		// Test read-only open
		cf1, err := OpenFileReadOnly(filename)
		if err != nil {
			t.Fatal(err)
		}
		cf1.Close()
		
		// Test read-write open
		cf2, err := OpenFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		cf2.Close()
		
		t.Log("Both read-only and read-write modes opened successfully")
		t.Log("Read-only mode prevents accidental modifications during query operations")
	})
}