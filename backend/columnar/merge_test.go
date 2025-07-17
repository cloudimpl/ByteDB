package columnar

import (
	"os"
	"testing"
)

// TestBasicMerge tests basic file merging
func TestBasicMerge(t *testing.T) {
	// Create first file
	file1 := "test_merge1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.AddColumn("name", DataTypeString, false)
	
	// Using global row numbers starting at 0
	data1 := []IntData{
		NewIntData(100, 0),
		NewIntData(200, 1),
		NewIntData(300, 2),
	}
	cf1.LoadIntColumn("id", data1)
	
	names1 := []StringData{
		NewStringData("Alice", 0),
		NewStringData("Bob", 1),
		NewStringData("Charlie", 2),
	}
	cf1.LoadStringColumn("name", names1)
	cf1.Close()
	
	// Create second file
	file2 := "test_merge2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("id", DataTypeInt64, false)
	cf2.AddColumn("name", DataTypeString, false)
	
	// Using global row numbers starting at 10 (to avoid overlap)
	data2 := []IntData{
		NewIntData(400, 10),
		NewIntData(500, 11),
		NewIntData(600, 12),
	}
	cf2.LoadIntColumn("id", data2)
	
	names2 := []StringData{
		NewStringData("David", 10),
		NewStringData("Eve", 11),
		NewStringData("Frank", 12),
	}
	cf2.LoadStringColumn("name", names2)
	cf2.Close()
	
	// Merge files
	outputFile := "test_merge_output.bytedb"
	defer os.Remove(outputFile)
	
	err = MergeFiles(outputFile, []string{file1, file2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify merged file
	merged, err := OpenFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	
	// Check all IDs are present
	for _, id := range []int64{100, 200, 300, 400, 500, 600} {
		bitmap, err := merged.QueryInt("id", id)
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Errorf("ID %d not found in merged file", id)
		}
	}
	
	// Check all names are present
	for _, name := range []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"} {
		bitmap, err := merged.QueryString("name", name)
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Name %s not found in merged file", name)
		}
	}
}

// TestMergeWithDeletion tests merging files with deleted rows
func TestMergeWithDeletion(t *testing.T) {
	// Create first file with some deleted rows
	// Using global row numbers starting at 100
	file1 := "test_merge_del1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.AddColumn("value", DataTypeInt32, false)
	
	data1 := []IntData{
		NewIntData(100, 100),
		NewIntData(200, 101),
		NewIntData(300, 102),
		NewIntData(400, 103),
	}
	cf1.LoadIntColumn("id", data1)
	
	values1 := []IntData{
		NewIntData(10, 100),
		NewIntData(20, 101),
		NewIntData(30, 102),
		NewIntData(40, 103),
	}
	cf1.LoadIntColumn("value", values1)
	
	// Mark rows 101 and 102 as deleted
	cf1.DeleteRow(101)
	cf1.DeleteRow(102)
	cf1.Close()
	
	// Create second file
	// Using global row numbers starting at 200
	file2 := "test_merge_del2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("id", DataTypeInt64, false)
	cf2.AddColumn("value", DataTypeInt32, false)
	
	data2 := []IntData{
		NewIntData(500, 200),
		NewIntData(600, 201),
	}
	cf2.LoadIntColumn("id", data2)
	
	values2 := []IntData{
		NewIntData(50, 200),
		NewIntData(60, 201),
	}
	cf2.LoadIntColumn("value", values2)
	
	// Mark row 200 as deleted
	cf2.DeleteRow(200)
	cf2.Close()
	
	// Test ExcludeDeleted mode (default)
	t.Run("ExcludeDeleted", func(t *testing.T) {
		outputFile := "test_merge_exclude.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeletedRowHandling: ExcludeDeleted,
		}
		
		err = MergeFiles(outputFile, []string{file1, file2}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// Should only have non-deleted rows: 100, 400, 600
		expectedIDs := []int64{100, 400, 600}
		for _, id := range expectedIDs {
			bitmap, err := merged.QueryInt("id", id)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 1 {
				t.Errorf("ID %d should be in merged file", id)
			}
		}
		
		// Deleted rows should not be present
		deletedIDs := []int64{200, 300, 500}
		for _, id := range deletedIDs {
			bitmap, err := merged.QueryInt("id", id)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 0 {
				t.Errorf("Deleted ID %d should not be in merged file", id)
			}
		}
		
		// Merged file should have no deleted rows
		if merged.GetDeletedCount() != 0 {
			t.Error("Merged file should have no deleted rows when using ExcludeDeleted")
		}
	})
	
	// Test ConsolidateDeleted mode
	t.Run("ConsolidateDeleted", func(t *testing.T) {
		outputFile := "test_merge_consolidate.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeletedRowHandling: ConsolidateDeleted,
			DeduplicateKeys:    false, // Keep all rows to test consolidation
		}
		
		err = MergeFiles(outputFile, []string{file1, file2}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// All rows should be present
		allIDs := []int64{100, 200, 300, 400, 500, 600}
		for _, id := range allIDs {
			bitmap, err := merged.QueryInt("id", id)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 1 {
				t.Errorf("ID %d should be in merged file", id)
			}
		}
		
		// Check that deleted bitmap is consolidated
		// Note: Row numbers will be different in merged file
		// We need to check which rows correspond to deleted IDs
		deletedCount := merged.GetDeletedCount()
		if deletedCount == 0 {
			t.Error("Merged file should have consolidated deleted bitmap")
		}
	})
}

// TestMergeWithDuplicates tests merging with duplicate keys
func TestMergeWithDuplicates(t *testing.T) {
	// Create files with overlapping data
	file1 := "test_merge_dup1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("key", DataTypeInt64, false)
	cf1.AddColumn("value", DataTypeString, false)
	
	// Using global row numbers starting at 500
	keys1 := []IntData{
		NewIntData(100, 500),
		NewIntData(200, 501),
		NewIntData(300, 502),
	}
	cf1.LoadIntColumn("key", keys1)
	
	values1 := []StringData{
		NewStringData("value1_100", 500),
		NewStringData("value1_200", 501),
		NewStringData("value1_300", 502),
	}
	cf1.LoadStringColumn("value", values1)
	cf1.Close()
	
	// Second file with some duplicate keys
	file2 := "test_merge_dup2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("key", DataTypeInt64, false)
	cf2.AddColumn("value", DataTypeString, false)
	
	// Using global row numbers starting at 600
	keys2 := []IntData{
		NewIntData(200, 600), // Duplicate key (different row)
		NewIntData(300, 601), // Duplicate key (different row)
		NewIntData(400, 602), // New key
	}
	cf2.LoadIntColumn("key", keys2)
	
	values2 := []StringData{
		NewStringData("value2_200", 600),
		NewStringData("value2_300", 601),
		NewStringData("value2_400", 602),
	}
	cf2.LoadStringColumn("value", values2)
	cf2.Close()
	
	// Test with deduplication enabled
	t.Run("WithDeduplication", func(t *testing.T) {
		outputFile := "test_merge_dedup.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeduplicateKeys: true,
		}
		
		err = MergeFiles(outputFile, []string{file1, file2}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// Each key should appear only once
		for _, key := range []int64{100, 200, 300, 400} {
			bitmap, err := merged.QueryInt("key", key)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 1 {
				t.Errorf("Key %d should appear exactly once, got %d", key, bitmap.GetCardinality())
			}
		}
	})
	
	// Test without deduplication
	t.Run("WithoutDeduplication", func(t *testing.T) {
		outputFile := "test_merge_no_dedup.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeduplicateKeys: false,
		}
		
		err = MergeFiles(outputFile, []string{file1, file2}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// Duplicate keys should have multiple rows
		duplicateKeys := []int64{200, 300}
		for _, key := range duplicateKeys {
			bitmap, err := merged.QueryInt("key", key)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 2 {
				t.Errorf("Duplicate key %d should appear twice, got %d", key, bitmap.GetCardinality())
			}
		}
		
		// Non-duplicate keys should appear once
		uniqueKeys := []int64{100, 400}
		for _, key := range uniqueKeys {
			bitmap, err := merged.QueryInt("key", key)
			if err != nil {
				t.Fatal(err)
			}
			if bitmap.GetCardinality() != 1 {
				t.Errorf("Unique key %d should appear once, got %d", key, bitmap.GetCardinality())
			}
		}
	})
}

// TestMergeWithNullableColumns tests merging with nullable columns
func TestMergeWithNullableColumns(t *testing.T) {
	file1 := "test_merge_null1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.AddColumn("optional", DataTypeString, true)
	
	// Using global row numbers starting at 300
	ids1 := []IntData{
		NewIntData(100, 300),
		NewIntData(200, 301),
		NewIntData(300, 302),
	}
	cf1.LoadIntColumn("id", ids1)
	
	optional1 := []StringData{
		NewStringData("data1", 300),
		NewNullStringData(301),
		NewStringData("data3", 302),
	}
	cf1.LoadStringColumn("optional", optional1)
	cf1.Close()
	
	file2 := "test_merge_null2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("id", DataTypeInt64, false)
	cf2.AddColumn("optional", DataTypeString, true)
	
	// Using global row numbers starting at 400
	ids2 := []IntData{
		NewIntData(400, 400),
		NewIntData(500, 401),
	}
	cf2.LoadIntColumn("id", ids2)
	
	optional2 := []StringData{
		NewNullStringData(400),
		NewStringData("data5", 401),
	}
	cf2.LoadStringColumn("optional", optional2)
	
	// Delete the NULL row to test deletion + null handling
	cf2.DeleteRow(400)
	cf2.Close()
	
	// Merge with default options (exclude deleted)
	outputFile := "test_merge_null_output.bytedb"
	defer os.Remove(outputFile)
	
	err = MergeFiles(outputFile, []string{file1, file2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	
	merged, err := OpenFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	
	// Check NULL values are preserved
	nullBitmap, err := merged.QueryNull("optional")
	if err != nil {
		t.Fatal(err)
	}
	
	// Should have 1 NULL value (row 1 from file1, row 0 from file2 was deleted)
	if nullBitmap.GetCardinality() != 1 {
		t.Errorf("Expected 1 NULL value, got %d", nullBitmap.GetCardinality())
	}
	
	// Check non-NULL values
	notNullBitmap, err := merged.QueryNotNull("optional")
	if err != nil {
		t.Fatal(err)
	}
	
	// Should have 3 non-NULL values
	if notNullBitmap.GetCardinality() != 3 {
		t.Errorf("Expected 3 non-NULL values, got %d", notNullBitmap.GetCardinality())
	}
}

// TestMergeEmptyFiles tests edge cases with empty files
func TestMergeEmptyFiles(t *testing.T) {
	// Create empty file
	emptyFile := "test_merge_empty.bytedb"
	defer os.Remove(emptyFile)
	
	cf1, err := CreateFile(emptyFile)
	if err != nil {
		t.Fatal(err)
	}
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.Close()
	
	// Create file with data
	dataFile := "test_merge_data.bytedb"
	defer os.Remove(dataFile)
	
	cf2, err := CreateFile(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	cf2.AddColumn("id", DataTypeInt64, false)
	// Using global row number 700
	data := []IntData{NewIntData(100, 700)}
	cf2.LoadIntColumn("id", data)
	cf2.Close()
	
	// Merge empty + data
	outputFile := "test_merge_empty_output.bytedb"
	defer os.Remove(outputFile)
	
	err = MergeFiles(outputFile, []string{emptyFile, dataFile}, nil)
	if err != nil {
		t.Fatal(err)
	}
	
	merged, err := OpenFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	
	// Should have the data from non-empty file
	bitmap, err := merged.QueryInt("id", 100)
	if err != nil {
		t.Fatal(err)
	}
	if bitmap.GetCardinality() != 1 {
		t.Error("Merged file should contain data from non-empty file")
	}
}

// TestMergeErrors tests error conditions
func TestMergeErrors(t *testing.T) {
	// Test no input files
	err := MergeFiles("output.bytedb", []string{}, nil)
	if err == nil {
		t.Error("MergeFiles should error with no input files")
	}
	
	// Test mismatched column types
	file1 := "test_merge_mismatch1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	cf1.AddColumn("data", DataTypeInt64, false)
	cf1.Close()
	
	file2 := "test_merge_mismatch2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	cf2.AddColumn("data", DataTypeString, false) // Different type
	cf2.Close()
	
	outputFile := "test_merge_mismatch_output.bytedb"
	defer os.Remove(outputFile)
	
	err = MergeFiles(outputFile, []string{file1, file2}, nil)
	if err == nil {
		t.Error("MergeFiles should error with mismatched column types")
	}
}

// TestMergeWithGlobalRowNumbers tests merging with globally unique row numbers
func TestMergeWithGlobalRowNumbers(t *testing.T) {
	// Simulate a scenario where row numbers are globally unique across files
	// File 1: rows 1000-1999
	// File 2: rows 2000-2999 with deleted bitmap referencing rows from file 1
	// File 3: rows 3000-3999 with deleted bitmap referencing rows from files 1 and 2
	
	// Create first file with global row numbers 1000-1004
	file1 := "test_merge_global1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.AddColumn("data", DataTypeString, false)
	
	// Using global row numbers starting at 1000
	ids1 := []IntData{
		NewIntData(100, 1000),
		NewIntData(101, 1001),
		NewIntData(102, 1002),
		NewIntData(103, 1003),
		NewIntData(104, 1004),
	}
	cf1.LoadIntColumn("id", ids1)
	
	data1 := []StringData{
		NewStringData("data1000", 1000),
		NewStringData("data1001", 1001),
		NewStringData("data1002", 1002),
		NewStringData("data1003", 1003),
		NewStringData("data1004", 1004),
	}
	cf1.LoadStringColumn("data", data1)
	cf1.Close()
	
	// Create second file with global row numbers 2000-2003
	// This file marks rows 1001 and 1003 (from file1) as deleted
	file2 := "test_merge_global2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("id", DataTypeInt64, false)
	cf2.AddColumn("data", DataTypeString, false)
	
	ids2 := []IntData{
		NewIntData(200, 2000),
		NewIntData(201, 2001),
		NewIntData(202, 2002),
		NewIntData(203, 2003),
	}
	cf2.LoadIntColumn("id", ids2)
	
	data2 := []StringData{
		NewStringData("data2000", 2000),
		NewStringData("data2001", 2001),
		NewStringData("data2002", 2002),
		NewStringData("data2003", 2003),
	}
	cf2.LoadStringColumn("data", data2)
	
	// Mark rows from file1 as deleted
	cf2.DeleteRow(1001) // Row from file1
	cf2.DeleteRow(1003) // Row from file1
	cf2.Close()
	
	// Create third file with global row numbers 3000-3002
	// This file marks rows from both previous files as deleted
	file3 := "test_merge_global3.bytedb"
	defer os.Remove(file3)
	
	cf3, err := CreateFile(file3)
	if err != nil {
		t.Fatal(err)
	}
	
	cf3.AddColumn("id", DataTypeInt64, false)
	cf3.AddColumn("data", DataTypeString, false)
	
	ids3 := []IntData{
		NewIntData(300, 3000),
		NewIntData(301, 3001),
		NewIntData(302, 3002),
	}
	cf3.LoadIntColumn("id", ids3)
	
	data3 := []StringData{
		NewStringData("data3000", 3000),
		NewStringData("data3001", 3001),
		NewStringData("data3002", 3002),
	}
	cf3.LoadStringColumn("data", data3)
	
	// Mark rows from previous files as deleted
	cf3.DeleteRow(1002) // Row from file1
	cf3.DeleteRow(2001) // Row from file2
	cf3.DeleteRow(2003) // Row from file2
	cf3.Close()
	
	// Merge all files with ExcludeDeleted
	outputFile := "test_merge_global_output.bytedb"
	defer os.Remove(outputFile)
	
	options := &MergeOptions{
		DeletedRowHandling: ExcludeDeleted,
		DeduplicateKeys:    false, // Keep original row numbers
	}
	
	err = MergeFiles(outputFile, []string{file1, file2, file3}, options)
	if err != nil {
		t.Fatal(err)
	}
	
	merged, err := OpenFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	
	// Expected rows after merge (excluding deleted):
	// From file1: 1000, 1004 (1001, 1002, 1003 deleted)
	// From file2: 2000, 2002 (2001, 2003 deleted)
	// From file3: 3000, 3001, 3002 (none deleted)
	
	// Check expected IDs are present
	expectedIDs := []int64{100, 104, 200, 202, 300, 301, 302}
	for _, id := range expectedIDs {
		bitmap, err := merged.QueryInt("id", id)
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Errorf("ID %d should be in merged file", id)
		}
	}
	
	// Check deleted IDs are not present
	deletedIDs := []int64{101, 102, 103, 201, 203}
	for _, id := range deletedIDs {
		bitmap, err := merged.QueryInt("id", id)
		if err != nil {
			t.Fatal(err)
		}
		if bitmap.GetCardinality() != 0 {
			t.Errorf("Deleted ID %d should not be in merged file", id)
		}
	}
	
	// Verify the merged file has no deleted rows
	if merged.GetDeletedCount() != 0 {
		t.Error("Merged file should have no deleted rows when using ExcludeDeleted")
	}
}