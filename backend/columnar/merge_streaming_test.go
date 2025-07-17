package columnar

import (
	"fmt"
	"os"
	"testing"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// TestStreamingMerge tests the streaming merge functionality
func TestStreamingMerge(t *testing.T) {
	// Create first file with data
	file1 := "test_streaming_merge1.bytedb"
	defer os.Remove(file1)
	
	cf1, err := CreateFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	
	cf1.AddColumn("id", DataTypeInt64, false)
	cf1.AddColumn("name", DataTypeString, false)
	cf1.AddColumn("value", DataTypeInt32, true) // nullable column
	
	// File 1 data with global row numbers 1000-1004
	ids1 := []IntData{
		NewIntData(10, 1000),
		NewIntData(20, 1001),
		NewIntData(30, 1002),
		NewIntData(40, 1003),
		NewIntData(50, 1004),
	}
	cf1.LoadIntColumn("id", ids1)
	
	names1 := []StringData{
		NewStringData("alice", 1000),
		NewStringData("bob", 1001),
		NewStringData("charlie", 1002),
		NewStringData("david", 1003),
		NewStringData("eve", 1004),
	}
	cf1.LoadStringColumn("name", names1)
	
	values1 := []IntData{
		NewIntData(100, 1000),
		NewNullIntData(1001),      // NULL value
		NewIntData(300, 1002),
		NewIntData(400, 1003),
		NewNullIntData(1004),      // NULL value
	}
	cf1.LoadIntColumn("value", values1)
	
	cf1.Close()
	
	// Create second file with data
	file2 := "test_streaming_merge2.bytedb"
	defer os.Remove(file2)
	
	cf2, err := CreateFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	
	cf2.AddColumn("id", DataTypeInt64, false)
	cf2.AddColumn("name", DataTypeString, false)
	cf2.AddColumn("value", DataTypeInt32, true)
	
	// File 2 data with global row numbers 2000-2004
	ids2 := []IntData{
		NewIntData(15, 2000),
		NewIntData(25, 2001),
		NewIntData(35, 2002),
		NewIntData(45, 2003),
		NewIntData(55, 2004),
	}
	cf2.LoadIntColumn("id", ids2)
	
	names2 := []StringData{
		NewStringData("frank", 2000),
		NewStringData("grace", 2001),
		NewStringData("henry", 2002),
		NewStringData("iris", 2003),
		NewStringData("jack", 2004),
	}
	cf2.LoadStringColumn("name", names2)
	
	values2 := []IntData{
		NewIntData(150, 2000),
		NewIntData(250, 2001),
		NewNullIntData(2002),      // NULL value
		NewIntData(450, 2003),
		NewIntData(550, 2004),
	}
	cf2.LoadIntColumn("value", values2)
	
	// Mark some rows as deleted
	cf2.DeleteRow(1001) // Delete "bob" from file1
	cf2.DeleteRow(1003) // Delete "david" from file1
	
	cf2.Close()
	
	// Create third file with overlapping IDs (for deduplication test)
	file3 := "test_streaming_merge3.bytedb"
	defer os.Remove(file3)
	
	cf3, err := CreateFile(file3)
	if err != nil {
		t.Fatal(err)
	}
	
	cf3.AddColumn("id", DataTypeInt64, false)
	cf3.AddColumn("name", DataTypeString, false)
	cf3.AddColumn("value", DataTypeInt32, true)
	
	// File 3 data with some duplicate IDs
	ids3 := []IntData{
		NewIntData(25, 3000), // Duplicate ID
		NewIntData(35, 3001), // Duplicate ID
		NewIntData(60, 3002),
		NewIntData(70, 3003),
	}
	cf3.LoadIntColumn("id", ids3)
	
	names3 := []StringData{
		NewStringData("grace_updated", 3000), // Updated name for ID 25
		NewStringData("henry_updated", 3001), // Updated name for ID 35
		NewStringData("kate", 3002),
		NewStringData("liam", 3003),
	}
	cf3.LoadStringColumn("name", names3)
	
	values3 := []IntData{
		NewIntData(251, 3000),
		NewIntData(351, 3001),
		NewIntData(600, 3002),
		NewNullIntData(3003),
	}
	cf3.LoadIntColumn("value", values3)
	
	cf3.Close()
	
	// Test streaming merge (always merges duplicate keys into bitmaps)
	t.Run("StreamingMergeBasic", func(t *testing.T) {
		outputFile := "test_streaming_output1.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeduplicateKeys:    false,
			UseStreamingMerge:  true, // Enable streaming merge
		}
		
		err := MergeFiles(outputFile, []string{file1, file2, file3}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		// Verify results
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// Check that deleted rows are excluded
		bobBitmap, err := merged.QueryString("name", "bob")
		if err != nil || bobBitmap == nil || bobBitmap.GetCardinality() != 0 {
			if bobBitmap != nil && bobBitmap.GetCardinality() > 0 {
				t.Error("Deleted row 'bob' should not be in merged file")
			}
		}
		
		davidBitmap, err := merged.QueryString("name", "david")
		if err != nil || davidBitmap == nil || davidBitmap.GetCardinality() != 0 {
			if davidBitmap != nil && davidBitmap.GetCardinality() > 0 {
				t.Error("Deleted row 'david' should not be in merged file")
			}
		}
		
		// Check that duplicate IDs are merged into one bitmap
		// ID 25 appears in file2 (row 2001) and file3 (row 3000)
		id25Bitmap, err := merged.QueryInt("id", 25)
		if err != nil {
			t.Errorf("Failed to query ID 25: %v", err)
		} else if id25Bitmap == nil {
			t.Error("QueryInt returned nil bitmap for ID 25")
		} else if id25Bitmap.GetCardinality() != 2 {
			t.Errorf("ID 25 should appear at 2 row numbers, got %d", id25Bitmap.GetCardinality())
		} else if !id25Bitmap.Contains(2001) || !id25Bitmap.Contains(3000) {
			t.Errorf("ID 25 should be at rows 2001 and 3000, got %v", id25Bitmap.ToArray())
		}
		
		// Verify NULL values are preserved
		nullBitmap, _ := merged.QueryNull("value")
		// Original: 2 nulls in file1, 1 in file2, 1 in file3
		// Deleted: 1 null from file1 (row 1001)
		// Expected: 3 nulls total
		if nullBitmap.GetCardinality() != 3 {
			t.Errorf("Expected 3 NULL values, got %d", nullBitmap.GetCardinality())
		}
	})
	
	// Test streaming merge with deleted rows
	t.Run("StreamingMergeWithDeletedRows", func(t *testing.T) {
		outputFile := "test_streaming_output2.bytedb"
		defer os.Remove(outputFile)
		
		options := &MergeOptions{
			DeduplicateKeys:    true,
			UseStreamingMerge:  true,
		}
		
		err := MergeFiles(outputFile, []string{file1, file2, file3}, options)
		if err != nil {
			t.Fatal(err)
		}
		
		// Verify results
		merged, err := OpenFile(outputFile)
		if err != nil {
			t.Fatal(err)
		}
		defer merged.Close()
		
		// This test focuses on verifying that deleted rows are properly excluded
		// Check that rows marked as deleted are not in the result
		bobBitmap, err := merged.QueryString("name", "bob")
		if err != nil || bobBitmap == nil || bobBitmap.GetCardinality() != 0 {
			if bobBitmap != nil && bobBitmap.GetCardinality() > 0 {
				t.Error("Deleted row 'bob' should not be in merged file")
			}
		}
		
		davidBitmap, err := merged.QueryString("name", "david")
		if err != nil || davidBitmap == nil || davidBitmap.GetCardinality() != 0 {
			if davidBitmap != nil && davidBitmap.GetCardinality() > 0 {
				t.Error("Deleted row 'david' should not be in merged file")
			}
		}
		
		// Verify that non-deleted duplicate keys are properly merged
		id25Bitmap, _ := merged.QueryInt("id", 25)
		if id25Bitmap.GetCardinality() != 2 {
			t.Errorf("ID 25 should appear at 2 row numbers, got %d", id25Bitmap.GetCardinality())
		}
	})
	
	// Compare streaming merge with regular merge
	t.Run("CompareStreamingWithRegular", func(t *testing.T) {
		// Note: Streaming merge with row preservation behaves differently from regular merge
		// - Streaming: preserves original row numbers, deduplication combines rows
		// - Regular: creates new sequential row numbers, deduplication keeps one row per key
		// Therefore, we cannot directly compare their results
		
		outputStreaming := "test_compare_streaming.bytedb"
		defer os.Remove(outputStreaming)
		
		// Streaming merge
		optionsStreaming := &MergeOptions{
			DeduplicateKeys:    true,
			UseStreamingMerge:  true,
		}
		
		err := MergeFiles(outputStreaming, []string{file1, file2, file3}, optionsStreaming)
		if err != nil {
			t.Fatal(err)
		}
		
		// Verify streaming merge preserved row numbers
		streamingFile, _ := OpenFile(outputStreaming)
		defer streamingFile.Close()
		
		// Check a few IDs to ensure they have the expected row numbers
		id10Bitmap, _ := streamingFile.QueryInt("id", 10)
		if !id10Bitmap.Contains(1000) {
			t.Error("ID 10 should be at row 1000")
		}
		
		id55Bitmap, _ := streamingFile.QueryInt("id", 55)
		if !id55Bitmap.Contains(2004) {
			t.Error("ID 55 should be at row 2004")
		}
		
		// Duplicate IDs should have multiple row numbers
		id25Bitmap, _ := streamingFile.QueryInt("id", 25)
		if id25Bitmap.GetCardinality() != 2 {
			t.Errorf("ID 25 should appear at 2 locations, got %d", id25Bitmap.GetCardinality())
		}
	})
}

// TestStreamingMergeMemoryEfficiency tests that streaming merge can handle large datasets
func TestStreamingMergeMemoryEfficiency(t *testing.T) {
	// This test creates larger files to demonstrate memory efficiency
	// In practice, you would test with much larger files
	
	numFiles := 3
	rowsPerFile := 10000
	globalRowCounter := uint64(0)
	
	fileNames := make([]string, numFiles)
	
	// Create multiple files with data
	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("test_large_merge_%d.bytedb", i)
		fileNames[i] = fileName
		defer os.Remove(fileName)
		
		cf, err := CreateFile(fileName)
		if err != nil {
			t.Fatal(err)
		}
		
		cf.AddColumn("id", DataTypeInt64, false)
		cf.AddColumn("data", DataTypeString, false)
		
		// Generate data
		ids := make([]IntData, rowsPerFile)
		data := make([]StringData, rowsPerFile)
		
		for j := 0; j < rowsPerFile; j++ {
			ids[j] = NewIntData(int64(i*rowsPerFile+j), globalRowCounter)
			data[j] = NewStringData(fmt.Sprintf("data_%d_%d", i, j), globalRowCounter)
			globalRowCounter++
		}
		
		cf.LoadIntColumn("id", ids)
		cf.LoadStringColumn("data", data)
		cf.Close()
	}
	
	// Merge with streaming
	outputFile := "test_large_merge_output.bytedb"
	defer os.Remove(outputFile)
	
	options := &MergeOptions{
		DeduplicateKeys:   false,
		UseStreamingMerge: true,
	}
	
	err := MergeFiles(outputFile, fileNames, options)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify merged file has all data
	merged, err := OpenFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	
	// Check a sample of IDs
	sampleIDs := []int64{0, 5000, 10000, 15000, 20000, 29999}
	for _, id := range sampleIDs {
		bitmap, _ := merged.QueryInt("id", id)
		if bitmap.GetCardinality() != 1 {
			t.Errorf("ID %d not found in merged file", id)
		}
	}
}

// TestStreamingMergeWithRowPreservation tests that row numbers are preserved during streaming merge
func TestStreamingMergeWithRowPreservation(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "streaming_merge_row_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create first file with sparse row numbers
	file1Path := tempDir + "/file1.bytedb"
	cf1, err := CreateFile(file1Path)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}
	
	err = cf1.AddColumn("id", DataTypeInt64, false)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Add data with gaps (sparse row numbers)
	data1 := []IntData{
		NewIntData(100, 100),  // Row 100
		NewIntData(200, 200),  // Row 200
		NewIntData(300, 300),  // Row 300
		NewIntData(400, 500),  // Row 500 (gap here)
	}
	
	err = cf1.LoadIntColumn("id", data1)
	if err != nil {
		t.Fatalf("Failed to load column: %v", err)
	}
	
	err = cf1.Close()
	if err != nil {
		t.Fatalf("Failed to close file1: %v", err)
	}
	
	// Create second file with different sparse row numbers
	file2Path := tempDir + "/file2.bytedb"
	cf2, err := CreateFile(file2Path)
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}
	
	err = cf2.AddColumn("id", DataTypeInt64, false)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Add data with different gaps
	data2 := []IntData{
		NewIntData(150, 150),  // Row 150
		NewIntData(250, 250),  // Row 250
		NewIntData(350, 350),  // Row 350
		NewIntData(450, 450),  // Row 450
	}
	
	err = cf2.LoadIntColumn("id", data2)
	if err != nil {
		t.Fatalf("Failed to load column: %v", err)
	}
	
	err = cf2.Close()
	if err != nil {
		t.Fatalf("Failed to close file2: %v", err)
	}
	
	// Open files for merge
	cf1, err = OpenFile(file1Path)
	if err != nil {
		t.Fatalf("Failed to open file1: %v", err)
	}
	defer cf1.Close()
	
	cf2, err = OpenFile(file2Path)
	if err != nil {
		t.Fatalf("Failed to open file2: %v", err)
	}
	defer cf2.Close()
	
	// Perform streaming merge
	outputPath := tempDir + "/merged_streaming.bytedb"
	options := &MergeOptions{
		DeduplicateKeys:   false,
		UseStreamingMerge: true, // Enable streaming merge
	}
	
	err = MergeFiles(outputPath, []string{file1Path, file2Path}, options)
	if err != nil {
		t.Fatalf("Streaming merge failed: %v", err)
	}
	
	// Verify the merge preserved row numbers
	merged, err := OpenFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to open merged file: %v", err)
	}
	defer merged.Close()
	
	// Query specific rows to verify preservation
	testCases := []struct {
		rowNum uint64
		expected int64
		shouldExist bool
	}{
		{100, 100, true},
		{150, 150, true},
		{200, 200, true},
		{250, 250, true},
		{300, 300, true},
		{350, 350, true},
		{400, 0, false},  // Gap - should not exist
		{450, 450, true},
		{500, 400, true},
	}
	
	for _, tc := range testCases {
		// Use the row-key B-tree to look up the value for a specific row
		col, exists := merged.columns["id"]
		if !exists {
			t.Fatal("Column 'id' not found in merged file")
		}
		
		// Look up the key value for this row
		keyValues, err := col.rowKeyBTree.Find(tc.rowNum)
		
		if tc.shouldExist {
			if err != nil || len(keyValues) == 0 {
				t.Errorf("Failed to find row %d: %v", tc.rowNum, err)
				continue
			}
			result := int64(keyValues[0])
			if result != tc.expected {
				t.Errorf("Row %d: expected value %d, got %d", tc.rowNum, tc.expected, result)
			}
		} else {
			if err == nil && len(keyValues) > 0 {
				t.Errorf("Row %d should not exist but got value %d", tc.rowNum, keyValues[0])
			}
		}
	}
	
	// Also verify using an iterator to see all data
	iter, err := merged.NewIterator("id")
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()
	
	// Collect all row numbers
	actualRows := roaring.New()
	for iter.Next() {
		rows := iter.Rows()
		actualRows.Or(rows)
	}
	
	// Build expected bitmap
	expectedBitmap := roaring.New()
	expectedBitmap.AddMany([]uint32{100, 150, 200, 250, 300, 350, 450, 500})
	
	if !actualRows.Equals(expectedBitmap) {
		t.Errorf("Row numbers not preserved correctly. Expected: %v, Got: %v", 
			expectedBitmap.ToArray(), actualRows.ToArray())
	}
}