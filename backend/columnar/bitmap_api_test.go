package columnar

import (
	"os"
	"testing"
	"time"
)

// TestBitmapAPI tests the new bitmap-based query API
func TestBitmapAPI(t *testing.T) {
	tmpFile := "test_bitmap_api.bytedb"
	defer os.Remove(tmpFile)
	
	// Create test data
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add columns
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	cf.AddColumn("value", DataTypeInt32, false)
	
	// Load test data
	idData := make([]struct{ Key int64; RowNum uint64 }, 1000)
	categoryData := make([]struct{ Key string; RowNum uint64 }, 1000)
	valueData := make([]struct{ Key uint64; RowNum uint64 }, 1000)
	
	for i := 0; i < 1000; i++ {
		idData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),
			RowNum: uint64(i),
		}
		// 10 categories with many duplicates
		categoryData[i] = struct{ Key string; RowNum uint64 }{
			Key:    categoryNames[i%10],
			RowNum: uint64(i),
		}
		// Values 0-99 repeating
		valueData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % 100),
			RowNum: uint64(i),
		}
	}
	
	cf.LoadIntColumn("id", idData)
	cf.LoadStringColumn("category", categoryData)
	
	col := cf.columns["value"]
	stats, err := col.btree.BulkLoadWithDuplicates(valueData)
	if err != nil {
		t.Fatalf("Failed to bulk load: %v", err)
	}
	col.metadata.RootPageID = col.btree.GetRootPageID()
	
	// Update metadata with statistics
	col.metadata.TotalKeys = stats.TotalKeys
	col.metadata.DistinctCount = stats.DistinctCount
	col.metadata.MinValueOffset = stats.MinValue
	col.metadata.MaxValueOffset = stats.MaxValue
	col.metadata.AverageKeySize = stats.AverageKeySize
	
	cf.Close()
	
	// Reopen for queries
	cf, err = OpenFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf.Close()
	
	t.Run("BasicBitmapQueries", func(t *testing.T) {
		// Test equality query
		bitmap, err := cf.QueryInt("id", 500)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Expected 1 result, got %d", bitmap.GetCardinality())
		}
		// Validate actual data: should contain row 500
		rows := BitmapToSlice(bitmap)
		if len(rows) != 1 || rows[0] != 500 {
			t.Errorf("Expected row [500], got %v", rows)
		}
		
		// Test string bitmap query
		bitmap, err = cf.QueryString("category", "cat_2")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		// Should return 100 rows (indices 2, 12, 22, ..., 992)
		if bitmap.GetCardinality() != 100 {
			t.Errorf("Expected 100 results, got %d", bitmap.GetCardinality())
		}
		// Validate actual data: should contain rows 2, 12, 22, ..., 992
		rows = BitmapToSlice(bitmap)
		expectedRows := make([]uint64, 100)
		for i := 0; i < 100; i++ {
			expectedRows[i] = uint64(2 + i*10)
		}
		if len(rows) != len(expectedRows) {
			t.Errorf("Expected %d rows, got %d", len(expectedRows), len(rows))
		} else {
			// Check first few and last few rows
			for i := 0; i < 5 && i < len(rows); i++ {
				if rows[i] != expectedRows[i] {
					t.Errorf("Row %d: expected %d, got %d", i, expectedRows[i], rows[i])
				}
			}
			for i := len(rows) - 5; i < len(rows) && i >= 0; i++ {
				if rows[i] != expectedRows[i] {
					t.Errorf("Row %d: expected %d, got %d", i, expectedRows[i], rows[i])
				}
			}
		}
		
		// Test range query
		bitmap, err = cf.RangeQueryInt("id", 100, 200)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		if bitmap.GetCardinality() != 101 { // 100-200 inclusive
			t.Errorf("Expected 101 results, got %d", bitmap.GetCardinality())
		}
		// Validate actual data: should contain rows 100-200
		rows = BitmapToSlice(bitmap)
		if len(rows) != 101 {
			t.Errorf("Expected 101 rows, got %d", len(rows))
		} else {
			// Check first, middle, and last values
			if rows[0] != 100 {
				t.Errorf("First row: expected 100, got %d", rows[0])
			}
			if rows[50] != 150 {
				t.Errorf("Middle row: expected 150, got %d", rows[50])
			}
			if rows[100] != 200 {
				t.Errorf("Last row: expected 200, got %d", rows[100])
			}
		}
	})
	
	t.Run("BitmapComparisonQueries", func(t *testing.T) {
		// Test greater than
		bitmap, err := cf.QueryGreaterThan("value", int32(90))
		if err != nil {
			t.Errorf("QueryGreaterThan failed: %v", err)
		}
		// Values > 90: 91-99, repeated 10 times = 90 results
		if bitmap.GetCardinality() != 90 {
			t.Errorf("Expected 90 results for > 90, got %d", bitmap.GetCardinality())
		}
		// Validate actual data: should contain rows where i%100 > 90
		rows := BitmapToSlice(bitmap)
		for _, row := range rows {
			if row%100 <= 90 {
				t.Errorf("Row %d has value %d which should not be > 90", row, row%100)
			}
		}
		// Check some specific expected rows
		expectedRows := make(map[uint64]bool)
		for i := 0; i < 1000; i++ {
			if uint64(i)%100 > 90 {
				expectedRows[uint64(i)] = true
			}
		}
		if len(rows) != len(expectedRows) {
			t.Errorf("Expected %d rows, got %d", len(expectedRows), len(rows))
		}
		
		// Test less than or equal
		bitmap, err = cf.QueryLessThanOrEqual("value", int32(10))
		if err != nil {
			t.Errorf("QueryLessThanOrEqual failed: %v", err)
		}
		// Values <= 10: 0-10, repeated 10 times = 110 results
		if bitmap.GetCardinality() != 110 {
			t.Errorf("Expected 110 results for <= 10, got %d", bitmap.GetCardinality())
		}
		// Validate actual data: should contain rows where i%100 <= 10
		rows = BitmapToSlice(bitmap)
		for _, row := range rows {
			if row%100 > 10 {
				t.Errorf("Row %d has value %d which should be <= 10", row, row%100)
			}
		}
		// Verify we have the right number of each value
		valueCounts := make(map[uint64]int)
		for _, row := range rows {
			valueCounts[row%100]++
		}
		for val := uint64(0); val <= 10; val++ {
			if valueCounts[val] != 10 {
				t.Errorf("Value %d should appear 10 times, got %d", val, valueCounts[val])
			}
		}
	})
	
	t.Run("BitmapLogicalOperations", func(t *testing.T) {
		// Get bitmaps for complex query
		// Find: category = "cat_1" AND value > 50
		catBitmap, _ := cf.QueryString("category", "cat_1")
		valueBitmap, _ := cf.QueryGreaterThan("value", int32(50))
		
		// Validate cat_1 bitmap first
		catRows := BitmapToSlice(catBitmap)
		for _, row := range catRows {
			if row%10 != 1 {
				t.Errorf("Row %d should have category cat_1 (row %% 10 should be 1)", row)
			}
		}
		
		// Perform AND operation directly on bitmaps
		result := cf.QueryAnd(catBitmap, valueBitmap)
		
		// cat_1: indices 1, 11, 21, ..., 991 (100 total)
		// value > 50: indices where i%100 > 50 (490 total)
		// Intersection should give indices like 51, 61, 71, 81, 91, 151, 161, ...
		// For each group of 100, cat_1 appears at index 1, 11, 21, 31, 41, 51, 61, 71, 81, 91
		// Of these, only 51, 61, 71, 81, 91 have value > 50
		// This pattern repeats 10 times, so 5 * 10 = 50
		expectedCount := 50
		if result.GetCardinality() != uint64(expectedCount) {
			t.Errorf("Expected %d results for AND operation, got %d", expectedCount, result.GetCardinality())
		}
		
		// Validate actual AND result data
		andRows := BitmapToSlice(result)
		for _, row := range andRows {
			// Must satisfy both conditions
			if row%10 != 1 {
				t.Errorf("AND result row %d should have category cat_1 (row %% 10 = 1)", row)
			}
			if row%100 <= 50 {
				t.Errorf("AND result row %d should have value > 50 (value = %d)", row, row%100)
			}
		}
		
		// Test OR operation
		result = cf.QueryOr(catBitmap, valueBitmap)
		// This should give all unique rows from both sets
		// We can verify it's more than either individual set
		if result.GetCardinality() <= catBitmap.GetCardinality() || result.GetCardinality() <= valueBitmap.GetCardinality() {
			t.Errorf("OR result should be larger than individual sets")
		}
		
		// Validate OR result data - every row should satisfy at least one condition
		orRows := BitmapToSlice(result)
		for _, row := range orRows {
			hasCat1 := (row%10 == 1)
			hasValueGT50 := (row%100 > 50)
			if !hasCat1 && !hasValueGT50 {
				t.Errorf("OR result row %d should satisfy at least one condition: cat_1=%v, value>50=%v", row, hasCat1, hasValueGT50)
			}
		}
		
		// Verify OR result includes all AND result rows
		andRowsMap := make(map[uint64]bool)
		for _, row := range andRows {
			andRowsMap[row] = true
		}
		for _, row := range andRows {
			found := false
			for _, orRow := range orRows {
				if row == orRow {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("OR result should include AND result row %d", row)
			}
		}
	})
}

// TestBitmapAPIPerformance compares performance of bitmap vs slice operations
func TestBitmapAPIPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
	
	tmpFile := "test_bitmap_perf.bytedb"
	defer os.Remove(tmpFile)
	
	// Create larger dataset
	cf, _ := CreateFile(tmpFile)
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	cf.AddColumn("status", DataTypeInt32, false)
	
	// Load 100k rows
	numRows := 100000
	idData := make([]struct{ Key int64; RowNum uint64 }, numRows)
	categoryData := make([]struct{ Key string; RowNum uint64 }, numRows)
	statusData := make([]struct{ Key uint64; RowNum uint64 }, numRows)
	
	for i := 0; i < numRows; i++ {
		idData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),
			RowNum: uint64(i),
		}
		categoryData[i] = struct{ Key string; RowNum uint64 }{
			Key:    categoryNames[i%10],
			RowNum: uint64(i),
		}
		statusData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % 5), // 5 different statuses
			RowNum: uint64(i),
		}
	}
	
	cf.LoadIntColumn("id", idData)
	cf.LoadStringColumn("category", categoryData)
	
	col := cf.columns["status"]
	stats, err := col.btree.BulkLoadWithDuplicates(statusData)
	if err != nil {
		t.Fatalf("Failed to bulk load: %v", err)
	}
	col.metadata.RootPageID = col.btree.GetRootPageID()
	
	// Update metadata with statistics
	col.metadata.TotalKeys = stats.TotalKeys
	col.metadata.DistinctCount = stats.DistinctCount
	col.metadata.MinValueOffset = stats.MinValue
	col.metadata.MaxValueOffset = stats.MaxValue
	col.metadata.AverageKeySize = stats.AverageKeySize
	
	cf.Close()
	
	// Reopen for queries
	cf, _ = OpenFile(tmpFile)
	defer cf.Close()
	
	// Benchmark complex query using bitmaps
	t.Run("BitmapBasedComplexQuery", func(t *testing.T) {
		start := time.Now()
		
		// Complex query: (category = "cat_1" OR category = "cat_2") AND status = 1
		cat1Bitmap, _ := cf.QueryString("category", "cat_1")
		cat2Bitmap, _ := cf.QueryString("category", "cat_2")
		orBitmap := cf.QueryOr(cat1Bitmap, cat2Bitmap)
		
		statusGTEBitmap, _ := cf.QueryGreaterThanOrEqual("status", int32(1))
		statusLTEBitmap, _ := cf.QueryLessThanOrEqual("status", int32(1))
		statusEquals1Bitmap := cf.QueryAnd(statusGTEBitmap, statusLTEBitmap)
		
		finalBitmap := cf.QueryAnd(orBitmap, statusEquals1Bitmap)
		
		// Get cardinality without converting to slice
		count := finalBitmap.GetCardinality()
		
		duration := time.Since(start)
		t.Logf("Bitmap-based query: %d results in %v", count, duration)
	})
	
	// Benchmark same query with slice conversion for comparison
	t.Run("BitmapToSliceComparison", func(t *testing.T) {
		start := time.Now()
		
		// Same complex query but with slice conversion
		cat1Bitmap, _ := cf.QueryString("category", "cat_1")
		cat2Bitmap, _ := cf.QueryString("category", "cat_2")
		orBitmap := cf.QueryOr(cat1Bitmap, cat2Bitmap)
		
		statusGTEBitmap, _ := cf.QueryGreaterThanOrEqual("status", int32(1))
		statusLTEBitmap, _ := cf.QueryLessThanOrEqual("status", int32(1))
		statusEquals1Bitmap := cf.QueryAnd(statusGTEBitmap, statusLTEBitmap)
		
		finalBitmap := cf.QueryAnd(orBitmap, statusEquals1Bitmap)
		
		// Convert to slice for comparison
		finalResults := BitmapToSlice(finalBitmap)
		
		duration := time.Since(start)
		t.Logf("Bitmap to slice conversion: %d results in %v", len(finalResults), duration)
	})
}

var categoryNames = []string{
	"cat_0", "cat_1", "cat_2", "cat_3", "cat_4",
	"cat_5", "cat_6", "cat_7", "cat_8", "cat_9",
}