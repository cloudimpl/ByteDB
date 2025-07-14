package columnar

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

func TestAllOperators(t *testing.T) {
	tmpFile := "test_operators.bytedb"
	defer os.Remove(tmpFile)
	
	// Create test data
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add columns with different data types
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("age", DataTypeUint8, false)
	cf.AddColumn("score", DataTypeInt32, false)
	cf.AddColumn("name", DataTypeString, false)
	cf.AddColumn("active", DataTypeBool, false)
	
	// Load test data
	// IDs: 1-10
	idData := []struct{ Key int64; RowNum uint64 }{
		{1, 0}, {2, 1}, {3, 2}, {4, 3}, {5, 4},
		{6, 5}, {7, 6}, {8, 7}, {9, 8}, {10, 9},
	}
	cf.LoadIntColumn("id", idData)
	
	// Ages: various values with duplicates
	ageData := []struct{ Key uint64; RowNum uint64 }{
		{20, 0}, {25, 1}, {30, 2}, {25, 3}, {35, 4},
		{40, 5}, {30, 6}, {45, 7}, {25, 8}, {50, 9},
	}
	col := cf.columns["age"]
	col.btree.BulkLoadWithDuplicates(ageData)
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(ageData))
	
	// Scores: 10-100 by 10s
	scoreData := []struct{ Key uint64; RowNum uint64 }{
		{10, 0}, {20, 1}, {30, 2}, {40, 3}, {50, 4},
		{60, 5}, {70, 6}, {80, 7}, {90, 8}, {100, 9},
	}
	col = cf.columns["score"]
	col.btree.BulkLoadWithDuplicates(scoreData)
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(scoreData))
	
	// Names
	nameData := []struct{ Key string; RowNum uint64 }{
		{"Alice", 0}, {"Bob", 1}, {"Charlie", 2}, {"David", 3}, {"Eve", 4},
		{"Frank", 5}, {"Grace", 6}, {"Henry", 7}, {"Iris", 8}, {"Jack", 9},
	}
	cf.LoadStringColumn("name", nameData)
	
	// Active flags
	activeData := []struct{ Key uint64; RowNum uint64 }{
		{1, 0}, {0, 1}, {1, 2}, {1, 3}, {0, 4},
		{1, 5}, {0, 6}, {1, 7}, {0, 8}, {1, 9},
	}
	col = cf.columns["active"]
	col.btree.BulkLoadWithDuplicates(activeData)
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(activeData))
	
	cf.Close()
	
	// Reopen for queries
	cf, err = OpenFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf.Close()
	
	// Test 1: Equality operators
	t.Run("EqualityOperators", func(t *testing.T) {
		// Test integer equality
		bitmap, err := cf.QueryInt("id", 5)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		if len(results) != 1 || results[0] != 4 {
			t.Errorf("Expected row [4], got %v", results)
		}
		
		// Test string equality
		bitmap, err = cf.QueryString("name", "Charlie")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 1 || results[0] != 2 {
			t.Errorf("Expected row [2], got %v", results)
		}
		
		// Test boolean equality
		bitmap, err = cf.QueryGreaterThanOrEqual("active", true)
		if err != nil {
			t.Errorf("Query active=true failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return rows with active=true: 0,2,3,5,7,9
		expectedActive := []uint64{0, 2, 3, 5, 7, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expectedActive) {
			t.Errorf("Expected rows %v, got %v", expectedActive, results)
		}
	})
	
	// Test 2: Greater than operators
	t.Run("GreaterThanOperators", func(t *testing.T) {
		// Test GT on integers
		bitmap, err := cf.QueryGreaterThan("score", int32(50))
		if err != nil {
			t.Errorf("QueryGreaterThan failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		// Should return scores > 50: 60,70,80,90,100 (rows 5,6,7,8,9)
		expected := []uint64{5, 6, 7, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("GT: Expected rows %v, got %v", expected, results)
		}
		
		// Test GTE on integers
		bitmap, err = cf.QueryGreaterThanOrEqual("score", int32(50))
		if err != nil {
			t.Errorf("QueryGreaterThanOrEqual failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return scores >= 50: 50,60,70,80,90,100 (rows 4,5,6,7,8,9)
		expected = []uint64{4, 5, 6, 7, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("GTE: Expected rows %v, got %v", expected, results)
		}
		
		// Test GT on strings
		bitmap, err = cf.QueryGreaterThan("name", "Eve")
		if err != nil {
			t.Errorf("QueryGreaterThan string failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return names > "Eve": Frank,Grace,Henry,Iris,Jack (rows 5,6,7,8,9)
		expected = []uint64{5, 6, 7, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("GT string: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 3: Less than operators
	t.Run("LessThanOperators", func(t *testing.T) {
		// Test LT on integers
		bitmap, err := cf.QueryLessThan("score", int32(50))
		if err != nil {
			t.Errorf("QueryLessThan failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		// Should return scores < 50: 10,20,30,40 (rows 0,1,2,3)
		expected := []uint64{0, 1, 2, 3}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("LT: Expected rows %v, got %v", expected, results)
		}
		
		// Test LTE on integers
		bitmap, err = cf.QueryLessThanOrEqual("score", int32(50))
		if err != nil {
			t.Errorf("QueryLessThanOrEqual failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return scores <= 50: 10,20,30,40,50 (rows 0,1,2,3,4)
		expected = []uint64{0, 1, 2, 3, 4}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("LTE: Expected rows %v, got %v", expected, results)
		}
		
		// Test LT on strings
		bitmap, err = cf.QueryLessThan("name", "Eve")
		if err != nil {
			t.Errorf("QueryLessThan string failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return names < "Eve": Alice,Bob,Charlie,David (rows 0,1,2,3)
		expected = []uint64{0, 1, 2, 3}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("LT string: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 4: Range queries (BETWEEN)
	t.Run("RangeQueries", func(t *testing.T) {
		// Test integer range
		bitmap, err := cf.RangeQueryInt("score", 30, 70)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		// Should return scores 30-70: 30,40,50,60,70 (rows 2,3,4,5,6)
		expected := []uint64{2, 3, 4, 5, 6}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("Range: Expected rows %v, got %v", expected, results)
		}
		
		// Test string range
		bitmap, err = cf.RangeQueryString("name", "Charlie", "Frank")
		if err != nil {
			t.Errorf("RangeQueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return names Charlie-Frank: Charlie,David,Eve,Frank (rows 2,3,4,5)
		expected = []uint64{2, 3, 4, 5}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("String range: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 5: AND operator
	t.Run("ANDOperator", func(t *testing.T) {
		// Find rows where score > 50 AND name < "Henry"
		scoreBitmap, _ := cf.QueryGreaterThan("score", int32(50))
		nameBitmap, _ := cf.QueryLessThan("name", "Henry")
		
		resultBitmap := cf.QueryAnd(scoreBitmap, nameBitmap)
		results := BitmapToSlice(resultBitmap)
		// score > 50: rows 5,6,7,8,9
		// name < "Henry": rows 0,1,2,3,4,5,6
		// AND: rows 5,6
		expected := []uint64{5, 6}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("AND: Expected rows %v, got %v", expected, results)
		}
		
		// Three-way AND: active=true AND score > 30 AND name < "Grace"
		activeBitmap, _ := cf.QueryGreaterThanOrEqual("active", true)
		scoreBitmap2, _ := cf.QueryGreaterThan("score", int32(30))
		nameBitmap2, _ := cf.QueryLessThan("name", "Grace")
		
		resultBitmap = cf.QueryAnd(activeBitmap, scoreBitmap2, nameBitmap2)
		results = BitmapToSlice(resultBitmap)
		// active=true: rows 0,2,3,5,7,9
		// score > 30: rows 3,4,5,6,7,8,9
		// name < "Grace": rows 0,1,2,3,4,5
		// AND: rows 3,5
		expected = []uint64{3, 5}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("Three-way AND: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 6: OR operator
	t.Run("OROperator", func(t *testing.T) {
		// Find rows where score < 30 OR score > 80
		lowScoresBitmap, _ := cf.QueryLessThan("score", int32(30))
		highScoresBitmap, _ := cf.QueryGreaterThan("score", int32(80))
		
		resultBitmap := cf.QueryOr(lowScoresBitmap, highScoresBitmap)
		results := BitmapToSlice(resultBitmap)
		// score < 30: rows 0,1
		// score > 80: rows 8,9
		// OR: rows 0,1,8,9
		expected := []uint64{0, 1, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("OR: Expected rows %v, got %v", expected, results)
		}
		
		// Three-way OR
		bitmap1, _ := cf.QueryString("name", "Alice")
		bitmap2, _ := cf.QueryString("name", "Eve")
		bitmap3, _ := cf.QueryString("name", "Jack")
		
		resultBitmap = cf.QueryOr(bitmap1, bitmap2, bitmap3)
		results = BitmapToSlice(resultBitmap)
		// Should return rows 0,4,9
		expected = []uint64{0, 4, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("Three-way OR: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 7: NOT operator
	t.Run("NOTOperator", func(t *testing.T) {
		// Find all rows where active != true
		activeTrueBitmap, _ := cf.QueryGreaterThanOrEqual("active", true)
		
		resultBitmap, err := cf.QueryNot("active", activeTrueBitmap)
		if err != nil {
			t.Errorf("QueryNot failed: %v", err)
		}
		results := BitmapToSlice(resultBitmap)
		// active=true: rows 0,2,3,5,7,9
		// NOT: rows 1,4,6,8
		expected := []uint64{1, 4, 6, 8}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("NOT: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 8: Complex combinations
	t.Run("ComplexQueries", func(t *testing.T) {
		// (score BETWEEN 30 AND 70) AND (active = true OR name > "Frank")
		rangeBitmap, _ := cf.RangeQueryInt("score", 30, 70)
		activeBitmap, _ := cf.QueryGreaterThanOrEqual("active", true)
		nameBitmap, _ := cf.QueryGreaterThan("name", "Frank")
		orBitmap := cf.QueryOr(activeBitmap, nameBitmap)
		
		resultBitmap := cf.QueryAnd(rangeBitmap, orBitmap)
		results := BitmapToSlice(resultBitmap)
		// score 30-70: rows 2,3,4,5,6
		// active=true: rows 0,2,3,5,7,9
		// name > "Frank": rows 6,7,8,9
		// active=true OR name>"Frank": rows 0,2,3,5,6,7,8,9
		// Final AND: rows 2,3,5,6
		expected := []uint64{2, 3, 5, 6}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("Complex query: Expected rows %v, got %v", expected, results)
		}
	})
	
	// Test 9: Queries on columns with duplicates
	t.Run("DuplicateHandling", func(t *testing.T) {
		// Query age = 25 (should have duplicates)
		gteBitmap, err := cf.QueryGreaterThanOrEqual("age", uint8(25))
		if err != nil {
			t.Errorf("Query age failed: %v", err)
		}
		lteBitmap, _ := cf.QueryLessThanOrEqual("age", uint8(25))
		age25Bitmap := cf.QueryAnd(gteBitmap, lteBitmap)
		age25 := BitmapToSlice(age25Bitmap)
		
		// age = 25: rows 1,3,8
		expected := []uint64{1, 3, 8}
		sort.Slice(age25, func(i, j int) bool { return age25[i] < age25[j] })
		if !equalSlices(age25, expected) {
			t.Errorf("Duplicate handling: Expected rows %v, got %v", expected, age25)
		}
	})
}

// TestOperatorEdgeCases tests edge cases for operators
func TestOperatorEdgeCases(t *testing.T) {
	tmpFile := "test_edge_cases.bytedb"
	defer os.Remove(tmpFile)
	
	cf, _ := CreateFile(tmpFile)
	
	// Create columns
	cf.AddColumn("value", DataTypeInt32, false)
	cf.AddColumn("name", DataTypeString, false)
	
	// Load data with edge values
	valueData := []struct{ Key uint64; RowNum uint64 }{
		{0, 0},           // Min value
		{2147483647, 1},  // Max int32
		{1000, 2},
		{1000, 3},        // Duplicate
		{1000, 4},        // Duplicate
	}
	col := cf.columns["value"]
	col.btree.BulkLoadWithDuplicates(valueData)
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(valueData))
	
	nameData := []struct{ Key string; RowNum uint64 }{
		{"", 0},          // Empty string
		{"A", 1},
		{"Z", 2},
		{"test", 3},
		{"test", 4},      // Duplicate
	}
	cf.LoadStringColumn("name", nameData)
	
	cf.Close()
	
	// Reopen and test
	cf, _ = OpenFile(tmpFile)
	defer cf.Close()
	
	t.Run("EdgeValues", func(t *testing.T) {
		// Test with min value
		bitmap, _ := cf.QueryGreaterThanOrEqual("value", int32(0))
		results := BitmapToSlice(bitmap)
		if len(results) != 5 {
			t.Errorf("Expected 5 results for >= 0, got %d", len(results))
		}
		
		// Test with max value
		bitmap, _ = cf.QueryLessThanOrEqual("value", int32(2147483647))
		results = BitmapToSlice(bitmap)
		if len(results) != 5 {
			t.Errorf("Expected 5 results for <= MAX_INT32, got %d", len(results))
		}
		
		// Test empty string
		bitmap, _ = cf.QueryString("name", "")
		results = BitmapToSlice(bitmap)
		if len(results) != 1 || results[0] != 0 {
			t.Errorf("Expected row [0] for empty string, got %v", results)
		}
		
		// Test string greater than all values
		bitmap, _ = cf.QueryLessThan("name", "zzz")
		results = BitmapToSlice(bitmap)
		if len(results) != 5 {
			t.Errorf("Expected 5 results for < 'zzz', got %d", len(results))
		}
	})
	
	t.Run("EmptyResults", func(t *testing.T) {
		// Query that returns no results
		bitmap, _ := cf.QueryGreaterThan("value", int32(2147483647))
		results := BitmapToSlice(bitmap)
		if len(results) != 0 {
			t.Errorf("Expected empty results, got %v", results)
		}
		
		// AND with empty set
		someBitmap, _ := cf.QueryLessThan("value", int32(1000))
		emptyBitmap := roaring.New()
		andBitmap := cf.QueryAnd(someBitmap, emptyBitmap)
		andResults := BitmapToSlice(andBitmap)
		if len(andResults) != 0 {
			t.Errorf("AND with empty set should be empty, got %v", andResults)
		}
		
		// OR with empty set
		orBitmap := cf.QueryOr(someBitmap, emptyBitmap)
		orResults := BitmapToSlice(orBitmap)
		someResults := BitmapToSlice(someBitmap)
		if !equalSlices(orResults, someResults) {
			t.Errorf("OR with empty set should return non-empty set")
		}
	})
}

// TestOperatorPerformance tests performance with large datasets
func TestOperatorPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
	
	tmpFile := "test_operator_perf.bytedb"
	defer os.Remove(tmpFile)
	
	cf, _ := CreateFile(tmpFile)
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	
	// Load 100k rows
	numRows := 100000
	idData := make([]struct{ Key int64; RowNum uint64 }, numRows)
	categoryData := make([]struct{ Key string; RowNum uint64 }, numRows)
	
	for i := 0; i < numRows; i++ {
		idData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),
			RowNum: uint64(i),
		}
		categoryData[i] = struct{ Key string; RowNum uint64 }{
			Key:    fmt.Sprintf("cat_%d", i%1000), // 1000 categories
			RowNum: uint64(i),
		}
	}
	
	cf.LoadIntColumn("id", idData)
	cf.LoadStringColumn("category", categoryData)
	cf.Close()
	
	// Reopen and benchmark queries
	cf, _ = OpenFile(tmpFile)
	defer cf.Close()
	
	// Benchmark different operators
	benchmarks := []struct {
		name string
		fn   func() ([]uint64, error)
	}{
		{
			"Equality",
			func() ([]uint64, error) {
				bitmap, err := cf.QueryInt("id", 50000)
				return BitmapToSlice(bitmap), err
			},
		},
		{
			"GreaterThan",
			func() ([]uint64, error) {
				bitmap, err := cf.QueryGreaterThan("id", int64(90000))
				return BitmapToSlice(bitmap), err
			},
		},
		{
			"LessThan",
			func() ([]uint64, error) {
				bitmap, err := cf.QueryLessThan("id", int64(10000))
				return BitmapToSlice(bitmap), err
			},
		},
		{
			"Range",
			func() ([]uint64, error) {
				bitmap, err := cf.RangeQueryInt("id", 40000, 60000)
				return BitmapToSlice(bitmap), err
			},
		},
		{
			"StringEquality",
			func() ([]uint64, error) {
				bitmap, err := cf.QueryString("category", "cat_500")
				return BitmapToSlice(bitmap), err
			},
		},
		{
			"ComplexAND",
			func() ([]uint64, error) {
				r1, _ := cf.QueryGreaterThan("id", int64(50000))
				r2, _ := cf.QueryLessThan("id", int64(60000))
				result := cf.QueryAnd(r1, r2)
				return BitmapToSlice(result), nil
			},
		},
		{
			"ComplexOR",
			func() ([]uint64, error) {
				r1, _ := cf.QueryLessThan("id", int64(1000))
				r2, _ := cf.QueryGreaterThan("id", int64(99000))
				result := cf.QueryOr(r1, r2)
				return BitmapToSlice(result), nil
			},
		},
	}
	
	for _, bm := range benchmarks {
		start := time.Now()
		results, err := bm.fn()
		duration := time.Since(start)
		
		if err != nil {
			t.Errorf("%s failed: %v", bm.name, err)
		} else {
			t.Logf("%s: %d results in %v", bm.name, len(results), duration)
		}
	}
}

// Helper function to compare slices
func equalSlices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}