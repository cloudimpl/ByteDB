package columnar

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

func TestAllOperators(t *testing.T) {
	tmpFile := "test_operators.db"
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
		results, err := cf.QueryInt("id", 5)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		if len(results) != 1 || results[0] != 4 {
			t.Errorf("Expected row [4], got %v", results)
		}
		
		// Test string equality
		results, err = cf.QueryString("name", "Charlie")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		if len(results) != 1 || results[0] != 2 {
			t.Errorf("Expected row [2], got %v", results)
		}
		
		// Test boolean equality
		results, err = cf.QueryGreaterThanOrEqual("active", true)
		if err != nil {
			t.Errorf("Query active=true failed: %v", err)
		}
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
		results, err := cf.QueryGreaterThan("score", int32(50))
		if err != nil {
			t.Errorf("QueryGreaterThan failed: %v", err)
		}
		// Should return scores > 50: 60,70,80,90,100 (rows 5,6,7,8,9)
		expected := []uint64{5, 6, 7, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("GT: Expected rows %v, got %v", expected, results)
		}
		
		// Test GTE on integers
		results, err = cf.QueryGreaterThanOrEqual("score", int32(50))
		if err != nil {
			t.Errorf("QueryGreaterThanOrEqual failed: %v", err)
		}
		// Should return scores >= 50: 50,60,70,80,90,100 (rows 4,5,6,7,8,9)
		expected = []uint64{4, 5, 6, 7, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("GTE: Expected rows %v, got %v", expected, results)
		}
		
		// Test GT on strings
		results, err = cf.QueryGreaterThan("name", "Eve")
		if err != nil {
			t.Errorf("QueryGreaterThan string failed: %v", err)
		}
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
		results, err := cf.QueryLessThan("score", int32(50))
		if err != nil {
			t.Errorf("QueryLessThan failed: %v", err)
		}
		// Should return scores < 50: 10,20,30,40 (rows 0,1,2,3)
		expected := []uint64{0, 1, 2, 3}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("LT: Expected rows %v, got %v", expected, results)
		}
		
		// Test LTE on integers
		results, err = cf.QueryLessThanOrEqual("score", int32(50))
		if err != nil {
			t.Errorf("QueryLessThanOrEqual failed: %v", err)
		}
		// Should return scores <= 50: 10,20,30,40,50 (rows 0,1,2,3,4)
		expected = []uint64{0, 1, 2, 3, 4}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("LTE: Expected rows %v, got %v", expected, results)
		}
		
		// Test LT on strings
		results, err = cf.QueryLessThan("name", "Eve")
		if err != nil {
			t.Errorf("QueryLessThan string failed: %v", err)
		}
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
		results, err := cf.RangeQueryInt("score", 30, 70)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		// Should return scores 30-70: 30,40,50,60,70 (rows 2,3,4,5,6)
		expected := []uint64{2, 3, 4, 5, 6}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("Range: Expected rows %v, got %v", expected, results)
		}
		
		// Test string range
		results, err = cf.RangeQueryString("name", "Charlie", "Frank")
		if err != nil {
			t.Errorf("RangeQueryString failed: %v", err)
		}
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
		scoreResults, _ := cf.QueryGreaterThan("score", int32(50))
		nameResults, _ := cf.QueryLessThan("name", "Henry")
		
		results := cf.QueryAnd(scoreResults, nameResults)
		// score > 50: rows 5,6,7,8,9
		// name < "Henry": rows 0,1,2,3,4,5,6
		// AND: rows 5,6
		expected := []uint64{5, 6}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("AND: Expected rows %v, got %v", expected, results)
		}
		
		// Three-way AND: active=true AND score > 30 AND name < "Grace"
		activeResults, _ := cf.QueryGreaterThanOrEqual("active", true)
		scoreResults2, _ := cf.QueryGreaterThan("score", int32(30))
		nameResults2, _ := cf.QueryLessThan("name", "Grace")
		
		results = cf.QueryAnd(activeResults, scoreResults2, nameResults2)
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
		lowScores, _ := cf.QueryLessThan("score", int32(30))
		highScores, _ := cf.QueryGreaterThan("score", int32(80))
		
		results := cf.QueryOr(lowScores, highScores)
		// score < 30: rows 0,1
		// score > 80: rows 8,9
		// OR: rows 0,1,8,9
		expected := []uint64{0, 1, 8, 9}
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		if !equalSlices(results, expected) {
			t.Errorf("OR: Expected rows %v, got %v", expected, results)
		}
		
		// Three-way OR
		results1, _ := cf.QueryString("name", "Alice")
		results2, _ := cf.QueryString("name", "Eve")
		results3, _ := cf.QueryString("name", "Jack")
		
		results = cf.QueryOr(results1, results2, results3)
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
		activeTrue, _ := cf.QueryGreaterThanOrEqual("active", true)
		
		results, err := cf.QueryNot("active", activeTrue)
		if err != nil {
			t.Errorf("QueryNot failed: %v", err)
		}
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
		rangeResults, _ := cf.RangeQueryInt("score", 30, 70)
		activeResults, _ := cf.QueryGreaterThanOrEqual("active", true)
		nameResults, _ := cf.QueryGreaterThan("name", "Frank")
		orResults := cf.QueryOr(activeResults, nameResults)
		
		results := cf.QueryAnd(rangeResults, orResults)
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
		results, err := cf.QueryGreaterThanOrEqual("age", uint8(25))
		if err != nil {
			t.Errorf("Query age failed: %v", err)
		}
		ageEqual25, _ := cf.QueryLessThanOrEqual("age", uint8(25))
		age25 := cf.QueryAnd(results, ageEqual25)
		
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
	tmpFile := "test_edge_cases.db"
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
		results, _ := cf.QueryGreaterThanOrEqual("value", int32(0))
		if len(results) != 5 {
			t.Errorf("Expected 5 results for >= 0, got %d", len(results))
		}
		
		// Test with max value
		results, _ = cf.QueryLessThanOrEqual("value", int32(2147483647))
		if len(results) != 5 {
			t.Errorf("Expected 5 results for <= MAX_INT32, got %d", len(results))
		}
		
		// Test empty string
		results, _ = cf.QueryString("name", "")
		if len(results) != 1 || results[0] != 0 {
			t.Errorf("Expected row [0] for empty string, got %v", results)
		}
		
		// Test string greater than all values
		results, _ = cf.QueryLessThan("name", "zzz")
		if len(results) != 5 {
			t.Errorf("Expected 5 results for < 'zzz', got %d", len(results))
		}
	})
	
	t.Run("EmptyResults", func(t *testing.T) {
		// Query that returns no results
		results, _ := cf.QueryGreaterThan("value", int32(2147483647))
		if len(results) != 0 {
			t.Errorf("Expected empty results, got %v", results)
		}
		
		// AND with empty set
		someResults, _ := cf.QueryLessThan("value", int32(1000))
		emptyResults := []uint64{}
		andResults := cf.QueryAnd(someResults, emptyResults)
		if len(andResults) != 0 {
			t.Errorf("AND with empty set should be empty, got %v", andResults)
		}
		
		// OR with empty set
		orResults := cf.QueryOr(someResults, emptyResults)
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
	
	tmpFile := "test_operator_perf.db"
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
			func() ([]uint64, error) { return cf.QueryInt("id", 50000) },
		},
		{
			"GreaterThan",
			func() ([]uint64, error) { return cf.QueryGreaterThan("id", int64(90000)) },
		},
		{
			"LessThan",
			func() ([]uint64, error) { return cf.QueryLessThan("id", int64(10000)) },
		},
		{
			"Range",
			func() ([]uint64, error) { return cf.RangeQueryInt("id", 40000, 60000) },
		},
		{
			"StringEquality",
			func() ([]uint64, error) { return cf.QueryString("category", "cat_500") },
		},
		{
			"ComplexAND",
			func() ([]uint64, error) {
				r1, _ := cf.QueryGreaterThan("id", int64(50000))
				r2, _ := cf.QueryLessThan("id", int64(60000))
				return cf.QueryAnd(r1, r2), nil
			},
		},
		{
			"ComplexOR",
			func() ([]uint64, error) {
				r1, _ := cf.QueryLessThan("id", int64(1000))
				r2, _ := cf.QueryGreaterThan("id", int64(99000))
				return cf.QueryOr(r1, r2), nil
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