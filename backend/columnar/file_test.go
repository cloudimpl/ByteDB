package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestColumnarFile(t *testing.T) {
	tmpFile := "test_columnar_file.bytedb"
	defer os.Remove(tmpFile)
	
	// Test 1: Create and basic operations
	t.Run("CreateAndBasicOps", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		// Add columns
		if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
			t.Errorf("Failed to add id column: %v", err)
		}
		
		if err := cf.AddColumn("name", DataTypeString, false); err != nil {
			t.Errorf("Failed to add name column: %v", err)
		}
		
		if err := cf.AddColumn("age", DataTypeInt64, true); err != nil {
			t.Errorf("Failed to add age column: %v", err)
		}
		
		// Verify columns
		cols := cf.GetColumns()
		if len(cols) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(cols))
		}
		
		// Check column types
		idType, err := cf.GetColumnType("id")
		if err != nil || idType != DataTypeInt64 {
			t.Errorf("Expected id to be Int64, got %v", idType)
		}
		
		nameType, err := cf.GetColumnType("name")
		if err != nil || nameType != DataTypeString {
			t.Errorf("Expected name to be String, got %v", nameType)
		}
		
		// Try to add duplicate column
		if err := cf.AddColumn("id", DataTypeInt64, false); err == nil {
			t.Error("Expected error when adding duplicate column")
		}
		
		cf.Close()
	})
	
	// Test 2: Load data and query
	t.Run("LoadDataAndQuery", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		// Add columns
		cf.AddColumn("id", DataTypeInt64, false)
		cf.AddColumn("name", DataTypeString, false)
		
		// Load integer data
		intData := []struct{ Key int64; RowNum uint64 }{
			{1, 0},
			{2, 1},
			{3, 2},
			{1, 3}, // Duplicate
			{4, 4},
			{2, 5}, // Duplicate
		}
		
		if err := cf.LoadIntColumn("id", intData); err != nil {
			t.Fatalf("Failed to load int data: %v", err)
		}
		
		// Load string data
		stringData := []struct{ Key string; RowNum uint64 }{
			{"Alice", 0},
			{"Bob", 1},
			{"Charlie", 2},
			{"Alice", 3}, // Duplicate
			{"David", 4},
			{"Bob", 5}, // Duplicate
		}
		
		if err := cf.LoadStringColumn("name", stringData); err != nil {
			t.Fatalf("Failed to load string data: %v", err)
		}
		
		// Query integer column
		bitmap, err := cf.QueryInt("id", 1)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		if len(results) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(results))
		} else if results[0] != 0 || results[1] != 3 {
			t.Errorf("Expected rows [0, 3], got %v", results)
		}
		
		// Query string column
		bitmap, err = cf.QueryString("name", "Bob")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 2 || results[0] != 1 || results[1] != 5 {
			t.Errorf("Expected rows [1, 5], got %v", results)
		}
		
		// Query non-existent value
		bitmap, err = cf.QueryString("name", "Eve")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 0 {
			t.Errorf("Expected empty results for non-existent value, got %v", results)
		}
		
		cf.Close()
	})
	
	// Test 3: Range queries
	t.Run("RangeQueries", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		// Add columns
		cf.AddColumn("value", DataTypeInt64, false)
		cf.AddColumn("category", DataTypeString, false)
		
		// Load data
		intData := []struct{ Key int64; RowNum uint64 }{
			{10, 0},
			{20, 1},
			{30, 2},
			{40, 3},
			{50, 4},
			{25, 5},
			{35, 6},
		}
		cf.LoadIntColumn("value", intData)
		
		stringData := []struct{ Key string; RowNum uint64 }{
			{"apple", 0},
			{"banana", 1},
			{"cherry", 2},
			{"date", 3},
			{"elderberry", 4},
			{"blueberry", 5},
			{"cranberry", 6},
		}
		cf.LoadStringColumn("category", stringData)
		
		// Integer range query
		bitmap, err := cf.RangeQueryInt("value", 20, 40)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		if len(results) != 5 { // 20, 25, 30, 35, 40
			t.Errorf("Expected 5 results, got %d", len(results))
		}
		
		// String range query
		bitmap, err = cf.RangeQueryString("category", "banana", "cranberry")
		if err != nil {
			t.Errorf("RangeQueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		// Should return: banana, blueberry, cherry, cranberry
		if len(results) != 4 {
			t.Errorf("Expected 4 results, got %d", len(results))
		}
		
		cf.Close()
	})
}

func TestColumnarFilePersistence(t *testing.T) {
	tmpFile := "test_columnar_persist.bytedb"
	defer os.Remove(tmpFile)
	
	// Phase 1: Create and populate
	t.Run("CreateAndPopulate", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		// Add columns
		cf.AddColumn("user_id", DataTypeInt64, false)
		cf.AddColumn("username", DataTypeString, false)
		cf.AddColumn("score", DataTypeInt64, false)
		
		// Load data
		userIDs := make([]struct{ Key int64; RowNum uint64 }, 100)
		usernames := make([]struct{ Key string; RowNum uint64 }, 100)
		scores := make([]struct{ Key int64; RowNum uint64 }, 100)
		
		for i := 0; i < 100; i++ {
			userIDs[i] = struct{ Key int64; RowNum uint64 }{
				Key:    int64(i % 20), // Create duplicates
				RowNum: uint64(i),
			}
			usernames[i] = struct{ Key string; RowNum uint64 }{
				Key:    fmt.Sprintf("user_%d", i%20),
				RowNum: uint64(i),
			}
			scores[i] = struct{ Key int64; RowNum uint64 }{
				Key:    int64(i * 10),
				RowNum: uint64(i),
			}
		}
		
		cf.LoadIntColumn("user_id", userIDs)
		cf.LoadStringColumn("username", usernames)
		cf.LoadIntColumn("score", scores)
		
		// Get stats before closing
		stats, _ := cf.GetStats("user_id")
		t.Logf("user_id stats: %v", stats)
		
		cf.Close()
	})
	
	// Phase 2: Open and query
	t.Run("OpenAndQuery", func(t *testing.T) {
		cf, err := OpenFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer cf.Close()
		
		// Verify columns
		cols := cf.GetColumns()
		if len(cols) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(cols))
		}
		
		// Query user_id = 5 (should have 5 rows)
		bitmap, err := cf.QueryInt("user_id", 5)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		if len(results) != 5 {
			t.Errorf("Expected 5 results for user_id=5, got %d", len(results))
		}
		
		// Query username
		bitmap, err = cf.QueryString("username", "user_10")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 5 {
			t.Errorf("Expected 5 results for username=user_10, got %d", len(results))
		}
		
		// Range query on scores
		bitmap, err = cf.RangeQueryInt("score", 200, 400)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 21 { // 200, 210, ..., 400
			t.Errorf("Expected 21 results for score range, got %d", len(results))
		}
		
		// Get stats
		stats, err := cf.GetStats("username")
		if err != nil {
			t.Errorf("GetStats failed: %v", err)
		}
		t.Logf("username stats after reload: %v", stats)
	})
}

func TestColumnarFileLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}
	
	tmpFile := "test_columnar_large.bytedb"
	defer os.Remove(tmpFile)
	
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer cf.Close()
	
	// Add columns
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	cf.AddColumn("value", DataTypeInt64, false)
	
	// Generate large dataset
	numRows := 100000
	numCategories := 1000
	
	// Prepare data
	ids := make([]struct{ Key int64; RowNum uint64 }, numRows)
	categories := make([]struct{ Key string; RowNum uint64 }, numRows)
	values := make([]struct{ Key int64; RowNum uint64 }, numRows)
	
	for i := 0; i < numRows; i++ {
		ids[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),
			RowNum: uint64(i),
		}
		categories[i] = struct{ Key string; RowNum uint64 }{
			Key:    fmt.Sprintf("category_%d", i%numCategories),
			RowNum: uint64(i),
		}
		values[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i % 10000), // Create duplicates
			RowNum: uint64(i),
		}
	}
	
	// Load data
	t.Log("Loading data...")
	if err := cf.LoadIntColumn("id", ids); err != nil {
		t.Fatalf("Failed to load ids: %v", err)
	}
	if err := cf.LoadStringColumn("category", categories); err != nil {
		t.Fatalf("Failed to load categories: %v", err)
	}
	if err := cf.LoadIntColumn("value", values); err != nil {
		t.Fatalf("Failed to load values: %v", err)
	}
	
	// Test queries
	t.Run("LargeScaleQueries", func(t *testing.T) {
		// Query specific category (should have 100 rows each)
		bitmap, err := cf.QueryString("category", "category_42")
		if err != nil {
			t.Errorf("QueryString failed: %v", err)
		}
		results := BitmapToSlice(bitmap)
		if len(results) != 100 {
			t.Errorf("Expected 100 results, got %d", len(results))
		}
		
		// Query value with many duplicates
		bitmap, err = cf.QueryInt("value", 42)
		if err != nil {
			t.Errorf("QueryInt failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 10 { // 42, 10042, 20042, ..., 90042
			t.Errorf("Expected 10 results for value=42, got %d", len(results))
		}
		
		// Range query
		bitmap, err = cf.RangeQueryInt("id", 50000, 50100)
		if err != nil {
			t.Errorf("RangeQueryInt failed: %v", err)
		}
		results = BitmapToSlice(bitmap)
		if len(results) != 101 {
			t.Errorf("Expected 101 results for range query, got %d", len(results))
		}
	})
	
	// Get final statistics
	for _, col := range cf.GetColumns() {
		stats, _ := cf.GetStats(col)
		t.Logf("%s stats: %v", col, stats)
	}
}

// Benchmark tests
func BenchmarkColumnarFileQueries(b *testing.B) {
	tmpFile := "bench_columnar.bytedb"
	defer os.Remove(tmpFile)
	
	// Setup
	cf, _ := CreateFile(tmpFile)
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)
	
	// Load data
	numRows := 10000
	ids := make([]struct{ Key int64; RowNum uint64 }, numRows)
	names := make([]struct{ Key string; RowNum uint64 }, numRows)
	
	for i := 0; i < numRows; i++ {
		ids[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i % 1000),
			RowNum: uint64(i),
		}
		names[i] = struct{ Key string; RowNum uint64 }{
			Key:    fmt.Sprintf("name_%d", i%1000),
			RowNum: uint64(i),
		}
	}
	
	cf.LoadIntColumn("id", ids)
	cf.LoadStringColumn("name", names)
	
	b.ResetTimer()
	
	b.Run("IntQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cf.QueryInt("id", int64(i%1000))
		}
	})
	
	b.Run("StringQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cf.QueryString("name", fmt.Sprintf("name_%d", i%1000))
		}
	})
	
	b.Run("IntRangeQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			min := int64(i % 900)
			cf.RangeQueryInt("id", min, min+100)
		}
	})
	
	cf.Close()
}