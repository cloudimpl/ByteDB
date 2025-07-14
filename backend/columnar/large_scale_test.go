package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// TestMultiMillionRowDatasets tests B+ tree implementation with very large datasets
func TestMultiMillionRowDatasets(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-million row tests in short mode")
	}

	testCases := []struct {
		name     string
		rowCount int
		testFunc func(t *testing.T, rowCount int)
	}{
		{"1MillionRows", 1_000_000, testLargeDataset},
		{"2MillionRows", 2_000_000, testLargeDataset},
		{"5MillionRows", 5_000_000, testLargeDataset},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Starting test with %d rows", tc.rowCount)
			start := time.Now()
			tc.testFunc(t, tc.rowCount)
			duration := time.Since(start)
			t.Logf("Completed %d rows in %v (%.2f rows/sec)", 
				tc.rowCount, duration, float64(tc.rowCount)/duration.Seconds())
		})
	}
}

func testLargeDataset(t *testing.T, rowCount int) {
	tmpFile := fmt.Sprintf("test_large_%d.db", rowCount)
	defer os.Remove(tmpFile)

	// Create file
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Add columns with different data types
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	cf.AddColumn("value", DataTypeInt32, false)
	cf.AddColumn("score", DataTypeFloat64, false)
	cf.AddColumn("active", DataTypeBool, false)

	t.Logf("Generating %d rows of test data...", rowCount)
	
	// Generate large dataset with patterns for validation
	idData := make([]struct{ Key int64; RowNum uint64 }, rowCount)
	categoryData := make([]struct{ Key string; RowNum uint64 }, rowCount)
	valueData := make([]struct{ Key uint64; RowNum uint64 }, rowCount)
	scoreData := make([]struct{ Key uint64; RowNum uint64 }, rowCount)
	activeData := make([]struct{ Key uint64; RowNum uint64 }, rowCount)

	// Create predictable patterns for validation
	categories := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	rand.Seed(12345) // Fixed seed for reproducible tests

	for i := 0; i < rowCount; i++ {
		idData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),
			RowNum: uint64(i),
		}
		
		categoryData[i] = struct{ Key string; RowNum uint64 }{
			Key:    categories[i%len(categories)],
			RowNum: uint64(i),
		}
		
		valueData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % 1000), // Values 0-999 repeating
			RowNum: uint64(i),
		}
		
		scoreData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(rand.Intn(10000)), // Random scores 0-9999
			RowNum: uint64(i),
		}
		
		activeData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % 2), // Alternating true/false
			RowNum: uint64(i),
		}
	}

	t.Logf("Loading data into columns...")
	
	// Load integer column
	if err := cf.LoadIntColumn("id", idData); err != nil {
		t.Fatalf("Failed to load id column: %v", err)
	}

	// Load string column
	if err := cf.LoadStringColumn("category", categoryData); err != nil {
		t.Fatalf("Failed to load category column: %v", err)
	}

	// Load other columns
	for colName, data := range map[string][]struct{ Key uint64; RowNum uint64 }{
		"value":  valueData,
		"score":  scoreData,
		"active": activeData,
	} {
		col := cf.columns[colName]
		stats, err := col.btree.BulkLoadWithDuplicates(data)
		if err != nil {
			t.Fatalf("Failed to load %s column: %v", colName, err)
		}
		col.metadata.RootPageID = col.btree.GetRootPageID()
		col.metadata.TotalKeys = stats.TotalKeys
		col.metadata.DistinctCount = stats.DistinctCount
		col.metadata.MinValueOffset = stats.MinValue
		col.metadata.MaxValueOffset = stats.MaxValue
		col.metadata.AverageKeySize = stats.AverageKeySize
	}

	cf.Close()

	// Reopen for queries
	cf, err = OpenFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf.Close()

	t.Logf("Running validation queries...")

	// Test 1: Point queries at different positions
	t.Run("PointQueries", func(t *testing.T) {
		testPositions := []int{0, rowCount/4, rowCount/2, 3*rowCount/4, rowCount-1}
		
		for _, pos := range testPositions {
			bitmap, err := cf.QueryInt("id", int64(pos))
			if err != nil {
				t.Errorf("Query for id=%d failed: %v", pos, err)
				continue
			}
			
			if bitmap.GetCardinality() != 1 {
				t.Errorf("Expected 1 result for id=%d, got %d", pos, bitmap.GetCardinality())
				continue
			}
			
			rows := BitmapToSlice(bitmap)
			if len(rows) != 1 || rows[0] != uint64(pos) {
				t.Errorf("Expected row [%d], got %v", pos, rows)
			}
		}
	})

	// Test 2: Range queries
	t.Run("RangeQueries", func(t *testing.T) {
		// Test various range sizes
		rangeTests := []struct {
			start, end int
			name       string
		}{
			{0, 999, "SmallRange"},
			{rowCount/4, rowCount/4 + 9999, "MidRange"},
			{rowCount - 10000, rowCount - 1, "EndRange"},
		}
		
		for _, rt := range rangeTests {
			bitmap, err := cf.RangeQueryInt("id", int64(rt.start), int64(rt.end))
			if err != nil {
				t.Errorf("Range query [%d,%d] failed: %v", rt.start, rt.end, err)
				continue
			}
			
			expectedCount := rt.end - rt.start + 1
			if bitmap.GetCardinality() != uint64(expectedCount) {
				t.Errorf("Range [%d,%d]: expected %d results, got %d", 
					rt.start, rt.end, expectedCount, bitmap.GetCardinality())
			}
			
			// Validate actual data
			rows := BitmapToSlice(bitmap)
			if len(rows) > 0 {
				if rows[0] != uint64(rt.start) {
					t.Errorf("Range [%d,%d]: first row should be %d, got %d", 
						rt.start, rt.end, rt.start, rows[0])
				}
				if rows[len(rows)-1] != uint64(rt.end) {
					t.Errorf("Range [%d,%d]: last row should be %d, got %d", 
						rt.start, rt.end, rt.end, rows[len(rows)-1])
				}
			}
		}
	})

	// Test 3: String queries with duplicates
	t.Run("StringQueries", func(t *testing.T) {
		for i, category := range categories[:5] { // Test first 5 categories
			bitmap, err := cf.QueryString("category", category)
			if err != nil {
				t.Errorf("Query for category=%s failed: %v", category, err)
				continue
			}
			
			// Each category should appear every 10 rows
			expectedCount := rowCount / len(categories)
			if int(bitmap.GetCardinality()) != expectedCount {
				t.Errorf("Category %s: expected %d results, got %d", 
					category, expectedCount, bitmap.GetCardinality())
				continue
			}
			
			// Validate pattern: rows should be i, i+10, i+20, ...
			rows := BitmapToSlice(bitmap)
			if len(rows) >= 2 {
				if rows[0] != uint64(i) {
					t.Errorf("Category %s: first row should be %d, got %d", category, i, rows[0])
				}
				if rows[1] != uint64(i+len(categories)) {
					t.Errorf("Category %s: second row should be %d, got %d", 
						category, i+len(categories), rows[1])
				}
			}
		}
	})

	// Test 4: Complex bitmap operations
	t.Run("ComplexBitmapOperations", func(t *testing.T) {
		// Query: category = "A" AND value < 100
		catABitmap, err := cf.QueryString("category", "A")
		if err != nil {
			t.Fatalf("Category A query failed: %v", err)
		}
		
		valueLT100Bitmap, err := cf.QueryLessThan("value", int32(100))
		if err != nil {
			t.Fatalf("Value < 100 query failed: %v", err)
		}
		
		andResult := cf.QueryAnd(catABitmap, valueLT100Bitmap)
		
		// Validate AND result - should be rows where i%10==0 AND i%1000<100
		// This means rows 0, 10, 20, ..., 90 in each group of 1000
		expectedPerGroup := 10 // rows 0, 10, 20, ..., 90
		expectedTotal := (rowCount / 1000) * expectedPerGroup
		
		if int(andResult.GetCardinality()) != expectedTotal {
			t.Errorf("AND operation: expected %d results, got %d", 
				expectedTotal, andResult.GetCardinality())
		}
		
		// Test OR operation
		orResult := cf.QueryOr(catABitmap, valueLT100Bitmap)
		if orResult.GetCardinality() <= catABitmap.GetCardinality() || 
		   orResult.GetCardinality() <= valueLT100Bitmap.GetCardinality() {
			t.Error("OR result should be larger than individual operands")
		}
	})

	// Test 5: B+ tree structure validation
	t.Run("BTreeStructureValidation", func(t *testing.T) {
		// Check that B+ tree structure is reasonable for large datasets
		for colName, col := range cf.columns {
			if col.metadata.RootPageID == 0 {
				continue // Skip empty columns
			}
			
			// Tree should have reasonable height (log_n base)
			// For millions of rows, height should be reasonable (< 10 levels)
			if col.btree.height > 10 {
				t.Errorf("Column %s: B+ tree height %d seems too high for %d rows", 
					colName, col.btree.height, rowCount)
			}
			
			// Child page mapping should be present for internal nodes
			if col.btree.height > 1 && len(col.btree.childPageMap) == 0 {
				t.Errorf("Column %s: Missing child page mapping for multi-level tree", colName)
			}
			
			t.Logf("Column %s: height=%d, root=%d, childPageMap entries=%d", 
				colName, col.btree.height, col.metadata.RootPageID, len(col.btree.childPageMap))
		}
	})

	// Test 6: Performance benchmarks
	t.Run("PerformanceBenchmarks", func(t *testing.T) {
		// Point query performance
		start := time.Now()
		for i := 0; i < 100; i++ {
			pos := rand.Intn(rowCount)
			_, err := cf.QueryInt("id", int64(pos))
			if err != nil {
				t.Errorf("Performance test query failed: %v", err)
			}
		}
		pointQueryTime := time.Since(start)
		avgPointQuery := pointQueryTime / 100
		
		t.Logf("Point query performance: %v average (100 queries on %d rows)", 
			avgPointQuery, rowCount)
		
		// Range query performance
		start = time.Now()
		_, err := cf.RangeQueryInt("id", 0, int64(rowCount/100)) // 1% of data
		if err != nil {
			t.Errorf("Range query performance test failed: %v", err)
		}
		rangeQueryTime := time.Since(start)
		
		t.Logf("Range query performance: %v (1%% of %d rows)", rangeQueryTime, rowCount)
		
		// Complex query performance
		start = time.Now()
		catBitmap, _ := cf.QueryString("category", "A")
		valueBitmap, _ := cf.QueryLessThan("value", int32(500))
		cf.QueryAnd(catBitmap, valueBitmap)
		complexQueryTime := time.Since(start)
		
		t.Logf("Complex query performance: %v (category + value + AND)", complexQueryTime)
	})

	// Get file size for space efficiency analysis
	fileInfo, err := os.Stat(tmpFile)
	if err == nil {
		mbSize := float64(fileInfo.Size()) / (1024 * 1024)
		bytesPerRow := float64(fileInfo.Size()) / float64(rowCount)
		t.Logf("File size: %.2f MB (%.2f bytes per row)", mbSize, bytesPerRow)
	}
}

// TestLargeDatasetEdgeCases tests edge cases with large datasets
func TestLargeDatasetEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset edge cases in short mode")
	}

	tmpFile := "test_large_edge_cases.db"
	defer os.Remove(tmpFile)

	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	cf.AddColumn("sorted", DataTypeInt64, false)
	cf.AddColumn("reverse", DataTypeInt64, false)
	cf.AddColumn("random", DataTypeInt64, false)

	const rowCount = 1_000_000

	t.Logf("Testing edge cases with %d rows", rowCount)

	// Test with different data patterns
	sortedData := make([]struct{ Key int64; RowNum uint64 }, rowCount)
	reverseData := make([]struct{ Key int64; RowNum uint64 }, rowCount)
	randomData := make([]struct{ Key uint64; RowNum uint64 }, rowCount)

	rand.Seed(54321)
	
	for i := 0; i < rowCount; i++ {
		sortedData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(i),           // Already sorted
			RowNum: uint64(i),
		}
		
		reverseData[i] = struct{ Key int64; RowNum uint64 }{
			Key:    int64(rowCount - 1 - i), // Reverse sorted
			RowNum: uint64(i),
		}
		
		randomData[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(rand.Intn(rowCount)), // Random
			RowNum: uint64(i),
		}
	}

	// Load data
	cf.LoadIntColumn("sorted", sortedData)
	cf.LoadIntColumn("reverse", reverseData)
	
	col := cf.columns["random"]
	stats, err := col.btree.BulkLoadWithDuplicates(randomData)
	if err != nil {
		t.Fatalf("Failed to load random data: %v", err)
	}
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = stats.TotalKeys
	col.metadata.DistinctCount = stats.DistinctCount
	col.metadata.MinValueOffset = stats.MinValue
	col.metadata.MaxValueOffset = stats.MaxValue
	col.metadata.AverageKeySize = stats.AverageKeySize

	cf.Close()

	// Reopen and test
	cf, err = OpenFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf.Close()

	// Test edge queries
	t.Run("EdgeQueries", func(t *testing.T) {
		// Query first element
		bitmap, err := cf.QueryInt("sorted", 0)
		if err != nil || bitmap.GetCardinality() != 1 {
			t.Errorf("First element query failed")
		}

		// Query last element
		bitmap, err = cf.QueryInt("sorted", int64(rowCount-1))
		if err != nil || bitmap.GetCardinality() != 1 {
			t.Errorf("Last element query failed")
		}

		// Query non-existent element
		bitmap, err = cf.QueryInt("sorted", int64(rowCount+1000))
		if err != nil || bitmap.GetCardinality() != 0 {
			t.Errorf("Non-existent element should return empty result")
		}

		// Large range query (entire dataset)
		bitmap, err = cf.RangeQueryInt("sorted", 0, int64(rowCount-1))
		if err != nil || bitmap.GetCardinality() != uint64(rowCount) {
			t.Errorf("Full range query failed: expected %d, got %d", 
				rowCount, bitmap.GetCardinality())
		}
	})
	
	t.Logf("Large dataset edge cases completed successfully")
}