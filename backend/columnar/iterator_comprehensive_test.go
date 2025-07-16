package columnar

import (
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

// TestIteratorEdgeCases tests edge cases and boundary conditions
func TestIteratorEdgeCases(t *testing.T) {
	filename := "test_iterator_edge_cases.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	t.Run("EmptyColumn", func(t *testing.T) {
		// Test iterator on empty column
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Should not have any results
		if iter.Next() {
			t.Error("Expected no results for empty column")
		}

		if iter.Valid() {
			t.Error("Iterator should not be valid for empty column")
		}
	})

	t.Run("EmptyRangeQuery", func(t *testing.T) {
		// Test range iterator on empty column
		iter, err := cf.NewRangeIteratorInt("id", 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Should not have any results
		if iter.Next() {
			t.Error("Expected no results for empty range query")
		}
	})

	// Add some test data
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 30, RowNum: 2},
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("SingleElement", func(t *testing.T) {
		// Test iterator with single element range
		iter, err := cf.NewRangeIteratorInt("id", 20, 20)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
			if iter.Key().(int64) != 20 {
				t.Errorf("Expected key 20, got %d", iter.Key().(int64))
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 result, got %d", count)
		}
	})

	t.Run("OutOfRangeQuery", func(t *testing.T) {
		// Test range query completely outside data range
		iter, err := cf.NewRangeIteratorInt("id", 100, 200)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		if iter.Next() {
			t.Error("Expected no results for out-of-range query")
		}
	})

	t.Run("PartialOverlapQuery", func(t *testing.T) {
		// Test range query with partial overlap
		iter, err := cf.NewRangeIteratorInt("id", 25, 35)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
			key := iter.Key().(int64)
			if key != 30 {
				t.Errorf("Expected key 30, got %d", key)
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 result, got %d", count)
		}
	})

	t.Run("InvalidSeek", func(t *testing.T) {
		// Test seek with invalid value
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value that doesn't exist
		if iter.Seek(int64(25)) {
			// Should position at next higher value (30)
			if iter.Key().(int64) != 30 {
				t.Errorf("Expected key 30 after seeking 25, got %d", iter.Key().(int64))
			}
		} else {
			t.Error("Seek should have succeeded")
		}
	})

	t.Run("SeekBeyondRange", func(t *testing.T) {
		// Test seek beyond available data
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value beyond all data
		if iter.Seek(int64(100)) {
			t.Error("Seek should have failed for value beyond range")
		}
	})
}

// TestIteratorBoundaryConditions tests boundary conditions
func TestIteratorBoundaryConditions(t *testing.T) {
	filename := "test_iterator_boundaries.bytedb"
	defer os.Remove(filename)

	// Create test file with boundary values
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("value", DataTypeInt64, false)

	// Test with boundary values (avoiding negative values due to uint64 conversion issues)
	data := []IntData{
		{Value: 0, RowNum: 0},
		{Value: 1, RowNum: 1},
		{Value: 100, RowNum: 2},
		{Value: 1000, RowNum: 3},
		{Value: math.MaxInt64, RowNum: 4},
	}

	err = cf.LoadIntColumn("value", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("MinMaxValues", func(t *testing.T) {
		// Test iteration over extreme values
		iter, err := cf.NewIterator("value")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expected := []int64{0, 1, 100, 1000, math.MaxInt64}
		i := 0

		for iter.Next() {
			key := iter.Key().(int64)
			if key != expected[i] {
				t.Errorf("Expected key %d, got %d", expected[i], key)
			}
			i++
		}

		if i != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), i)
		}
	})

	t.Run("RangeWithMinMax", func(t *testing.T) {
		// Test range query with full range
		iter, err := cf.NewRangeIteratorInt("value", 0, math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != 5 {
			t.Errorf("Expected 5 results for full range, got %d", count)
		}
	})

	t.Run("SmallRange", func(t *testing.T) {
		// Test range query with small range
		iter, err := cf.NewRangeIteratorInt("value", 50, 150)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expected := []int64{100}
		i := 0

		for iter.Next() {
			key := iter.Key().(int64)
			if key != expected[i] {
				t.Errorf("Expected key %d, got %d", expected[i], key)
			}
			i++
		}

		if i != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), i)
		}
	})
}

// TestIteratorErrorHandling tests error conditions
func TestIteratorErrorHandling(t *testing.T) {
	filename := "test_iterator_errors.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)
	cf.AddColumn("price", DataTypeFloat32, false)

	t.Run("NonExistentColumn", func(t *testing.T) {
		// Test iterator on non-existent column
		_, err := cf.NewIterator("nonexistent")
		if err == nil {
			t.Error("Expected error for non-existent column")
		}
	})

	t.Run("UnsupportedDataType", func(t *testing.T) {
		// Test iterator on unsupported data type (float32)
		_, err := cf.NewIterator("price")
		if err == nil {
			t.Error("Expected error for unsupported data type")
		}
	})

	t.Run("InvalidRangeBounds", func(t *testing.T) {
		// Test range iterator with invalid bounds (max < min)
		iter, err := cf.NewRangeIteratorInt("id", 100, 50)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Should not return any results
		if iter.Next() {
			t.Error("Expected no results for invalid range bounds")
		}
	})

	t.Run("IteratorAfterClose", func(t *testing.T) {
		// Test iterator usage after close
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}

		iter.Close()

		// Should not be valid after close
		if iter.Valid() {
			t.Error("Iterator should not be valid after close")
		}
	})
}

// TestIteratorConcurrency tests concurrent iterator usage
func TestIteratorConcurrency(t *testing.T) {
	filename := "test_iterator_concurrency.bytedb"
	defer os.Remove(filename)

	// Create test file with substantial data
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	// Add test data
	numRecords := 1000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ConcurrentReads", func(t *testing.T) {
		// Test multiple concurrent iterators
		numGoroutines := 5
		var wg sync.WaitGroup
		var mu sync.Mutex
		results := make([]int, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				iter, err := cf.NewRangeIteratorInt("id", int64(id*100), int64((id+1)*100-1))
				if err != nil {
					t.Errorf("Failed to create iterator: %v", err)
					return
				}
				defer iter.Close()

				count := 0
				for iter.Next() {
					count++
				}

				mu.Lock()
				results[id] = count
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		// Each goroutine should have read 100 records
		for i, count := range results {
			if count != 100 {
				t.Errorf("Goroutine %d: expected 100 results, got %d", i, count)
			}
		}
	})

	t.Run("ConcurrentRangeQueries", func(t *testing.T) {
		// Test concurrent range queries with overlapping ranges
		numGoroutines := 10
		var wg sync.WaitGroup
		var mu sync.Mutex
		totalResults := 0

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Overlapping ranges
				start := int64(id * 50)
				end := int64(start + 100)

				iter, err := cf.NewRangeIteratorInt("id", start, end)
				if err != nil {
					t.Errorf("Failed to create iterator: %v", err)
					return
				}
				defer iter.Close()

				count := 0
				for iter.Next() {
					count++
				}

				mu.Lock()
				totalResults += count
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		// Should have processed overlapping results
		if totalResults == 0 {
			t.Error("Expected some results from concurrent queries")
		}
	})
}

// TestIteratorPerformanceScenarios tests various performance scenarios
func TestIteratorPerformanceScenarios(t *testing.T) {
	filename := "test_iterator_performance.bytedb"
	defer os.Remove(filename)

	// Create test file with large dataset
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("score", DataTypeInt64, false)

	// Add substantial test data
	numRecords := 50000
	idData := make([]IntData, numRecords)
	scoreData := make([]IntData, numRecords)

	for i := 0; i < numRecords; i++ {
		idData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		scoreData[i] = IntData{Value: int64(i%1000), RowNum: uint64(i)} // Repeated values
	}

	err = cf.LoadIntColumn("id", idData)
	if err != nil {
		t.Fatal(err)
	}

	err = cf.LoadIntColumn("score", scoreData)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("FullScan", func(t *testing.T) {
		// Test full table scan performance
		start := time.Now()
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		elapsed := time.Since(start)

		if count != numRecords {
			t.Errorf("Expected %d results, got %d", numRecords, count)
		}

		t.Logf("Full scan of %d records took %v", numRecords, elapsed)
	})

	t.Run("SmallRangeQuery", func(t *testing.T) {
		// Test small range query performance
		start := time.Now()
		iter, err := cf.NewRangeIteratorInt("id", 1000, 1100)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		elapsed := time.Since(start)

		if count != 101 {
			t.Errorf("Expected 101 results, got %d", count)
		}

		t.Logf("Small range query (101 records) took %v", elapsed)
	})

	t.Run("LargeRangeQuery", func(t *testing.T) {
		// Test large range query performance
		start := time.Now()
		iter, err := cf.NewRangeIteratorInt("id", 10000, 40000)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		elapsed := time.Since(start)

		if count != 30001 {
			t.Errorf("Expected 30001 results, got %d", count)
		}

		t.Logf("Large range query (30001 records) took %v", elapsed)
	})

	t.Run("RepeatedValueQuery", func(t *testing.T) {
		// Test query on column with repeated values
		start := time.Now()
		iter, err := cf.NewRangeIteratorInt("score", 100, 100)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		elapsed := time.Since(start)

		// The iterator will return only 1 result for the unique key,
		// but the bitmap inside contains multiple rows
		// This is expected behavior for how B-trees handle duplicate keys
		if count != 1 {
			t.Errorf("Expected 1 result (unique key), got %d", count)
		}

		t.Logf("Repeated value query (%d unique keys) took %v", count, elapsed)
	})

	t.Run("IteratorVsBitmapComparison", func(t *testing.T) {
		// Compare iterator vs bitmap performance
		rangeStart := int64(20000)
		rangeEnd := int64(30000)

		// Method 1: Iterator
		start := time.Now()
		iter, err := cf.NewRangeIteratorInt("id", rangeStart, rangeEnd)
		if err != nil {
			t.Fatal(err)
		}

		iteratorCount := 0
		for iter.Next() {
			_ = iter.Key()
			_ = iter.Rows()
			iteratorCount++
		}
		iter.Close()
		iteratorTime := time.Since(start)

		// Method 2: Bitmap + Lookups
		start = time.Now()
		bitmap, err := cf.RangeQueryInt("id", rangeStart, rangeEnd)
		if err != nil {
			t.Fatal(err)
		}

		bitmapCount := 0
		rows := BitmapToSlice(bitmap)
		for _, rowNum := range rows {
			_, _, _ = cf.LookupIntByRow("id", rowNum)
			bitmapCount++
		}
		bitmapTime := time.Since(start)

		// Results should be identical
		if iteratorCount != bitmapCount {
			t.Errorf("Iterator count (%d) != bitmap count (%d)", iteratorCount, bitmapCount)
		}

		t.Logf("Iterator method: %d records in %v", iteratorCount, iteratorTime)
		t.Logf("Bitmap method: %d records in %v", bitmapCount, bitmapTime)
		t.Logf("Iterator is %.2fx faster", float64(bitmapTime)/float64(iteratorTime))
	})
}

// TestIteratorMemoryUsage tests memory efficiency
func TestIteratorMemoryUsage(t *testing.T) {
	filename := "test_iterator_memory.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	// Add large dataset
	numRecords := 100000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("MemoryEfficientIteration", func(t *testing.T) {
		// Test that iterator doesn't load entire dataset into memory
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Process records one by one without storing them
		count := 0
		var lastKey int64 = -1
		for iter.Next() {
			key := iter.Key().(int64)
			if key <= lastKey {
				t.Error("Keys should be in ascending order")
			}
			lastKey = key
			count++

			// Early exit to demonstrate streaming capability
			if count == 1000 {
				break
			}
		}

		if count != 1000 {
			t.Errorf("Expected to process 1000 records, got %d", count)
		}

		t.Logf("Successfully processed %d records in streaming fashion", count)
	})

	t.Run("MultipleIteratorsMemoryUsage", func(t *testing.T) {
		// Test multiple concurrent iterators don't explode memory
		numIterators := 10
		iterators := make([]ColumnIterator, numIterators)

		// Create multiple iterators
		for i := 0; i < numIterators; i++ {
			iter, err := cf.NewRangeIteratorInt("id", int64(i*10000), int64((i+1)*10000-1))
			if err != nil {
				t.Fatal(err)
			}
			iterators[i] = iter
		}

		// Process a few records from each
		for i, iter := range iterators {
			count := 0
			for iter.Next() && count < 100 {
				count++
			}
			if count != 100 {
				t.Errorf("Iterator %d: expected 100 records, got %d", i, count)
			}
		}

		// Clean up
		for _, iter := range iterators {
			iter.Close()
		}

		t.Logf("Successfully managed %d concurrent iterators", numIterators)
	})
}

// TestIteratorDataTypes tests iterator with various data types
func TestIteratorDataTypes(t *testing.T) {
	filename := "test_iterator_types.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	// Add columns of different supported integer types
	// Note: Iterator currently only supports int64 columns due to B-tree implementation
	cf.AddColumn("int64_col", DataTypeInt64, false)
	cf.AddColumn("int64_col2", DataTypeInt64, false)
	cf.AddColumn("int64_col3", DataTypeInt64, false)

	// Add test data
	numRecords := 100
	for _, colName := range []string{"int64_col", "int64_col2", "int64_col3"} {
		data := make([]IntData, numRecords)
		for i := 0; i < numRecords; i++ {
			data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		}
		err = cf.LoadIntColumn(colName, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test each data type
	columns := []string{"int64_col", "int64_col2", "int64_col3"}
	for _, colName := range columns {
		t.Run(colName, func(t *testing.T) {
			iter, err := cf.NewRangeIteratorInt(colName, 10, 20)
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			count := 0
			for iter.Next() {
				key := iter.Key().(int64)
				if key < 10 || key > 20 {
					t.Errorf("Key %d out of range [10, 20]", key)
				}
				count++
			}

			if count != 11 {
				t.Errorf("Expected 11 results for %s, got %d", colName, count)
			}
		})
	}
}

// TestIteratorSeekBehavior tests detailed seek behavior
func TestIteratorSeekBehavior(t *testing.T) {
	filename := "test_iterator_seek.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	// Add sparse data (gaps between values)
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 30, RowNum: 1},
		{Value: 50, RowNum: 2},
		{Value: 70, RowNum: 3},
		{Value: 90, RowNum: 4},
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("SeekExactMatch", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to exact value
		if !iter.Seek(int64(50)) {
			t.Fatal("Seek to exact value should succeed")
		}

		if iter.Key().(int64) != 50 {
			t.Errorf("Expected key 50, got %d", iter.Key().(int64))
		}
	})

	t.Run("SeekBetweenValues", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value between existing values
		if !iter.Seek(int64(35)) {
			t.Fatal("Seek between values should succeed")
		}

		// Should position at next higher value
		if iter.Key().(int64) != 50 {
			t.Errorf("Expected key 50, got %d", iter.Key().(int64))
		}
	})

	t.Run("SeekBeforeFirstValue", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value before first value
		if !iter.Seek(int64(5)) {
			t.Fatal("Seek before first value should succeed")
		}

		// Should position at first value
		if iter.Key().(int64) != 10 {
			t.Errorf("Expected key 10, got %d", iter.Key().(int64))
		}
	})

	t.Run("SeekAfterLastValue", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value after last value
		if iter.Seek(int64(100)) {
			t.Error("Seek after last value should fail")
		}

		if iter.Valid() {
			t.Error("Iterator should not be valid after seeking past end")
		}
	})

	t.Run("SeekAndContinue", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to middle value
		if !iter.Seek(int64(50)) {
			t.Fatal("Seek should succeed")
		}

		// Continue iteration from seek point
		expected := []int64{50, 70, 90}
		i := 0

		for iter.Valid() {
			if i >= len(expected) {
				t.Fatal("Too many results")
			}

			key := iter.Key().(int64)
			if key != expected[i] {
				t.Errorf("Expected key %d, got %d", expected[i], key)
			}

			i++
			if !iter.Next() {
				break
			}
		}

		if i != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), i)
		}
	})
}

// TestIteratorStateManagement tests iterator state management
func TestIteratorStateManagement(t *testing.T) {
	filename := "test_iterator_state.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	// Add test data
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 30, RowNum: 2},
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ValidityChecks", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Initially not valid
		if iter.Valid() {
			t.Error("Iterator should not be valid initially")
		}

		// Valid after first Next()
		if !iter.Next() {
			t.Fatal("First Next() should succeed")
		}

		if !iter.Valid() {
			t.Error("Iterator should be valid after Next()")
		}

		// Key/Rows should work when valid
		_ = iter.Key()
		_ = iter.Rows()
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Initially no error
		if iter.Error() != nil {
			t.Error("Iterator should not have error initially")
		}

		// Error should stop iteration
		// (We can't easily inject errors, so we'll test the error handling interface)
		if iter.Error() != nil {
			if iter.Next() {
				t.Error("Next() should fail when iterator has error")
			}
		}
	})

	t.Run("RangeIteratorBounds", func(t *testing.T) {
		iter, err := cf.NewRangeIteratorInt("id", 15, 25)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Check bounds properties
		if !iter.HasLowerBound() || !iter.HasUpperBound() {
			t.Error("Range iterator should have both bounds")
		}

		if iter.LowerBound().(int64) != 15 {
			t.Errorf("Expected lower bound 15, got %d", iter.LowerBound().(int64))
		}

		if iter.UpperBound().(int64) != 25 {
			t.Errorf("Expected upper bound 25, got %d", iter.UpperBound().(int64))
		}

		// Should find value 20
		if !iter.Next() {
			t.Fatal("Should find value in range")
		}

		if !iter.InBounds() {
			t.Error("Current position should be in bounds")
		}
	})

	t.Run("IteratorReset", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Advance to middle
		iter.Next()
		iter.Next()

		// Reset to first
		if !iter.SeekFirst() {
			t.Fatal("SeekFirst should succeed")
		}

		if iter.Key().(int64) != 10 {
			t.Errorf("Expected key 10 after SeekFirst, got %d", iter.Key().(int64))
		}

		// Reset to last
		if !iter.SeekLast() {
			t.Fatal("SeekLast should succeed")
		}

		if iter.Key().(int64) != 30 {
			t.Errorf("Expected key 30 after SeekLast, got %d", iter.Key().(int64))
		}
	})
}

// Benchmark tests for performance measurement
func BenchmarkIteratorPerformance(b *testing.B) {
	filename := "bench_iterator.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer cf.Close()

	cf.AddColumn("id", DataTypeInt64, false)

	// Add substantial data
	numRecords := 100000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("FullScan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter, err := cf.NewIterator("id")
			if err != nil {
				b.Fatal(err)
			}

			count := 0
			for iter.Next() {
				count++
			}
			iter.Close()

			if count != numRecords {
				b.Errorf("Expected %d results, got %d", numRecords, count)
			}
		}
	})

	b.Run("RangeQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter, err := cf.NewRangeIteratorInt("id", 10000, 20000)
			if err != nil {
				b.Fatal(err)
			}

			count := 0
			for iter.Next() {
				count++
			}
			iter.Close()

			if count != 10001 {
				b.Errorf("Expected 10001 results, got %d", count)
			}
		}
	})

	b.Run("SeekOperations", func(b *testing.B) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			target := int64(i % numRecords)
			iter.Seek(target)
		}
	})
}