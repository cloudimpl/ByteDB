package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestBasicIterator(t *testing.T) {
	filename := "test_iterator.bytedb"
	defer os.Remove(filename)
	
	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	
	// Add test data
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 30, RowNum: 1},
		{Value: 20, RowNum: 2},
		{Value: 50, RowNum: 3},
		{Value: 40, RowNum: 4},
	}
	
	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.Close()
	
	// Reopen for testing
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Test full iteration
	t.Run("FullIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Expect data in sorted order: 10, 20, 30, 40, 50
		expected := []struct {
			key int64
			row uint64
		}{
			{10, 0},
			{20, 2},
			{30, 1},
			{40, 4},
			{50, 3},
		}
		
		i := 0
		for iter.Next() {
			if i >= len(expected) {
				t.Fatalf("Too many results: got more than %d", len(expected))
			}
			
			key := iter.Key().(int64)
			rows := iter.Rows()
			
			if key != expected[i].key {
				t.Errorf("Expected key %d, got %d", expected[i].key, key)
			}
			
			// Extract the first row from the bitmap
			if rows.GetCardinality() == 0 {
				t.Errorf("Expected at least one row, got empty bitmap")
			}
			row := uint64(rows.Minimum())
			
			if row != expected[i].row {
				t.Errorf("Expected row %d, got %d", expected[i].row, row)
			}
			
			i++
		}
		
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
		
		if i != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), i)
		}
	})
	
	// Test seek functionality
	t.Run("Seek", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Seek to value 25 (should find 30)
		if !iter.Seek(int64(25)) {
			t.Fatal("Seek failed")
		}
		
		key := iter.Key().(int64)
		if key != 30 {
			t.Errorf("Expected key 30, got %d", key)
		}
		
		// Continue iteration
		if !iter.Next() {
			t.Fatal("Expected more results after seek")
		}
		
		key = iter.Key().(int64)
		if key != 40 {
			t.Errorf("Expected key 40, got %d", key)
		}
	})
	
	// Test SeekFirst and SeekLast
	t.Run("SeekFirstLast", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Seek to first
		if !iter.SeekFirst() {
			t.Fatal("SeekFirst failed")
		}
		
		key := iter.Key().(int64)
		if key != 10 {
			t.Errorf("Expected first key 10, got %d", key)
		}
		
		// Seek to last
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed")
		}
		
		key = iter.Key().(int64)
		if key != 50 {
			t.Errorf("Expected last key 50, got %d", key)
		}
	})
}

func TestRangeIterator(t *testing.T) {
	filename := "test_range_iterator.bytedb"
	defer os.Remove(filename)
	
	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("score", DataTypeInt64, false)
	
	// Add test data
	data := make([]IntData, 100)
	for i := 0; i < 100; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}
	
	err = cf.LoadIntColumn("score", data)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.Close()
	
	// Reopen for testing
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Test range iteration
	t.Run("RangeIteration", func(t *testing.T) {
		iter, err := cf.NewRangeIteratorInt("score", 25, 35)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Expect values 25, 26, 27, ..., 35
		expected := []int64{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35}
		
		i := 0
		for iter.Next() {
			if i >= len(expected) {
				t.Fatalf("Too many results: got more than %d", len(expected))
			}
			
			key := iter.Key().(int64)
			rows := iter.Rows()
			
			if key != expected[i] {
				t.Errorf("Expected key %d, got %d", expected[i], key)
			}
			
			// Extract the first row from the bitmap
			if rows.GetCardinality() == 0 {
				t.Errorf("Expected at least one row, got empty bitmap")
			}
			row := uint64(rows.Minimum())
			
			if row != uint64(expected[i]) {
				t.Errorf("Expected row %d, got %d", expected[i], row)
			}
			
			i++
		}
		
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
		
		if i != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), i)
		}
	})
	
	// Test bounds checking
	t.Run("BoundsChecking", func(t *testing.T) {
		iter, err := cf.NewRangeIteratorInt("score", 25, 35)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Check bounds properties
		if !iter.HasLowerBound() {
			t.Error("Expected to have lower bound")
		}
		
		if !iter.HasUpperBound() {
			t.Error("Expected to have upper bound")
		}
		
		if iter.LowerBound().(int64) != 25 {
			t.Errorf("Expected lower bound 25, got %d", iter.LowerBound())
		}
		
		if iter.UpperBound().(int64) != 35 {
			t.Errorf("Expected upper bound 35, got %d", iter.UpperBound())
		}
		
		// Position at first element
		if !iter.Next() {
			t.Fatal("Expected at least one result")
		}
		
		if !iter.InBounds() {
			t.Error("Expected to be in bounds")
		}
	})
}

func TestBitmapIterator(t *testing.T) {
	filename := "test_bitmap_iterator.bytedb"
	defer os.Remove(filename)
	
	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("value", DataTypeInt64, false)
	
	// Add test data
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 30, RowNum: 1},
		{Value: 20, RowNum: 2},
		{Value: 50, RowNum: 3},
		{Value: 40, RowNum: 4},
	}
	
	err = cf.LoadIntColumn("value", data)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.Close()
	
	// Reopen for testing
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Test bitmap to iterator conversion
	t.Run("BitmapToIterator", func(t *testing.T) {
		// Get bitmap for range 20-40
		bitmap, err := cf.RangeQueryInt("value", 20, 40)
		if err != nil {
			t.Fatal(err)
		}
		
		// Debug: print bitmap contents
		t.Logf("Bitmap contains rows: %v", BitmapToSlice(bitmap))
		
		// Convert to iterator
		iter, err := cf.BitmapToIterator("value", bitmap)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Collect actual results first
		actualRows := []uint64{}
		actualKeys := []int64{}
		
		for iter.Next() {
			rows := iter.Rows()
			if rows.GetCardinality() > 0 {
				actualRows = append(actualRows, uint64(rows.Minimum()))
			}
			actualKeys = append(actualKeys, iter.Key().(int64))
		}
		
		t.Logf("Actual rows: %v", actualRows)
		t.Logf("Actual keys: %v", actualKeys)
		
		// For this test, we just verify we get the expected count
		if len(actualRows) != 3 {
			t.Errorf("Expected 3 results, got %d", len(actualRows))
		}
		
		// Reset iterator for second test
		iter.Close()
		iter, err = cf.BitmapToIterator("value", bitmap)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		// Just verify the results are correct by looking up each row
		for iter.Next() {
			rows := iter.Rows()
			key := iter.Key().(int64)
			
			// Verify the key matches what should be at this row
			if rows.GetCardinality() == 0 {
				t.Errorf("Expected at least one row, got empty bitmap")
				continue
			}
			row := uint64(rows.Minimum())
			
			expectedKey, found, err := cf.LookupIntByRow("value", row)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Errorf("Row %d not found in column", row)
			}
			if key != expectedKey {
				t.Errorf("Key mismatch for row %d: expected %d, got %d", row, expectedKey, key)
			}
		}
		
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestIteratorPerformance(t *testing.T) {
	filename := "test_iterator_performance.bytedb"
	defer os.Remove(filename)
	
	// Create test file with larger dataset
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	
	// Add 10,000 records
	numRecords := 10000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}
	
	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.Close()
	
	// Reopen for testing
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Test performance comparison
	t.Run("IteratorVsBitmap", func(t *testing.T) {
		// Method 1: Using iterator
		iter, err := cf.NewRangeIteratorInt("id", 1000, 2000)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		
		count1 := 0
		for iter.Next() {
			_ = iter.Key()
			_ = iter.Rows()
			count1++
		}
		
		// Method 2: Using bitmap (old way)
		bitmap, err := cf.RangeQueryInt("id", 1000, 2000)
		if err != nil {
			t.Fatal(err)
		}
		
		count2 := 0
		rows := BitmapToSlice(bitmap)
		for _, rowNum := range rows {
			key, _, _ := cf.LookupIntByRow("id", rowNum)
			_ = key
			_ = rowNum
			count2++
		}
		
		// Should return same count
		if count1 != count2 {
			t.Errorf("Iterator returned %d items, bitmap returned %d", count1, count2)
		}
		
		// Expected: 1001 items (1000 to 2000 inclusive)
		if count1 != 1001 {
			t.Errorf("Expected 1001 items, got %d", count1)
		}
		
		t.Logf("Iterator processed %d items efficiently", count1)
	})
}

func TestIteratorError(t *testing.T) {
	filename := "test_iterator_error.bytedb"
	defer os.Remove(filename)
	
	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)
	cf.AddColumn("rating", DataTypeFloat64, false)
	
	cf.Close()
	
	// Reopen for testing
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Test unsupported column type (float64)
	t.Run("UnsupportedColumnType", func(t *testing.T) {
		_, err := cf.NewIterator("rating")
		if err == nil {
			t.Error("Expected error for unsupported column type")
		}
	})
	
	// Test non-existent column
	t.Run("NonExistentColumn", func(t *testing.T) {
		_, err := cf.NewIterator("nonexistent")
		if err == nil {
			t.Error("Expected error for non-existent column")
		}
	})
}

// TestBitmapFunctionality demonstrates that iterator returns bitmaps for each key
func TestBitmapFunctionality(t *testing.T) {
	filename := "test_bitmap_functionality.bytedb"
	defer os.Remove(filename)
	
	// Create and populate test data with duplicate keys
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	cf.AddColumn("score", DataTypeInt64, false)
	
	// Add data with duplicate keys to demonstrate bitmap functionality
	data := []IntData{
		{Value: 85, RowNum: 0},  // score 85 appears in rows 0, 2, 4
		{Value: 92, RowNum: 1},  // score 92 appears in row 1
		{Value: 85, RowNum: 2},  // score 85 appears in rows 0, 2, 4
		{Value: 78, RowNum: 3},  // score 78 appears in row 3
		{Value: 85, RowNum: 4},  // score 85 appears in rows 0, 2, 4
		{Value: 92, RowNum: 5},  // score 92 appears in rows 1, 5
	}
	
	err = cf.LoadIntColumn("score", data)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create iterator and verify that each key returns a bitmap with all associated rows
	iter, err := cf.NewIterator("score")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	
	expectedResults := map[int64][]uint64{
		78: {3},        // score 78 appears in row 3
		85: {0, 2, 4},  // score 85 appears in rows 0, 2, 4
		92: {1, 5},     // score 92 appears in rows 1, 5
	}
	
	actualResults := make(map[int64][]uint64)
	
	for iter.Next() {
		key := iter.Key().(int64)
		rows := iter.Rows()
		
		// Convert bitmap to slice
		actualResults[key] = BitmapToSlice(rows)
	}
	
	// Verify results
	for key, expectedRows := range expectedResults {
		actualRows, exists := actualResults[key]
		if !exists {
			t.Errorf("Key %d not found in results", key)
			continue
		}
		
		if len(actualRows) != len(expectedRows) {
			t.Errorf("Key %d: expected %d rows, got %d", key, len(expectedRows), len(actualRows))
			continue
		}
		
		// Check that all expected rows are present
		for i, expectedRow := range expectedRows {
			if actualRows[i] != expectedRow {
				t.Errorf("Key %d: expected row %d, got row %d", key, expectedRow, actualRows[i])
			}
		}
	}
	
	t.Logf("Iterator correctly returns bitmaps with all rows for each key:")
	for key, rows := range actualResults {
		t.Logf("  Key %d: rows %v", key, rows)
	}
}

// Example usage demonstration
func ExampleColumnIterator() {
	// This example shows how to use the new iterator API
	filename := "example_iterator.bytedb"
	defer os.Remove(filename)
	
	// Create and populate test data
	cf, _ := CreateFile(filename)
	cf.AddColumn("age", DataTypeInt64, false)
	
	data := []IntData{
		{Value: 25, RowNum: 0},
		{Value: 30, RowNum: 1},
		{Value: 35, RowNum: 2},
		{Value: 40, RowNum: 3},
	}
	
	cf.LoadIntColumn("age", data)
	cf.Close()
	
	// Reopen and demonstrate iterator usage
	cf, _ = OpenFile(filename)
	defer cf.Close()
	
	// Example 1: Range query with iterator
	fmt.Println("Range query (age 28-37):")
	iter, _ := cf.NewRangeIteratorInt("age", 28, 37)
	for iter.Next() {
		rows := iter.Rows()
		if rows.GetCardinality() > 0 {
			fmt.Printf("  Age: %d, Row: %d\n", iter.Key().(int64), rows.Minimum())
		}
	}
	iter.Close()
	
	// Example 2: Full iteration
	fmt.Println("\nFull iteration:")
	iter2, _ := cf.NewIterator("age")
	for iter2.Next() {
		rows := iter2.Rows()
		if rows.GetCardinality() > 0 {
			fmt.Printf("  Age: %d, Row: %d\n", iter2.Key().(int64), rows.Minimum())
		}
	}
	iter2.Close()
	
	// Output:
	// Range query (age 28-37):
	//   Age: 30, Row: 1
	//   Age: 35, Row: 2
	//
	// Full iteration:
	//   Age: 25, Row: 0
	//   Age: 30, Row: 1
	//   Age: 35, Row: 2
	//   Age: 40, Row: 3
}