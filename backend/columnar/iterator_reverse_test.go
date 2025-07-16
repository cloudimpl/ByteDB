package columnar

import (
	"os"
	"testing"
)

// TestReverseIterator tests reverse iteration functionality
func TestReverseIterator(t *testing.T) {
	filename := "test_reverse_iterator.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	// Add columns
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)

	// Add test data
	intData := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 30, RowNum: 1},
		{Value: 20, RowNum: 2},
		{Value: 50, RowNum: 3},
		{Value: 40, RowNum: 4},
	}

	stringData := []StringData{
		{Value: "apple", RowNum: 0},
		{Value: "cherry", RowNum: 1},
		{Value: "banana", RowNum: 2},
		{Value: "elderberry", RowNum: 3},
		{Value: "date", RowNum: 4},
	}

	err = cf.LoadIntColumn("id", intData)
	if err != nil {
		t.Fatal(err)
	}

	err = cf.LoadStringColumn("name", stringData)
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

	// Test reverse iteration from end
	t.Run("ReverseFromEnd", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to last
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed")
		}

		// Collect values in reverse order
		expectedReverse := []int64{50, 40, 30, 20, 10}
		actualReverse := []int64{}

		// Start from last element
		actualReverse = append(actualReverse, iter.Key().(int64))

		// Iterate backwards
		for iter.Prev() {
			actualReverse = append(actualReverse, iter.Key().(int64))
		}

		// Verify reverse order
		if len(actualReverse) != len(expectedReverse) {
			t.Errorf("Expected %d values, got %d", len(expectedReverse), len(actualReverse))
		}

		for i, expected := range expectedReverse {
			if i < len(actualReverse) && actualReverse[i] != expected {
				t.Errorf("At index %d: expected %d, got %d", i, expected, actualReverse[i])
			}
		}

		t.Logf("Reverse iteration from end: %v", actualReverse)
	})

	// Test reverse iteration from middle
	t.Run("ReverseFromMiddle", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to middle value
		if !iter.Seek(int64(30)) {
			t.Fatal("Seek to 30 failed")
		}

		// Verify we're at 30
		if iter.Key().(int64) != 30 {
			t.Errorf("Expected to be at 30, got %d", iter.Key().(int64))
		}

		// Go backwards
		if !iter.Prev() {
			t.Fatal("Prev from 30 failed")
		}

		if iter.Key().(int64) != 20 {
			t.Errorf("Expected 20 after Prev from 30, got %d", iter.Key().(int64))
		}

		// Continue backwards
		if !iter.Prev() {
			t.Fatal("Prev from 20 failed")
		}

		if iter.Key().(int64) != 10 {
			t.Errorf("Expected 10 after Prev from 20, got %d", iter.Key().(int64))
		}

		// Should fail going before first
		if iter.Prev() {
			t.Error("Prev should fail when at first element")
		}
	})

	// Test mixed forward and reverse iteration
	t.Run("MixedForwardReverse", func(t *testing.T) {
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Start from beginning
		if !iter.SeekFirst() {
			t.Fatal("SeekFirst failed")
		}

		// Go forward: 10 -> 20
		if !iter.Next() {
			t.Fatal("Next failed")
		}
		if iter.Key().(int64) != 20 {
			t.Errorf("Expected 20, got %d", iter.Key().(int64))
		}

		// Go forward: 20 -> 30
		if !iter.Next() {
			t.Fatal("Next failed")
		}
		if iter.Key().(int64) != 30 {
			t.Errorf("Expected 30, got %d", iter.Key().(int64))
		}

		// Go backward: 30 -> 20
		if !iter.Prev() {
			t.Fatal("Prev failed")
		}
		if iter.Key().(int64) != 20 {
			t.Errorf("Expected 20, got %d", iter.Key().(int64))
		}

		// Go forward again: 20 -> 30
		if !iter.Next() {
			t.Fatal("Next failed")
		}
		if iter.Key().(int64) != 30 {
			t.Errorf("Expected 30, got %d", iter.Key().(int64))
		}
	})

	// Test reverse iteration with strings
	t.Run("ReverseStringIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("name")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to last
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed for strings")
		}

		// Expected reverse order: elderberry, date, cherry, banana, apple
		expectedReverse := []string{"elderberry", "date", "cherry", "banana", "apple"}
		actualReverse := []string{}

		// Start from last
		actualReverse = append(actualReverse, iter.Key().(string))

		// Iterate backwards
		for iter.Prev() {
			actualReverse = append(actualReverse, iter.Key().(string))
		}

		// Verify
		if len(actualReverse) != len(expectedReverse) {
			t.Errorf("Expected %d strings, got %d", len(expectedReverse), len(actualReverse))
		}

		for i, expected := range expectedReverse {
			if i < len(actualReverse) && actualReverse[i] != expected {
				t.Errorf("At index %d: expected %s, got %s", i, expected, actualReverse[i])
			}
		}

		t.Logf("Reverse string iteration: %v", actualReverse)
	})

	// Test reverse iteration with range bounds
	t.Run("ReverseWithRangeBounds", func(t *testing.T) {
		iter, err := cf.NewRangeIteratorInt("id", 20, 40)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to last within range
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed for range iterator")
		}

		// Should be at 40
		if iter.Key().(int64) != 40 {
			t.Errorf("Expected to be at 40, got %d", iter.Key().(int64))
		}

		// Collect values in reverse within range
		values := []int64{iter.Key().(int64)}
		for iter.Prev() {
			values = append(values, iter.Key().(int64))
		}

		// Should have [40, 30, 20]
		expectedValues := []int64{40, 30, 20}
		if len(values) != len(expectedValues) {
			t.Errorf("Expected %d values in range, got %d", len(expectedValues), len(values))
		}

		for i, expected := range expectedValues {
			if i < len(values) && values[i] != expected {
				t.Errorf("At index %d: expected %d, got %d", i, expected, values[i])
			}
		}

		t.Logf("Reverse range iteration: %v", values)
	})

	// Test edge cases
	t.Run("ReverseEdgeCases", func(t *testing.T) {
		// Test Prev on fresh iterator (not positioned)
		iter, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Prev without positioning should seek to last
		if !iter.Prev() {
			t.Fatal("Prev on fresh iterator should seek to last")
		}

		// Should be at last element (50)
		if iter.Key().(int64) != 50 {
			t.Errorf("Expected to be at 50 (last), got %d", iter.Key().(int64))
		}

		// Test Prev at beginning
		iter2, err := cf.NewIterator("id")
		if err != nil {
			t.Fatal(err)
		}
		defer iter2.Close()

		iter2.SeekFirst()
		if iter2.Prev() {
			t.Error("Prev at first element should return false")
		}
	})
}

// TestReverseIteratorWithDuplicates tests reverse iteration with duplicate values
func TestReverseIteratorWithDuplicates(t *testing.T) {
	filename := "test_reverse_duplicates.bytedb"
	defer os.Remove(filename)

	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("score", DataTypeInt64, false)

	// Add data with duplicates
	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 10, RowNum: 2},
		{Value: 30, RowNum: 3},
		{Value: 20, RowNum: 4},
		{Value: 10, RowNum: 5},
		{Value: 40, RowNum: 6},
		{Value: 20, RowNum: 7},
	}

	err = cf.LoadIntColumn("score", data)
	if err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Reopen
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	iter, err := cf.NewIterator("score")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	// Seek to last
	if !iter.SeekLast() {
		t.Fatal("SeekLast failed")
	}

	// Collect all unique keys in reverse
	reverseKeys := []int64{}
	reverseBitmaps := [][]uint64{}

	// Add last element
	reverseKeys = append(reverseKeys, iter.Key().(int64))
	reverseBitmaps = append(reverseBitmaps, BitmapToSlice(iter.Rows()))

	// Iterate backwards
	for iter.Prev() {
		reverseKeys = append(reverseKeys, iter.Key().(int64))
		reverseBitmaps = append(reverseBitmaps, BitmapToSlice(iter.Rows()))
	}

	// Expected: 40, 30, 20, 10
	expectedKeys := []int64{40, 30, 20, 10}
	if len(reverseKeys) != len(expectedKeys) {
		t.Errorf("Expected %d unique keys, got %d", len(expectedKeys), len(reverseKeys))
	}

	for i, expected := range expectedKeys {
		if i < len(reverseKeys) && reverseKeys[i] != expected {
			t.Errorf("At index %d: expected key %d, got %d", i, expected, reverseKeys[i])
		}
	}

	// Verify bitmaps contain correct row numbers
	// Key 20 should have rows [1, 4, 7]
	for i, key := range reverseKeys {
		if key == 20 {
			expectedRows := 3
			if len(reverseBitmaps[i]) != expectedRows {
				t.Errorf("Key 20 should have %d rows, got %d", expectedRows, len(reverseBitmaps[i]))
			}
			t.Logf("Key 20 has rows: %v", reverseBitmaps[i])
		}
	}

	t.Logf("Reverse iteration with duplicates: %v", reverseKeys)
}

// TestReverseIteratorWithCompression tests reverse iteration with compression
func TestReverseIteratorWithCompression(t *testing.T) {
	filename := "test_reverse_compressed.bytedb"
	defer os.Remove(filename)

	// Create file with compression
	opts := NewCompressionOptions().
		WithPageCompression(CompressionGzip, CompressionLevelBest)

	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("value", DataTypeInt32, false)

	// Add larger dataset
	numRecords := 1000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i * 10), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("value", data)
	if err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Reopen
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	// Test reverse iteration performance
	t.Run("CompressedReversePerformance", func(t *testing.T) {
		iter, err := cf.NewIterator("value")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to last
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed")
		}

		count := 0
		lastValue := int32(10000) // Start with value higher than any in dataset

		// First element
		currentValue := iter.Key().(int32)
		if currentValue >= lastValue {
			t.Error("Values should be in descending order")
		}
		lastValue = currentValue
		count++

		// Iterate backwards through all elements
		for iter.Prev() {
			currentValue = iter.Key().(int32)
			if currentValue >= lastValue {
				t.Errorf("Expected descending order, but %d >= %d", currentValue, lastValue)
			}
			lastValue = currentValue
			count++
		}

		if count != numRecords {
			t.Errorf("Expected %d records, got %d", numRecords, count)
		}

		t.Logf("Successfully iterated %d records in reverse with compression", count)
	})

	// Test reverse range iteration with compression
	t.Run("CompressedReverseRange", func(t *testing.T) {
		// Range from 2000 to 5000
		iter, err := cf.NewRangeIterator("value", int32(2000), int32(5000))
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to last in range
		if !iter.SeekLast() {
			t.Fatal("SeekLast failed for range")
		}

		// Should be at 5000
		if iter.Key().(int32) != 5000 {
			t.Errorf("Expected to be at 5000, got %d", iter.Key().(int32))
		}

		// Count elements going backwards
		count := 1 // Already at one element
		for iter.Prev() {
			count++
		}

		// Should have 301 elements (2000, 2010, ..., 5000)
		expectedCount := 301
		if count != expectedCount {
			t.Errorf("Expected %d elements in range, got %d", expectedCount, count)
		}
	})
}

// TestBitmapIteratorReverse tests that bitmap iterator correctly reports it doesn't support reverse
func TestBitmapIteratorReverse(t *testing.T) {
	filename := "test_bitmap_reverse.bytedb"
	defer os.Remove(filename)

	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("id", DataTypeInt64, false)

	data := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 30, RowNum: 2},
	}

	cf.LoadIntColumn("id", data)
	cf.Close()

	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	// Get a bitmap
	bitmap, err := cf.RangeQueryInt("id", 10, 30)
	if err != nil {
		t.Fatal(err)
	}

	// Create bitmap iterator
	iter, err := cf.BitmapToIterator("id", bitmap)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	// Try Prev - should fail
	if iter.Prev() {
		t.Error("BitmapIterator.Prev() should return false")
	}

	// Check error
	if iter.Error() == nil {
		t.Error("Expected error from BitmapIterator.Prev()")
	}

	t.Logf("BitmapIterator correctly reports reverse iteration not supported")
}