package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestIteratorFreshOpenCompressed tests iterator functionality after closing and reopening compressed files
func TestIteratorFreshOpenCompressed(t *testing.T) {
	compressionModes := []struct {
		name      string
		compType  CompressionType
		compLevel CompressionLevel
	}{
		{"NoCompression", CompressionNone, CompressionLevelDefault},
		{"GzipDefault", CompressionGzip, CompressionLevelDefault},
		{"GzipBest", CompressionGzip, CompressionLevelBest},
		{"SnappyDefault", CompressionSnappy, CompressionLevelDefault},
		{"ZstdDefault", CompressionZstd, CompressionLevelDefault},
		{"ZstdBest", CompressionZstd, CompressionLevelBest},
	}

	// Test data that we'll write once and read multiple times
	testData := []IntData{
		{Value: 100, RowNum: 0},
		{Value: 200, RowNum: 1},
		{Value: 100, RowNum: 2},
		{Value: 300, RowNum: 3},
		{Value: 200, RowNum: 4},
		{Value: 100, RowNum: 5},
		{Value: 400, RowNum: 6},
		{Value: 200, RowNum: 7},
		{Value: 500, RowNum: 8},
		{Value: 100, RowNum: 9},
	}

	expectedResults := map[int64][]uint64{
		100: {0, 2, 5, 9},
		200: {1, 4, 7},
		300: {3},
		400: {6},
		500: {8},
	}

	for _, mode := range compressionModes {
		t.Run(mode.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_fresh_open_%s.bytedb", mode.name)
			defer os.Remove(filename)

			// STEP 1: Create file with compression and write data
			t.Run("CreateAndWrite", func(t *testing.T) {
				var cf *ColumnarFile
				var err error

				if mode.compType == CompressionNone {
					cf, err = CreateFile(filename)
				} else {
					opts := NewCompressionOptions().
						WithPageCompression(mode.compType, mode.compLevel)
					cf, err = CreateFileWithOptions(filename, opts)
				}

				if err != nil {
					t.Fatalf("Failed to create file: %v", err)
				}

				// Add column
				if err := cf.AddColumn("value", DataTypeInt64, false); err != nil {
					t.Fatal(err)
				}

				// Load data
				if err := cf.LoadIntColumn("value", testData); err != nil {
					t.Fatal(err)
				}

				// IMPORTANT: Close the file completely
				if err := cf.Close(); err != nil {
					t.Fatalf("Failed to close file: %v", err)
				}

				t.Logf("Created and closed %s file", mode.name)
			})

			// STEP 2: Open file fresh and test basic iteration
			t.Run("FreshOpenBasicIteration", func(t *testing.T) {
				// Open the file fresh
				cf, err := OpenFile(filename)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer cf.Close()

				// Create iterator
				iter, err := cf.NewIterator("value")
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				// Collect results
				actualResults := make(map[int64][]uint64)
				for iter.Next() {
					key := iter.Key().(int64)
					rows := iter.Rows()
					actualResults[key] = BitmapToSlice(rows)
				}

				// Verify results
				if len(actualResults) != len(expectedResults) {
					t.Errorf("Expected %d unique keys, got %d", len(expectedResults), len(actualResults))
				}

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

					for i, expectedRow := range expectedRows {
						if actualRows[i] != expectedRow {
							t.Errorf("Key %d: expected row %d at index %d, got %d", 
								key, expectedRow, i, actualRows[i])
						}
					}
				}

				t.Logf("Fresh open basic iteration passed for %s", mode.name)
			})

			// STEP 3: Open file fresh again and test range iteration
			t.Run("FreshOpenRangeIteration", func(t *testing.T) {
				// Open the file fresh again
				cf, err := OpenFile(filename)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer cf.Close()

				// Create range iterator
				iter, err := cf.NewRangeIteratorInt("value", 150, 350)
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				// Expected keys in range [150, 350]
				expectedKeysInRange := []int64{200, 300}
				actualKeysInRange := []int64{}

				for iter.Next() {
					key := iter.Key().(int64)
					actualKeysInRange = append(actualKeysInRange, key)

					// Verify key is in range
					if key < 150 || key > 350 {
						t.Errorf("Key %d is out of range [150, 350]", key)
					}
				}

				if len(actualKeysInRange) != len(expectedKeysInRange) {
					t.Errorf("Expected %d keys in range, got %d", 
						len(expectedKeysInRange), len(actualKeysInRange))
				}

				for i, expectedKey := range expectedKeysInRange {
					if i < len(actualKeysInRange) && actualKeysInRange[i] != expectedKey {
						t.Errorf("Expected key %d at index %d, got %d", 
							expectedKey, i, actualKeysInRange[i])
					}
				}

				t.Logf("Fresh open range iteration passed for %s", mode.name)
			})

			// STEP 4: Open file fresh once more and test bitmap operations
			t.Run("FreshOpenBitmapOperations", func(t *testing.T) {
				// Open the file fresh again
				cf, err := OpenFile(filename)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer cf.Close()

				// Get bitmap for a range query
				bitmap, err := cf.RangeQueryInt("value", 100, 300)
				if err != nil {
					t.Fatal(err)
				}

				// Convert bitmap to iterator
				iter, err := cf.BitmapToIterator("value", bitmap)
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				// Count results
				count := 0
				keysSeen := make(map[int64]bool)
				for iter.Next() {
					key := iter.Key().(int64)
					if key < 100 || key > 300 {
						t.Errorf("Key %d is out of range [100, 300]", key)
					}
					keysSeen[key] = true
					count++
				}

				// Should see keys 100, 200, 300
				expectedKeys := []int64{100, 200, 300}
				for _, expectedKey := range expectedKeys {
					if !keysSeen[expectedKey] {
						t.Errorf("Expected to see key %d in bitmap results", expectedKey)
					}
				}

				// Count should be sum of rows for keys 100, 200, 300
				expectedCount := len(expectedResults[100]) + len(expectedResults[200]) + len(expectedResults[300])
				if count != expectedCount {
					t.Errorf("Expected %d results from bitmap iterator, got %d", expectedCount, count)
				}

				t.Logf("Fresh open bitmap operations passed for %s", mode.name)
			})

			// STEP 5: Verify file info shows correct compression
			t.Run("VerifyCompressionMetadata", func(t *testing.T) {
				cf, err := OpenFile(filename)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer cf.Close()

				// Check that compression settings were preserved
				if cf.header.DefaultCompression != uint8(mode.compType) {
					t.Errorf("Expected compression type %d, got %d", 
						mode.compType, cf.header.DefaultCompression)
				}

				t.Logf("Compression metadata verified for %s", mode.name)
			})
		})
	}
}

// TestIteratorMultipleFreshOpens tests opening the same file multiple times in sequence
func TestIteratorMultipleFreshOpens(t *testing.T) {
	filename := "test_multiple_fresh_opens.bytedb"
	defer os.Remove(filename)

	// Create compressed file with test data
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelBest)

	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("id", DataTypeInt64, false)

	// Add larger dataset
	numRecords := 1000
	data := make([]IntData, numRecords)
	for i := 0; i < numRecords; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("id", data)
	if err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Open and iterate multiple times
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("OpenAttempt%d", i+1), func(t *testing.T) {
			cf, err := OpenFile(filename)
			if err != nil {
				t.Fatalf("Failed to open file on attempt %d: %v", i+1, err)
			}
			defer cf.Close()

			// Test iteration
			iter, err := cf.NewIterator("id")
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			count := 0
			lastKey := int64(-1)
			for iter.Next() {
				key := iter.Key().(int64)
				if key <= lastKey {
					t.Errorf("Keys not in order: %d <= %d", key, lastKey)
				}
				lastKey = key
				count++
			}

			if count != numRecords {
				t.Errorf("Attempt %d: Expected %d records, got %d", i+1, numRecords, count)
			}

			// Also test a range query
			rangeIter, err := cf.NewRangeIteratorInt("id", 100, 200)
			if err != nil {
				t.Fatal(err)
			}
			defer rangeIter.Close()

			rangeCount := 0
			for rangeIter.Next() {
				rangeCount++
			}

			expectedRangeCount := 101 // 100 to 200 inclusive
			if rangeCount != expectedRangeCount {
				t.Errorf("Attempt %d: Expected %d range results, got %d", 
					i+1, expectedRangeCount, rangeCount)
			}

			t.Logf("Open attempt %d successful: %d total records, %d range records", 
				i+1, count, rangeCount)
		})
	}
}