package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestIteratorWithCompression tests that iterator functionality works correctly with compression
func TestIteratorWithCompression(t *testing.T) {
	compressionTypes := []struct {
		name       string
		compType   CompressionType
		compLevel  CompressionLevel
	}{
		{"NoCompression", CompressionNone, CompressionLevelDefault},
		{"GzipDefault", CompressionGzip, CompressionLevelDefault},
		{"GzipFastest", CompressionGzip, CompressionLevelFastest},
		{"GzipBest", CompressionGzip, CompressionLevelBest},
		{"Snappy", CompressionSnappy, CompressionLevelDefault},
		{"ZstdDefault", CompressionZstd, CompressionLevelDefault},
		{"ZstdFastest", CompressionZstd, CompressionLevelFastest},
		{"ZstdBest", CompressionZstd, CompressionLevelBest},
	}

	for _, compTest := range compressionTypes {
		t.Run(compTest.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_iterator_compression_%s.bytedb", compTest.name)
			defer os.Remove(filename)

			// Create compression options
			opts := NewCompressionOptions().
				WithPageCompression(compTest.compType, compTest.compLevel)

			// Create file with compression
			cf, err := CreateFileWithOptions(filename, opts)
			if err != nil {
				t.Fatalf("Failed to create file with compression: %v", err)
			}

			// Add column
			if err := cf.AddColumn("score", DataTypeInt64, false); err != nil {
				t.Fatal(err)
			}

			// Add test data with duplicate keys to test bitmap functionality
			data := []IntData{
				{Value: 85, RowNum: 0},  // score 85 appears in rows 0, 2, 4
				{Value: 92, RowNum: 1},  // score 92 appears in row 1
				{Value: 85, RowNum: 2},  // score 85 appears in rows 0, 2, 4
				{Value: 78, RowNum: 3},  // score 78 appears in row 3
				{Value: 85, RowNum: 4},  // score 85 appears in rows 0, 2, 4
				{Value: 92, RowNum: 5},  // score 92 appears in rows 1, 5
				{Value: 95, RowNum: 6},  // score 95 appears in row 6
			}

			err = cf.LoadIntColumn("score", data)
			if err != nil {
				t.Fatal(err)
			}

			cf.Close()

			// Reopen file and test iterator
			cf, err = OpenFile(filename)
			if err != nil {
				t.Fatalf("Failed to reopen compressed file: %v", err)
			}
			defer cf.Close()

			t.Run("BasicIteration", func(t *testing.T) {
				// Test basic iteration
				iter, err := cf.NewIterator("score")
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				expectedResults := map[int64][]uint64{
					78: {3},        // score 78 appears in row 3
					85: {0, 2, 4},  // score 85 appears in rows 0, 2, 4
					92: {1, 5},     // score 92 appears in rows 1, 5
					95: {6},        // score 95 appears in row 6
				}

				actualResults := make(map[int64][]uint64)
				count := 0

				for iter.Next() {
					count++
					key := iter.Key().(int64)
					rows := iter.Rows()

					// Convert bitmap to slice
					actualResults[key] = BitmapToSlice(rows)
				}

				// Verify we got the expected number of unique keys
				if count != len(expectedResults) {
					t.Errorf("Expected %d unique keys, got %d", len(expectedResults), count)
				}

				// Verify each key's bitmap
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
			})

			t.Run("RangeIteration", func(t *testing.T) {
				// Test range iteration
				iter, err := cf.NewRangeIteratorInt("score", 80, 94)
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				expectedKeys := []int64{85, 92} // Keys in range [80, 94]
				actualKeys := []int64{}

				for iter.Next() {
					key := iter.Key().(int64)
					actualKeys = append(actualKeys, key)
					
					// Verify key is in range
					if key < 80 || key > 94 {
						t.Errorf("Key %d is out of range [80, 94]", key)
					}
				}

				if len(actualKeys) != len(expectedKeys) {
					t.Errorf("Expected %d keys in range, got %d", len(expectedKeys), len(actualKeys))
				}

				for i, expectedKey := range expectedKeys {
					if actualKeys[i] != expectedKey {
						t.Errorf("Expected key %d, got %d", expectedKey, actualKeys[i])
					}
				}
			})

			t.Run("BitmapToIterator", func(t *testing.T) {
				// Test bitmap to iterator conversion
				bitmap, err := cf.RangeQueryInt("score", 85, 95)
				if err != nil {
					t.Fatal(err)
				}

				iter, err := cf.BitmapToIterator("score", bitmap)
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				count := 0
				for iter.Next() {
					count++
					key := iter.Key().(int64)
					rows := iter.Rows()

					// Verify key is in range
					if key < 85 || key > 95 {
						t.Errorf("Key %d is out of range [85, 95]", key)
					}

					// Verify we have at least one row
					if rows.GetCardinality() == 0 {
						t.Errorf("Expected at least one row for key %d", key)
					}
				}

				if count == 0 {
					t.Error("Expected at least one result from bitmap iterator")
				}
			})
		})
	}
}

// TestIteratorPerformanceWithCompression tests that iterators maintain performance with compression
func TestIteratorPerformanceWithCompression(t *testing.T) {
	filename := "test_iterator_compression_performance.bytedb"
	defer os.Remove(filename)

	// Create file with compression
	opts := NewCompressionOptions().
		WithPageCompression(CompressionGzip, CompressionLevelDefault)

	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatalf("Failed to create file with compression: %v", err)
	}

	// Add column
	if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
		t.Fatal(err)
	}

	// Add substantial test data
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

	// Reopen and test performance
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatalf("Failed to reopen compressed file: %v", err)
	}
	defer cf.Close()

	// Test full iteration
	iter, err := cf.NewIterator("id")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		_ = iter.Key()
		_ = iter.Rows()
		count++
	}

	if count != numRecords {
		t.Errorf("Expected %d records, got %d", numRecords, count)
	}

	// Test range iteration
	iter2, err := cf.NewRangeIteratorInt("id", 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	defer iter2.Close()

	rangeCount := 0
	for iter2.Next() {
		rangeCount++
	}

	expectedRangeCount := 1001 // 1000 to 2000 inclusive
	if rangeCount != expectedRangeCount {
		t.Errorf("Expected %d records in range, got %d", expectedRangeCount, rangeCount)
	}

	t.Logf("Successfully processed %d records with compression", count)
}