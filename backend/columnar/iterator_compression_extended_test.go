package columnar

import (
	"os"
	"testing"
	"time"
)

// TestIteratorWithCompressionExtended tests iterator with various compression scenarios
func TestIteratorWithCompressionExtended(t *testing.T) {
	filename := "test_iterator_compression_extended.bytedb"
	defer os.Remove(filename)

	// Create file with high compression
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelBest).
		WithPageSize(8192) // Smaller pages for better compression testing

	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatalf("Failed to create file with compression: %v", err)
	}

	// Add columns
	if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
		t.Fatal(err)
	}
	if err := cf.AddColumn("category", DataTypeInt64, false); err != nil {
		t.Fatal(err)
	}

	// Add large dataset with patterns that compress well
	numRecords := 50000
	idData := make([]IntData, numRecords)
	categoryData := make([]IntData, numRecords)
	
	for i := 0; i < numRecords; i++ {
		idData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		// Create repeating patterns for better compression
		categoryData[i] = IntData{Value: int64(i % 100), RowNum: uint64(i)}
	}

	err = cf.LoadIntColumn("id", idData)
	if err != nil {
		t.Fatal(err)
	}
	
	err = cf.LoadIntColumn("category", categoryData)
	if err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Reopen and test
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatalf("Failed to reopen compressed file: %v", err)
	}
	defer cf.Close()

	t.Run("FullScanWithCompression", func(t *testing.T) {
		start := time.Now()
		
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

		elapsed := time.Since(start)
		
		if count != numRecords {
			t.Errorf("Expected %d records, got %d", numRecords, count)
		}
		
		t.Logf("Full scan of %d compressed records took %v", count, elapsed)
	})

	t.Run("RangeQueryWithCompression", func(t *testing.T) {
		start := time.Now()
		
		iter, err := cf.NewRangeIteratorInt("id", 10000, 20000)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key().(int64)
			if key < 10000 || key > 20000 {
				t.Errorf("Key %d out of range [10000, 20000]", key)
			}
			count++
		}

		elapsed := time.Since(start)
		
		expectedCount := 10001 // 10000 to 20000 inclusive
		if count != expectedCount {
			t.Errorf("Expected %d records, got %d", expectedCount, count)
		}
		
		t.Logf("Range query of %d compressed records took %v", count, elapsed)
	})

	t.Run("RepeatingValuesWithCompression", func(t *testing.T) {
		// Test category column which has repeating values (good for compression)
		start := time.Now()
		
		iter, err := cf.NewRangeIteratorInt("category", 50, 60)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		totalRows := 0
		uniqueKeys := 0
		
		for iter.Next() {
			key := iter.Key().(int64)
			rows := iter.Rows()
			
			if key < 50 || key > 60 {
				t.Errorf("Key %d out of range [50, 60]", key)
			}
			
			uniqueKeys++
			totalRows += int(rows.GetCardinality())
		}

		elapsed := time.Since(start)
		
		expectedUniqueKeys := 11 // 50 to 60 inclusive
		if uniqueKeys != expectedUniqueKeys {
			t.Errorf("Expected %d unique keys, got %d", expectedUniqueKeys, uniqueKeys)
		}
		
		// Each category value should appear in approximately 500 rows (50000/100)
		expectedTotalRows := expectedUniqueKeys * 500
		if totalRows != expectedTotalRows {
			t.Errorf("Expected %d total rows, got %d", expectedTotalRows, totalRows)
		}
		
		t.Logf("Repeating values query: %d unique keys, %d total rows, took %v", uniqueKeys, totalRows, elapsed)
	})

	t.Run("BitmapConversionWithCompression", func(t *testing.T) {
		// Test that bitmap conversion still works with compression
		bitmap, err := cf.RangeQueryInt("category", 25, 30)
		if err != nil {
			t.Fatal(err)
		}
		
		iter, err := cf.BitmapToIterator("category", bitmap)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key().(int64)
			if key < 25 || key > 30 {
				t.Errorf("Key %d out of range [25, 30]", key)
			}
			count++
		}

		// BitmapToIterator returns individual rows, not unique keys
		// Each category value appears in ~500 rows, so 6 categories * 500 rows = ~3000 rows
		expectedCount := 6 * 500 // 25 to 30 inclusive, each appears in ~500 rows
		if count != expectedCount {
			t.Errorf("Expected %d results from bitmap iterator, got %d", expectedCount, count)
		}
		
		t.Logf("Bitmap conversion with compression: %d results", count)
	})
}

// TestIteratorCompressionConsistency verifies that results are identical with/without compression
func TestIteratorCompressionConsistency(t *testing.T) {
	// Test data
	testData := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 10, RowNum: 2},
		{Value: 30, RowNum: 3},
		{Value: 20, RowNum: 4},
		{Value: 10, RowNum: 5},
	}

	var uncompressedResults, compressedResults []struct {
		key  int64
		rows []uint64
	}

	// Test without compression
	t.Run("Uncompressed", func(t *testing.T) {
		filename := "test_consistency_uncompressed.bytedb"
		defer os.Remove(filename)

		cf, err := CreateFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		
		cf.AddColumn("value", DataTypeInt64, false)
		err = cf.LoadIntColumn("value", testData)
		if err != nil {
			t.Fatal(err)
		}
		cf.Close()

		cf, err = OpenFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf.Close()

		iter, err := cf.NewIterator("value")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		for iter.Next() {
			key := iter.Key().(int64)
			rows := BitmapToSlice(iter.Rows())
			uncompressedResults = append(uncompressedResults, struct {
				key  int64
				rows []uint64
			}{key, rows})
		}
	})

	// Test with compression
	t.Run("Compressed", func(t *testing.T) {
		filename := "test_consistency_compressed.bytedb"
		defer os.Remove(filename)

		opts := NewCompressionOptions().WithPageCompression(CompressionGzip, CompressionLevelBest)
		cf, err := CreateFileWithOptions(filename, opts)
		if err != nil {
			t.Fatal(err)
		}
		
		cf.AddColumn("value", DataTypeInt64, false)
		err = cf.LoadIntColumn("value", testData)
		if err != nil {
			t.Fatal(err)
		}
		cf.Close()

		cf, err = OpenFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf.Close()

		iter, err := cf.NewIterator("value")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		for iter.Next() {
			key := iter.Key().(int64)
			rows := BitmapToSlice(iter.Rows())
			compressedResults = append(compressedResults, struct {
				key  int64
				rows []uint64
			}{key, rows})
		}
	})

	// Compare results
	if len(uncompressedResults) != len(compressedResults) {
		t.Fatalf("Result count mismatch: uncompressed %d, compressed %d", 
			len(uncompressedResults), len(compressedResults))
	}

	for i, uncompressed := range uncompressedResults {
		compressed := compressedResults[i]
		
		if uncompressed.key != compressed.key {
			t.Errorf("Key mismatch at index %d: uncompressed %d, compressed %d", 
				i, uncompressed.key, compressed.key)
		}
		
		if len(uncompressed.rows) != len(compressed.rows) {
			t.Errorf("Row count mismatch for key %d: uncompressed %d, compressed %d", 
				uncompressed.key, len(uncompressed.rows), len(compressed.rows))
			continue
		}
		
		for j, uncompressedRow := range uncompressed.rows {
			if uncompressedRow != compressed.rows[j] {
				t.Errorf("Row mismatch for key %d at index %d: uncompressed %d, compressed %d", 
					uncompressed.key, j, uncompressedRow, compressed.rows[j])
			}
		}
	}
	
	t.Logf("Compression consistency verified: %d keys match exactly", len(uncompressedResults))
}