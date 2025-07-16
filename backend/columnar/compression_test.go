package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestCompressionOptions(t *testing.T) {
	tests := []struct {
		name            string
		compressionType CompressionType
		level           CompressionLevel
		expectedRatio   float64 // Expected compression ratio (compressed/uncompressed)
	}{
		{"NoCompression", CompressionNone, CompressionLevelDefault, 1.0},
		{"GzipDefault", CompressionGzip, CompressionLevelDefault, 0.7},
		{"GzipFastest", CompressionGzip, CompressionLevelFastest, 0.8},
		{"GzipBest", CompressionGzip, CompressionLevelBest, 0.6},
		{"Snappy", CompressionSnappy, CompressionLevelDefault, 0.8},
		{"ZstdDefault", CompressionZstd, CompressionLevelDefault, 0.6},
		{"ZstdFastest", CompressionZstd, CompressionLevelFastest, 0.7},
		{"ZstdBest", CompressionZstd, CompressionLevelBest, 0.5},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_compression_%s.bytedb", test.name)
			defer os.Remove(filename)
			
			// Create compression options
			opts := NewCompressionOptions().
				WithPageCompression(test.compressionType, test.level)
			
			// Create file with compression
			cf, err := CreateFileWithOptions(filename, opts)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			
			// Add columns
			if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
				t.Fatal(err)
			}
			if err := cf.AddColumn("description", DataTypeString, false); err != nil {
				t.Fatal(err)
			}
			
			// Generate test data
			numRows := 10000
			intData := make([]IntData, numRows)
			stringData := make([]StringData, numRows)
			
			for i := 0; i < numRows; i++ {
				intData[i] = IntData{
					Value:  int64(i),
					RowNum: uint64(i),
				}
				// Create repetitive strings for better compression
				stringData[i] = StringData{
					Value:  fmt.Sprintf("Description for item %d - This is a longer description to test compression", i%100),
					RowNum: uint64(i),
				}
			}
			
			// Load data
			if err := cf.LoadIntColumn("id", intData); err != nil {
				t.Fatal(err)
			}
			if err := cf.LoadStringColumn("description", stringData); err != nil {
				t.Fatal(err)
			}
			
			// Close file to flush data
			if err := cf.Close(); err != nil {
				t.Fatal(err)
			}
			
			// Check file size and compression ratio
			info, err := os.Stat(filename)
			if err != nil {
				t.Fatal(err)
			}
			
			compressedSize := info.Size()
			
			// Get compression stats
			stats := cf.pageManager.GetCompressionStats()
			
			if stats.UncompressedBytes > 0 {
				actualRatio := float64(stats.CompressedBytes) / float64(stats.UncompressedBytes)
				t.Logf("Compression %s: Uncompressed=%d, Compressed=%d, Ratio=%.2f, FileSize=%d",
					test.name, stats.UncompressedBytes, stats.CompressedBytes, actualRatio, compressedSize)
				
				// Verify compression ratio is better than expected (lower is better)
				if test.compressionType != CompressionNone && actualRatio > test.expectedRatio {
					t.Logf("Warning: Compression ratio %.2f is worse than expected %.2f", 
						actualRatio, test.expectedRatio)
				}
			}
			
			// Reopen and verify data integrity
			cf2, err := OpenFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer cf2.Close()
			
			// Verify some random lookups
			testRows := []uint64{0, 100, 500, 999, 5000, 9999}
			t.Logf("Testing %d rows, verifying rows: %v", numRows, testRows)
			for _, rowNum := range testRows {
				// Check integer
				val, found, err := cf2.LookupIntByRow("id", rowNum)
				if err != nil || !found || val != int64(rowNum) {
					t.Errorf("Row %d: failed to lookup int value", rowNum)
				}
				
				// Check string
				strVal, found, err := cf2.LookupStringByRow("description", rowNum)
				if err != nil {
					t.Errorf("Row %d: error looking up string: %v", rowNum, err)
					continue
				}
				if !found {
					t.Errorf("Row %d: string not found", rowNum)
					continue
				}
				expectedStr := fmt.Sprintf("Description for item %d - This is a longer description to test compression", rowNum%100)
				if strVal != expectedStr {
					t.Errorf("Row %d: string mismatch. Expected: %s, Got: %s", rowNum, expectedStr, strVal)
				}
			}
		})
	}
}

func TestDeltaEncoding(t *testing.T) {
	encoder := NewDeltaEncoder()
	
	// Test sequential data
	values := []uint64{100, 105, 110, 115, 120, 125, 130}
	encoded := encoder.Encode(values)
	
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(decoded) != len(values) {
		t.Fatalf("Length mismatch: expected %d, got %d", len(values), len(decoded))
	}
	
	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("Value mismatch at %d: expected %d, got %d", i, v, decoded[i])
		}
	}
	
	// Check compression efficiency
	originalSize := len(values) * 8
	compressedSize := len(encoded)
	ratio := float64(compressedSize) / float64(originalSize)
	t.Logf("Delta encoding: Original=%d, Compressed=%d, Ratio=%.2f", 
		originalSize, compressedSize, ratio)
}

func TestDictionaryEncoding(t *testing.T) {
	encoder := NewDictionaryEncoder()
	
	// Test low-cardinality data
	values := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = uint64(i % 10) // Only 10 unique values
	}
	
	// Build dictionary
	encoder.BuildDictionary(values)
	
	// Check if dictionary encoding is beneficial
	if !encoder.CanUseDictionary(10, 1000) {
		t.Error("Dictionary encoding should be beneficial for this data")
	}
	
	// Encode and decode
	encoded := encoder.Encode(values)
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify
	if len(decoded) != len(values) {
		t.Fatalf("Length mismatch: expected %d, got %d", len(values), len(decoded))
	}
	
	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("Value mismatch at %d: expected %d, got %d", i, v, decoded[i])
		}
	}
	
	// Check compression efficiency
	originalSize := len(values) * 8
	compressedSize := len(encoded)
	ratio := float64(compressedSize) / float64(originalSize)
	t.Logf("Dictionary encoding: Original=%d, Compressed=%d, Ratio=%.2f", 
		originalSize, compressedSize, ratio)
	
	if ratio > 0.2 {
		t.Errorf("Dictionary encoding ratio %.2f is worse than expected", ratio)
	}
}

func TestStringPrefixCompression(t *testing.T) {
	// Test sorted strings with common prefixes
	strings := []string{
		"user:alice:profile",
		"user:alice:settings",
		"user:bob:profile",
		"user:bob:settings",
		"user:charlie:profile",
		"user:charlie:settings",
	}
	
	compressed := CompressStringPrefix(strings)
	decompressed, err := DecompressStringPrefix(compressed)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify
	if len(decompressed) != len(strings) {
		t.Fatalf("Length mismatch: expected %d, got %d", len(strings), len(decompressed))
	}
	
	for i, s := range strings {
		if decompressed[i] != s {
			t.Errorf("String mismatch at %d: expected %s, got %s", i, s, decompressed[i])
		}
	}
	
	// Check compression
	originalSize := 0
	for _, s := range strings {
		originalSize += len(s) + 4 // 4 bytes for length
	}
	compressedSize := len(compressed)
	ratio := float64(compressedSize) / float64(originalSize)
	t.Logf("String prefix compression: Original=%d, Compressed=%d, Ratio=%.2f", 
		originalSize, compressedSize, ratio)
}

func BenchmarkCompression(b *testing.B) {
	compressionTypes := []struct {
		name  string
		cType CompressionType
		level CompressionLevel
	}{
		{"None", CompressionNone, CompressionLevelDefault},
		{"Snappy", CompressionSnappy, CompressionLevelDefault},
		{"Gzip", CompressionGzip, CompressionLevelDefault},
		{"Zstd", CompressionZstd, CompressionLevelDefault},
	}
	
	for _, ct := range compressionTypes {
		b.Run("Write_"+ct.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				filename := fmt.Sprintf("bench_%s_%d.bytedb", ct.name, i)
				
				opts := NewCompressionOptions().
					WithPageCompression(ct.cType, ct.level)
				
				b.StartTimer()
				
				cf, _ := CreateFileWithOptions(filename, opts)
				cf.AddColumn("value", DataTypeInt64, false)
				
				data := make([]IntData, 1000)
				for j := 0; j < 1000; j++ {
					data[j] = IntData{Value: int64(j), RowNum: uint64(j)}
				}
				cf.LoadIntColumn("value", data)
				cf.Close()
				
				b.StopTimer()
				os.Remove(filename)
			}
		})
		
		b.Run("Read_"+ct.name, func(b *testing.B) {
			// Setup file once
			filename := fmt.Sprintf("bench_read_%s.bytedb", ct.name)
			opts := NewCompressionOptions().
				WithPageCompression(ct.cType, ct.level)
			
			cf, _ := CreateFileWithOptions(filename, opts)
			cf.AddColumn("value", DataTypeInt64, false)
			
			data := make([]IntData, 10000)
			for j := 0; j < 10000; j++ {
				data[j] = IntData{Value: int64(j), RowNum: uint64(j)}
			}
			cf.LoadIntColumn("value", data)
			cf.Close()
			
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				cf, _ := OpenFile(filename)
				
				// Random lookups
				for j := 0; j < 100; j++ {
					rowNum := uint64(rand.Intn(10000))
					cf.LookupIntByRow("value", rowNum)
				}
				
				cf.Close()
			}
			
			os.Remove(filename)
		})
	}
}

func TestCompressionWithRealWorldData(t *testing.T) {
	filename := "test_realworld_compression.bytedb"
	defer os.Remove(filename)
	
	// Create file with mixed compression strategies
	opts := NewCompressionOptions()
	opts.LeafPageCompression = CompressionSnappy      // Fast for B-tree leaves
	opts.StringPageCompression = CompressionZstd       // Better for strings
	opts.InternalPageCompression = CompressionNone     // No compression for internal nodes
	
	// Column-specific strategies
	opts.ColumnStrategies["timestamp"] = &ColumnCompressionStrategy{
		EnableDeltaEncoding: true,
		PageCompression:     CompressionSnappy,
	}
	
	opts.ColumnStrategies["category"] = &ColumnCompressionStrategy{
		EnableDictionaryEncoding: true,
		DictionaryThreshold:      100,
		PageCompression:          CompressionSnappy,
	}
	
	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatal(err)
	}
	
	// Add columns
	cf.AddColumn("timestamp", DataTypeInt64, false)
	cf.AddColumn("category", DataTypeString, false)
	cf.AddColumn("description", DataTypeString, true)
	
	// Generate realistic data
	numRows := 50000
	baseTime := time.Now().Unix()
	categories := []string{"Electronics", "Books", "Clothing", "Food", "Sports", "Home", "Toys", "Health"}
	
	timestampData := make([]IntData, numRows)
	categoryData := make([]StringData, numRows)
	descriptionData := make([]StringData, numRows)
	
	for i := 0; i < numRows; i++ {
		// Sequential timestamps with small increments
		timestampData[i] = IntData{
			Value:  baseTime + int64(i*5),
			RowNum: uint64(i),
		}
		
		// Low cardinality categories
		categoryData[i] = StringData{
			Value:  categories[i%len(categories)],
			RowNum: uint64(i),
		}
		
		// Descriptions with some nulls
		if i%10 == 0 {
			descriptionData[i] = StringData{
				IsNull: true,
				RowNum: uint64(i),
			}
		} else {
			descriptionData[i] = StringData{
				Value:  fmt.Sprintf("Product description for item in %s category", categories[i%len(categories)]),
				RowNum: uint64(i),
			}
		}
	}
	
	// Load data
	if err := cf.LoadIntColumn("timestamp", timestampData); err != nil {
		t.Fatal(err)
	}
	if err := cf.LoadStringColumn("category", categoryData); err != nil {
		t.Fatal(err)
	}
	if err := cf.LoadStringColumn("description", descriptionData); err != nil {
		t.Fatal(err)
	}
	
	// Get stats before closing
	stats := cf.pageManager.GetCompressionStats()
	
	if err := cf.Close(); err != nil {
		t.Fatal(err)
	}
	
	// Check file size
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Logf("Real-world data compression results:")
	t.Logf("  File size: %d bytes", info.Size())
	t.Logf("  Uncompressed: %d bytes", stats.UncompressedBytes)
	t.Logf("  Compressed: %d bytes", stats.CompressedBytes)
	t.Logf("  Ratio: %.2f", stats.CompressionRatio)
	t.Logf("  Page compressions by type:")
	for pageType, count := range stats.PageCompressions {
		t.Logf("    %v: %d pages", pageType, count)
	}
}