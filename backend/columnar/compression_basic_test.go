package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestBasicCompression(t *testing.T) {
	tests := []struct {
		name            string
		compressionType CompressionType
	}{
		{"NoCompression", CompressionNone},
		{"GzipCompression", CompressionGzip},
		{"SnappyCompression", CompressionSnappy},
		{"ZstdCompression", CompressionZstd},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_basic_%s.bytedb", test.name)
			defer os.Remove(filename)
			
			// Create with compression
			opts := NewCompressionOptions().
				WithPageCompression(test.compressionType, CompressionLevelDefault)
			
			cf, err := CreateFileWithOptions(filename, opts)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			
			// Add a simple column
			if err := cf.AddColumn("value", DataTypeInt64, false); err != nil {
				t.Fatal(err)
			}
			
			// Add 1000 rows
			data := make([]IntData, 1000)
			for i := 0; i < 1000; i++ {
				data[i] = IntData{
					Value:  int64(i),
					RowNum: uint64(i),
				}
			}
			
			if err := cf.LoadIntColumn("value", data); err != nil {
				t.Fatal(err)
			}
			
			// Get compression stats before closing
			stats := cf.pageManager.GetCompressionStats()
			
			if err := cf.Close(); err != nil {
				t.Fatal(err)
			}
			
			// Log file size
			info, err := os.Stat(filename)
			if err != nil {
				t.Fatal(err)
			}
			
			t.Logf("%s: FileSize=%d, Uncompressed=%d, Compressed=%d, Ratio=%.2f",
				test.name, info.Size(), stats.UncompressedBytes, stats.CompressedBytes, stats.CompressionRatio)
			
			// Reopen and verify
			cf2, err := OpenFile(filename)
			if err != nil {
				t.Fatalf("Failed to reopen: %v", err)
			}
			defer cf2.Close()
			
			// Debug: check column metadata
			col, exists := cf2.columns["value"]
			if !exists {
				t.Fatal("Column 'value' not found")
			}
			t.Logf("Column metadata: TotalKeys=%d, DistinctCount=%d, RowKeyRootPageID=%d", 
				col.metadata.TotalKeys, col.metadata.DistinctCount, col.metadata.RowKeyRootPageID)
			
			// Test a few lookups
			for _, rowNum := range []uint64{0, 500, 999} {
				val, found, err := cf2.LookupIntByRow("value", rowNum)
				if err != nil {
					t.Errorf("Row %d: error: %v", rowNum, err)
				}
				if !found {
					t.Errorf("Row %d: not found", rowNum)
					// Debug: check if row exists in key->row mapping
					bitmap, _ := cf2.QueryInt("value", int64(rowNum))
					if bitmap != nil && bitmap.GetCardinality() > 0 {
						t.Logf("  Debug: Row %d exists in key->row mapping", rowNum)
					} else {
						t.Logf("  Debug: Row %d NOT in key->row mapping", rowNum)
					}
				}
				if found && val != int64(rowNum) {
					t.Errorf("Row %d: expected %d, got %d", rowNum, rowNum, val)
				}
			}
		})
	}
}

func TestCompressionSavings(t *testing.T) {
	// Test with highly compressible data
	filename1 := "test_uncompressed.bytedb"
	filename2 := "test_compressed.bytedb"
	defer os.Remove(filename1)
	defer os.Remove(filename2)
	
	// Create uncompressed file
	cf1, err := CreateFile(filename1)
	if err != nil {
		t.Fatal(err)
	}
	cf1.AddColumn("data", DataTypeString, false)
	
	// Create compressed file with Zstd
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelDefault)
	cf2, err := CreateFileWithOptions(filename2, opts)
	if err != nil {
		t.Fatal(err)
	}
	cf2.AddColumn("data", DataTypeString, false)
	
	// Add repetitive data
	numRows := 10000
	stringData := make([]StringData, numRows)
	for i := 0; i < numRows; i++ {
		// Repetitive pattern for good compression
		stringData[i] = StringData{
			Value:  fmt.Sprintf("Row %d: This is a test string that repeats the same pattern over and over again to demonstrate compression", i%100),
			RowNum: uint64(i),
		}
	}
	
	cf1.LoadStringColumn("data", stringData)
	cf2.LoadStringColumn("data", stringData)
	
	stats2 := cf2.pageManager.GetCompressionStats()
	
	cf1.Close()
	cf2.Close()
	
	// Compare file sizes
	info1, _ := os.Stat(filename1)
	info2, _ := os.Stat(filename2)
	
	fileSavings := float64(info1.Size()-info2.Size()) / float64(info1.Size()) * 100
	
	t.Logf("File sizes:")
	t.Logf("  Uncompressed file: %d bytes", info1.Size())
	t.Logf("  Compressed file: %d bytes", info2.Size())
	t.Logf("  File size savings: %.1f%%", fileSavings)
	
	t.Logf("\nCompression stats:")
	t.Logf("  Uncompressed data: %d bytes", stats2.UncompressedBytes)
	t.Logf("  Compressed data: %d bytes", stats2.CompressedBytes) 
	t.Logf("  Compression ratio: %.2f", stats2.CompressionRatio)
	
	// Check actual data compression (not file size)
	if stats2.UncompressedBytes > 0 {
		dataSavings := float64(stats2.UncompressedBytes-stats2.CompressedBytes) / float64(stats2.UncompressedBytes) * 100
		t.Logf("  Data compression savings: %.1f%%", dataSavings)
		
		if dataSavings < 50 {
			t.Errorf("Expected at least 50%% data compression savings, got %.1f%%", dataSavings)
		}
	}
}

func TestPageTypeCompression(t *testing.T) {
	filename := "test_page_types.bytedb"
	defer os.Remove(filename)
	
	// Different compression for different page types
	opts := NewCompressionOptions()
	opts.LeafPageCompression = CompressionSnappy       // Fast for lookups
	opts.StringPageCompression = CompressionZstd        // High compression for strings
	opts.InternalPageCompression = CompressionNone      // No compression for small internal nodes
	opts.BitmapPageCompression = CompressionNone        // Already compressed by Roaring
	
	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatal(err)
	}
	
	// Add columns
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)
	
	// Add data
	numRows := 5000
	intData := make([]IntData, numRows)
	stringData := make([]StringData, numRows)
	
	for i := 0; i < numRows; i++ {
		intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		stringData[i] = StringData{Value: fmt.Sprintf("Name_%d", i), RowNum: uint64(i)}
	}
	
	cf.LoadIntColumn("id", intData)
	cf.LoadStringColumn("name", stringData)
	
	stats := cf.pageManager.GetCompressionStats()
	
	cf.Close()
	
	// Check compression by page type
	t.Logf("Compression by page type:")
	for pageType, count := range stats.PageCompressions {
		t.Logf("  %v: %d pages compressed", pageType, count)
	}
	
	// Verify data integrity
	cf2, _ := OpenFile(filename)
	defer cf2.Close()
	
	// Test lookups
	val, found, _ := cf2.LookupIntByRow("id", 2500)
	if !found || val != 2500 {
		t.Error("Failed to lookup int value")
	}
	
	strVal, found, _ := cf2.LookupStringByRow("name", 2500)
	if !found || strVal != "Name_2500" {
		t.Error("Failed to lookup string value")
	}
}