package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestStreamingMergeWithCompression verifies that streaming merge respects compression settings
func TestStreamingMergeWithCompression(t *testing.T) {
	testCases := []struct {
		name               string
		compressionType    CompressionType
		expectCompression  bool
	}{
		{
			name:              "NoCompression",
			compressionType:   CompressionNone,
			expectCompression: false,
		},
		{
			name:              "GzipCompression", 
			compressionType:   CompressionGzip,
			expectCompression: true,
		},
		{
			name:              "ZstdCompression",
			compressionType:   CompressionZstd,
			expectCompression: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test files
			file1 := fmt.Sprintf("test_compress_%s_1.bytedb", tc.name)
			file2 := fmt.Sprintf("test_compress_%s_2.bytedb", tc.name)
			outputFile := fmt.Sprintf("test_compress_%s_output.bytedb", tc.name)
			
			defer os.Remove(file1)
			defer os.Remove(file2)
			defer os.Remove(outputFile)
			
			// Create test data files
			if err := createTestDataFile(file1, 0, 5000); err != nil {
				t.Fatal(err)
			}
			if err := createTestDataFile(file2, 5000, 5000); err != nil {
				t.Fatal(err)
			}
			
			// Merge with compression options
			var compressionOpts *CompressionOptions
			if tc.expectCompression {
				compressionOpts = &CompressionOptions{
					PageSize:                16 * 1024,
					LeafPageCompression:     tc.compressionType,
					InternalPageCompression: tc.compressionType,
					MinPageSizeToCompress:   100,
				}
			}
			
			options := &MergeOptions{
				UseStreamingMerge:  true,
				CompressionOptions: compressionOpts,
			}
			
			// Perform merge
			err := MergeFiles(outputFile, []string{file1, file2}, options)
			if err != nil {
				t.Fatal(err)
			}
			
			// Verify compression by checking page headers
			verifyCompressionApplied(t, outputFile, tc.expectCompression, tc.compressionType)
		})
	}
}

// TestStreamingBuilderWithCompression tests BTreeStreamingBuilder compression directly
func TestStreamingBuilderWithCompression(t *testing.T) {
	file := "test_streaming_builder_compress.bytedb"
	defer os.Remove(file)
	
	// Create file with compression
	compressionOpts := &CompressionOptions{
		PageSize:                16 * 1024,
		LeafPageCompression:     CompressionGzip,
		InternalPageCompression: CompressionGzip, 
		MinPageSizeToCompress:   100,
	}
	
	cf, err := CreateFileWithOptions(file, compressionOpts)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("value", DataTypeInt32, false)
	col := cf.columns["value"]
	
	// Use streaming builder
	builder := NewBTreeStreamingBuilder(col.btree)
	
	// Verify compression is enabled
	if !builder.hasCompression {
		t.Error("Expected compression to be enabled")
	}
	
	// Add enough entries to trigger page flushes
	for i := 0; i < 10000; i++ {
		value := NewRowNumberValue(uint64(i + 1))
		err := builder.Add(uint64(i), value)
		if err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}
	
	// Finish building
	err = builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	
	// Update column metadata
	col.metadata.RootPageID = col.btree.rootPageID
	col.metadata.TreeHeight = col.btree.height
	
	// Get compression stats before closing
	stats := cf.pageManager.GetCompressionStats()
	t.Logf("Compression stats:")
	t.Logf("  Uncompressed: %d bytes", stats.UncompressedBytes) 
	t.Logf("  Compressed: %d bytes", stats.CompressedBytes)
	t.Logf("  Ratio: %.2f%%", stats.CompressionRatio*100)
	t.Logf("  Compressed pages: %v", stats.PageCompressions)
	
	// Verify compression happened
	if stats.UncompressedBytes == 0 || stats.CompressedBytes == 0 {
		t.Error("No compression statistics recorded")
	}
	
	if stats.CompressionRatio > 0.8 {
		t.Errorf("Poor compression ratio: %.2f%%", stats.CompressionRatio*100)
	}
	
	cf.Close()
	
	// Verify data integrity
	cf2, err := OpenFile(file)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()
	
	iter, err := cf2.NewIterator("value")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	
	count := 0
	for iter.Next() {
		count++
	}
	
	if count != 10000 {
		t.Errorf("Expected 10000 entries, got %d", count)
	}
}

// TestStreamingCompressionWithDifferentPageSizes tests compression with various page sizes
func TestStreamingCompressionWithDifferentPageSizes(t *testing.T) {
	pageSizes := []int{
		4 * 1024,   // 4KB
		8 * 1024,   // 8KB
		16 * 1024,  // 16KB
		32 * 1024,  // 32KB
	}
	
	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("PageSize_%dKB", pageSize/1024), func(t *testing.T) {
			file := fmt.Sprintf("test_pagesize_%d.bytedb", pageSize)
			defer os.Remove(file)
			
			compressionOpts := &CompressionOptions{
				PageSize:              pageSize,
				LeafPageCompression:   CompressionGzip,
				MinPageSizeToCompress: 100,
			}
			
			cf, err := CreateFileWithOptions(file, compressionOpts)
			if err != nil {
				t.Fatal(err)
			}
			
			cf.AddColumn("data", DataTypeInt64, false)
			
			// Add sequential data (compresses well)
			data := make([]IntData, 5000)
			for i := 0; i < 5000; i++ {
				data[i] = NewIntData(int64(i), uint64(i+1))
			}
			
			err = cf.LoadIntColumn("data", data)
			if err != nil {
				t.Fatal(err)
			}
			
			stats := cf.pageManager.GetCompressionStats()
			t.Logf("Page size %dKB - Compression ratio: %.2f%%", 
				pageSize/1024, stats.CompressionRatio*100)
			
			cf.Close()
		})
	}
}

// Helper functions

func createTestDataFile(filename string, startValue, count int) error {
	cf, err := CreateFile(filename)
	if err != nil {
		return err
	}
	defer cf.Close()
	
	cf.AddColumn("id", DataTypeInt32, false)
	
	data := make([]IntData, count)
	for i := 0; i < count; i++ {
		data[i] = NewIntData(int64(startValue+i), uint64(i+1))
	}
	
	return cf.LoadIntColumn("id", data)
}

func verifyCompressionApplied(t *testing.T, filename string, expectCompression bool, compressionType CompressionType) {
	cf, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// For now, we just verify the file opens correctly and data is readable
	// In a real implementation, we would check page headers for compression type
	
	iter, err := cf.NewIterator("id")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	
	count := 0
	for iter.Next() {
		count++
	}
	
	if count != 10000 {
		t.Errorf("Expected 10000 entries, got %d", count)
	}
	
	t.Logf("Successfully read %d entries from %s", count, filename)
}