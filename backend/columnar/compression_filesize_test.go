package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestCompressionReducesFileSize(t *testing.T) {
	// Test that compression-aware bulk loading actually reduces file size
	
	tests := []struct {
		name            string
		compressionType CompressionType
		numRows         int
		expectedRatio   float64 // Expected file size ratio (compressed/uncompressed)
	}{
		{"SmallData_Zstd", CompressionZstd, 1000, 0.8},
		{"MediumData_Zstd", CompressionZstd, 10000, 0.5},
		{"LargeData_Zstd", CompressionZstd, 50000, 0.3},
		{"MediumData_Snappy", CompressionSnappy, 10000, 0.7},
		{"MediumData_Gzip", CompressionGzip, 10000, 0.6},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create uncompressed file
			filename1 := fmt.Sprintf("test_filesize_uncompressed_%s.bytedb", test.name)
			defer os.Remove(filename1)
			
			cf1, err := CreateFile(filename1)
			if err != nil {
				t.Fatal(err)
			}
			
			// Add columns
			cf1.AddColumn("id", DataTypeInt64, false)
			cf1.AddColumn("data", DataTypeString, false)
			
			// Generate test data with good compression potential
			intData := make([]IntData, test.numRows)
			stringData := make([]StringData, test.numRows)
			
			for i := 0; i < test.numRows; i++ {
				intData[i] = IntData{
					Value:  int64(i),
					RowNum: uint64(i),
				}
				// Repetitive strings for good compression
				stringData[i] = StringData{
					Value:  fmt.Sprintf("Record %d: This is a test record with some repetitive content that should compress well. Category: %s", i, []string{"A", "B", "C", "D", "E"}[i%5]),
					RowNum: uint64(i),
				}
			}
			
			cf1.LoadIntColumn("id", intData)
			cf1.LoadStringColumn("data", stringData)
			cf1.Close()
			
			// Create compressed file
			filename2 := fmt.Sprintf("test_filesize_compressed_%s.bytedb", test.name)
			defer os.Remove(filename2)
			
			opts := NewCompressionOptions().
				WithPageCompression(test.compressionType, CompressionLevelDefault)
			
			cf2, err := CreateFileWithOptions(filename2, opts)
			if err != nil {
				t.Fatal(err)
			}
			
			cf2.AddColumn("id", DataTypeInt64, false)
			cf2.AddColumn("data", DataTypeString, false)
			
			cf2.LoadIntColumn("id", intData)
			cf2.LoadStringColumn("data", stringData)
			
			stats := cf2.pageManager.GetCompressionStats()
			totalPages := cf2.pageManager.GetPageCount()
			
			cf2.Close()
			
			// Compare file sizes
			info1, _ := os.Stat(filename1)
			info2, _ := os.Stat(filename2)
			
			sizeRatio := float64(info2.Size()) / float64(info1.Size())
			
			t.Logf("%s results:", test.name)
			t.Logf("  Uncompressed: %d bytes (%d pages)", info1.Size(), info1.Size()/PageSize)
			t.Logf("  Compressed: %d bytes (%d pages)", info2.Size(), totalPages)
			t.Logf("  Size ratio: %.2f (expected < %.2f)", sizeRatio, test.expectedRatio)
			t.Logf("  Page reduction: %.1f%%", (1.0-float64(totalPages)/(float64(info1.Size())/PageSize))*100)
			t.Logf("  Data compression ratio: %.2f", stats.CompressionRatio)
			
			// Verify file is actually smaller
			if info2.Size() >= info1.Size() {
				t.Errorf("Compressed file (%d) is not smaller than uncompressed (%d)", 
					info2.Size(), info1.Size())
			}
			
			// Verify it meets expected ratio
			if sizeRatio > test.expectedRatio {
				t.Errorf("Size ratio %.2f is worse than expected %.2f", 
					sizeRatio, test.expectedRatio)
			}
			
			// Verify data integrity
			cf3, err := OpenFile(filename2)
			if err != nil {
				t.Fatal(err)
			}
			defer cf3.Close()
			
			// Test some lookups
			testRows := []uint64{0, uint64(test.numRows/2), uint64(test.numRows-1)}
			for _, rowNum := range testRows {
				val, found, err := cf3.LookupIntByRow("id", rowNum)
				if err != nil || !found || val != int64(rowNum) {
					t.Errorf("Failed to verify row %d", rowNum)
				}
			}
		})
	}
}

func TestCompressionPageCount(t *testing.T) {
	// Direct test of page count reduction
	filename1 := "test_page_count_uncompressed.bytedb"
	filename2 := "test_page_count_compressed.bytedb"
	defer os.Remove(filename1)
	defer os.Remove(filename2)
	
	// Create files
	cf1, _ := CreateFile(filename1)
	
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelBest)
	cf2, _ := CreateFileWithOptions(filename2, opts)
	
	// Add column
	cf1.AddColumn("value", DataTypeInt64, false)
	cf2.AddColumn("value", DataTypeInt64, false)
	
	// Load same data
	numRows := 100000
	data := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = IntData{
			Value:  int64(i % 1000), // Repetitive for compression
			RowNum: uint64(i),
		}
	}
	
	cf1.LoadIntColumn("value", data)
	cf2.LoadIntColumn("value", data)
	
	pages1 := cf1.pageManager.GetPageCount()
	pages2 := cf2.pageManager.GetPageCount()
	
	cf1.Close()
	cf2.Close()
	
	info1, _ := os.Stat(filename1)
	info2, _ := os.Stat(filename2)
	
	t.Logf("Page count comparison:")
	t.Logf("  Uncompressed: %d pages, %d bytes", pages1, info1.Size())
	t.Logf("  Compressed: %d pages, %d bytes", pages2, info2.Size())
	t.Logf("  Page reduction: %.1f%%", (1.0-float64(pages2)/float64(pages1))*100)
	t.Logf("  Size reduction: %.1f%%", (1.0-float64(info2.Size())/float64(info1.Size()))*100)
	
	if pages2 >= pages1 {
		t.Errorf("Compressed file has %d pages, expected less than %d", pages2, pages1)
	}
	
	// Verify significant reduction
	if float64(pages2)/float64(pages1) > 0.5 {
		t.Errorf("Expected at least 50%% page reduction, got %.1f%%", 
			(1.0-float64(pages2)/float64(pages1))*100)
	}
}

func TestCompressionWithDifferentDataPatterns(t *testing.T) {
	patterns := []struct {
		name        string
		dataGen     func(i int) string
		compression CompressionType
	}{
		{
			name: "HighlyRepetitive",
			dataGen: func(i int) string {
				return "AAAAAAAAAA"
			},
			compression: CompressionZstd,
		},
		{
			name: "Sequential",
			dataGen: func(i int) string {
				return fmt.Sprintf("SEQ_%010d", i)
			},
			compression: CompressionZstd,
		},
		{
			name: "LowCardinality",
			dataGen: func(i int) string {
				categories := []string{"RED", "GREEN", "BLUE", "YELLOW", "ORANGE"}
				return categories[i%len(categories)]
			},
			compression: CompressionSnappy,
		},
	}
	
	for _, pattern := range patterns {
		t.Run(pattern.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_pattern_%s.bytedb", pattern.name)
			defer os.Remove(filename)
			
			opts := NewCompressionOptions().
				WithPageCompression(pattern.compression, CompressionLevelDefault)
			
			cf, _ := CreateFileWithOptions(filename, opts)
			cf.AddColumn("data", DataTypeString, false)
			
			numRows := 10000
			stringData := make([]StringData, numRows)
			for i := 0; i < numRows; i++ {
				stringData[i] = StringData{
					Value:  pattern.dataGen(i),
					RowNum: uint64(i),
				}
			}
			
			cf.LoadStringColumn("data", stringData)
			
			pageCount := cf.pageManager.GetPageCount()
			stats := cf.pageManager.GetCompressionStats()
			
			cf.Close()
			
			info, _ := os.Stat(filename)
			
			// Calculate expected uncompressed size (rough estimate)
			avgStringLen := 0
			for i := 0; i < 100; i++ {
				avgStringLen += len(pattern.dataGen(i))
			}
			avgStringLen /= 100
			
			expectedUncompressed := numRows * (avgStringLen + 16) // String + overhead
			compressionFactor := float64(expectedUncompressed) / float64(info.Size())
			
			t.Logf("%s pattern:", pattern.name)
			t.Logf("  File size: %d bytes", info.Size())
			t.Logf("  Page count: %d", pageCount)
			t.Logf("  Compression ratio: %.2f", stats.CompressionRatio)
			t.Logf("  Effective compression factor: %.1fx", compressionFactor)
		})
	}
}