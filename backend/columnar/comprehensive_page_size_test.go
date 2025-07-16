package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestComprehensivePageSizes tests all supported page sizes comprehensively
func TestComprehensivePageSizes(t *testing.T) {
	pageSizes := []int{4096, 8192, 16384, 32768}
	
	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("%dKB", pageSize/1024), func(t *testing.T) {
			filename := fmt.Sprintf("test_comprehensive_%d.bytedb", pageSize)
			defer os.Remove(filename)
			
			// Create file
			opts := NewCompressionOptions().WithPageSize(pageSize)
			t.Logf("Creating file with page size: %d", pageSize)
			
			cf, err := CreateFileWithOptions(filename, opts)
			if err != nil {
				t.Fatalf("Failed to create file with %d page size: %v", pageSize, err)
			}
			
			// Verify page manager has correct size
			if cf.pageManager.GetPageSize() != pageSize {
				t.Errorf("Page manager size mismatch after create: expected %d, got %d",
					pageSize, cf.pageManager.GetPageSize())
			}
			
			// Add a column
			cf.AddColumn("test", DataTypeInt64, false)
			
			// Load some data
			data := []IntData{{Value: 42, RowNum: 0}}
			if err := cf.LoadIntColumn("test", data); err != nil {
				t.Fatalf("Failed to load data: %v", err)
			}
			
			// Close file
			cf.Close()
			
			// Verify file was created
			info, err := os.Stat(filename)
			if err != nil {
				t.Fatalf("Failed to stat file: %v", err)
			}
			t.Logf("File created, size: %d bytes", info.Size())
			
			// Open file
			t.Log("Opening file...")
			cf2, err := OpenFile(filename)
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			defer cf2.Close()
			
			// Verify page size
			if cf2.pageManager.GetPageSize() != pageSize {
				t.Errorf("Page size mismatch after open: expected %d, got %d",
					pageSize, cf2.pageManager.GetPageSize())
			}
			
			// Query data
			result, err := cf2.QueryInt("test", 42)
			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}
			
			if result.GetCardinality() != 1 {
				t.Errorf("Expected 1 result, got %d", result.GetCardinality())
			}
			
			t.Logf("Successfully created and opened file with %d page size", pageSize)
		})
	}
}