package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestPageSizeWithCompression tests different page sizes with compression
func TestPageSizeWithCompression(t *testing.T) {
	t.Log("\n=== PAGE SIZE WITH COMPRESSION TEST ===\n")
	
	pageSizes := []int{4096, 8192, 16384, 32768}
	compressionTypes := []struct {
		name string
		ct   CompressionType
	}{
		{"None", CompressionNone},
		{"Snappy", CompressionSnappy},
		{"Gzip", CompressionGzip},
		{"Zstd", CompressionZstd},
	}
	
	for _, pageSize := range pageSizes {
		for _, comp := range compressionTypes {
			testName := fmt.Sprintf("%dKB_%s", pageSize/1024, comp.name)
			filename := fmt.Sprintf("test_%s.bytedb", testName)
			
			t.Run(testName, func(t *testing.T) {
				defer os.Remove(filename)
				
				// Create file with specific page size and compression
				opts := NewCompressionOptions().
					WithPageSize(pageSize).
					WithPageCompression(comp.ct, CompressionLevelDefault)
				
				cf, err := CreateFileWithOptions(filename, opts)
				if err != nil {
					t.Fatalf("Failed to create file: %v", err)
				}
				
				// Add columns
				cf.AddColumn("id", DataTypeInt64, false)
				cf.AddColumn("data", DataTypeString, false)
				
				// Load test data
				numRows := 1000
				intData := make([]IntData, numRows)
				stringData := make([]StringData, numRows)
				
				for i := 0; i < numRows; i++ {
					intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
					// Create repetitive strings that compress well
					stringData[i] = StringData{
						Value:  fmt.Sprintf("test_data_%d_padding_padding_padding", i%10),
						RowNum: uint64(i),
					}
				}
				
				if err := cf.LoadIntColumn("id", intData); err != nil {
					t.Fatalf("Failed to load int column: %v", err)
				}
				
				if err := cf.LoadStringColumn("data", stringData); err != nil {
					t.Fatalf("Failed to load string column: %v", err)
				}
				
				// Get compression stats
				stats := cf.pageManager.GetCompressionStats()
				
				// Close file
				cf.Close()
				
				// Get file size
				info, err := os.Stat(filename)
				if err != nil {
					t.Fatalf("Failed to stat file: %v", err)
				}
				
				// Open and verify
				cf2, err := OpenFile(filename)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer cf2.Close()
				
				// Verify page size
				if cf2.pageManager.GetPageSize() != pageSize {
					t.Errorf("Page size mismatch: expected %d, got %d", 
						pageSize, cf2.pageManager.GetPageSize())
				}
				
				// Query data
				result, err := cf2.QueryInt("id", 42)
				if err != nil {
					t.Fatalf("Failed to query: %v", err)
				}
				
				if result.GetCardinality() != 1 {
					t.Errorf("Expected 1 result, got %d", result.GetCardinality())
				}
				
				t.Logf("Page size: %d, Compression: %s, File size: %d KB, Compression ratio: %.2f",
					pageSize, comp.name, info.Size()/1024, stats.CompressionRatio)
			})
		}
	}
	
	t.Log("\n=== COMPRESSION EFFICIENCY BY PAGE SIZE ===")
	t.Log("Larger pages generally provide better compression ratios")
	t.Log("due to more data available for pattern detection")
}