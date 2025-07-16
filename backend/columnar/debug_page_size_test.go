package columnar

import (
	"os"
	"testing"
)

func TestDebugPageSize(t *testing.T) {
	filename := "debug_page_size.bytedb"
	defer os.Remove(filename)
	
	// Create file with 4KB page size
	opts := NewCompressionOptions().WithPageSize(4096)
	t.Logf("Creating file with page size: %d", opts.PageSize)
	
	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add a column to ensure header is written
	cf.AddColumn("test", DataTypeInt64, false)
	
	// Check page manager
	t.Logf("Page manager page size: %d", cf.pageManager.GetPageSize())
	t.Logf("Header page size: %d", cf.header.PageSize)
	
	// Close file
	cf.Close()
	
	// Try to open the file
	t.Log("Opening file...")
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf2.Close()
	
	t.Logf("Opened file - page size: %d", cf2.pageManager.GetPageSize())
	t.Logf("Header page size: %d", cf2.header.PageSize)
}