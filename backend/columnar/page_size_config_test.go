package columnar

import (
	"os"
	"testing"
)

// TestPageSizeConfiguration tests the page size configuration functionality
func TestPageSizeConfiguration(t *testing.T) {
	t.Log("\n=== PAGE SIZE CONFIGURATION TESTS ===\n")
	
	// Test 1: Default page size
	t.Run("DefaultPageSize", func(t *testing.T) {
		opts := NewCompressionOptions()
		if opts.PageSize != 4096 {
			t.Errorf("Expected default page size of 4KB (currently only supported size), got %d", opts.PageSize)
		}
	})
	
	// Test 2: Setting valid page sizes
	t.Run("ValidPageSizes", func(t *testing.T) {
		validSizes := []int{1024, 2048, 4096, 8192, 16384, 32768, 65536}
		
		for _, size := range validSizes {
			opts := NewCompressionOptions().WithPageSize(size)
			if opts.PageSize != size {
				t.Errorf("Expected page size %d, got %d", size, opts.PageSize)
			}
		}
	})
	
	// Test 3: Invalid page sizes (should panic)
	t.Run("InvalidPageSizes", func(t *testing.T) {
		invalidCases := []struct {
			size int
			desc string
		}{
			{512, "too small"},
			{256 * 1024, "too large"},
			{3000, "not power of 2"},
			{12288, "not power of 2 (12KB)"},
		}
		
		for _, tc := range invalidCases {
			t.Run(tc.desc, func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for page size %d (%s)", tc.size, tc.desc)
					}
				}()
				
				NewCompressionOptions().WithPageSize(tc.size)
			})
		}
	})
	
	// Test 4: Page size validation
	t.Run("PageSizeValidation", func(t *testing.T) {
		// Currently only 4KB is supported
		opts4KB := NewCompressionOptions().WithPageSize(4096)
		if err := opts4KB.ValidatePageSize(); err != nil {
			t.Errorf("4KB page size should be valid: %v", err)
		}
		
		// Other sizes should fail validation
		opts16KB := NewCompressionOptions().WithPageSize(16384)
		if err := opts16KB.ValidatePageSize(); err == nil {
			t.Error("16KB page size should fail validation (not yet implemented)")
		}
		
		opts32KB := NewCompressionOptions().WithPageSize(32768)
		if err := opts32KB.ValidatePageSize(); err == nil {
			t.Error("32KB page size should fail validation (not yet implemented)")
		}
	})
	
	// Test 5: File creation with different page sizes
	t.Run("FileCreationWithPageSize", func(t *testing.T) {
		// Test with supported 4KB page size
		filename4KB := "test_4kb_pages.bytedb"
		defer os.Remove(filename4KB)
		
		opts4KB := NewCompressionOptions().WithPageSize(4096)
		cf4KB, err := CreateFileWithOptions(filename4KB, opts4KB)
		if err != nil {
			t.Fatalf("Failed to create file with 4KB pages: %v", err)
		}
		cf4KB.Close()
		
		// Verify file header has correct page size
		cf4KBRead, err := OpenFile(filename4KB)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		if cf4KBRead.header.PageSize != 4096 {
			t.Errorf("Expected page size 4096 in header, got %d", cf4KBRead.header.PageSize)
		}
		cf4KBRead.Close()
		
		// Test with unsupported page size (should fail)
		filename16KB := "test_16kb_pages.bytedb"
		defer os.Remove(filename16KB)
		
		opts16KB := NewCompressionOptions().WithPageSize(16384)
		_, err = CreateFileWithOptions(filename16KB, opts16KB)
		if err == nil {
			t.Error("Expected error creating file with 16KB pages (not yet supported)")
		}
		if err != nil && err.Error() != "page size validation failed: dynamic page size not yet supported: requested 16384 bytes, system currently only supports 4096 bytes" {
			t.Errorf("Unexpected error message: %v", err)
		}
	})
	
	// Test 6: Storing page size for future compatibility
	t.Run("PageSizeStoredInHeader", func(t *testing.T) {
		// Even though only 4KB works, the configuration should be stored
		filename := "test_page_size_header.bytedb"
		defer os.Remove(filename)
		
		// Create with 4KB (the only supported size)
		opts := NewCompressionOptions().WithPageSize(4096)
		cf, err := CreateFileWithOptions(filename, opts)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		// Add some data
		cf.AddColumn("test", DataTypeInt64, false)
		cf.Close()
		
		// Read back and verify
		cfRead, err := OpenFile(filename)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer cfRead.Close()
		
		// Check header
		if cfRead.header.PageSize != 4096 {
			t.Errorf("Expected page size 4096 in header, got %d", cfRead.header.PageSize)
		}
		
		// Check reconstructed options
		if cfRead.compressionOptions.PageSize != 4096 {
			t.Errorf("Expected page size 4096 in options, got %d", cfRead.compressionOptions.PageSize)
		}
	})
}

// TestPageSizeDocumentation provides examples of how to use page size configuration
func TestPageSizeDocumentation(t *testing.T) {
	t.Log("\n=== PAGE SIZE CONFIGURATION EXAMPLES ===\n")
	
	t.Log("Current Status:")
	t.Log("- Page size configuration is stored in file header")
	t.Log("- Only 4KB (4096 bytes) is currently supported")
	t.Log("- Other sizes will fail with validation error")
	t.Log("- Full dynamic page size support planned for future release")
	
	t.Log("\nExample Usage:")
	t.Log("```go")
	t.Log("// Default configuration (16KB - will fail validation)")
	t.Log("opts := NewCompressionOptions()")
	t.Log("")
	t.Log("// Configure 4KB pages (currently the only supported size)")
	t.Log("opts = NewCompressionOptions().WithPageSize(4096)")
	t.Log("")
	t.Log("// Future: Configure 16KB pages for better compression")
	t.Log("// opts = NewCompressionOptions().WithPageSize(16*1024)")
	t.Log("")
	t.Log("// Create file with page size configuration")
	t.Log("cf, err := CreateFileWithOptions(\"data.bytedb\", opts)")
	t.Log("```")
	
	t.Log("\nBenefits when fully implemented:")
	t.Log("- 16KB pages: 15-35% better compression")
	t.Log("- 32KB pages: 25-50% better compression")
	t.Log("- 64KB pages: 30-65% better compression")
	t.Log("- Fewer I/O operations with larger pages")
}