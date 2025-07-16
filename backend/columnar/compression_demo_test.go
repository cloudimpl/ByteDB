package columnar

import (
	"os"
	"testing"
)

// TestCompressionQueryDemo demonstrates how compression and queries work
func TestCompressionQueryDemo(t *testing.T) {
	t.Log("\n=== COMPRESSION AND QUERY DEMONSTRATION ===\n")
	
	// Create two identical files - one compressed, one not
	uncompFile := "demo_uncompressed.bytedb"
	compFile := "demo_compressed.bytedb"
	defer func() {
		os.Remove(uncompFile)
		os.Remove(compFile)
	}()
	
	// Step 1: Create files
	t.Log("STEP 1: Creating files with identical data")
	t.Log("----------------------------------------")
	
	// Uncompressed file
	cf1, _ := CreateFile(uncompFile)
	cf1.AddColumn("user_id", DataTypeInt64, false)
	cf1.AddColumn("product", DataTypeString, false)
	cf1.AddColumn("quantity", DataTypeInt64, false)
	
	// Compressed file (using Snappy)
	opts := NewCompressionOptions().WithPageCompression(CompressionSnappy, CompressionLevelDefault)
	cf2, _ := CreateFileWithOptions(compFile, opts)
	cf2.AddColumn("user_id", DataTypeInt64, false)
	cf2.AddColumn("product", DataTypeString, false)
	cf2.AddColumn("quantity", DataTypeInt64, false)
	
	// Load identical data
	numRows := 10000
	userData := make([]IntData, numRows)
	productData := make([]StringData, numRows)
	quantityData := make([]IntData, numRows)
	
	// Generate data with patterns that compress well
	products := []string{"laptop", "mouse", "keyboard", "monitor", "cable"}
	
	for i := 0; i < numRows; i++ {
		// User IDs: 100 unique users (highly repetitive)
		userData[i] = IntData{Value: int64(i % 100), RowNum: uint64(i)}
		
		// Products: 5 products (very repetitive)
		productData[i] = StringData{Value: products[i%5], RowNum: uint64(i)}
		
		// Quantities: small range
		quantityData[i] = IntData{Value: int64(1 + i%10), RowNum: uint64(i)}
	}
	
	// Load into both files
	cf1.LoadIntColumn("user_id", userData)
	cf1.LoadStringColumn("product", productData)
	cf1.LoadIntColumn("quantity", quantityData)
	
	cf2.LoadIntColumn("user_id", userData)
	cf2.LoadStringColumn("product", productData)
	cf2.LoadIntColumn("quantity", quantityData)
	
	// Get statistics
	stats1 := cf1.pageManager.GetCompressionStats()
	stats2 := cf2.pageManager.GetCompressionStats()
	
	t.Logf("Uncompressed file:")
	t.Logf("  - Pages: %d", cf1.pageManager.GetPageCount())
	t.Logf("  - Data size: %.2f MB", float64(stats1.UncompressedBytes)/(1024*1024))
	
	t.Logf("Compressed file (Snappy):")
	t.Logf("  - Pages: %d", cf2.pageManager.GetPageCount())
	t.Logf("  - Uncompressed data: %.2f MB", float64(stats2.UncompressedBytes)/(1024*1024))
	t.Logf("  - Compressed data: %.2f MB", float64(stats2.CompressedBytes)/(1024*1024))
	t.Logf("  - Compression ratio: %.2f", stats2.CompressionRatio)
	t.Logf("  - Space saved: %.1f%%", (1-stats2.CompressionRatio)*100)
	
	cf1.Close()
	cf2.Close()
	
	// Step 2: Demonstrate query execution
	t.Log("\n\nSTEP 2: Query Execution Comparison")
	t.Log("-----------------------------------")
	
	// Open files for querying
	cf1, _ = OpenFile(uncompFile)
	cf2, _ = OpenFile(compFile)
	defer cf1.Close()
	defer cf2.Close()
	
	// Example query: Find all orders for user_id = 42
	t.Log("\nQuery: Find all orders for user_id = 42")
	
	// Trace through uncompressed query
	t.Log("\nUNCOMPRESSED QUERY EXECUTION:")
	result1, _ := cf1.QueryInt("user_id", 42)
	t.Logf("1. Load B-tree root page (no decompression needed)")
	t.Logf("2. Binary search to find child page")
	t.Logf("3. Load leaf pages containing user_id values 40-50")
	t.Logf("4. Scan pages for user_id = 42")
	t.Logf("5. Found %d matching rows", result1.GetCardinality())
	
	// Trace through compressed query
	t.Log("\nCOMPRESSED QUERY EXECUTION:")
	result2, _ := cf2.QueryInt("user_id", 42)
	t.Logf("1. Load B-tree root page")
	t.Logf("   - Read compressed data from disk (smaller I/O)")
	t.Logf("   - Decompress with Snappy (very fast)")
	t.Logf("2. Binary search decompressed data")
	t.Logf("3. Load compressed leaf pages")
	t.Logf("   - Decompress each page once in memory")
	t.Logf("4. Scan decompressed pages for user_id = 42")
	t.Logf("5. Found %d matching rows", result2.GetCardinality())
	
	// Verify same results
	if result1.GetCardinality() != result2.GetCardinality() {
		t.Error("Query results differ between compressed and uncompressed!")
	}
	
	// Step 3: Show different query types
	t.Log("\n\nSTEP 3: Different Query Types")
	t.Log("------------------------------")
	
	// String query
	t.Log("\nString Query: product = 'laptop'")
	laptopResults1, _ := cf1.QueryString("product", "laptop")
	laptopResults2, _ := cf2.QueryString("product", "laptop")
	t.Logf("  Uncompressed: %d results", laptopResults1.GetCardinality())
	t.Logf("  Compressed: %d results", laptopResults2.GetCardinality())
	
	// Range query
	t.Log("\nRange Query: quantity BETWEEN 5 AND 8")
	rangeResults1, _ := cf1.RangeQueryInt("quantity", 5, 8)
	rangeResults2, _ := cf2.RangeQueryInt("quantity", 5, 8)
	t.Logf("  Uncompressed: %d results", rangeResults1.GetCardinality())
	t.Logf("  Compressed: %d results", rangeResults2.GetCardinality())
	
	// Complex query
	t.Log("\nComplex Query: user_id < 10 AND product = 'mouse'")
	userBitmap1, _ := cf1.QueryLessThan("user_id", 10)
	mouseBitmap1, _ := cf1.QueryString("product", "mouse")
	complex1 := cf1.QueryAnd(userBitmap1, mouseBitmap1)
	
	userBitmap2, _ := cf2.QueryLessThan("user_id", 10)
	mouseBitmap2, _ := cf2.QueryString("product", "mouse")
	complex2 := cf2.QueryAnd(userBitmap2, mouseBitmap2)
	
	t.Logf("  Uncompressed: %d results", complex1.GetCardinality())
	t.Logf("  Compressed: %d results", complex2.GetCardinality())
	
	// Step 4: Explain page structure
	t.Log("\n\nSTEP 4: Page Structure Visualization")
	t.Log("------------------------------------")
	
	t.Log("\nPage Layout (4KB pages):")
	t.Log("┌─────────────────────────┐")
	t.Log("│   Page Header (128B)    │ <- Never compressed")
	t.Log("├─────────────────────────┤")
	t.Log("│                         │")
	t.Log("│    Page Data (3968B)    │ <- Compressed/Uncompressed")
	t.Log("│                         │")
	t.Log("└─────────────────────────┘")
	
	t.Log("\nCompression Process:")
	t.Log("1. Original data: [42,42,42,42,42,42,42,42...]")
	t.Log("2. Snappy finds pattern: '42' repeated")
	t.Log("3. Compressed: [repeat:42,count:8,...]")
	t.Log("4. Result: 3968 bytes → ~800 bytes")
	
	// Step 5: Performance implications
	t.Log("\n\nSTEP 5: Performance Analysis")
	t.Log("----------------------------")
	
	t.Log("\nWhy compressed queries can be faster:")
	t.Log("1. DISK I/O REDUCTION:")
	t.Logf("   - Uncompressed: Read %.1f MB from disk", float64(stats1.UncompressedBytes)/(1024*1024))
	t.Logf("   - Compressed: Read %.1f MB from disk", float64(stats2.CompressedBytes)/(1024*1024))
	t.Logf("   - I/O Saved: %.1f%%", (1-stats2.CompressionRatio)*100)
	
	t.Log("\n2. MEMORY EFFICIENCY:")
	t.Log("   - More compressed pages fit in RAM")
	t.Log("   - Better CPU cache utilization")
	t.Log("   - Fewer page faults")
	
	t.Log("\n3. DECOMPRESSION OVERHEAD:")
	t.Log("   - Snappy: ~1-2 microseconds per 4KB page")
	t.Log("   - Disk read: ~100 microseconds per 4KB")
	t.Log("   - Net benefit: Decompression is 50x faster than saved I/O")
}

// TestCompressionAlgorithmComparison shows how different algorithms work
func TestCompressionAlgorithmComparison(t *testing.T) {
	t.Log("\n=== COMPRESSION ALGORITHM COMPARISON ===\n")
	
	// Sample data patterns (described in text below)
	
	t.Log("How different algorithms compress data:\n")
	
	t.Log("1. SNAPPY (Speed-optimized):")
	t.Log("   - Looks for: Repeated byte sequences")
	t.Log("   - Example: 'user_123' repeated 4 times")
	t.Log("   - Encodes as: [literal:'user_123'][copy:offset=0,length=8,count=3]")
	t.Log("   - Best for: Repeated patterns, fast processing")
	
	t.Log("\n2. GZIP (Balanced):")
	t.Log("   - Looks for: Patterns + frequency analysis")
	t.Log("   - Builds: Huffman tree of common sequences")
	t.Log("   - Example: Assigns short codes to common values")
	t.Log("   - Best for: General purpose, good compression")
	
	t.Log("\n3. ZSTD (Modern algorithm):")
	t.Log("   - Looks for: Complex patterns + dictionary")
	t.Log("   - Learns: Optimal encoding from data")
	t.Log("   - Example: Builds custom dictionary for your data")
	t.Log("   - Best for: Maximum compression, good speed")
	
	t.Log("\nCompression Effectiveness by Data Type:")
	t.Log("┌─────────────────┬─────────┬──────┬──────┐")
	t.Log("│ Data Pattern    │ Snappy  │ Gzip │ Zstd │")
	t.Log("├─────────────────┼─────────┼──────┼──────┤")
	t.Log("│ Sequential IDs  │ Good    │ Best │ Best │")
	t.Log("│ Repeated values │ Best    │ Good │ Best │")
	t.Log("│ Random data     │ Poor    │ Poor │ Poor │")
	t.Log("│ Text strings    │ Good    │ Best │ Best │")
	t.Log("│ Timestamps      │ Good    │ Good │ Best │")
	t.Log("└─────────────────┴─────────┴──────┴──────┘")
}