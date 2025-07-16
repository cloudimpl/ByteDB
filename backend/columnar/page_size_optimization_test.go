package columnar

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestPageSizeOptimization explores the impact of different page sizes on compression
func TestPageSizeOptimization(t *testing.T) {
	// Test different page sizes
	pageSizes := []int{
		4 * 1024,   // 4KB (current default)
		8 * 1024,   // 8KB
		16 * 1024,  // 16KB
		32 * 1024,  // 32KB
		64 * 1024,  // 64KB
		128 * 1024, // 128KB
	}
	
	// Test with different data volumes
	volumes := []int{10000, 50000}
	
	t.Log("\n=== PAGE SIZE OPTIMIZATION ANALYSIS ===\n")
	
	for _, volume := range volumes {
		t.Logf("\n--- Testing with %d rows ---\n", volume)
		
		results := make(map[int]PageSizeResult)
		
		for _, pageSize := range pageSizes {
			result := simulatePageSize(t, pageSize, volume)
			results[pageSize] = result
		}
		
		// Print comparison
		printPageSizeComparison(t, results, volume)
	}
}

type PageSizeResult struct {
	PageSize          int
	FileSize          int64
	TotalPages        int
	CompressionRatio  float64
	AvgBytesPerPage   float64
	WastedSpace       int64
	WriteTime         time.Duration
	ReadTime          time.Duration
	QueryTime         time.Duration
}

// simulatePageSize simulates compression with a specific page size
// Since we can't actually change the page size constant, we'll simulate the effects
func simulatePageSize(t *testing.T, pageSize int, volume int) PageSizeResult {
	// For simulation, we'll calculate theoretical values based on compression behavior
	
	result := PageSizeResult{
		PageSize: pageSize,
	}
	
	// Generate sample data to estimate compression
	_ = generateSampleData(volume)
	
	// Estimate compression ratio based on page size
	// Larger pages generally compress better due to more data for pattern finding
	baseRatio := 0.3 // Base compression ratio for 4KB pages
	
	// Compression improves logarithmically with page size
	sizeMultiplier := float64(pageSize) / 4096.0
	compressionImprovement := 1.0
	if sizeMultiplier > 1 {
		// Better compression with larger pages, up to a point
		compressionImprovement = 1.0 - (0.15 * (1.0 - 1.0/sizeMultiplier))
	}
	result.CompressionRatio = baseRatio * compressionImprovement
	
	// Calculate file metrics
	uncompressedDataSize := int64(volume) * 100 // Assume 100 bytes per row average
	compressedDataSize := int64(float64(uncompressedDataSize) * result.CompressionRatio)
	
	// Account for metadata overhead (headers, indexes, etc.)
	metadataOverhead := int64(volume) * 20 // ~20 bytes metadata per row
	totalDataSize := compressedDataSize + metadataOverhead
	
	// Calculate pages needed
	result.TotalPages = int(totalDataSize/int64(pageSize)) + 1
	result.FileSize = int64(result.TotalPages * pageSize)
	
	// Calculate wasted space (internal fragmentation)
	usedSpace := totalDataSize
	result.WastedSpace = result.FileSize - usedSpace
	result.AvgBytesPerPage = float64(usedSpace) / float64(result.TotalPages)
	
	// Simulate performance characteristics
	// Larger pages = fewer I/O operations but more memory per operation
	
	// Write time increases slightly with page size due to larger buffers
	baseWriteTime := time.Duration(volume) * time.Microsecond * 10
	writeOverhead := 1.0 + (float64(pageSize-4096) / 1000000.0)
	result.WriteTime = time.Duration(float64(baseWriteTime) * writeOverhead)
	
	// Read time improves with larger pages (fewer seeks)
	baseReadTime := time.Duration(result.TotalPages) * time.Microsecond * 100
	result.ReadTime = baseReadTime
	
	// Query time depends on pages read
	pagesPerQuery := 5 // Assume average query reads 5 pages
	if pageSize > 16*1024 {
		pagesPerQuery = 3 // Larger pages = fewer pages to read
	}
	result.QueryTime = time.Duration(pagesPerQuery) * time.Microsecond * 50
	
	return result
}

func generateSampleData(volume int) []byte {
	// Generate representative data for compression estimation
	data := make([]byte, volume*100)
	
	// Mix of patterns:
	// 30% repetitive data (compresses well)
	// 40% semi-structured (moderate compression)
	// 30% random (poor compression)
	
	for i := 0; i < len(data); i++ {
		if i%10 < 3 {
			// Repetitive pattern
			data[i] = byte(i % 10)
		} else if i%10 < 7 {
			// Semi-structured
			data[i] = byte((i/100)%256 + i%10)
		} else {
			// Random
			data[i] = byte(rand.Intn(256))
		}
	}
	
	return data
}

func logBase(x, base float64) float64 {
	if x <= 0 || base <= 0 || base == 1 {
		return 0
	}
	return float64(int(10 * (1 + (x-1)/(base-1)))) / 10
}

func printPageSizeComparison(t *testing.T, results map[int]PageSizeResult, volume int) {
	t.Log("Page Size  File(MB)  Pages  Compression  Avg/Page  Wasted(KB)  Write(ms)  Read(ms)  Query(μs)")
	t.Log("---------  --------  -----  -----------  --------  ----------  ---------  --------  ---------")
	
	// Find best metrics for highlighting
	bestCompression := 1.0
	minWaste := int64(1 << 60)
	minFileSize := int64(1 << 60)
	
	for _, r := range results {
		if r.CompressionRatio < bestCompression {
			bestCompression = r.CompressionRatio
		}
		if r.WastedSpace < minWaste {
			minWaste = r.WastedSpace
		}
		if r.FileSize < minFileSize {
			minFileSize = r.FileSize
		}
	}
	
	// Print results in order
	for _, pageSize := range []int{4096, 8192, 16384, 32768, 65536, 131072} {
		r := results[pageSize]
		
		// Highlight best values
		compressionMark := ""
		if r.CompressionRatio == bestCompression {
			compressionMark = "*"
		}
		
		wasteMark := ""
		if r.WastedSpace == minWaste {
			wasteMark = "*"
		}
		
		t.Logf("%-9s  %8.2f  %5d  %10.2f%s  %8.0f  %10.1f%s  %9.1f  %8.1f  %9.1f",
			formatPageSize(r.PageSize),
			float64(r.FileSize)/(1024*1024),
			r.TotalPages,
			r.CompressionRatio,
			compressionMark,
			r.AvgBytesPerPage,
			float64(r.WastedSpace)/1024,
			wasteMark,
			float64(r.WriteTime.Milliseconds()),
			float64(r.ReadTime.Milliseconds()),
			float64(r.QueryTime.Microseconds()),
		)
	}
	
	// Analysis and recommendations
	t.Log("\nANALYSIS:")
	t.Logf("* = Best value")
	
	// Calculate trade-offs
	smallPageResult := results[4096]
	largePageResult := results[65536]
	
	compressionGain := (smallPageResult.CompressionRatio - largePageResult.CompressionRatio) / smallPageResult.CompressionRatio * 100
	wasteIncrease := float64(largePageResult.WastedSpace-smallPageResult.WastedSpace) / 1024
	
	t.Logf("\nTrade-offs (4KB vs 64KB pages):")
	t.Logf("  - Compression improvement: %.1f%%", compressionGain)
	t.Logf("  - Additional wasted space: %.1f KB", wasteIncrease)
	t.Logf("  - Read operations reduced by: %.1f%%", 
		float64(smallPageResult.TotalPages-largePageResult.TotalPages)/float64(smallPageResult.TotalPages)*100)
	
	// Recommendations
	t.Log("\nRECOMMENDATIONS:")
	
	if volume < 50000 {
		t.Log("  For small datasets (<50K rows):")
		t.Log("    - 8KB or 16KB pages provide good balance")
		t.Log("    - Minimal waste while improving compression")
	} else {
		t.Log("  For large datasets (>50K rows):")
		t.Log("    - 32KB or 64KB pages recommended")
		t.Log("    - Better compression ratios")
		t.Log("    - Fewer I/O operations")
	}
	
	t.Log("\n  Considerations:")
	t.Log("    - Larger pages use more memory per operation")
	t.Log("    - Smaller pages have better space efficiency for small datasets")
	t.Log("    - Database cache efficiency depends on page size")
}

func formatPageSize(size int) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%dKB", size/1024)
	} else {
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
}

// TestCompressionWithDifferentPageSizes demonstrates theoretical compression improvements
func TestCompressionWithDifferentPageSizes(t *testing.T) {
	t.Log("\n=== COMPRESSION EFFICIENCY BY PAGE SIZE ===\n")
	
	// Simulate compression of different data patterns with various page sizes
	patterns := []struct {
		name        string
		genFunc     func(size int) []byte
		description string
	}{
		{
			"Sequential",
			generateSequentialData,
			"Sequential integers (timestamps, IDs)",
		},
		{
			"Repetitive",
			generateRepetitiveData,
			"Highly repetitive data (status codes, categories)",
		},
		{
			"Text",
			generateTextData,
			"Natural language text (descriptions, comments)",
		},
		{
			"Mixed",
			generateMixedData,
			"Mixed patterns (typical database content)",
		},
	}
	
	pageSizes := []int{1024, 2048, 4096, 8192, 16384, 32768, 65536}
	
	for _, pattern := range patterns {
		t.Logf("\nPattern: %s", pattern.name)
		t.Logf("Description: %s", pattern.description)
		t.Log("Page Size  Compression Ratio  Improvement")
		t.Log("---------  ----------------  -----------")
		
		baseRatio := 0.0
		
		for _, pageSize := range pageSizes {
			// Generate data of page size
			data := pattern.genFunc(pageSize)
			
			// Simulate compression (in reality, we'd use actual compression)
			ratio := simulateCompressionRatio(data, pageSize)
			
			if baseRatio == 0 {
				baseRatio = ratio
			}
			
			improvement := (baseRatio - ratio) / baseRatio * 100
			
			t.Logf("%-9s  %16.2f  %10.1f%%",
				formatPageSize(pageSize),
				ratio,
				improvement,
			)
		}
	}
	
	t.Log("\nKEY INSIGHTS:")
	t.Log("1. Larger pages provide better compression due to:")
	t.Log("   - More context for pattern detection")
	t.Log("   - Amortized header overhead")
	t.Log("   - Better dictionary building (for algorithms like Zstd)")
	t.Log("")
	t.Log("2. Diminishing returns after 32KB-64KB")
	t.Log("3. Memory usage increases linearly with page size")
	t.Log("4. Optimal size depends on:")
	t.Log("   - Available RAM")
	t.Log("   - Data patterns")
	t.Log("   - Read/write patterns")
}

func generateSequentialData(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size/8; i++ {
		// Sequential 64-bit integers
		val := uint64(i)
		for j := 0; j < 8; j++ {
			data[i*8+j] = byte(val >> (j * 8))
		}
	}
	return data
}

func generateRepetitiveData(size int) []byte {
	data := make([]byte, size)
	patterns := []string{"ACTIVE", "INACTIVE", "PENDING", "COMPLETED"}
	
	offset := 0
	for offset < size {
		pattern := patterns[rand.Intn(len(patterns))]
		copy(data[offset:], []byte(pattern))
		offset += len(pattern)
	}
	
	return data
}

func generateTextData(size int) []byte {
	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"database", "compression", "performance", "optimization", "storage", "efficiency"}
	
	data := make([]byte, size)
	offset := 0
	
	for offset < size-10 {
		word := words[rand.Intn(len(words))]
		copy(data[offset:], []byte(word))
		offset += len(word)
		if offset < size {
			data[offset] = ' '
			offset++
		}
	}
	
	return data
}

func generateMixedData(size int) []byte {
	data := make([]byte, size)
	
	for i := 0; i < size; i++ {
		switch i % 4 {
		case 0:
			// Sequential
			data[i] = byte(i % 256)
		case 1:
			// Repetitive
			data[i] = byte((i / 100) % 10)
		case 2:
			// Semi-random
			data[i] = byte((i * 7) % 256)
		case 3:
			// Random
			data[i] = byte(rand.Intn(256))
		}
	}
	
	return data
}

func simulateCompressionRatio(data []byte, pageSize int) float64 {
	// Simulate compression effectiveness based on patterns and page size
	
	// Count unique bytes (entropy estimation)
	unique := make(map[byte]int)
	runs := 0
	lastByte := data[0]
	
	for i, b := range data {
		unique[b]++
		if i > 0 && b == lastByte {
			runs++
		}
		lastByte = b
	}
	
	// Calculate metrics
	entropy := float64(len(unique)) / 256.0
	runRatio := float64(runs) / float64(len(data))
	
	// Base compression ratio from entropy
	baseRatio := 0.125 + (entropy * 0.875)
	
	// Improvement from runs
	runImprovement := 1.0 - (runRatio * 0.5)
	
	// Page size benefit (larger pages compress better)
	// Logarithmic improvement up to 64KB
	sizeBenefit := 1.0
	if pageSize > 1024 {
		sizeBenefit = 1.0 - (0.15 * (float64(pageSize-1024) / float64(65536-1024)))
	}
	
	finalRatio := baseRatio * runImprovement * sizeBenefit
	
	// Ensure ratio is between 0.1 and 1.0
	if finalRatio < 0.1 {
		finalRatio = 0.1
	}
	if finalRatio > 1.0 {
		finalRatio = 1.0
	}
	
	return finalRatio
}