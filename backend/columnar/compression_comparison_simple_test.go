package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// TestSimpleCompressionComparison provides a focused comparison of compressed vs uncompressed
func TestSimpleCompressionComparison(t *testing.T) {
	volumes := []int{1000, 10000, 50000}
	
	t.Log("\n=== COMPRESSION VS UNCOMPRESSED COMPARISON ===\n")
	
	for _, volume := range volumes {
		t.Logf("\n--- Testing with %d rows ---\n", volume)
		
		// Test uncompressed
		uncompResult := runSimpleBenchmark(t, "Uncompressed", CompressionNone, volume)
		
		// Test with compression
		compResults := []SimpleBenchmarkResult{
			runSimpleBenchmark(t, "Snappy", CompressionSnappy, volume),
			runSimpleBenchmark(t, "Gzip", CompressionGzip, volume),
			runSimpleBenchmark(t, "Zstd", CompressionZstd, volume),
		}
		
		// Print comparison
		printSimpleComparison(t, uncompResult, compResults)
	}
}

type SimpleBenchmarkResult struct {
	Mode               string
	Volume             int
	FileSize           int64
	TotalPages         uint64
	UncompressedBytes  int64
	CompressedBytes    int64
	CompressionRatio   float64
	WriteTime          time.Duration
	OpenTime           time.Duration
	QueryTime          time.Duration
	RowLookupTime      time.Duration
}

func runSimpleBenchmark(t *testing.T, mode string, compressionType CompressionType, volume int) SimpleBenchmarkResult {
	filename := fmt.Sprintf("simple_%s_%d.bytedb", mode, volume)
	defer os.Remove(filename)
	
	result := SimpleBenchmarkResult{
		Mode:   mode,
		Volume: volume,
	}
	
	// CREATE AND WRITE DATA
	start := time.Now()
	
	var cf *ColumnarFile
	var err error
	
	if compressionType == CompressionNone {
		cf, err = CreateFile(filename)
	} else {
		opts := NewCompressionOptions().WithPageCompression(compressionType, CompressionLevelDefault)
		cf, err = CreateFileWithOptions(filename, opts)
	}
	
	if err != nil {
		t.Fatal(err)
	}
	
	// Add columns representing typical use cases
	cf.AddColumn("id", DataTypeInt64, false)              // Sequential IDs
	cf.AddColumn("user_id", DataTypeInt64, false)         // Repeated values (good for compression)
	cf.AddColumn("timestamp", DataTypeInt64, false)       // Sequential timestamps
	cf.AddColumn("amount", DataTypeInt64, true)           // Random values with nulls
	cf.AddColumn("status", DataTypeString, false)         // Low cardinality strings
	cf.AddColumn("description", DataTypeString, true)     // High cardinality strings with nulls
	
	// Generate realistic data patterns
	data := generateSimpleTestData(volume)
	
	// Load data
	cf.LoadIntColumn("id", data.ids)
	cf.LoadIntColumn("user_id", data.userIDs)
	cf.LoadIntColumn("timestamp", data.timestamps)
	cf.LoadIntColumn("amount", data.amounts)
	cf.LoadStringColumn("status", data.statuses)
	cf.LoadStringColumn("description", data.descriptions)
	
	result.WriteTime = time.Since(start)
	
	// Get compression statistics
	stats := cf.pageManager.GetCompressionStats()
	result.UncompressedBytes = stats.UncompressedBytes
	result.CompressedBytes = stats.CompressedBytes
	result.CompressionRatio = stats.CompressionRatio
	result.TotalPages = cf.pageManager.GetPageCount()
	
	cf.Close()
	
	// Get file size
	info, _ := os.Stat(filename)
	result.FileSize = info.Size()
	
	// BENCHMARK READ OPERATIONS
	start = time.Now()
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	result.OpenTime = time.Since(start)
	
	// Benchmark queries
	start = time.Now()
	// Test various query patterns
	for i := 0; i < 100; i++ {
		// Random ID lookup
		targetID := rand.Intn(volume)
		bitmap, _ := cf2.QueryInt("id", int64(targetID))
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Expected 1 result for ID %d", targetID)
		}
		
		// Status query (low cardinality)
		statuses := []string{"active", "pending", "completed", "cancelled"}
		status := statuses[i%len(statuses)]
		bitmap, _ = cf2.QueryString("status", status)
		_ = bitmap.GetCardinality()
		
		// Range query
		minAmount := rand.Intn(5000)
		maxAmount := minAmount + 2000
		bitmap, _ = cf2.RangeQueryInt("amount", int64(minAmount), int64(maxAmount))
		_ = bitmap.GetCardinality()
	}
	result.QueryTime = time.Since(start) / 100
	
	// Benchmark row lookups
	start = time.Now()
	for i := 0; i < 100; i++ {
		row := uint64(rand.Intn(volume))
		cf2.LookupIntByRow("id", row)
		cf2.LookupStringByRow("status", row)
	}
	result.RowLookupTime = time.Since(start) / 200
	
	cf2.Close()
	
	return result
}

type SimpleTestData struct {
	ids          []IntData
	userIDs      []IntData
	timestamps   []IntData
	amounts      []IntData
	statuses     []StringData
	descriptions []StringData
}

func generateSimpleTestData(volume int) SimpleTestData {
	data := SimpleTestData{
		ids:          make([]IntData, volume),
		userIDs:      make([]IntData, volume),
		timestamps:   make([]IntData, volume),
		amounts:      make([]IntData, volume),
		statuses:     make([]StringData, volume),
		descriptions: make([]StringData, volume),
	}
	
	statuses := []string{"active", "pending", "completed", "cancelled"}
	baseTime := time.Now().Unix()
	
	for i := 0; i < volume; i++ {
		// Sequential IDs
		data.ids[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		
		// User IDs - simulate 100 users (highly repetitive, good for compression)
		data.userIDs[i] = IntData{Value: int64(i % 100), RowNum: uint64(i)}
		
		// Sequential timestamps (compresses well)
		data.timestamps[i] = IntData{Value: baseTime + int64(i*60), RowNum: uint64(i)}
		
		// Random amounts with 20% nulls
		if i%5 == 0 {
			data.amounts[i] = IntData{IsNull: true, RowNum: uint64(i)}
		} else {
			data.amounts[i] = IntData{Value: int64(rand.Intn(10000)), RowNum: uint64(i)}
		}
		
		// Low cardinality status
		data.statuses[i] = StringData{Value: statuses[i%len(statuses)], RowNum: uint64(i)}
		
		// Descriptions - mix of repeated and unique with 30% nulls
		if i%10 < 3 {
			data.descriptions[i] = StringData{IsNull: true, RowNum: uint64(i)}
		} else if i%10 < 7 {
			// Repeated descriptions (good for compression)
			data.descriptions[i] = StringData{
				Value:  fmt.Sprintf("Standard transaction description type %d", i%20),
				RowNum: uint64(i),
			}
		} else {
			// Unique descriptions
			data.descriptions[i] = StringData{
				Value:  fmt.Sprintf("Unique description for transaction %d with additional details and metadata", i),
				RowNum: uint64(i),
			}
		}
	}
	
	return data
}

func printSimpleComparison(t *testing.T, uncompressed SimpleBenchmarkResult, compressed []SimpleBenchmarkResult) {
	// Header
	t.Log("Mode          File(KB)  Pages  Data(MB)  Compressed(MB)  Ratio  Savings  Write(ms)  Open(ms)  Query(μs)  Lookup(μs)")
	t.Log("----          --------  -----  --------  --------------  -----  -------  ---------  --------  ---------  ----------")
	
	// Print uncompressed baseline
	printResultRow(t, uncompressed, 0.0)
	
	// Print compressed results with savings
	for _, result := range compressed {
		savings := 0.0
		if uncompressed.FileSize > 0 {
			savings = float64(uncompressed.FileSize-result.FileSize) / float64(uncompressed.FileSize) * 100
		}
		printResultRow(t, result, savings)
	}
	
	// Summary
	t.Log("\nSUMMARY:")
	
	// Find best compression
	bestCompression := ""
	bestDataSavings := 0.0
	bestFileSavings := 0.0
	
	for _, result := range compressed {
		dataSavings := 0.0
		if result.UncompressedBytes > 0 {
			dataSavings = float64(result.UncompressedBytes-result.CompressedBytes) / float64(result.UncompressedBytes) * 100
		}
		
		fileSavings := 0.0
		if uncompressed.FileSize > 0 {
			fileSavings = float64(uncompressed.FileSize-result.FileSize) / float64(uncompressed.FileSize) * 100
		}
		
		if dataSavings > bestDataSavings {
			bestDataSavings = dataSavings
			bestFileSavings = fileSavings
			bestCompression = result.Mode
		}
	}
	
	if bestDataSavings > 0 {
		t.Logf("  Best compression: %s", bestCompression)
		t.Logf("    - Data reduction: %.1f%%", bestDataSavings)
		t.Logf("    - File size reduction: %.1f%%", bestFileSavings)
	}
	
	// Performance comparison
	t.Logf("\n  Performance impact:")
	for _, result := range compressed {
		writeOverhead := float64(result.WriteTime-uncompressed.WriteTime) / float64(uncompressed.WriteTime) * 100
		queryOverhead := float64(result.QueryTime-uncompressed.QueryTime) / float64(uncompressed.QueryTime) * 100
		
		t.Logf("    %s:", result.Mode)
		t.Logf("      - Write time: %+.0f%%", writeOverhead)
		t.Logf("      - Query time: %+.0f%%", queryOverhead)
	}
}

func printResultRow(t *testing.T, result SimpleBenchmarkResult, fileSavings float64) {
	t.Logf("%-12s  %8.1f  %5d  %8.2f  %14.2f  %5.2f  %6.1f%%  %9.1f  %8.1f  %9.1f  %10.1f",
		result.Mode,
		float64(result.FileSize)/1024,
		result.TotalPages,
		float64(result.UncompressedBytes)/(1024*1024),
		float64(result.CompressedBytes)/(1024*1024),
		result.CompressionRatio,
		fileSavings,
		float64(result.WriteTime.Milliseconds()),
		float64(result.OpenTime.Milliseconds()),
		float64(result.QueryTime.Microseconds()),
		float64(result.RowLookupTime.Microseconds()),
	)
}