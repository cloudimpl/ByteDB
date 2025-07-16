package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// DetailedBenchmarkResult holds comprehensive metrics
type DetailedBenchmarkResult struct {
	Mode               string
	CompressionType    string
	Volume             int
	FileSize           int64
	DataPages          uint64
	IndexPages         uint64
	TotalPages         uint64
	UncompressedBytes  uint64
	CompressedBytes    uint64
	CompressionRatio   float64
	ActualSavings      float64
	WriteTime          time.Duration
	ReadTime           time.Duration
	QueryMetrics       QueryPerformance
}

type QueryPerformance struct {
	ExactMatchTime   time.Duration
	RangeQueryTime   time.Duration
	RowLookupTime    time.Duration
	StringQueryTime  time.Duration
	NullQueryTime    time.Duration
	ComplexQueryTime time.Duration
}

// TestDetailedCompressionAnalysis provides detailed compression analysis
func TestDetailedCompressionAnalysis(t *testing.T) {
	volumes := []int{1000, 10000, 100000}
	allResults := make(map[int][]DetailedBenchmarkResult)

	for _, volume := range volumes {
		t.Run(fmt.Sprintf("Volume_%d", volume), func(t *testing.T) {
			results := []DetailedBenchmarkResult{}

			// Test uncompressed
			uncompResult := runDetailedBenchmark(t, "Uncompressed", CompressionNone, volume)
			results = append(results, uncompResult)

			// Test compressed modes
			compressionModes := []struct {
				name  string
				cType CompressionType
				level CompressionLevel
			}{
				{"Snappy", CompressionSnappy, CompressionLevelDefault},
				{"Gzip-Fastest", CompressionGzip, CompressionLevelFastest},
				{"Gzip-Default", CompressionGzip, CompressionLevelDefault},
				{"Gzip-Best", CompressionGzip, CompressionLevelBest},
				{"Zstd-Fastest", CompressionZstd, CompressionLevelFastest},
				{"Zstd-Default", CompressionZstd, CompressionLevelDefault},
				{"Zstd-Best", CompressionZstd, CompressionLevelBest},
			}

			for _, mode := range compressionModes {
				compResult := runDetailedBenchmarkWithLevel(t, mode.name, mode.cType, mode.level, volume)
				results = append(results, compResult)
			}

			allResults[volume] = results
		})
	}

	// Print detailed report
	printDetailedReport(t, allResults)
}

func runDetailedBenchmark(t *testing.T, modeName string, compressionType CompressionType, volume int) DetailedBenchmarkResult {
	return runDetailedBenchmarkWithLevel(t, modeName, compressionType, CompressionLevelDefault, volume)
}

func runDetailedBenchmarkWithLevel(t *testing.T, modeName string, compressionType CompressionType, level CompressionLevel, volume int) DetailedBenchmarkResult {
	filename := fmt.Sprintf("detailed_%s_%d.bytedb", strings.ToLower(strings.ReplaceAll(modeName, "-", "_")), volume)
	defer os.Remove(filename)

	result := DetailedBenchmarkResult{
		Mode:            modeName,
		CompressionType: modeName,
		Volume:          volume,
	}

	// Create file
	start := time.Now()
	
	var cf *ColumnarFile
	var err error
	
	if compressionType == CompressionNone {
		cf, err = CreateFile(filename)
	} else {
		opts := NewCompressionOptions().WithPageCompression(compressionType, level)
		cf, err = CreateFileWithOptions(filename, opts)
	}
	
	if err != nil {
		t.Fatal(err)
	}

	// Add diverse columns to test compression effectiveness
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("timestamp", DataTypeInt64, false)     // Sequential timestamps
	cf.AddColumn("user_id", DataTypeInt64, false)       // Repeated values
	cf.AddColumn("amount", DataTypeInt64, true)         // Random with nulls
	cf.AddColumn("status", DataTypeString, false)       // Low cardinality strings
	cf.AddColumn("description", DataTypeString, true)   // High cardinality strings
	cf.AddColumn("tags", DataTypeString, false)         // Medium cardinality
	cf.AddColumn("metrics", DataTypeInt64, true)        // Sparse data with many nulls

	// Generate test data with varying compression characteristics
	data := generateCompressibleData(volume)
	
	// Load all columns
	cf.LoadIntColumn("id", data.ids)
	cf.LoadIntColumn("timestamp", data.timestamps)
	cf.LoadIntColumn("user_id", data.userIDs)
	cf.LoadIntColumn("amount", data.amounts)
	cf.LoadStringColumn("status", data.statuses)
	cf.LoadStringColumn("description", data.descriptions)
	cf.LoadStringColumn("tags", data.tags)
	cf.LoadIntColumn("metrics", data.metrics)

	result.WriteTime = time.Since(start)

	// Get detailed compression statistics
	stats := cf.pageManager.GetCompressionStats()
	result.UncompressedBytes = uint64(stats.UncompressedBytes)
	result.CompressedBytes = uint64(stats.CompressedBytes)
	result.CompressionRatio = stats.CompressionRatio
	
	if stats.UncompressedBytes > 0 {
		result.ActualSavings = float64(stats.UncompressedBytes-stats.CompressedBytes) / float64(stats.UncompressedBytes) * 100
	}

	// Count page types
	pageCount := cf.pageManager.GetPageCount()
	result.TotalPages = pageCount
	
	// Estimate data vs index pages (rough approximation)
	// In a real implementation, we'd track this in PageManager
	result.DataPages = pageCount * 60 / 100  // Assume ~60% data pages
	result.IndexPages = pageCount - result.DataPages

	cf.Close()

	// Get file size
	info, err := os.Stat(filename)
	if err == nil {
		result.FileSize = info.Size()
	}

	// Benchmark read and query operations
	result.QueryMetrics = benchmarkQueries(t, filename, volume)
	
	start = time.Now()
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	result.ReadTime = time.Since(start)
	cf2.Close()

	return result
}

type CompressibleData struct {
	ids          []IntData
	timestamps   []IntData
	userIDs      []IntData
	amounts      []IntData
	statuses     []StringData
	descriptions []StringData
	tags         []StringData
	metrics      []IntData
}

func generateCompressibleData(volume int) CompressibleData {
	data := CompressibleData{
		ids:          make([]IntData, volume),
		timestamps:   make([]IntData, volume),
		userIDs:      make([]IntData, volume),
		amounts:      make([]IntData, volume),
		statuses:     make([]StringData, volume),
		descriptions: make([]StringData, volume),
		tags:         make([]StringData, volume),
		metrics:      make([]IntData, volume),
	}

	// Predefined values for better compression
	statuses := []string{"pending", "approved", "rejected", "processing", "completed"}
	tagList := []string{"urgent", "normal", "low", "review", "followup", "resolved", "escalated", "new"}
	baseTime := time.Now().Unix()

	// Generate data with compression-friendly patterns
	for i := 0; i < volume; i++ {
		// Sequential IDs
		data.ids[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		
		// Sequential timestamps (compresses well)
		data.timestamps[i] = IntData{Value: baseTime + int64(i*60), RowNum: uint64(i)}
		
		// User IDs with repetition (simulating 1000 users)
		data.userIDs[i] = IntData{Value: int64(i % 1000), RowNum: uint64(i)}
		
		// Amounts with some nulls (20%)
		if i%5 == 0 {
			data.amounts[i] = IntData{IsNull: true, RowNum: uint64(i)}
		} else {
			// Amounts in a limited range for better compression
			data.amounts[i] = IntData{Value: int64(rand.Intn(10000)), RowNum: uint64(i)}
		}
		
		// Low cardinality status field
		data.statuses[i] = StringData{Value: statuses[i%len(statuses)], RowNum: uint64(i)}
		
		// Descriptions with some repetition and nulls (30%)
		if i%3 == 0 {
			data.descriptions[i] = StringData{IsNull: true, RowNum: uint64(i)}
		} else {
			// Mix of repeated and unique descriptions
			if i%10 < 7 {
				data.descriptions[i] = StringData{
					Value:  fmt.Sprintf("Standard description for item type %d", i%50),
					RowNum: uint64(i),
				}
			} else {
				data.descriptions[i] = StringData{
					Value:  fmt.Sprintf("Unique description for item %d with additional details", i),
					RowNum: uint64(i),
				}
			}
		}
		
		// Medium cardinality tags
		tagIndex := (i / 100) % len(tagList)
		data.tags[i] = StringData{Value: tagList[tagIndex], RowNum: uint64(i)}
		
		// Sparse metrics (80% nulls)
		if i%5 == 0 {
			data.metrics[i] = IntData{Value: int64(rand.Intn(1000)), RowNum: uint64(i)}
		} else {
			data.metrics[i] = IntData{IsNull: true, RowNum: uint64(i)}
		}
	}

	return data
}

func benchmarkQueries(t *testing.T, filename string, volume int) QueryPerformance {
	cf, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	perf := QueryPerformance{}

	// 1. Exact match queries
	start := time.Now()
	for i := 0; i < 100; i++ {
		targetID := rand.Intn(volume)
		bitmap, _ := cf.QueryInt("id", int64(targetID))
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Expected 1 result for ID %d", targetID)
		}
	}
	perf.ExactMatchTime = time.Since(start) / 100

	// 2. Range queries
	start = time.Now()
	for i := 0; i < 50; i++ {
		minAmount := rand.Intn(5000)
		maxAmount := minAmount + rand.Intn(3000)
		bitmap, _ := cf.RangeQueryInt("amount", int64(minAmount), int64(maxAmount))
		_ = bitmap.GetCardinality()
	}
	perf.RangeQueryTime = time.Since(start) / 50

	// 3. Row lookups
	start = time.Now()
	for i := 0; i < 100; i++ {
		row := uint64(rand.Intn(volume))
		cf.LookupIntByRow("id", row)
		cf.LookupStringByRow("status", row)
	}
	perf.RowLookupTime = time.Since(start) / 200

	// 4. String queries
	start = time.Now()
	statuses := []string{"pending", "approved", "rejected", "processing", "completed"}
	for _, status := range statuses {
		bitmap, _ := cf.QueryString("status", status)
		_ = bitmap.GetCardinality()
	}
	perf.StringQueryTime = time.Since(start) / time.Duration(len(statuses))

	// 5. Null queries
	start = time.Now()
	cf.QueryNull("amount")
	cf.QueryNull("description")
	cf.QueryNull("metrics")
	perf.NullQueryTime = time.Since(start) / 3

	// 6. Complex query
	start = time.Now()
	bitmap1, _ := cf.QueryString("status", "approved")
	bitmap2, _ := cf.RangeQueryInt("amount", 1000, 5000)
	bitmap3, _ := cf.QueryNotNull("description")
	result := cf.QueryAnd(bitmap1, bitmap2)
	cf.QueryAnd(result, bitmap3)
	perf.ComplexQueryTime = time.Since(start)

	return perf
}

func printDetailedReport(t *testing.T, allResults map[int][]DetailedBenchmarkResult) {
	t.Log("\n=== DETAILED COMPRESSION ANALYSIS REPORT ===\n")
	
	for _, volume := range []int{1000, 10000, 100000} {
		results, ok := allResults[volume]
		if !ok {
			continue
		}
		
		t.Logf("\n=== Volume: %d rows ===\n", volume)
		
		// Find baseline (not used for file savings in this version)
		// var baseline DetailedBenchmarkResult
		// for _, r := range results {
		// 	if r.Mode == "Uncompressed" {
		// 		baseline = r
		// 		break
		// 	}
		// }
		
		// Print compression effectiveness
		t.Log("COMPRESSION EFFECTIVENESS:")
		t.Log("Mode              File(MB)  Pages   Data(MB)  Compressed(MB)  Ratio  Savings  Write(ms)")
		t.Log("----              --------  ------  --------  --------------  -----  -------  ---------")
		
		for _, r := range results {
			t.Logf("%-16s  %8.2f  %6d  %8.2f  %14.2f  %5.2f  %6.1f%%  %9.1f",
				r.Mode,
				float64(r.FileSize)/(1024*1024),
				r.TotalPages,
				float64(r.UncompressedBytes)/(1024*1024),
				float64(r.CompressedBytes)/(1024*1024),
				r.CompressionRatio,
				r.ActualSavings,
				float64(r.WriteTime.Milliseconds()),
			)
		}
		
		// Print query performance
		t.Log("\nQUERY PERFORMANCE (microseconds):")
		t.Log("Mode              Exact  Range  Lookup  String  Null  Complex  Total")
		t.Log("----              -----  -----  ------  ------  ----  -------  -----")
		
		for _, r := range results {
			total := r.QueryMetrics.ExactMatchTime + r.QueryMetrics.RangeQueryTime +
				r.QueryMetrics.RowLookupTime + r.QueryMetrics.StringQueryTime +
				r.QueryMetrics.NullQueryTime + r.QueryMetrics.ComplexQueryTime
			
			t.Logf("%-16s  %5d  %5d  %6d  %6d  %4d  %7d  %5d",
				r.Mode,
				r.QueryMetrics.ExactMatchTime.Microseconds(),
				r.QueryMetrics.RangeQueryTime.Microseconds(),
				r.QueryMetrics.RowLookupTime.Microseconds(),
				r.QueryMetrics.StringQueryTime.Microseconds(),
				r.QueryMetrics.NullQueryTime.Microseconds(),
				r.QueryMetrics.ComplexQueryTime.Microseconds(),
				total.Microseconds(),
			)
		}
		
		// Summary
		t.Logf("\nSUMMARY:")
		bestCompression, bestSavings := findBestCompression(results)
		t.Logf("  Best compression: %s (%.1f%% data reduction)", bestCompression, bestSavings)
		t.Logf("  Fastest writes: %s", findFastestWrite(results))
		t.Logf("  Fastest queries: %s", findFastestQuery(results))
		
		// Recommendations
		t.Logf("\nRECOMMENDATIONS:")
		if bestSavings > 50 {
			t.Logf("  - Strong compression benefits observed, recommend using %s", bestCompression)
		} else if bestSavings > 20 {
			t.Logf("  - Moderate compression benefits, consider %s for storage-constrained environments", bestCompression)
		} else {
			t.Logf("  - Limited compression benefits for this data pattern")
		}
	}
}

func findBestCompression(results []DetailedBenchmarkResult) (string, float64) {
	best := ""
	maxSavings := 0.0
	
	for _, r := range results {
		if r.ActualSavings > maxSavings {
			maxSavings = r.ActualSavings
			best = r.Mode
		}
	}
	
	return best, maxSavings
}

func findFastestWrite(results []DetailedBenchmarkResult) string {
	fastest := ""
	minTime := time.Hour
	
	for _, r := range results {
		if r.WriteTime < minTime {
			minTime = r.WriteTime
			fastest = r.Mode
		}
	}
	
	return fastest
}

func findFastestQuery(results []DetailedBenchmarkResult) string {
	fastest := ""
	minTime := time.Hour
	
	for _, r := range results {
		total := r.QueryMetrics.ExactMatchTime + r.QueryMetrics.RangeQueryTime +
			r.QueryMetrics.RowLookupTime + r.QueryMetrics.StringQueryTime +
			r.QueryMetrics.NullQueryTime + r.QueryMetrics.ComplexQueryTime
		
		if total < minTime {
			minTime = total
			fastest = r.Mode
		}
	}
	
	return fastest
}