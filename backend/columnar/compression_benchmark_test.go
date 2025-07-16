package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// BenchmarkResult holds performance metrics for a single test
type BenchmarkResult struct {
	Mode              string
	CompressionType   string
	Volume            int
	FileSize          int64
	PageCount         uint64
	CompressionRatio  float64
	WriteTime         time.Duration
	ReadTime          time.Duration
	QueryTime         time.Duration
	RangeQueryTime    time.Duration
	RowLookupTime     time.Duration
	StringQueryTime   time.Duration
	NullQueryTime     time.Duration
	ComplexQueryTime  time.Duration
}

// TestCompressionBenchmark runs comprehensive benchmarks comparing compressed vs uncompressed
func TestCompressionBenchmark(t *testing.T) {
	volumes := []int{1000, 10000, 50000}
	results := []BenchmarkResult{}

	for _, volume := range volumes {
		t.Run(fmt.Sprintf("Volume_%d", volume), func(t *testing.T) {
			// Test uncompressed
			uncompResult := benchmarkMode(t, "Uncompressed", CompressionNone, volume)
			results = append(results, uncompResult)

			// Test with different compression types
			compressionTypes := []struct {
				name  string
				cType CompressionType
			}{
				{"Snappy", CompressionSnappy},
				{"Gzip", CompressionGzip},
				{"Zstd", CompressionZstd},
			}

			for _, ct := range compressionTypes {
				compResult := benchmarkMode(t, ct.name, ct.cType, volume)
				results = append(results, compResult)
			}
		})
	}

	// Print comprehensive report
	printBenchmarkReport(t, results)
}

func benchmarkMode(t *testing.T, modeName string, compressionType CompressionType, volume int) BenchmarkResult {
	filename := fmt.Sprintf("benchmark_%s_%d.bytedb", strings.ToLower(modeName), volume)
	defer os.Remove(filename)

	result := BenchmarkResult{
		Mode:            modeName,
		CompressionType: modeName,
		Volume:          volume,
	}

	// Create file with appropriate compression
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

	// Add columns with all supported data types
	cf.AddColumn("id", DataTypeInt64, false)           // Non-nullable int
	cf.AddColumn("value", DataTypeInt64, true)         // Nullable int
	cf.AddColumn("name", DataTypeString, false)        // Non-nullable string
	cf.AddColumn("description", DataTypeString, true)  // Nullable string
	cf.AddColumn("category", DataTypeString, false)    // For testing string queries
	cf.AddColumn("score", DataTypeInt64, true)         // For range queries

	// Generate test data
	intData := make([]IntData, volume)
	valueData := make([]IntData, volume)
	nameData := make([]StringData, volume)
	descData := make([]StringData, volume)
	categoryData := make([]StringData, volume)
	scoreData := make([]IntData, volume)

	categories := []string{"Electronics", "Books", "Clothing", "Food", "Sports", "Home", "Toys", "Health", "Beauty", "Auto"}
	names := generateNames(volume)
	descriptions := generateDescriptions()

	for i := 0; i < volume; i++ {
		// ID column - sequential
		intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		
		// Value column - random with 10% nulls
		if i%10 == 0 {
			valueData[i] = IntData{IsNull: true, RowNum: uint64(i)}
		} else {
			valueData[i] = IntData{Value: rand.Int63n(1000000), RowNum: uint64(i)}
		}
		
		// Name column - unique names
		nameData[i] = StringData{Value: names[i], RowNum: uint64(i)}
		
		// Description column - repeated text with 20% nulls
		if i%5 == 0 {
			descData[i] = StringData{IsNull: true, RowNum: uint64(i)}
		} else {
			descData[i] = StringData{Value: descriptions[i%len(descriptions)], RowNum: uint64(i)}
		}
		
		// Category column - limited set for grouping
		categoryData[i] = StringData{Value: categories[i%len(categories)], RowNum: uint64(i)}
		
		// Score column - range 0-100 with 15% nulls
		if i%7 == 0 {
			scoreData[i] = IntData{IsNull: true, RowNum: uint64(i)}
		} else {
			scoreData[i] = IntData{Value: int64(rand.Intn(101)), RowNum: uint64(i)}
		}
	}

	// Load data
	cf.LoadIntColumn("id", intData)
	cf.LoadIntColumn("value", valueData)
	cf.LoadStringColumn("name", nameData)
	cf.LoadStringColumn("description", descData)
	cf.LoadStringColumn("category", categoryData)
	cf.LoadIntColumn("score", scoreData)

	result.WriteTime = time.Since(start)
	
	// Get compression stats
	stats := cf.pageManager.GetCompressionStats()
	result.CompressionRatio = stats.CompressionRatio
	result.PageCount = cf.pageManager.GetPageCount()

	cf.Close()

	// Get file size
	info, err := os.Stat(filename)
	if err == nil {
		result.FileSize = info.Size()
	}

	// Benchmark read operations
	start = time.Now()
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	result.ReadTime = time.Since(start)

	// Benchmark different query types
	
	// 1. Exact match query
	start = time.Now()
	for i := 0; i < 10; i++ {
		targetID := rand.Intn(volume)
		bitmap, _ := cf2.QueryInt("id", int64(targetID))
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Expected 1 result for ID %d, got %d", targetID, bitmap.GetCardinality())
		}
	}
	result.QueryTime = time.Since(start) / 10

	// 2. Range query
	start = time.Now()
	for i := 0; i < 5; i++ {
		minScore := rand.Intn(50)
		maxScore := minScore + rand.Intn(30)
		bitmap, _ := cf2.RangeQueryInt("score", int64(minScore), int64(maxScore))
		_ = bitmap.GetCardinality()
	}
	result.RangeQueryTime = time.Since(start) / 5

	// 3. Row lookup
	start = time.Now()
	for i := 0; i < 10; i++ {
		row := uint64(rand.Intn(volume))
		_, _, _ = cf2.LookupIntByRow("id", row)
		_, _, _ = cf2.LookupStringByRow("name", row)
	}
	result.RowLookupTime = time.Since(start) / 20

	// 4. String query
	start = time.Now()
	for _, category := range categories[:5] {
		bitmap, _ := cf2.QueryString("category", category)
		_ = bitmap.GetCardinality()
	}
	result.StringQueryTime = time.Since(start) / 5

	// 5. Null query
	start = time.Now()
	cf2.QueryNull("value")
	cf2.QueryNull("description")
	cf2.QueryNull("score")
	result.NullQueryTime = time.Since(start) / 3

	// 6. Complex query (AND of multiple conditions)
	start = time.Now()
	bitmap1, _ := cf2.QueryString("category", "Electronics")
	bitmap2, _ := cf2.RangeQueryInt("score", 50, 100)
	bitmap3, _ := cf2.QueryNotNull("value")
	result1 := cf2.QueryAnd(bitmap1, bitmap2)
	_ = cf2.QueryAnd(result1, bitmap3)
	result.ComplexQueryTime = time.Since(start)

	cf2.Close()

	return result
}

func generateNames(count int) []string {
	names := make([]string, count)
	firstNames := []string{"John", "Jane", "Bob", "Alice", "Charlie", "Emma", "David", "Olivia", "Frank", "Sophia"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Anderson"}
	
	for i := 0; i < count; i++ {
		first := firstNames[i%len(firstNames)]
		last := lastNames[(i/len(firstNames))%len(lastNames)]
		names[i] = fmt.Sprintf("%s_%s_%d", first, last, i)
	}
	return names
}

func generateDescriptions() []string {
	return []string{
		"High-quality product with excellent features and great value for money",
		"Standard item suitable for everyday use with reliable performance",
		"Premium selection featuring advanced technology and superior design",
		"Budget-friendly option that doesn't compromise on essential features",
		"Professional-grade equipment designed for heavy-duty applications",
		"Compact and portable solution perfect for travel and on-the-go use",
		"Eco-friendly product made from sustainable materials",
		"Limited edition item with unique characteristics and special packaging",
		"Versatile product suitable for multiple applications and use cases",
		"Entry-level option ideal for beginners and casual users",
	}
}

func printBenchmarkReport(t *testing.T, results []BenchmarkResult) {
	t.Log("\n=== COMPRESSION BENCHMARK REPORT ===\n")
	
	// Group results by volume
	volumeGroups := make(map[int][]BenchmarkResult)
	for _, r := range results {
		volumeGroups[r.Volume] = append(volumeGroups[r.Volume], r)
	}
	
	for _, volume := range []int{1000, 10000, 50000} {
		if results, ok := volumeGroups[volume]; ok {
			t.Logf("\n--- Volume: %d rows ---\n", volume)
			
			// Find uncompressed baseline
			var baseline BenchmarkResult
			for _, r := range results {
				if r.Mode == "Uncompressed" {
					baseline = r
					break
				}
			}
			
			// Print header
			t.Log("Mode            Size(KB)  Pages  Ratio  Write(ms)  Read(ms)  Query(μs)  Range(μs)  Lookup(μs)  String(μs)  Null(μs)  Complex(μs)  Savings")
			t.Log("----            --------  -----  -----  ---------  --------  ---------  ---------  ----------  ----------  --------  -----------  -------")
			
			// Print results
			for _, r := range results {
				savings := float64(baseline.FileSize-r.FileSize) / float64(baseline.FileSize) * 100
				if r.Mode == "Uncompressed" {
					savings = 0
				}
				
				t.Logf("%-14s  %8.1f  %5d  %5.2f  %9.1f  %8.1f  %9.1f  %9.1f  %10.1f  %10.1f  %8.1f  %11.1f  %6.1f%%",
					r.Mode,
					float64(r.FileSize)/1024,
					r.PageCount,
					r.CompressionRatio,
					float64(r.WriteTime.Milliseconds()),
					float64(r.ReadTime.Milliseconds()),
					float64(r.QueryTime.Microseconds()),
					float64(r.RangeQueryTime.Microseconds()),
					float64(r.RowLookupTime.Microseconds()),
					float64(r.StringQueryTime.Microseconds()),
					float64(r.NullQueryTime.Microseconds()),
					float64(r.ComplexQueryTime.Microseconds()),
					savings,
				)
			}
			
			// Summary statistics
			t.Logf("\nSummary for %d rows:", volume)
			bestMode, bestSavings := getBestCompression(results, baseline)
			t.Logf("  Best compression: %s (%.1f%% space savings)", bestMode, bestSavings)
			t.Logf("  Fastest write: %s", getFastestWrite(results))
			t.Logf("  Fastest queries: %s", getFastestQueries(results))
		}
	}
}

func getBestCompression(results []BenchmarkResult, baseline BenchmarkResult) (string, float64) {
	bestMode := ""
	bestSavings := 0.0
	
	for _, r := range results {
		if r.Mode != "Uncompressed" {
			savings := float64(baseline.FileSize-r.FileSize) / float64(baseline.FileSize) * 100
			if savings > bestSavings {
				bestSavings = savings
				bestMode = r.Mode
			}
		}
	}
	
	return bestMode, bestSavings
}

func getFastestWrite(results []BenchmarkResult) string {
	fastest := ""
	minTime := time.Duration(1<<63 - 1)
	
	for _, r := range results {
		if r.WriteTime < minTime {
			minTime = r.WriteTime
			fastest = r.Mode
		}
	}
	
	return fastest
}

func getFastestQueries(results []BenchmarkResult) string {
	fastest := ""
	minTime := time.Duration(1<<63 - 1)
	
	for _, r := range results {
		totalQueryTime := r.QueryTime + r.RangeQueryTime + r.RowLookupTime + 
			r.StringQueryTime + r.NullQueryTime + r.ComplexQueryTime
		if totalQueryTime < minTime {
			minTime = totalQueryTime
			fastest = r.Mode
		}
	}
	
	return fastest
}