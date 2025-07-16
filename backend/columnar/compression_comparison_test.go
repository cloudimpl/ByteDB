package columnar

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// TestCompressionComparison compares compressed vs uncompressed performance and size
func TestCompressionComparison(t *testing.T) {
	// Test volumes
	volumes := []struct {
		name string
		rows int
	}{
		{"Small", 1000},
		{"Medium", 10000},
		{"Large", 50000},
		{"XLarge", 100000},
	}

	// Compression types to test
	compressionTypes := []struct {
		name string
		cType CompressionType
		level CompressionLevel
	}{
		{"NoCompression", CompressionNone, CompressionLevelDefault},
		{"Snappy", CompressionSnappy, CompressionLevelDefault},
		{"Gzip-Fast", CompressionGzip, CompressionLevelFastest},
		{"Gzip-Default", CompressionGzip, CompressionLevelDefault},
		{"Gzip-Best", CompressionGzip, CompressionLevelBest},
		{"Zstd-Fast", CompressionZstd, CompressionLevelFastest},
		{"Zstd-Default", CompressionZstd, CompressionLevelDefault},
		{"Zstd-Best", CompressionZstd, CompressionLevelBest},
	}

	// Run tests for each volume
	for _, vol := range volumes {
		t.Run(vol.name, func(t *testing.T) {
			results := make([]CompressionResult, 0)
			
			// Test each compression type
			for _, ct := range compressionTypes {
				result := testCompressionType(t, vol.rows, ct.name, ct.cType, ct.level)
				results = append(results, result)
			}
			
			// Print comparison table
			printComparisonTable(t, vol.name, vol.rows, results)
		})
	}
}

type CompressionResult struct {
	Name              string
	FileSize          int64
	PageCount         uint64
	CompressionRatio  float64
	WriteTime         time.Duration
	ReadTime          time.Duration
	QueryTime         time.Duration
	RowLookupTime     time.Duration
	DataIntegrity     bool
}

func testCompressionType(t *testing.T, numRows int, name string, cType CompressionType, level CompressionLevel) CompressionResult {
	filename := fmt.Sprintf("test_comparison_%s_%d.bytedb", name, numRows)
	defer os.Remove(filename)
	
	result := CompressionResult{Name: name}
	
	// Create file with compression options
	var cf *ColumnarFile
	var err error
	
	writeStart := time.Now()
	
	if cType == CompressionNone {
		cf, err = CreateFile(filename)
	} else {
		opts := NewCompressionOptions().
			WithPageCompression(cType, level)
		cf, err = CreateFileWithOptions(filename, opts)
	}
	
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add columns for testable data types
	// Note: Currently only Int64 and String types have load methods
	columns := []struct {
		name     string
		dataType DataType
		nullable bool
	}{
		{"id", DataTypeInt64, false},
		{"age", DataTypeInt64, false},
		{"score", DataTypeInt64, false},
		{"count", DataTypeInt64, true},
		{"name", DataTypeString, true},
		{"email", DataTypeString, false},
		{"description", DataTypeString, false},
		{"flags", DataTypeInt64, false},
		{"timestamp", DataTypeInt64, false},
		{"category", DataTypeString, false},
	}
	
	for _, col := range columns {
		if err := cf.AddColumn(col.name, col.dataType, col.nullable); err != nil {
			t.Fatal(err)
		}
	}
	
	// Generate test data
	rand.Seed(time.Now().UnixNano())
	
	// Load data for each column
	loadColumnData(t, cf, numRows)
	
	// Get compression stats before closing
	stats := cf.pageManager.GetCompressionStats()
	result.CompressionRatio = stats.CompressionRatio
	
	writeTime := time.Since(writeStart)
	result.WriteTime = writeTime
	
	if err := cf.Close(); err != nil {
		t.Fatal(err)
	}
	
	// Get file info
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	result.FileSize = info.Size()
	
	// Reopen and test read performance
	readStart := time.Now()
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()
	
	result.ReadTime = time.Since(readStart)
	result.PageCount = cf2.pageManager.GetPageCount()
	
	// Test query performance
	queryStart := time.Now()
	result.DataIntegrity = testQueries(t, cf2, numRows)
	result.QueryTime = time.Since(queryStart)
	
	// Test row lookup performance
	lookupStart := time.Now()
	testRowLookups(t, cf2, numRows)
	result.RowLookupTime = time.Since(lookupStart)
	
	return result
}

func loadColumnData(t *testing.T, cf *ColumnarFile, numRows int) {
	// Int64 data (id) - sequential
	idData := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		idData[i] = IntData{
			Value:  int64(i),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadIntColumn("id", idData); err != nil {
		t.Fatal(err)
	}
	
	// Int64 data (age) - low cardinality
	ageData := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		ageData[i] = IntData{
			Value:  int64(rand.Intn(100)),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadIntColumn("age", ageData); err != nil {
		t.Fatal(err)
	}
	
	// Int64 data (score) - medium cardinality
	scoreData := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		scoreData[i] = IntData{
			Value:  int64(rand.Intn(10000)),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadIntColumn("score", scoreData); err != nil {
		t.Fatal(err)
	}
	
	// Int64 data (count) - nullable with some nulls
	countData := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		if i%25 == 0 { // 4% nulls
			countData[i] = IntData{
				IsNull: true,
				RowNum: uint64(i),
			}
		} else {
			countData[i] = IntData{
				Value:  int64(rand.Intn(1000)),
				RowNum: uint64(i),
			}
		}
	}
	if err := cf.LoadIntColumn("count", countData); err != nil {
		t.Fatal(err)
	}
	
	// String data (name) - nullable
	nameData := make([]StringData, numRows)
	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"}
	for i := 0; i < numRows; i++ {
		if i%20 == 0 { // 5% nulls
			nameData[i] = StringData{
				IsNull: true,
				RowNum: uint64(i),
			}
		} else {
			nameData[i] = StringData{
				Value:  fmt.Sprintf("%s_%d", names[i%len(names)], i),
				RowNum: uint64(i),
			}
		}
	}
	if err := cf.LoadStringColumn("name", nameData); err != nil {
		t.Fatal(err)
	}
	
	// String data (email)
	emailData := make([]StringData, numRows)
	domains := []string{"gmail.com", "yahoo.com", "outlook.com", "company.com", "test.org"}
	for i := 0; i < numRows; i++ {
		emailData[i] = StringData{
			Value:  fmt.Sprintf("user%d@%s", i, domains[i%len(domains)]),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadStringColumn("email", emailData); err != nil {
		t.Fatal(err)
	}
	
	// String data (description) - high cardinality
	descData := make([]StringData, numRows)
	for i := 0; i < numRows; i++ {
		descData[i] = StringData{
			Value:  fmt.Sprintf("This is a detailed description for item %d with some repetitive text patterns that should compress well", i),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadStringColumn("description", descData); err != nil {
		t.Fatal(err)
	}
	
	// Int64 data (flags) - random values
	flagsData := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		flagsData[i] = IntData{
			Value:  int64(rand.Uint32()),
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadIntColumn("flags", flagsData); err != nil {
		t.Fatal(err)
	}
	
	// Int64 data (timestamp)
	timestampData := make([]IntData, numRows)
	baseTime := time.Now().Unix()
	for i := 0; i < numRows; i++ {
		timestampData[i] = IntData{
			Value:  baseTime + int64(i*60), // 1 minute intervals
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadIntColumn("timestamp", timestampData); err != nil {
		t.Fatal(err)
	}
	
	// String data (category) - low cardinality
	categoryData := make([]StringData, numRows)
	categories := []string{"Electronics", "Books", "Clothing", "Food", "Sports"}
	for i := 0; i < numRows; i++ {
		categoryData[i] = StringData{
			Value:  categories[i%len(categories)],
			RowNum: uint64(i),
		}
	}
	if err := cf.LoadStringColumn("category", categoryData); err != nil {
		t.Fatal(err)
	}
}

func testQueries(t *testing.T, cf *ColumnarFile, numRows int) bool {
	// Test various queries
	
	// 1. Exact match query
	bitmap, err := cf.QueryInt("id", int64(numRows/2))
	if err != nil {
		t.Errorf("Query error: %v", err)
		return false
	}
	if bitmap.GetCardinality() != 1 {
		t.Errorf("Expected 1 result, got %d", bitmap.GetCardinality())
		return false
	}
	
	// 2. Range query
	bitmap, err = cf.RangeQueryInt("age", 25, 35)
	if err != nil {
		t.Errorf("Range query error: %v", err)
		return false
	}
	
	// 3. String query
	bitmap, err = cf.QueryString("category", "Electronics")
	if err != nil {
		t.Errorf("String query error: %v", err)
		return false
	}
	expectedCount := numRows / 5 // 5 categories
	tolerance := expectedCount / 10 // 10% tolerance
	if int(bitmap.GetCardinality()) < expectedCount-tolerance || int(bitmap.GetCardinality()) > expectedCount+tolerance {
		t.Errorf("Expected ~%d results for category query, got %d", expectedCount, bitmap.GetCardinality())
		return false
	}
	
	// 4. Null query
	bitmap, err = cf.QueryNull("name")
	if err != nil {
		t.Errorf("Null query error: %v", err)
		return false
	}
	
	return true
}

func testRowLookups(t *testing.T, cf *ColumnarFile, numRows int) {
	// Test row lookups at various positions
	testRows := []uint64{0, uint64(numRows/4), uint64(numRows/2), uint64(numRows*3/4), uint64(numRows-1)}
	
	for _, row := range testRows {
		if row >= uint64(numRows) {
			continue
		}
		
		// Test int lookup
		val, found, err := cf.LookupIntByRow("id", row)
		if err != nil || !found || val != int64(row) {
			t.Errorf("Row %d: int lookup failed", row)
		}
		
		// Test string lookup (skip nulls)
		if row%20 != 0 {
			_, found, err = cf.LookupStringByRow("name", row)
			if err != nil || !found {
				t.Errorf("Row %d: string lookup failed", row)
			}
		}
		
		// Test generic lookup on age column
		ageVal, found, err := cf.LookupValueByRow("age", row)
		if err != nil || !found {
			t.Errorf("Row %d: age lookup failed", row)
		}
		// Just verify it's an int64
		if _, ok := ageVal.(int64); !ok {
			t.Errorf("Row %d: age value is not int64", row)
		}
	}
}

func printComparisonTable(t *testing.T, volumeName string, numRows int, results []CompressionResult) {
	t.Logf("\n=== Compression Comparison: %s Volume (%d rows) ===", volumeName, numRows)
	t.Logf("%-15s %10s %8s %12s %10s %10s %10s %10s %10s",
		"Type", "Size(KB)", "Pages", "Compression", "Write(ms)", "Read(ms)", "Query(ms)", "Lookup(ms)", "Integrity")
	t.Logf("%-15s %10s %8s %12s %10s %10s %10s %10s %10s",
		"----", "--------", "-----", "-----------", "---------", "--------", "---------", "----------", "---------")
	
	baseSize := results[0].FileSize // NoCompression as baseline
	
	for _, r := range results {
		sizeReduction := ""
		if r.FileSize < baseSize {
			reduction := float64(baseSize-r.FileSize) / float64(baseSize) * 100
			sizeReduction = fmt.Sprintf("%.1f%%", reduction)
		} else {
			sizeReduction = "0%"
		}
		
		integrity := "PASS"
		if !r.DataIntegrity {
			integrity = "FAIL"
		}
		
		t.Logf("%-15s %10.1f %8d %12s %10.1f %10.1f %10.1f %10.1f %10s",
			r.Name,
			float64(r.FileSize)/1024,
			r.PageCount,
			sizeReduction,
			float64(r.WriteTime.Microseconds())/1000,
			float64(r.ReadTime.Microseconds())/1000,
			float64(r.QueryTime.Microseconds())/1000,
			float64(r.RowLookupTime.Microseconds())/1000,
			integrity)
	}
	
	// Find best compression
	bestSize := baseSize
	bestName := "NoCompression"
	for _, r := range results {
		if r.FileSize < bestSize {
			bestSize = r.FileSize
			bestName = r.Name
		}
	}
	
	if bestName != "NoCompression" {
		reduction := float64(baseSize-bestSize) / float64(baseSize) * 100
		t.Logf("\nBest compression: %s (%.1f%% reduction)", bestName, reduction)
	}
}

// TestCompressionDataPatterns tests compression effectiveness with different data patterns
func TestCompressionDataPatterns(t *testing.T) {
	patterns := []struct {
		name        string
		generator   func(i int) string
		cardinality string
	}{
		{
			name:        "HighlyRepetitive",
			generator:   func(i int) string { return "AAAAAAAAAA" },
			cardinality: "1 unique",
		},
		{
			name:        "LowCardinality",
			generator:   func(i int) string { return fmt.Sprintf("Category_%d", i%10) },
			cardinality: "10 unique",
		},
		{
			name:        "MediumCardinality",
			generator:   func(i int) string { return fmt.Sprintf("Item_%d", i%1000) },
			cardinality: "1000 unique",
		},
		{
			name:        "HighCardinality",
			generator:   func(i int) string { return fmt.Sprintf("Unique_%d_%d", i, rand.Int()) },
			cardinality: "all unique",
		},
		{
			name:        "Sequential",
			generator:   func(i int) string { return fmt.Sprintf("%010d", i) },
			cardinality: "sequential",
		},
		{
			name:        "JSONLike",
			generator:   func(i int) string { 
				return fmt.Sprintf(`{"id":%d,"name":"User%d","status":"active","score":%d}`, i, i%100, i%1000)
			},
			cardinality: "JSON pattern",
		},
	}
	
	compressionTypes := []struct {
		name  string
		cType CompressionType
	}{
		{"NoCompression", CompressionNone},
		{"Snappy", CompressionSnappy},
		{"Gzip", CompressionGzip},
		{"Zstd", CompressionZstd},
	}
	
	numRows := 10000
	
	t.Logf("\n=== Compression Effectiveness by Data Pattern (%d rows) ===", numRows)
	t.Logf("%-20s %-15s %15s %10s %15s %15s",
		"Pattern", "Cardinality", "Compression", "Size(KB)", "Ratio", "Best For")
	t.Logf("%-20s %-15s %15s %10s %15s %15s",
		"-------", "-----------", "-----------", "--------", "-----", "--------")
	
	for _, pattern := range patterns {
		sizes := make(map[string]int64)
		ratios := make(map[string]float64)
		
		for _, ct := range compressionTypes {
			filename := fmt.Sprintf("test_pattern_%s_%s.bytedb", pattern.name, ct.name)
			defer os.Remove(filename)
			
			var cf *ColumnarFile
			var err error
			
			if ct.cType == CompressionNone {
				cf, err = CreateFile(filename)
			} else {
				opts := NewCompressionOptions().
					WithPageCompression(ct.cType, CompressionLevelDefault)
				cf, err = CreateFileWithOptions(filename, opts)
			}
			
			if err != nil {
				t.Fatal(err)
			}
			
			cf.AddColumn("data", DataTypeString, false)
			
			// Generate data with pattern
			stringData := make([]StringData, numRows)
			for i := 0; i < numRows; i++ {
				stringData[i] = StringData{
					Value:  pattern.generator(i),
					RowNum: uint64(i),
				}
			}
			
			cf.LoadStringColumn("data", stringData)
			
			stats := cf.pageManager.GetCompressionStats()
			ratios[ct.name] = stats.CompressionRatio
			
			cf.Close()
			
			info, _ := os.Stat(filename)
			sizes[ct.name] = info.Size()
		}
		
		// Find best compression
		baseSize := sizes["NoCompression"]
		bestType := "None"
		bestReduction := 0.0
		
		for _, ct := range compressionTypes[1:] { // Skip NoCompression
			reduction := float64(baseSize-sizes[ct.name]) / float64(baseSize) * 100
			if reduction > bestReduction {
				bestReduction = reduction
				bestType = ct.name
			}
			
			t.Logf("%-20s %-15s %15s %10.1f %14.1f%% %15s",
				pattern.name,
				pattern.cardinality,
				ct.name,
				float64(sizes[ct.name])/1024,
				reduction,
				"")
		}
		
		if bestReduction > 0 {
			t.Logf("%-20s %-15s %15s %10s %15s %15s",
				"", "", "", "", "", fmt.Sprintf("%s (%.1f%%)", bestType, bestReduction))
		}
	}
}