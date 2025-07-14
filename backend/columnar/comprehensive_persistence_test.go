package columnar

import (
	"fmt"
	"os"
	"testing"
	"time"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// TestComprehensivePersistence tests all aspects of file persistence
func TestComprehensivePersistence(t *testing.T) {
	tmpFile := "test_comprehensive_persistence.bytedb"
	defer os.Remove(tmpFile)

	// Expected data for validation
	var expectedStats map[string]map[string]interface{}
	var expectedQueryResults map[string][]uint64

	t.Run("Phase1_CreateAndValidateInMemory", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		// Add diverse column types
		cf.AddColumn("int8_col", DataTypeInt8, false)
		cf.AddColumn("int16_col", DataTypeInt16, false)
		cf.AddColumn("int32_col", DataTypeInt32, false)
		cf.AddColumn("int64_col", DataTypeInt64, false)
		cf.AddColumn("uint8_col", DataTypeUint8, false)
		cf.AddColumn("uint16_col", DataTypeUint16, false)
		cf.AddColumn("uint32_col", DataTypeUint32, false)
		cf.AddColumn("uint64_col", DataTypeUint64, false)
		cf.AddColumn("float32_col", DataTypeFloat32, false)
		cf.AddColumn("float64_col", DataTypeFloat64, false)
		cf.AddColumn("bool_col", DataTypeBool, false)
		cf.AddColumn("string_col", DataTypeString, false)

		// Load data with known patterns
		const rowCount = 1000

		// Integer columns with different patterns
		int64Data := make([]IntData, rowCount)
		stringData := make([]StringData, rowCount)
		
		for i := 0; i < rowCount; i++ {
			int64Data[i] = NewIntData(int64(i % 100), uint64(i)) // 100 distinct values, 10 duplicates each
			stringData[i] = NewStringData(fmt.Sprintf("value_%d", i%50), uint64(i)) // 50 distinct strings, 20 duplicates each
		}

		err = cf.LoadIntColumn("int64_col", int64Data)
		if err != nil {
			t.Fatalf("Failed to load int64 column: %v", err)
		}

		err = cf.LoadStringColumn("string_col", stringData)
		if err != nil {
			t.Fatalf("Failed to load string column: %v", err)
		}

		// Test in-memory queries and collect expected results
		expectedQueryResults = make(map[string][]uint64)
		expectedStats = make(map[string]map[string]interface{})

		// Test point query
		bitmap, err := cf.QueryInt("int64_col", 25)
		if err != nil {
			t.Fatalf("In-memory query failed: %v", err)
		}
		expectedQueryResults["int64_point_25"] = BitmapToSlice(bitmap)
		
		// Test range query
		bitmap, err = cf.RangeQueryInt("int64_col", 10, 20)
		if err != nil {
			t.Fatalf("In-memory range query failed: %v", err)
		}
		expectedQueryResults["int64_range_10_20"] = BitmapToSlice(bitmap)

		// Test string query
		bitmap, err = cf.QueryString("string_col", "value_15")
		if err != nil {
			t.Fatalf("In-memory string query failed: %v", err)
		}
		expectedQueryResults["string_point_value_15"] = BitmapToSlice(bitmap)

		// Collect statistics
		for _, colName := range []string{"int64_col", "string_col"} {
			stats, err := cf.GetStats(colName)
			if err != nil {
				t.Fatalf("Failed to get stats for %s: %v", colName, err)
			}
			expectedStats[colName] = stats
			t.Logf("In-memory stats for %s: %v", colName, stats)
		}

		cf.Close()
	})

	t.Run("Phase2_ReopenAndValidateIdenticalResults", func(t *testing.T) {
		cf, err := OpenFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open persisted file: %v", err)
		}
		defer cf.Close()

		// Verify column structure
		columns := cf.GetColumns()
		expectedColumns := []string{"int8_col", "int16_col", "int32_col", "int64_col", 
									"uint8_col", "uint16_col", "uint32_col", "uint64_col",
									"float32_col", "float64_col", "bool_col", "string_col"}
		
		if len(columns) != len(expectedColumns) {
			t.Errorf("Column count mismatch: expected %d, got %d", len(expectedColumns), len(columns))
		}

		// Verify all expected columns exist
		columnMap := make(map[string]bool)
		for _, col := range columns {
			columnMap[col] = true
		}
		for _, expectedCol := range expectedColumns {
			if !columnMap[expectedCol] {
				t.Errorf("Missing column after reopen: %s", expectedCol)
			}
		}

		// Verify exact query results match
		for queryName, expectedResults := range expectedQueryResults {
			var bitmap *roaring.Bitmap
			var err error

			switch queryName {
			case "int64_point_25":
				bitmap, err = cf.QueryInt("int64_col", 25)
			case "int64_range_10_20":
				bitmap, err = cf.RangeQueryInt("int64_col", 10, 20)
			case "string_point_value_15":
				bitmap, err = cf.QueryString("string_col", "value_15")
			default:
				t.Errorf("Unknown query: %s", queryName)
				continue
			}

			if err != nil {
				t.Errorf("Query %s failed after reopen: %v", queryName, err)
				continue
			}

			actualResults := BitmapToSlice(bitmap)
			
			// Verify exact match
			if len(actualResults) != len(expectedResults) {
				t.Errorf("Query %s: result count mismatch. Expected %d, got %d", 
					queryName, len(expectedResults), len(actualResults))
				continue
			}

			// Convert to sets for comparison
			expectedSet := make(map[uint64]bool)
			actualSet := make(map[uint64]bool)
			
			for _, row := range expectedResults {
				expectedSet[row] = true
			}
			for _, row := range actualResults {
				actualSet[row] = true
			}

			// Check for differences
			for row := range expectedSet {
				if !actualSet[row] {
					t.Errorf("Query %s: missing row %d in persisted results", queryName, row)
				}
			}
			for row := range actualSet {
				if !expectedSet[row] {
					t.Errorf("Query %s: unexpected row %d in persisted results", queryName, row)
				}
			}

			t.Logf("Query %s: ✅ Identical results (%d rows)", queryName, len(actualResults))
		}

		// Verify statistics match exactly
		for colName, expectedStats := range expectedStats {
			actualStats, err := cf.GetStats(colName)
			if err != nil {
				t.Errorf("Failed to get stats for %s after reopen: %v", colName, err)
				continue
			}

			// Compare each statistic
			for statName, expectedValue := range expectedStats {
				actualValue, exists := actualStats[statName]
				if !exists {
					t.Errorf("Column %s: missing statistic %s after reopen", colName, statName)
					continue
				}

				if actualValue != expectedValue {
					t.Errorf("Column %s: statistic %s mismatch. Expected %v, got %v", 
						colName, statName, expectedValue, actualValue)
				}
			}

			t.Logf("Column %s: ✅ Statistics match exactly", colName)
		}
	})

	t.Run("Phase3_ComplexQueriesPostPersistence", func(t *testing.T) {
		cf, err := OpenFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open file for complex queries: %v", err)
		}
		defer cf.Close()

		// Test complex bitmap operations
		bitmap1, _ := cf.QueryInt("int64_col", 25)
		bitmap2, _ := cf.RangeQueryInt("int64_col", 20, 30)
		bitmap3, _ := cf.QueryString("string_col", "value_25")

		// AND operation
		andResult := cf.QueryAnd(bitmap1, bitmap2)
		if andResult.GetCardinality() == 0 {
			t.Error("AND operation returned empty result")
		}

		// OR operation  
		orResult := cf.QueryOr(bitmap1, bitmap3)
		if orResult.GetCardinality() < bitmap1.GetCardinality() {
			t.Error("OR operation result smaller than individual operand")
		}

		// Performance validation
		start := time.Now()
		for i := 0; i < 100; i++ {
			_, err := cf.QueryInt("int64_col", int64(i%100))
			if err != nil {
				t.Errorf("Performance test query %d failed: %v", i, err)
			}
		}
		duration := time.Since(start)
		avgQuery := duration / 100
		
		if avgQuery > 1*time.Millisecond {
			t.Logf("⚠️  Average query time: %v (might be slow)", avgQuery)
		} else {
			t.Logf("✅ Average query time: %v (good performance)", avgQuery)
		}
	})
}

// TestCorruptionDetection tests file corruption scenarios
func TestCorruptionDetection(t *testing.T) {
	tmpFile := "test_corruption.bytedb"
	defer os.Remove(tmpFile)

	// Create valid file
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	cf.AddColumn("test_col", DataTypeInt64, false)
	testData := []IntData{
		NewIntData(100, 0), NewIntData(200, 1), NewIntData(300, 2),
	}
	cf.LoadIntColumn("test_col", testData)
	cf.Close()

	// Test normal opening
	cf, err = OpenFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open valid file: %v", err)
	}
	cf.Close()

	// TODO: Add corruption detection tests when checksums are implemented
	t.Log("✅ Basic file integrity validation passed")
	t.Log("⚠️  Advanced corruption detection tests pending checksum implementation")
}