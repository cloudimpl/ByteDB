package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestSpaceEfficiencyWithDifferentDataTypes(t *testing.T) {
	// Test how much space we save with smaller data types
	
	testCases := []struct {
		name     string
		dataType DataType
		values   []struct{ Key uint64; RowNum uint64 }
	}{
		{
			name:     "BooleanColumn",
			dataType: DataTypeBool,
			values:   generateTestData(1000, 2), // 2 distinct values (true/false)
		},
		{
			name:     "ByteColumn",
			dataType: DataTypeUint8,
			values:   generateTestData(1000, 256), // 256 distinct values
		},
		{
			name:     "ShortColumn",
			dataType: DataTypeUint16,
			values:   generateTestData(1000, 1000), // 1000 distinct values
		},
		{
			name:     "IntColumn",
			dataType: DataTypeUint32,
			values:   generateTestData(1000, 1000), // 1000 distinct values
		},
		{
			name:     "LongColumn",
			dataType: DataTypeUint64,
			values:   generateTestData(1000, 1000), // 1000 distinct values
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile := fmt.Sprintf("test_%s.db", tc.name)
			defer os.Remove(tmpFile)
			
			// Create file
			cf, err := CreateFile(tmpFile)
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			
			// Add column with specific data type
			if err := cf.AddColumn("value", tc.dataType, false); err != nil {
				t.Fatalf("Failed to add column: %v", err)
			}
			
			// Create B+ tree and load data
			col := cf.columns["value"]
			_, err = col.btree.BulkLoadWithDuplicates(tc.values)
			if err != nil {
				t.Fatalf("Failed to load data: %v", err)
			}
			
			// Update metadata
			col.metadata.RootPageID = col.btree.GetRootPageID()
			col.metadata.TotalKeys = uint64(len(tc.values))
			
			// Test a few queries to validate data integrity
			testKey := tc.values[0].Key
			bitmap, err := col.btree.FindBitmap(testKey)
			if err != nil {
				t.Fatalf("Failed to query test key %d: %v", testKey, err)
			}
			results := BitmapToSlice(bitmap)
			
			// Count expected occurrences of test key
			expectedCount := 0
			for _, v := range tc.values {
				if v.Key == testKey {
					expectedCount++
				}
			}
			
			if len(results) != expectedCount {
				t.Errorf("Query for key %d returned %d results, expected %d", testKey, len(results), expectedCount)
			}
			
			// Validate actual row numbers match what we expect
			expectedRows := make([]uint64, 0)
			for _, v := range tc.values {
				if v.Key == testKey {
					expectedRows = append(expectedRows, v.RowNum)
				}
			}
			
			if len(results) == len(expectedRows) {
				for _, result := range results {
					found := false
					for _, expected := range expectedRows {
						if result == expected {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Query returned unexpected row %d", result)
					}
				}
			}
			
			// Close file
			cf.Close()
			
			// Check file size
			info, err := os.Stat(tmpFile)
			if err != nil {
				t.Fatalf("Failed to stat file: %v", err)
			}
			
			// Calculate theoretical minimum size
			keySize := GetDataTypeSize(tc.dataType)
			theoreticalSize := len(tc.values) * (keySize + 8) // key + value
			
			t.Logf("Data type: %v, Key size: %d bytes", tc.dataType, keySize)
			t.Logf("Number of entries: %d", len(tc.values))
			t.Logf("Theoretical minimum: %d bytes", theoreticalSize)
			t.Logf("Actual file size: %d bytes", info.Size())
			t.Logf("Overhead: %.2f%%", float64(info.Size()-int64(theoreticalSize))/float64(theoreticalSize)*100)
		})
	}
}

func generateTestData(count int, maxValue int) []struct{ Key uint64; RowNum uint64 } {
	data := make([]struct{ Key uint64; RowNum uint64 }, count)
	for i := 0; i < count; i++ {
		data[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % maxValue),
			RowNum: uint64(i),
		}
	}
	return data
}

func TestBooleanColumnEfficiency(t *testing.T) {
	tmpFile := "test_bool_efficiency.db"
	defer os.Remove(tmpFile)
	
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add boolean column
	if err := cf.AddColumn("active", DataTypeBool, false); err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Load data - 10000 rows with only true/false values
	data := make([]struct{ Key uint64; RowNum uint64 }, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(i % 2), // 0 or 1 (false or true)
			RowNum: uint64(i),
		}
	}
	
	col := cf.columns["active"]
	_, err = col.btree.BulkLoadWithDuplicates(data)
	if err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}
	
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(data))
	
	// Get some statistics before closing
	pageCount := cf.pageManager.GetPageCount()
	
	cf.Close()
	
	// Check file size
	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	
	// With boolean keys (1 byte) instead of uint64 (8 bytes), we save 7 bytes per key
	// in internal nodes. For a B+ tree with 10000 entries and ~100 keys per internal node,
	// we'd have about 100 leaf entries in internal nodes, saving ~700 bytes
	t.Logf("Boolean column file size: %d bytes", info.Size())
	t.Logf("Page count: %d", pageCount)
	t.Logf("With 10000 boolean values, using 1-byte keys instead of 8-byte keys")
	
	// Now create the same with uint64 for comparison
	tmpFile2 := "test_uint64_efficiency.db"
	defer os.Remove(tmpFile2)
	
	cf2, _ := CreateFile(tmpFile2)
	cf2.AddColumn("active", DataTypeUint64, false)
	
	col2 := cf2.columns["active"]
	col2.btree.BulkLoadWithDuplicates(data)
	col2.metadata.RootPageID = col2.btree.GetRootPageID()
	col2.metadata.TotalKeys = uint64(len(data))
	
	pageCount2 := cf2.pageManager.GetPageCount()
	cf2.Close()
	
	info2, _ := os.Stat(tmpFile2)
	
	t.Logf("Uint64 column file size: %d bytes", info2.Size())
	t.Logf("Page count: %d", pageCount2)
	t.Logf("Space saved: %d bytes (%.2f%%)", info2.Size()-info.Size(), 
		float64(info2.Size()-info.Size())/float64(info2.Size())*100)
}