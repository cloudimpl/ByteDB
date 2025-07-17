package columnar

import (
	"fmt"
	"os"
	"sort"
	"testing"
)

// TestSortOrderAllDataTypes tests sort order for all data types
func TestSortOrderAllDataTypes(t *testing.T) {
	testCases := []struct {
		name     string
		dataType DataType
		values   []interface{}
		expected []interface{}
	}{
		{
			name:     "Int8_PositiveAndNegative",
			dataType: DataTypeInt8,
			values:   []interface{}{int64(100), int64(-100), int64(0), int64(-50), int64(50), int64(127), int64(-128)},
			expected: []interface{}{int8(-128), int8(-100), int8(-50), int8(0), int8(50), int8(100), int8(127)},
		},
		{
			name:     "Int16_PositiveAndNegative",
			dataType: DataTypeInt16,
			values:   []interface{}{int64(1000), int64(-1000), int64(0), int64(-500), int64(500), int64(32767), int64(-32768)},
			expected: []interface{}{int16(-32768), int16(-1000), int16(-500), int16(0), int16(500), int16(1000), int16(32767)},
		},
		{
			name:     "Int32_PositiveAndNegative",
			dataType: DataTypeInt32,
			values:   []interface{}{int64(100000), int64(-100000), int64(0), int64(-50000), int64(50000), int64(2147483647), int64(-2147483648)},
			expected: []interface{}{int32(-2147483648), int32(-100000), int32(-50000), int32(0), int32(50000), int32(100000), int32(2147483647)},
		},
		{
			name:     "Int64_PositiveAndNegative",
			dataType: DataTypeInt64,
			values:   []interface{}{int64(1000000), int64(-1000000), int64(0), int64(-500000), int64(500000)},
			expected: []interface{}{int64(-1000000), int64(-500000), int64(0), int64(500000), int64(1000000)},
		},
		{
			name:     "Uint8_AllPositive",
			dataType: DataTypeUint8,
			values:   []interface{}{int64(200), int64(100), int64(0), int64(50), int64(255)},
			expected: []interface{}{uint8(0), uint8(50), uint8(100), uint8(200), uint8(255)},
		},
		{
			name:     "Uint16_AllPositive",
			dataType: DataTypeUint16,
			values:   []interface{}{int64(30000), int64(10000), int64(0), int64(5000), int64(65535)},
			expected: []interface{}{uint16(0), uint16(5000), uint16(10000), uint16(30000), uint16(65535)},
		},
		{
			name:     "Uint32_AllPositive",
			dataType: DataTypeUint32,
			values:   []interface{}{int64(3000000), int64(1000000), int64(0), int64(500000), int64(4294967295)},
			expected: []interface{}{uint32(0), uint32(500000), uint32(1000000), uint32(3000000), uint32(4294967295)},
		},
		{
			name:     "Uint64_AllPositive",
			dataType: DataTypeUint64,
			values:   []interface{}{int64(3000000), int64(1000000), int64(0), int64(500000)},
			expected: []interface{}{uint64(0), uint64(500000), uint64(1000000), uint64(3000000)},
		},
		{
			name:     "Bool_FalseBeforeTrue",
			dataType: DataTypeBool,
			values:   []interface{}{int64(1), int64(0), int64(1), int64(0)},
			expected: []interface{}{false, true}, // Only unique values are returned
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create file
			file := fmt.Sprintf("test_sort_%s.bytedb", tc.name)
			defer os.Remove(file)

			cf, err := CreateFile(file)
			if err != nil {
				t.Fatal(err)
			}

			// Add column
			cf.AddColumn("value", tc.dataType, false)

			// Load data
			data := make([]IntData, len(tc.values))
			for i, v := range tc.values {
				data[i] = NewIntData(v.(int64), uint64(i+1))
			}
			err = cf.LoadIntColumn("value", data)
			if err != nil {
				t.Fatal(err)
			}
			cf.Close()

			// Reopen and verify sort order
			cf, err = OpenFile(file)
			if err != nil {
				t.Fatal(err)
			}
			defer cf.Close()

			// Create iterator
			iter, err := cf.NewIterator("value")
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			// Collect values
			var actual []interface{}
			for iter.Next() {
				actual = append(actual, iter.Key())
			}

			// Verify count
			if len(actual) != len(tc.expected) {
				t.Errorf("Expected %d values, got %d", len(tc.expected), len(actual))
			}

			// Verify order
			for i := range tc.expected {
				if i < len(actual) {
					if fmt.Sprintf("%v", actual[i]) != fmt.Sprintf("%v", tc.expected[i]) {
						t.Errorf("Position %d: expected %v (%T), got %v (%T)", 
							i, tc.expected[i], tc.expected[i], actual[i], actual[i])
					}
				}
			}
		})
	}
}

// TestStringTypeSortOrder tests string sorting
func TestStringTypeSortOrder(t *testing.T) {
	file := "test_string_sort.bytedb"
	defer os.Remove(file)

	cf, err := CreateFile(file)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("name", DataTypeString, false)

	// Test strings including special cases
	strings := []string{
		"zebra", "apple", "banana", "Apple", "APPLE", 
		"123", "012", "12", "2", "10",
		"", "a", "aa", "aaa", "b",
		"test123", "test12", "test2", "test",
		"中文", "русский", "العربية", "日本語",
	}

	data := make([]StringData, len(strings))
	for i, s := range strings {
		data[i] = NewStringData(s, uint64(i+1))
	}

	err = cf.LoadStringColumn("name", data)
	if err != nil {
		t.Fatal(err)
	}
	cf.Close()

	// Reopen and verify
	cf, err = OpenFile(file)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	iter, err := cf.NewIterator("name")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var actual []string
	for iter.Next() {
		actual = append(actual, iter.Key().(string))
	}

	// Strings should be lexicographically sorted
	expected := make([]string, len(strings))
	copy(expected, strings)
	sort.Strings(expected)

	// Verify order
	for i := range expected {
		if i < len(actual) && actual[i] != expected[i] {
			t.Errorf("Position %d: expected %q, got %q", i, expected[i], actual[i])
		}
	}
}

// TestMergeSignedIntegerSortOrder tests merge with mixed signed values
func TestMergeSignedIntegerSortOrder(t *testing.T) {
	dataTypes := []struct {
		name     string
		dataType DataType
		file1    []int64
		file2    []int64
		expected []interface{}
	}{
		{
			name:     "Int8_Merge",
			dataType: DataTypeInt8,
			file1:    []int64{-100, -10, 10, 100},
			file2:    []int64{-50, 0, 50},
			expected: []interface{}{int8(-100), int8(-50), int8(-10), int8(0), int8(10), int8(50), int8(100)},
		},
		{
			name:     "Int16_Merge",
			dataType: DataTypeInt16,
			file1:    []int64{-1000, -100, 100, 1000},
			file2:    []int64{-500, 0, 500},
			expected: []interface{}{int16(-1000), int16(-500), int16(-100), int16(0), int16(100), int16(500), int16(1000)},
		},
		{
			name:     "Int32_Merge",
			dataType: DataTypeInt32,
			file1:    []int64{-100000, -10000, 10000, 100000},
			file2:    []int64{-50000, 0, 50000},
			expected: []interface{}{int32(-100000), int32(-50000), int32(-10000), int32(0), int32(10000), int32(50000), int32(100000)},
		},
	}

	for _, tc := range dataTypes {
		t.Run(tc.name, func(t *testing.T) {
			// Create first file
			file1 := fmt.Sprintf("test_merge1_%s.bytedb", tc.name)
			defer os.Remove(file1)

			cf1, _ := CreateFile(file1)
			cf1.AddColumn("value", tc.dataType, false)
			
			data1 := make([]IntData, len(tc.file1))
			for i, v := range tc.file1 {
				data1[i] = NewIntData(v, uint64(i+1))
			}
			cf1.LoadIntColumn("value", data1)
			cf1.Close()

			// Create second file
			file2 := fmt.Sprintf("test_merge2_%s.bytedb", tc.name)
			defer os.Remove(file2)

			cf2, _ := CreateFile(file2)
			cf2.AddColumn("value", tc.dataType, false)
			
			data2 := make([]IntData, len(tc.file2))
			for i, v := range tc.file2 {
				data2[i] = NewIntData(v, uint64(i+100)) // Different row numbers
			}
			cf2.LoadIntColumn("value", data2)
			cf2.Close()

			// Test both regular and streaming merge
			for _, useStreaming := range []bool{false, true} {
				testName := "Regular"
				if useStreaming {
					testName = "Streaming"
				}
				
				t.Run(testName, func(t *testing.T) {
					output := fmt.Sprintf("test_merge_output_%s_%s.bytedb", tc.name, testName)
					defer os.Remove(output)

					options := &MergeOptions{
						UseStreamingMerge: useStreaming,
					}

					err := MergeFiles(output, []string{file1, file2}, options)
					if err != nil {
						t.Fatal(err)
					}

					// Verify merged result
					merged, _ := OpenFile(output)
					defer merged.Close()

					iter, _ := merged.NewIterator("value")
					defer iter.Close()

					var actual []interface{}
					for iter.Next() {
						actual = append(actual, iter.Key())
					}

					// Verify order
					for i := range tc.expected {
						if i < len(actual) {
							if fmt.Sprintf("%v", actual[i]) != fmt.Sprintf("%v", tc.expected[i]) {
								t.Errorf("Position %d: expected %v, got %v", 
									i, tc.expected[i], actual[i])
							}
						}
					}
				})
			}
		})
	}
}

// TestRangeQueriesWithSignedIntegers tests range queries on signed integers
func TestRangeQueriesWithSignedIntegers(t *testing.T) {
	file := "test_range_signed.bytedb"
	defer os.Remove(file)

	cf, _ := CreateFile(file)
	cf.AddColumn("value", DataTypeInt32, false)

	// Add values including negative
	values := []int64{-1000, -500, -100, -50, -10, 0, 10, 50, 100, 500, 1000}
	data := make([]IntData, len(values))
	for i, v := range values {
		data[i] = NewIntData(v, uint64(i+1))
	}
	cf.LoadIntColumn("value", data)
	cf.Close()

	// Reopen for queries
	cf, _ = OpenFile(file)
	defer cf.Close()

	testCases := []struct {
		name     string
		min, max int64
		expected int
	}{
		{"AllNegative", -1000, -10, 5},      // -1000, -500, -100, -50, -10
		{"NegativeToPositive", -100, 100, 7}, // -100, -50, -10, 0, 10, 50, 100
		{"AllPositive", 10, 1000, 5},         // 10, 50, 100, 500, 1000
		{"SingleValue", 0, 0, 1},             // 0
		{"NoResults", -2000, -1500, 0},      // none
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := cf.RangeQueryInt("value", tc.min, tc.max)
			if err != nil {
				t.Fatal(err)
			}
			
			if result.GetCardinality() != uint64(tc.expected) {
				t.Errorf("Expected %d results, got %d", tc.expected, result.GetCardinality())
			}
		})
	}
}

// TestNullHandlingWithSortOrder tests null handling doesn't break sort order
func TestNullHandlingWithSortOrder(t *testing.T) {
	file := "test_null_sort.bytedb"
	defer os.Remove(file)

	cf, _ := CreateFile(file)
	cf.AddColumn("value", DataTypeInt32, true) // nullable

	// Mix of values and nulls
	data := []IntData{
		NewIntData(-100, 1),
		NewNullIntData(2),
		NewIntData(0, 3),
		NewNullIntData(4),
		NewIntData(100, 5),
		NewIntData(-50, 6),
		NewNullIntData(7),
		NewIntData(50, 8),
	}

	cf.LoadIntColumn("value", data)
	cf.Close()

	// Reopen and verify
	cf, _ = OpenFile(file)
	defer cf.Close()

	// Check non-null values are sorted
	iter, _ := cf.NewIterator("value")
	defer iter.Close()

	expected := []int32{-100, -50, 0, 50, 100}
	actual := []int32{}
	
	for iter.Next() {
		actual = append(actual, iter.Key().(int32))
	}

	for i := range expected {
		if i < len(actual) && actual[i] != expected[i] {
			t.Errorf("Position %d: expected %d, got %d", i, expected[i], actual[i])
		}
	}

	// Verify null bitmap
	nullBitmap, _ := cf.QueryNull("value")
	if nullBitmap.GetCardinality() != 3 {
		t.Errorf("Expected 3 nulls, got %d", nullBitmap.GetCardinality())
	}
}