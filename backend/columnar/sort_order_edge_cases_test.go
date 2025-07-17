package columnar

import (
	"math"
	"os"
	"testing"
)

// TestSignedIntegerBoundaryValues tests min/max values for signed types
func TestSignedIntegerBoundaryValues(t *testing.T) {
	testCases := []struct {
		name     string
		dataType DataType
		values   []int64
		expected []interface{}
	}{
		{
			name:     "Int8_Boundaries",
			dataType: DataTypeInt8,
			values:   []int64{127, -128, 0, -127, 126},
			expected: []interface{}{int8(-128), int8(-127), int8(0), int8(126), int8(127)},
		},
		{
			name:     "Int16_Boundaries",
			dataType: DataTypeInt16,
			values:   []int64{32767, -32768, 0, -32767, 32766},
			expected: []interface{}{int16(-32768), int16(-32767), int16(0), int16(32766), int16(32767)},
		},
		{
			name:     "Int32_Boundaries",
			dataType: DataTypeInt32,
			values:   []int64{2147483647, -2147483648, 0, -2147483647, 2147483646},
			expected: []interface{}{int32(-2147483648), int32(-2147483647), int32(0), int32(2147483646), int32(2147483647)},
		},
		{
			name:     "Int64_Boundaries",
			dataType: DataTypeInt64,
			values:   []int64{math.MaxInt64, math.MinInt64, 0, math.MinInt64 + 1, math.MaxInt64 - 1},
			expected: []interface{}{int64(math.MinInt64), int64(math.MinInt64 + 1), int64(0), int64(math.MaxInt64 - 1), int64(math.MaxInt64)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file := "test_boundaries_" + tc.name + ".bytedb"
			defer os.Remove(file)

			cf, err := CreateFile(file)
			if err != nil {
				t.Fatal(err)
			}

			cf.AddColumn("value", tc.dataType, false)

			data := make([]IntData, len(tc.values))
			for i, v := range tc.values {
				data[i] = NewIntData(v, uint64(i+1))
			}

			err = cf.LoadIntColumn("value", data)
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

			iter, err := cf.NewIterator("value")
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			idx := 0
			for iter.Next() {
				if idx < len(tc.expected) {
					key := iter.Key()
					if key != tc.expected[idx] {
						t.Errorf("Position %d: expected %v (%T), got %v (%T)",
							idx, tc.expected[idx], tc.expected[idx], key, key)
					}
				}
				idx++
			}

			if idx != len(tc.expected) {
				t.Errorf("Expected %d values, got %d", len(tc.expected), idx)
			}
		})
	}
}

// TestMixedSignUnsignedQueries tests queries with mixed types
func TestMixedSignUnsignedQueries(t *testing.T) {
	// Test that signed and unsigned columns can coexist
	file := "test_mixed_types.bytedb"
	defer os.Remove(file)

	cf, _ := CreateFile(file)
	cf.AddColumn("signed", DataTypeInt32, false)
	cf.AddColumn("unsigned", DataTypeUint32, false)

	// Add same numeric values to both columns
	signedData := []IntData{
		NewIntData(-100, 1),
		NewIntData(0, 2),
		NewIntData(100, 3),
		NewIntData(1000, 4),
	}

	unsignedData := []IntData{
		NewIntData(0, 1),
		NewIntData(100, 2),
		NewIntData(1000, 3),
		NewIntData(4294967295, 4), // max uint32
	}

	cf.LoadIntColumn("signed", signedData)
	cf.LoadIntColumn("unsigned", unsignedData)
	cf.Close()

	// Reopen and query
	cf, _ = OpenFile(file)
	defer cf.Close()

	// Query signed column using Find directly
	col, _ := cf.columns["signed"]
	signedResult, _ := col.btree.FindBitmap(toUint64WithType(int64(100), DataTypeInt32))
	if signedResult == nil || signedResult.GetCardinality() != 1 {
		t.Error("Failed to find signed value 100")
	}

	// Query unsigned column
	col2, _ := cf.columns["unsigned"]
	unsignedResult, _ := col2.btree.FindBitmap(toUint64WithType(int64(100), DataTypeUint32))
	if unsignedResult == nil || unsignedResult.GetCardinality() != 1 {
		t.Error("Failed to find unsigned value 100")
	}

	// Range query on signed
	signedRange, _ := cf.RangeQueryInt("signed", -100, 100)
	if signedRange.GetCardinality() != 3 { // -100, 0, 100
		t.Errorf("Expected 3 results for signed range, got %d", signedRange.GetCardinality())
	}

	// Range query on unsigned
	unsignedRange, _ := cf.RangeQueryInt("unsigned", 0, 1000)
	if unsignedRange.GetCardinality() != 3 { // 0, 100, 1000
		t.Errorf("Expected 3 results for unsigned range, got %d", unsignedRange.GetCardinality())
	}
}

// TestIteratorSeekWithSignedIntegers tests iterator seek functionality
func TestIteratorSeekWithSignedIntegers(t *testing.T) {
	file := "test_seek_signed.bytedb"
	defer os.Remove(file)

	cf, _ := CreateFile(file)
	cf.AddColumn("value", DataTypeInt32, false)

	values := []int64{-1000, -500, -100, 0, 100, 500, 1000}
	data := make([]IntData, len(values))
	for i, v := range values {
		data[i] = NewIntData(v, uint64(i+1))
	}

	cf.LoadIntColumn("value", data)
	cf.Close()

	// Reopen
	cf, _ = OpenFile(file)
	defer cf.Close()

	iter, _ := cf.NewIterator("value")
	defer iter.Close()

	// Test seeking to negative value
	found := iter.Seek(int32(-100))
	if !found {
		t.Error("Failed to seek to -100")
	}
	if iter.Key().(int32) != -100 {
		t.Errorf("Expected to seek to -100, got %v", iter.Key())
	}

	// Test seeking past all values
	found = iter.Seek(int32(2000))
	if found {
		t.Error("Should not find value past maximum")
	}

	// Test seeking before all values
	iter.SeekFirst()
	if iter.Key().(int32) != -1000 {
		t.Errorf("Expected first value to be -1000, got %v", iter.Key())
	}

	// Test seeking to last
	iter.SeekLast()
	if iter.Key().(int32) != 1000 {
		t.Errorf("Expected last value to be 1000, got %v", iter.Key())
	}
}