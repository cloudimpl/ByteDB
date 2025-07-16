package columnar

import (
	"os"
	"testing"
)

// TestIteratorAllTypesWithCompression tests all data types with compression enabled
func TestIteratorAllTypesWithCompression(t *testing.T) {
	filename := "test_iterator_alltypes_compressed.bytedb"
	defer os.Remove(filename)

	// Create file with compression
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelBest)

	cf, err := CreateFileWithOptions(filename, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Add columns for each supported type
	cf.AddColumn("bool_col", DataTypeBool, false)
	cf.AddColumn("int8_col", DataTypeInt8, false)
	cf.AddColumn("int16_col", DataTypeInt16, false)
	cf.AddColumn("int32_col", DataTypeInt32, false)
	cf.AddColumn("int64_col", DataTypeInt64, false)
	cf.AddColumn("uint8_col", DataTypeUint8, false)
	cf.AddColumn("uint16_col", DataTypeUint16, false)
	cf.AddColumn("uint32_col", DataTypeUint32, false)
	cf.AddColumn("uint64_col", DataTypeUint64, false)
	cf.AddColumn("string_col", DataTypeString, false)

	// Load test data
	// Boolean column with duplicates
	boolData := []IntData{
		{Value: 1, RowNum: 0}, // true
		{Value: 0, RowNum: 1}, // false
		{Value: 1, RowNum: 2}, // true
		{Value: 1, RowNum: 3}, // true
		{Value: 0, RowNum: 4}, // false
		{Value: 1, RowNum: 5}, // true
		{Value: 0, RowNum: 6}, // false
		{Value: 1, RowNum: 7}, // true
		{Value: 0, RowNum: 8}, // false
		{Value: 1, RowNum: 9}, // true
	}
	cf.LoadIntColumn("bool_col", boolData)

	// Integer columns with duplicates
	intData := []IntData{
		{Value: 100, RowNum: 0},
		{Value: 200, RowNum: 1},
		{Value: 100, RowNum: 2},
		{Value: 300, RowNum: 3},
		{Value: 200, RowNum: 4},
		{Value: 100, RowNum: 5},
		{Value: 400, RowNum: 6},
		{Value: 200, RowNum: 7},
		{Value: 500, RowNum: 8},
		{Value: 100, RowNum: 9},
	}

	for _, colName := range []string{"int8_col", "int16_col", "int32_col", "int64_col",
		"uint8_col", "uint16_col", "uint32_col", "uint64_col"} {
		cf.LoadIntColumn(colName, intData)
	}

	// String column with duplicates
	stringData := []StringData{
		{Value: "red", RowNum: 0},
		{Value: "blue", RowNum: 1},
		{Value: "red", RowNum: 2},
		{Value: "green", RowNum: 3},
		{Value: "blue", RowNum: 4},
		{Value: "red", RowNum: 5},
		{Value: "yellow", RowNum: 6},
		{Value: "blue", RowNum: 7},
		{Value: "purple", RowNum: 8},
		{Value: "red", RowNum: 9},
	}
	cf.LoadStringColumn("string_col", stringData)

	cf.Close()

	// Reopen and test iteration
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	// Test boolean column
	t.Run("CompressedBooleanIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("bool_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedResults := map[bool]int{
			false: 4, // rows 1, 4, 6, 8
			true:  6, // rows 0, 2, 3, 5, 7, 9
		}

		for iter.Next() {
			key := iter.Key().(bool)
			rows := iter.Rows()
			cardinality := int(rows.GetCardinality())

			if expectedCount, ok := expectedResults[key]; ok {
				if cardinality != expectedCount {
					t.Errorf("Bool %v: expected %d rows, got %d", key, expectedCount, cardinality)
				}
			} else {
				t.Errorf("Unexpected bool value: %v", key)
			}

			t.Logf("Bool %v: %d rows", key, cardinality)
		}
	})

	// Test integer columns
	t.Run("CompressedIntegerIteration", func(t *testing.T) {
		expectedResults := map[int64]int{
			100: 4, // rows 0, 2, 5, 9
			200: 3, // rows 1, 4, 7
			300: 1, // row 3
			400: 1, // row 6
			500: 1, // row 8
		}

		iter, err := cf.NewIterator("int32_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		for iter.Next() {
			key := iter.Key().(int32)
			rows := iter.Rows()
			cardinality := int(rows.GetCardinality())

			if expectedCount, ok := expectedResults[int64(key)]; ok {
				if cardinality != expectedCount {
					t.Errorf("Int32 %d: expected %d rows, got %d", key, expectedCount, cardinality)
				}
			} else {
				t.Errorf("Unexpected int32 value: %d", key)
			}

			t.Logf("Int32 %d: %d rows", key, cardinality)
		}
	})

	// Test string column
	t.Run("CompressedStringIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("string_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedResults := map[string]int{
			"blue":   3, // rows 1, 4, 7
			"green":  1, // row 3
			"purple": 1, // row 8
			"red":    4, // rows 0, 2, 5, 9
			"yellow": 1, // row 6
		}

		for iter.Next() {
			key := iter.Key().(string)
			rows := iter.Rows()
			cardinality := int(rows.GetCardinality())

			if expectedCount, ok := expectedResults[key]; ok {
				if cardinality != expectedCount {
					t.Errorf("String %s: expected %d rows, got %d", key, expectedCount, cardinality)
				}
			} else {
				t.Errorf("Unexpected string value: %s", key)
			}

			t.Logf("String %s: %d rows", key, cardinality)
		}
	})

	// Test range queries with compression
	t.Run("CompressedRangeQueries", func(t *testing.T) {
		// Integer range query
		intIter, err := cf.NewRangeIterator("uint16_col", uint16(150), uint16(350))
		if err != nil {
			t.Fatal(err)
		}
		defer intIter.Close()

		intCount := 0
		for intIter.Next() {
			key := intIter.Key().(uint16)
			if key < 150 || key > 350 {
				t.Errorf("Uint16 %d out of range [150, 350]", key)
			}
			intCount++
		}

		if intCount != 2 { // Should find 200 and 300
			t.Errorf("Expected 2 values in uint16 range, got %d", intCount)
		}

		// String range query
		strIter, err := cf.NewRangeIterator("string_col", "green", "red")
		if err != nil {
			t.Fatal(err)
		}
		defer strIter.Close()

		strCount := 0
		for strIter.Next() {
			key := strIter.Key().(string)
			if key < "green" || key > "red" {
				t.Errorf("String %s out of range [green, red]", key)
			}
			strCount++
		}

		if strCount != 3 { // Should find green, purple, red
			t.Errorf("Expected 3 values in string range, got %d", strCount)
		}
	})

	// Test seek with compression
	t.Run("CompressedSeek", func(t *testing.T) {
		iter, err := cf.NewIterator("string_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to a value that exists
		if !iter.Seek("green") {
			t.Fatal("Seek to 'green' failed")
		}

		if iter.Key().(string) != "green" {
			t.Errorf("Expected to seek to 'green', got %s", iter.Key().(string))
		}

		// Seek to a value that doesn't exist (should find next)
		iter2, err := cf.NewIterator("string_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter2.Close()

		if !iter2.Seek("orange") {
			t.Fatal("Seek to 'orange' failed")
		}

		// Should position at 'purple' (next string after 'orange' alphabetically)
		if iter2.Key().(string) != "purple" {
			t.Errorf("Expected to seek to 'purple' (next after 'orange'), got %s", iter2.Key().(string))
		}
	})

	t.Logf("All data types work correctly with compression enabled")
}