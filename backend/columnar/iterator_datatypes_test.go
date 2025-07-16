package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestIteratorAllDataTypes tests iterator functionality for all supported data types
func TestIteratorAllDataTypes(t *testing.T) {
	filename := "test_iterator_datatypes.bytedb"
	defer os.Remove(filename)

	// Create test file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	// Add columns for each data type
	dataTypes := []struct {
		name     string
		dataType DataType
	}{
		{"bool_col", DataTypeBool},
		{"int8_col", DataTypeInt8},
		{"int16_col", DataTypeInt16},
		{"int32_col", DataTypeInt32},
		{"int64_col", DataTypeInt64},
		{"uint8_col", DataTypeUint8},
		{"uint16_col", DataTypeUint16},
		{"uint32_col", DataTypeUint32},
		{"uint64_col", DataTypeUint64},
		{"string_col", DataTypeString},
	}

	for _, dt := range dataTypes {
		if err := cf.AddColumn(dt.name, dt.dataType, false); err != nil {
			t.Fatal(err)
		}
	}

	// Load test data
	// Boolean column
	boolData := []IntData{
		{Value: 0, RowNum: 0}, // false
		{Value: 1, RowNum: 1}, // true
		{Value: 0, RowNum: 2}, // false
		{Value: 1, RowNum: 3}, // true
		{Value: 1, RowNum: 4}, // true
	}
	cf.LoadIntColumn("bool_col", boolData)

	// Integer columns (using same pattern for simplicity)
	intData := []IntData{
		{Value: 10, RowNum: 0},
		{Value: 20, RowNum: 1},
		{Value: 30, RowNum: 2},
		{Value: 40, RowNum: 3},
		{Value: 50, RowNum: 4},
	}

	for _, dt := range dataTypes {
		if dt.dataType != DataTypeBool && dt.dataType != DataTypeString {
			cf.LoadIntColumn(dt.name, intData)
		}
	}

	// String column
	stringData := []StringData{
		{Value: "apple", RowNum: 0},
		{Value: "banana", RowNum: 1},
		{Value: "cherry", RowNum: 2},
		{Value: "date", RowNum: 3},
		{Value: "elderberry", RowNum: 4},
	}
	cf.LoadStringColumn("string_col", stringData)

	cf.Close()

	// Reopen and test iteration
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	// Test boolean iteration
	t.Run("BooleanIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("bool_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedBools := []bool{false, true}
		i := 0
		for iter.Next() {
			key := iter.Key().(bool)
			rows := iter.Rows()

			if i >= len(expectedBools) {
				t.Fatalf("Too many results")
			}

			if key != expectedBools[i] {
				t.Errorf("Expected bool %v, got %v", expectedBools[i], key)
			}

			// Verify we have the right rows for each bool value
			if key == false && rows.GetCardinality() != 2 { // rows 0, 2
				t.Errorf("Expected 2 rows for false, got %d", rows.GetCardinality())
			}
			if key == true && rows.GetCardinality() != 3 { // rows 1, 3, 4
				t.Errorf("Expected 3 rows for true, got %d", rows.GetCardinality())
			}

			t.Logf("Bool %v: rows %v", key, BitmapToSlice(rows))
			i++
		}

		if i != len(expectedBools) {
			t.Errorf("Expected %d unique bool values, got %d", len(expectedBools), i)
		}
	})

	// Test integer types iteration
	integerTypes := []struct {
		colName  string
		dataType DataType
	}{
		{"int8_col", DataTypeInt8},
		{"int16_col", DataTypeInt16},
		{"int32_col", DataTypeInt32},
		{"int64_col", DataTypeInt64},
		{"uint8_col", DataTypeUint8},
		{"uint16_col", DataTypeUint16},
		{"uint32_col", DataTypeUint32},
		{"uint64_col", DataTypeUint64},
	}

	for _, intType := range integerTypes {
		t.Run(fmt.Sprintf("%sIteration", intType.colName), func(t *testing.T) {
			iter, err := cf.NewIterator(intType.colName)
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			expectedValues := []int64{10, 20, 30, 40, 50}
			i := 0

			for iter.Next() {
				if i >= len(expectedValues) {
					t.Fatalf("Too many results")
				}

				// Verify the key is of the correct type and value
				switch intType.dataType {
				case DataTypeInt8:
					key := iter.Key().(int8)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeInt16:
					key := iter.Key().(int16)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeInt32:
					key := iter.Key().(int32)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeInt64:
					key := iter.Key().(int64)
					if key != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeUint8:
					key := iter.Key().(uint8)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeUint16:
					key := iter.Key().(uint16)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeUint32:
					key := iter.Key().(uint32)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				case DataTypeUint64:
					key := iter.Key().(uint64)
					if int64(key) != expectedValues[i] {
						t.Errorf("Expected %d, got %d", expectedValues[i], key)
					}
				}

				i++
			}

			if i != len(expectedValues) {
				t.Errorf("Expected %d values, got %d", len(expectedValues), i)
			}
		})
	}

	// Test string iteration
	t.Run("StringIteration", func(t *testing.T) {
		iter, err := cf.NewIterator("string_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedStrings := []string{"apple", "banana", "cherry", "date", "elderberry"}
		i := 0

		for iter.Next() {
			key := iter.Key().(string)
			rows := iter.Rows()

			if i >= len(expectedStrings) {
				t.Fatalf("Too many results")
			}

			if key != expectedStrings[i] {
				t.Errorf("Expected string %s, got %s", expectedStrings[i], key)
			}

			// Each string should have exactly one row
			if rows.GetCardinality() != 1 {
				t.Errorf("Expected 1 row for string %s, got %d", key, rows.GetCardinality())
			}

			t.Logf("String %s: row %v", key, BitmapToSlice(rows))
			i++
		}

		if i != len(expectedStrings) {
			t.Errorf("Expected %d strings, got %d", len(expectedStrings), i)
		}
	})

	// Test range queries for different types
	t.Run("IntegerRangeQuery", func(t *testing.T) {
		iter, err := cf.NewRangeIterator("int32_col", int32(20), int32(40))
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedValues := []int32{20, 30, 40}
		i := 0

		for iter.Next() {
			key := iter.Key().(int32)
			if i >= len(expectedValues) {
				t.Fatalf("Too many results")
			}

			if key != expectedValues[i] {
				t.Errorf("Expected %d, got %d", expectedValues[i], key)
			}
			i++
		}

		if i != len(expectedValues) {
			t.Errorf("Expected %d values in range, got %d", len(expectedValues), i)
		}
	})

	t.Run("StringRangeQuery", func(t *testing.T) {
		// First check what strings are in the column
		allIter, _ := cf.NewIterator("string_col")
		t.Logf("All strings in column:")
		for allIter.Next() {
			t.Logf("  %s", allIter.Key().(string))
		}
		allIter.Close()
		
		// Check string segment
		col := cf.columns["string_col"]
		if col.stringSegment != nil {
			t.Logf("String segment offsets:")
			for str, offset := range col.stringSegment.stringMap {
				t.Logf("  %s -> %d", str, offset)
			}
		}
		
		iter, err := cf.NewRangeIterator("string_col", "banana", "date")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		expectedStrings := []string{"banana", "cherry", "date"}
		actualStrings := []string{}

		for iter.Next() {
			key := iter.Key().(string)
			actualStrings = append(actualStrings, key)
			t.Logf("Range query returned: %s", key)
		}

		if len(actualStrings) != len(expectedStrings) {
			t.Errorf("Expected %d strings in range, got %d", len(expectedStrings), len(actualStrings))
			t.Logf("Expected: %v", expectedStrings)
			t.Logf("Actual: %v", actualStrings)
		}
		
		for i, expected := range expectedStrings {
			if i < len(actualStrings) && actualStrings[i] != expected {
				t.Errorf("At index %d: expected %s, got %s", i, expected, actualStrings[i])
			}
		}
	})

	// Test seek functionality
	t.Run("StringSeek", func(t *testing.T) {
		iter, err := cf.NewIterator("string_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to "cherry"
		if !iter.Seek("cherry") {
			t.Fatal("Seek to 'cherry' failed")
		}

		key := iter.Key().(string)
		if key != "cherry" {
			t.Errorf("Expected to seek to 'cherry', got %s", key)
		}

		// Continue iteration
		if !iter.Next() {
			t.Fatal("Expected more results after seek")
		}

		key = iter.Key().(string)
		if key != "date" {
			t.Errorf("Expected 'date' after 'cherry', got %s", key)
		}
	})

	t.Run("IntegerSeek", func(t *testing.T) {
		iter, err := cf.NewIterator("uint16_col")
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// Seek to value 35 (should position at 40)
		if !iter.Seek(uint16(35)) {
			t.Fatal("Seek to 35 failed")
		}

		key := iter.Key().(uint16)
		if key != 40 {
			t.Errorf("Expected to seek to 40 (next value after 35), got %d", key)
		}
	})
}

// TestIteratorWithDuplicateValues tests that the iterator correctly handles duplicate values
func TestIteratorWithDuplicateValues(t *testing.T) {
	filename := "test_iterator_duplicates.bytedb"
	defer os.Remove(filename)

	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("category", DataTypeString, false)

	// Add duplicate string values
	stringData := []StringData{
		{Value: "fruit", RowNum: 0},
		{Value: "vegetable", RowNum: 1},
		{Value: "fruit", RowNum: 2},
		{Value: "meat", RowNum: 3},
		{Value: "fruit", RowNum: 4},
		{Value: "vegetable", RowNum: 5},
		{Value: "fruit", RowNum: 6},
	}

	err = cf.LoadStringColumn("category", stringData)
	if err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Reopen and test
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()

	iter, err := cf.NewIterator("category")
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	expectedResults := map[string][]uint64{
		"fruit":     {0, 2, 4, 6},
		"meat":      {3},
		"vegetable": {1, 5},
	}

	actualResults := make(map[string][]uint64)

	for iter.Next() {
		key := iter.Key().(string)
		rows := iter.Rows()
		actualResults[key] = BitmapToSlice(rows)
	}

	// Verify results
	for key, expectedRows := range expectedResults {
		actualRows, exists := actualResults[key]
		if !exists {
			t.Errorf("Key %s not found in results", key)
			continue
		}

		if len(actualRows) != len(expectedRows) {
			t.Errorf("Key %s: expected %d rows, got %d", key, len(expectedRows), len(actualRows))
			continue
		}

		for i, expectedRow := range expectedRows {
			if actualRows[i] != expectedRow {
				t.Errorf("Key %s: expected row %d at index %d, got %d", key, expectedRow, i, actualRows[i])
			}
		}

		t.Logf("Category %s: rows %v", key, actualRows)
	}
}