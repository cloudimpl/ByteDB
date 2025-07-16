package columnar

import (
	"os"
	"testing"
)

func TestBidirectionalLookup(t *testing.T) {
	filename := "test_bidirectional.bytedb"
	defer os.Remove(filename)
	
	// Create file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	
	// Add columns
	if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
		t.Fatalf("Failed to add id column: %v", err)
	}
	
	if err := cf.AddColumn("name", DataTypeString, true); err != nil {
		t.Fatalf("Failed to add name column: %v", err)
	}
	
	// Load integer data
	intData := []IntData{
		{Value: 100, RowNum: 0},
		{Value: 200, RowNum: 1},
		{Value: 300, RowNum: 2},
		{Value: 100, RowNum: 3}, // Duplicate value
		{Value: 400, RowNum: 4},
	}
	
	if err := cf.LoadIntColumn("id", intData); err != nil {
		t.Fatalf("Failed to load int column: %v", err)
	}
	
	// Load string data with nulls
	stringData := []StringData{
		{Value: "Alice", RowNum: 0, IsNull: false},
		{Value: "Bob", RowNum: 1, IsNull: false},
		{Value: "", RowNum: 2, IsNull: true}, // NULL
		{Value: "Charlie", RowNum: 3, IsNull: false},
		{Value: "Bob", RowNum: 4, IsNull: false}, // Duplicate value
	}
	
	if err := cf.LoadStringColumn("name", stringData); err != nil {
		t.Fatalf("Failed to load string column: %v", err)
	}
	
	// Close and reopen to test persistence
	if err := cf.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}
	
	cf, err = OpenFile(filename)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer cf.Close()
	
	t.Run("IntegerLookupByRow", func(t *testing.T) {
		tests := []struct {
			rowNum     uint64
			expected   int64
			shouldFind bool
		}{
			{0, 100, true},
			{1, 200, true},
			{2, 300, true},
			{3, 100, true},
			{4, 400, true},
			{5, 0, false}, // Non-existent row
		}
		
		for _, test := range tests {
			value, found, err := cf.LookupIntByRow("id", test.rowNum)
			if err != nil {
				t.Errorf("Row %d: unexpected error: %v", test.rowNum, err)
				continue
			}
			
			if found != test.shouldFind {
				t.Errorf("Row %d: expected found=%v, got %v", test.rowNum, test.shouldFind, found)
			}
			
			if found && value != test.expected {
				t.Errorf("Row %d: expected value=%d, got %d", test.rowNum, test.expected, value)
			}
		}
	})
	
	t.Run("StringLookupByRow", func(t *testing.T) {
		tests := []struct {
			rowNum     uint64
			expected   string
			shouldFind bool
			isNull     bool
		}{
			{0, "Alice", true, false},
			{1, "Bob", true, false},
			{2, "", false, true}, // NULL
			{3, "Charlie", true, false},
			{4, "Bob", true, false},
			{5, "", false, false}, // Non-existent row
		}
		
		for _, test := range tests {
			value, found, err := cf.LookupStringByRow("name", test.rowNum)
			if err != nil {
				t.Errorf("Row %d: unexpected error: %v", test.rowNum, err)
				continue
			}
			
			if test.isNull {
				if found {
					t.Errorf("Row %d: expected NULL but found value: %s", test.rowNum, value)
				}
			} else {
				if found != test.shouldFind {
					t.Errorf("Row %d: expected found=%v, got %v", test.rowNum, test.shouldFind, found)
				}
				
				if found && value != test.expected {
					t.Errorf("Row %d: expected value=%s, got %s", test.rowNum, test.expected, value)
				}
			}
		}
	})
	
	t.Run("ForwardBackwardConsistency", func(t *testing.T) {
		// Test that forward lookup (key→row) and backward lookup (row→key) are consistent
		
		// For integer column
		for _, data := range intData {
			// Forward lookup: value → rows
			bitmap, err := cf.QueryInt("id", data.Value)
			if err != nil {
				t.Fatalf("Failed to query int value %d: %v", data.Value, err)
			}
			
			rows := BitmapToSlice(bitmap)
			
			// Verify that our row is in the result
			found := false
			for _, row := range rows {
				if row == data.RowNum {
					found = true
					break
				}
			}
			
			if !found {
				t.Errorf("Row %d not found in forward lookup for value %d", data.RowNum, data.Value)
			}
			
			// Backward lookup: row → value
			value, found, err := cf.LookupIntByRow("id", data.RowNum)
			if err != nil {
				t.Fatalf("Failed to lookup row %d: %v", data.RowNum, err)
			}
			
			if !found {
				t.Errorf("Row %d not found in backward lookup", data.RowNum)
			}
			
			if found && value != data.Value {
				t.Errorf("Row %d: forward lookup returned %d, backward lookup returned %d", 
					data.RowNum, data.Value, value)
			}
		}
		
		// For string column (excluding nulls)
		for _, data := range stringData {
			if data.IsNull {
				continue
			}
			
			// Forward lookup: value → rows
			bitmap, err := cf.QueryString("name", data.Value)
			if err != nil {
				t.Fatalf("Failed to query string value %s: %v", data.Value, err)
			}
			
			rows := BitmapToSlice(bitmap)
			
			// Verify that our row is in the result
			found := false
			for _, row := range rows {
				if row == data.RowNum {
					found = true
					break
				}
			}
			
			if !found {
				t.Errorf("Row %d not found in forward lookup for value %s", data.RowNum, data.Value)
			}
			
			// Backward lookup: row → value
			value, found, err := cf.LookupStringByRow("name", data.RowNum)
			if err != nil {
				t.Fatalf("Failed to lookup row %d: %v", data.RowNum, err)
			}
			
			if !found {
				t.Errorf("Row %d not found in backward lookup", data.RowNum)
			}
			
			if found && value != data.Value {
				t.Errorf("Row %d: forward lookup returned %s, backward lookup returned %s", 
					data.RowNum, data.Value, value)
			}
		}
	})
	
	t.Run("GenericLookupByRow", func(t *testing.T) {
		// Test the generic LookupValueByRow method
		
		// Integer column
		value, found, err := cf.LookupValueByRow("id", 0)
		if err != nil {
			t.Fatalf("Failed to lookup row 0: %v", err)
		}
		
		if !found {
			t.Error("Row 0 not found")
		}
		
		if intValue, ok := value.(int64); !ok || intValue != 100 {
			t.Errorf("Expected int64(100), got %T(%v)", value, value)
		}
		
		// String column
		value, found, err = cf.LookupValueByRow("name", 0)
		if err != nil {
			t.Fatalf("Failed to lookup row 0: %v", err)
		}
		
		if !found {
			t.Error("Row 0 not found")
		}
		
		if strValue, ok := value.(string); !ok || strValue != "Alice" {
			t.Errorf("Expected string(Alice), got %T(%v)", value, value)
		}
		
		// NULL value
		value, found, err = cf.LookupValueByRow("name", 2)
		if err != nil {
			t.Fatalf("Failed to lookup row 2: %v", err)
		}
		
		if found {
			t.Errorf("Expected NULL (not found), got %v", value)
		}
	})
}

func TestRowKeyIndexPerformance(t *testing.T) {
	filename := "test_rowkey_performance.bytedb"
	defer os.Remove(filename)
	
	// Create file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer cf.Close()
	
	// Add column
	if err := cf.AddColumn("value", DataTypeInt64, false); err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Load large dataset
	numRows := 100000
	data := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = IntData{
			Value:  int64(i % 1000), // Create some duplicates
			RowNum: uint64(i),
		}
	}
	
	if err := cf.LoadIntColumn("value", data); err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}
	
	// Test lookup performance
	t.Run("RandomRowLookups", func(t *testing.T) {
		// Perform random lookups
		testRows := []uint64{0, 999, 50000, 99999, 12345, 67890}
		
		for _, rowNum := range testRows {
			value, found, err := cf.LookupIntByRow("value", rowNum)
			if err != nil {
				t.Errorf("Failed to lookup row %d: %v", rowNum, err)
				continue
			}
			
			if !found {
				t.Errorf("Row %d not found", rowNum)
				continue
			}
			
			expectedValue := int64(rowNum % 1000)
			if value != expectedValue {
				t.Errorf("Row %d: expected value %d, got %d", rowNum, expectedValue, value)
			}
		}
	})
}

func BenchmarkRowKeyLookup(b *testing.B) {
	filename := "bench_rowkey.bytedb"
	defer os.Remove(filename)
	
	// Setup
	cf, _ := CreateFile(filename)
	cf.AddColumn("id", DataTypeInt64, false)
	
	numRows := 10000
	data := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = IntData{Value: int64(i), RowNum: uint64(i)}
	}
	cf.LoadIntColumn("id", data)
	cf.Close()
	
	// Reopen for benchmark
	cf, _ = OpenFile(filename)
	defer cf.Close()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		rowNum := uint64(i % numRows)
		_, _, err := cf.LookupIntByRow("id", rowNum)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRowKeyIndexWithDuplicates(t *testing.T) {
	filename := "test_rowkey_duplicates.bytedb"
	defer os.Remove(filename)
	
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer cf.Close()
	
	// Add column
	if err := cf.AddColumn("category", DataTypeString, false); err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Load data with many duplicates
	stringData := []StringData{
		{Value: "A", RowNum: 0},
		{Value: "B", RowNum: 1},
		{Value: "A", RowNum: 2},
		{Value: "C", RowNum: 3},
		{Value: "B", RowNum: 4},
		{Value: "A", RowNum: 5},
		{Value: "D", RowNum: 6},
		{Value: "B", RowNum: 7},
		{Value: "A", RowNum: 8},
		{Value: "C", RowNum: 9},
	}
	
	if err := cf.LoadStringColumn("category", stringData); err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}
	
	// Verify each row's lookup
	for _, data := range stringData {
		value, found, err := cf.LookupStringByRow("category", data.RowNum)
		if err != nil {
			t.Errorf("Failed to lookup row %d: %v", data.RowNum, err)
			continue
		}
		
		if !found {
			t.Errorf("Row %d not found", data.RowNum)
			continue
		}
		
		if value != data.Value {
			t.Errorf("Row %d: expected %s, got %s", data.RowNum, data.Value, value)
		}
	}
	
	// Verify forward lookups return all rows
	expectedRows := map[string][]uint64{
		"A": {0, 2, 5, 8},
		"B": {1, 4, 7},
		"C": {3, 9},
		"D": {6},
	}
	
	for category, expectedRowList := range expectedRows {
		bitmap, err := cf.QueryString("category", category)
		if err != nil {
			t.Errorf("Failed to query category %s: %v", category, err)
			continue
		}
		
		rows := BitmapToSlice(bitmap)
		
		if len(rows) != len(expectedRowList) {
			t.Errorf("Category %s: expected %d rows, got %d", category, len(expectedRowList), len(rows))
			continue
		}
		
		// Verify all expected rows are present
		rowMap := make(map[uint64]bool)
		for _, row := range rows {
			rowMap[row] = true
		}
		
		for _, expectedRow := range expectedRowList {
			if !rowMap[expectedRow] {
				t.Errorf("Category %s: missing row %d", category, expectedRow)
			}
		}
	}
}