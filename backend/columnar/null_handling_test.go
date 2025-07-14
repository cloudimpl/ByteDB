package columnar

import (
	"os"
	"testing"
)

func TestNullHandling(t *testing.T) {
	t.Run("NullableColumnCreation", func(t *testing.T) {
		tmpFile := "test_nullable.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()
		
		// Create nullable and non-nullable columns
		if err := cf.AddColumn("nullable_int", DataTypeInt64, true); err != nil {
			t.Fatalf("Failed to add nullable column: %v", err)
		}
		
		if err := cf.AddColumn("non_nullable_int", DataTypeInt64, false); err != nil {
			t.Fatalf("Failed to add non-nullable column: %v", err)
		}
		
		// Verify metadata
		nullableCol := cf.columns["nullable_int"]
		if !nullableCol.metadata.IsNullable {
			t.Error("Nullable column not marked as nullable")
		}
		
		nonNullableCol := cf.columns["non_nullable_int"]
		if nonNullableCol.metadata.IsNullable {
			t.Error("Non-nullable column marked as nullable")
		}
	})
	
	t.Run("LoadDataWithNulls", func(t *testing.T) {
		tmpFile := "test_null_data.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()
		
		// Create nullable columns
		cf.AddColumn("id", DataTypeInt64, false)
		cf.AddColumn("value", DataTypeInt64, true)
		cf.AddColumn("name", DataTypeString, true)
		
		// Load non-nullable data first
		idData := []IntData{
			NewIntData(1, 0),
			NewIntData(2, 1),
			NewIntData(3, 2),
			NewIntData(4, 3),
			NewIntData(5, 4),
		}
		
		if err := cf.LoadIntColumn("id", idData); err != nil {
			t.Fatalf("Failed to load id column: %v", err)
		}
		
		// Load nullable integer data
		valueData := []IntData{
			NewIntData(100, 0),       // Non-null
			NewNullIntData(1),        // NULL value
			NewIntData(300, 2),       // Non-null
			NewNullIntData(3),        // NULL value
			NewIntData(500, 4),       // Non-null
		}
		
		if err := cf.LoadIntColumn("value", valueData); err != nil {
			t.Fatalf("Failed to load value column: %v", err)
		}
		
		// Load nullable string data
		nameData := []StringData{
			NewStringData("Alice", 0),   // Non-null
			NewStringData("Bob", 1),     // Non-null
			NewNullStringData(2),        // NULL value
			NewStringData("Charlie", 3), // Non-null
			NewNullStringData(4),        // NULL value
		}
		
		if err := cf.LoadStringColumn("name", nameData); err != nil {
			t.Fatalf("Failed to load name column: %v", err)
		}
		
		// Verify statistics
		valueCol := cf.columns["value"]
		if valueCol.metadata.NullCount != 2 {
			t.Errorf("Expected NullCount=2 for value column, got %d", valueCol.metadata.NullCount)
		}
		
		nameCol := cf.columns["name"]
		if nameCol.metadata.NullCount != 2 {
			t.Errorf("Expected NullCount=2 for name column, got %d", nameCol.metadata.NullCount)
		}
	})
	
	t.Run("QueryNullValues", func(t *testing.T) {
		tmpFile := "test_null_queries.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()
		
		// Create nullable column
		cf.AddColumn("value", DataTypeInt64, true)
		
		// Load data with nulls
		data := []IntData{
			NewIntData(100, 0),       // Non-null
			NewNullIntData(1),        // NULL
			NewIntData(200, 2),       // Non-null
			NewNullIntData(3),        // NULL
			NewIntData(300, 4),       // Non-null
		}
		
		if err := cf.LoadIntColumn("value", data); err != nil {
			t.Fatalf("Failed to load data: %v", err)
		}
		
		// Test QueryNull
		nullRows, err := cf.QueryNull("value")
		if err != nil {
			t.Fatalf("QueryNull failed: %v", err)
		}
		
		expectedNulls := []uint32{1, 3}
		actualNulls := nullRows.ToArray()
		if len(actualNulls) != len(expectedNulls) {
			t.Errorf("QueryNull: expected %v, got %v", expectedNulls, actualNulls)
		}
		for i, expected := range expectedNulls {
			if i >= len(actualNulls) || actualNulls[i] != expected {
				t.Errorf("QueryNull: expected %v, got %v", expectedNulls, actualNulls)
				break
			}
		}
		
		// Test QueryNotNull
		notNullRows, err := cf.QueryNotNull("value")
		if err != nil {
			t.Fatalf("QueryNotNull failed: %v", err)
		}
		
		expectedNotNulls := []uint32{0, 2, 4}
		actualNotNulls := notNullRows.ToArray()
		if len(actualNotNulls) != len(expectedNotNulls) {
			t.Errorf("QueryNotNull: expected %v, got %v", expectedNotNulls, actualNotNulls)
		}
		for i, expected := range expectedNotNulls {
			if i >= len(actualNotNulls) || actualNotNulls[i] != expected {
				t.Errorf("QueryNotNull: expected %v, got %v", expectedNotNulls, actualNotNulls)
				break
			}
		}
		
		// Test regular queries exclude nulls
		result, err := cf.QueryInt("value", 100)
		if err != nil {
			t.Fatalf("QueryInt failed: %v", err)
		}
		
		expectedResults := []uint32{0}
		actualResults := result.ToArray()
		if len(actualResults) != len(expectedResults) {
			t.Errorf("QueryInt: expected %v, got %v", expectedResults, actualResults)
		}
	})
	
	t.Run("NullStatistics", func(t *testing.T) {
		tmpFile := "test_null_stats.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		
		cf.AddColumn("nullable_col", DataTypeInt64, true)
		
		// Load data without nulls for now
		data := make([]IntData, 100)
		for i := 0; i < 100; i++ {
			data[i] = NewIntData(int64(i), uint64(i))
		}
		
		cf.LoadIntColumn("nullable_col", data)
		
		// Check null count
		col := cf.columns["nullable_col"]
		if col.metadata.NullCount != 0 {
			t.Errorf("Expected NullCount=0, got %d", col.metadata.NullCount)
		}
		
		cf.Close()
	})
	
	t.Run("NullBitmapStorage", func(t *testing.T) {
		t.Skip("Null handling not yet implemented")
		
		// Expected implementation:
		// - Each nullable column should have a null bitmap
		// - Null bitmap tracks which rows have NULL values
		// - Should be stored alongside the B+ tree
		// - Should be loaded/saved with column metadata
	})
	
	t.Run("RangeQueriesWithNulls", func(t *testing.T) {
		tmpFile := "test_null_ranges.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()
		
		// Create nullable column
		cf.AddColumn("value", DataTypeInt64, true)
		
		// Load data: 100, NULL, 200, NULL, 300
		data := []IntData{
			NewIntData(100, 0),
			NewNullIntData(1),
			NewIntData(200, 2),
			NewNullIntData(3),
			NewIntData(300, 4),
		}
		
		if err := cf.LoadIntColumn("value", data); err != nil {
			t.Fatalf("Failed to load data: %v", err)
		}
		
		// Test range query excluding nulls
		result, err := cf.RangeQueryIntWithNulls("value", 150, 250, NullsFirst, false)
		if err != nil {
			t.Fatalf("Range query failed: %v", err)
		}
		
		expectedResults := []uint32{2} // Only row 2 (value=200)
		actualResults := result.ToArray()
		if len(actualResults) != len(expectedResults) || actualResults[0] != expectedResults[0] {
			t.Errorf("Range query (exclude nulls): expected %v, got %v", expectedResults, actualResults)
		}
		
		// Test range query including nulls (NullsFirst)
		result, err = cf.RangeQueryIntWithNulls("value", 150, 250, NullsFirst, true)
		if err != nil {
			t.Fatalf("Range query with nulls failed: %v", err)
		}
		
		expectedResults = []uint32{1, 2, 3} // Nulls + row 2
		actualResults = result.ToArray()
		if len(actualResults) != len(expectedResults) {
			t.Errorf("Range query (include nulls): expected %v, got %v", expectedResults, actualResults)
		}
		
		// Test < query with NullsFirst
		result, err = cf.QueryLessThanWithNulls("value", int64(200), NullsFirst)
		if err != nil {
			t.Fatalf("Less than query failed: %v", err)
		}
		
		expectedResults = []uint32{0, 1, 3} // 100 + nulls (nulls first)
		actualResults = result.ToArray()
		if len(actualResults) != len(expectedResults) {
			t.Errorf("Less than (NullsFirst): expected %v, got %v", expectedResults, actualResults)
		}
		
		// Test > query with NullsLast
		result, err = cf.QueryGreaterThanWithNulls("value", int64(200), NullsLast)
		if err != nil {
			t.Fatalf("Greater than query failed: %v", err)
		}
		
		expectedResults = []uint32{1, 3, 4} // 300 + nulls (nulls last)
		actualResults = result.ToArray()
		if len(actualResults) != len(expectedResults) {
			t.Errorf("Greater than (NullsLast): expected %v, got %v", expectedResults, actualResults)
		}
	})
	
	t.Run("MixedNullAndNonNullQueries", func(t *testing.T) {
		tmpFile := "test_mixed_nulls.bytedb"
		defer os.Remove(tmpFile)
		
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()
		
		// Create two nullable columns
		cf.AddColumn("col1", DataTypeInt64, true)
		cf.AddColumn("col2", DataTypeInt64, true)
		
		// Load data:
		// Row 0: col1=100, col2=200
		// Row 1: col1=NULL, col2=300
		// Row 2: col1=400, col2=NULL
		// Row 3: col1=NULL, col2=NULL
		
		col1Data := []IntData{
			NewIntData(100, 0),
			NewNullIntData(1),
			NewIntData(400, 2),
			NewNullIntData(3),
		}
		
		col2Data := []IntData{
			NewIntData(200, 0),
			NewIntData(300, 1),
			NewNullIntData(2),
			NewNullIntData(3),
		}
		
		if err := cf.LoadIntColumn("col1", col1Data); err != nil {
			t.Fatalf("Failed to load col1: %v", err)
		}
		
		if err := cf.LoadIntColumn("col2", col2Data); err != nil {
			t.Fatalf("Failed to load col2: %v", err)
		}
		
		// Test AND with mixed nulls
		col1NotNull, _ := cf.QueryNotNull("col1")
		col2NotNull, _ := cf.QueryNotNull("col2")
		andResult := cf.QueryAnd(col1NotNull, col2NotNull)
		
		expectedAnd := []uint32{0} // Only row 0 has both non-null
		actualAnd := andResult.ToArray()
		if len(actualAnd) != len(expectedAnd) || actualAnd[0] != expectedAnd[0] {
			t.Errorf("AND with nulls: expected %v, got %v", expectedAnd, actualAnd)
		}
		
		// Test OR with mixed nulls
		orResult := cf.QueryOr(col1NotNull, col2NotNull)
		
		expectedOr := []uint32{0, 1, 2} // Rows with at least one non-null
		actualOr := orResult.ToArray()
		if len(actualOr) != len(expectedOr) {
			t.Errorf("OR with nulls: expected %v, got %v", expectedOr, actualOr)
		}
	})
}

// Helper functions removed - no longer needed with IsNull API

// TestNullHandlingDesign documents the expected null handling design
func TestNullHandlingDesign(t *testing.T) {
	t.Log("NULL Handling Design Requirements:")
	t.Log("1. Storage:")
	t.Log("   - Each nullable column needs a null bitmap")
	t.Log("   - Null bitmap stored as a roaring bitmap like other data")
	t.Log("   - NULL values don't need entries in the B+ tree")
	t.Log("")
	t.Log("2. Loading Data:")
	t.Log("   - LoadIntColumn needs to accept nullable data (using pointers)")
	t.Log("   - Track NULL rows in the null bitmap")
	t.Log("   - Update NullCount in statistics")
	t.Log("")
	t.Log("3. Querying:")
	t.Log("   - QueryNull(column) returns rows where column IS NULL")
	t.Log("   - QueryNotNull(column) returns rows where column IS NOT NULL")
	t.Log("   - Regular queries automatically exclude NULL rows")
	t.Log("   - Range queries exclude NULL rows")
	t.Log("")
	t.Log("4. Boolean Operations:")
	t.Log("   - AND: NULL in any column makes the row excluded")
	t.Log("   - OR: NULL values are treated as not matching")
	t.Log("   - NOT: NULL values remain NULL")
	t.Log("")
	t.Log("5. Statistics:")
	t.Log("   - NullCount tracks number of NULL values")
	t.Log("   - DistinctCount doesn't include NULL as a distinct value")
	t.Log("   - Min/Max calculations ignore NULL values")
}

// TestCurrentNullSupport verifies current (limited) null support
func TestCurrentNullSupport(t *testing.T) {
	tmpFile := "test_current_null.bytedb"
	defer os.Remove(tmpFile)
	
	cf, err := CreateFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer cf.Close()
	
	// Currently, we can mark columns as nullable but can't actually store nulls
	cf.AddColumn("nullable_column", DataTypeInt64, true)
	cf.AddColumn("non_nullable_column", DataTypeInt64, false)
	
	// Verify the nullable flag is stored
	nullableCol := cf.columns["nullable_column"]
	nonNullableCol := cf.columns["non_nullable_column"]
	
	if !nullableCol.metadata.IsNullable {
		t.Error("Nullable column should have IsNullable=true")
	}
	
	if nonNullableCol.metadata.IsNullable {
		t.Error("Non-nullable column should have IsNullable=false")
	}
	
	// Currently, we can only load non-null data
	data := []IntData{
		NewIntData(1, 0),
		NewIntData(2, 1),
		NewIntData(3, 2),
	}
	
	if err := cf.LoadIntColumn("nullable_column", data); err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}
	
	// Null count should be 0 since we didn't load any nulls
	if nullableCol.metadata.NullCount != 0 {
		t.Errorf("Expected NullCount=0, got %d", nullableCol.metadata.NullCount)
	}
}