package columnar

import (
	"os"
	"strings"
	"testing"
)

// TestNullableValidation tests that null values are only accepted in nullable columns
func TestNullableValidation(t *testing.T) {
	tmpFile := "test_nullable_validation.bytedb"
	defer os.Remove(tmpFile)

	t.Run("NonNullableIntColumn_RejectsNulls", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create non-nullable integer column
		if err := cf.AddColumn("id", DataTypeInt64, false); err != nil {
			t.Fatalf("Failed to add non-nullable column: %v", err)
		}

		// Try to load data with null values into non-nullable column
		data := []IntData{
			NewIntData(1, 0),     // Valid non-null
			NewIntData(2, 1),     // Valid non-null
			NewNullIntData(2),    // Invalid null for non-nullable column
			NewIntData(3, 3),     // Valid non-null
		}

		err = cf.LoadIntColumn("id", data)
		if err == nil {
			t.Error("Expected error when loading null values into non-nullable column, but got none")
		}

		if !strings.Contains(err.Error(), "non-nullable") {
			t.Errorf("Expected error message to mention 'non-nullable', got: %v", err)
		}

		if !strings.Contains(err.Error(), "row 2") {
			t.Errorf("Expected error message to mention 'row 2', got: %v", err)
		}
	})

	t.Run("NonNullableStringColumn_RejectsNulls", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create non-nullable string column
		if err := cf.AddColumn("name", DataTypeString, false); err != nil {
			t.Fatalf("Failed to add non-nullable column: %v", err)
		}

		// Try to load data with null values into non-nullable column
		data := []StringData{
			NewStringData("Alice", 0),  // Valid non-null
			NewNullStringData(1),       // Invalid null for non-nullable column
			NewStringData("Bob", 2),    // Valid non-null
		}

		err = cf.LoadStringColumn("name", data)
		if err == nil {
			t.Error("Expected error when loading null values into non-nullable column, but got none")
		}

		if !strings.Contains(err.Error(), "non-nullable") {
			t.Errorf("Expected error message to mention 'non-nullable', got: %v", err)
		}

		if !strings.Contains(err.Error(), "row 1") {
			t.Errorf("Expected error message to mention 'row 1', got: %v", err)
		}
	})

	t.Run("NullableIntColumn_AcceptsNulls", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create nullable integer column
		if err := cf.AddColumn("score", DataTypeInt64, true); err != nil {
			t.Fatalf("Failed to add nullable column: %v", err)
		}

		// Load data with null values into nullable column - should succeed
		data := []IntData{
			NewIntData(100, 0),   // Valid non-null
			NewNullIntData(1),    // Valid null for nullable column
			NewIntData(200, 2),   // Valid non-null
			NewNullIntData(3),    // Valid null for nullable column
		}

		err = cf.LoadIntColumn("score", data)
		if err != nil {
			t.Errorf("Expected success when loading null values into nullable column, got error: %v", err)
		}

		// Verify null count
		col := cf.columns["score"]
		if col.metadata.NullCount != 2 {
			t.Errorf("Expected NullCount=2, got %d", col.metadata.NullCount)
		}

		// Verify we can query for nulls
		nullRows, err := cf.QueryNull("score")
		if err != nil {
			t.Errorf("QueryNull failed: %v", err)
		}

		if nullRows.GetCardinality() != 2 {
			t.Errorf("Expected 2 null rows, got %d", nullRows.GetCardinality())
		}
	})

	t.Run("NullableStringColumn_AcceptsNulls", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create nullable string column
		if err := cf.AddColumn("description", DataTypeString, true); err != nil {
			t.Fatalf("Failed to add nullable column: %v", err)
		}

		// Load data with null values into nullable column - should succeed
		data := []StringData{
			NewStringData("Good", 0),   // Valid non-null
			NewNullStringData(1),       // Valid null for nullable column
			NewStringData("Bad", 2),    // Valid non-null
		}

		err = cf.LoadStringColumn("description", data)
		if err != nil {
			t.Errorf("Expected success when loading null values into nullable column, got error: %v", err)
		}

		// Verify null count
		col := cf.columns["description"]
		if col.metadata.NullCount != 1 {
			t.Errorf("Expected NullCount=1, got %d", col.metadata.NullCount)
		}
	})

	t.Run("NonNullableColumn_AcceptsOnlyNonNulls", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create non-nullable column
		if err := cf.AddColumn("required_field", DataTypeInt64, false); err != nil {
			t.Fatalf("Failed to add non-nullable column: %v", err)
		}

		// Load only non-null data - should succeed
		data := []IntData{
			NewIntData(10, 0),
			NewIntData(20, 1),
			NewIntData(30, 2),
		}

		err = cf.LoadIntColumn("required_field", data)
		if err != nil {
			t.Errorf("Expected success when loading only non-null values into non-nullable column, got error: %v", err)
		}

		// Verify no nulls tracked
		col := cf.columns["required_field"]
		if col.metadata.NullCount != 0 {
			t.Errorf("Expected NullCount=0 for non-nullable column, got %d", col.metadata.NullCount)
		}

		// Verify QueryNull returns empty result
		nullRows, err := cf.QueryNull("required_field")
		if err != nil {
			t.Errorf("QueryNull failed: %v", err)
		}

		if nullRows.GetCardinality() != 0 {
			t.Errorf("Expected 0 null rows for non-nullable column, got %d", nullRows.GetCardinality())
		}
	})

	t.Run("MultipleNullsInNonNullableColumn_ReportsFirstError", func(t *testing.T) {
		cf, err := CreateFile(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer cf.Close()

		// Create non-nullable column
		if err := cf.AddColumn("test_col", DataTypeInt64, false); err != nil {
			t.Fatalf("Failed to add non-nullable column: %v", err)
		}

		// Try to load data with multiple null values
		data := []IntData{
			NewIntData(1, 0),     // Valid
			NewNullIntData(1),    // First null - should be reported
			NewIntData(2, 2),     // Valid
			NewNullIntData(3),    // Second null
			NewIntData(3, 4),     // Valid
		}

		err = cf.LoadIntColumn("test_col", data)
		if err == nil {
			t.Error("Expected error when loading null values into non-nullable column")
		}

		// Should report the first null encountered (row 1)
		if !strings.Contains(err.Error(), "row 1") {
			t.Errorf("Expected error to mention first null at row 1, got: %v", err)
		}
	})
}