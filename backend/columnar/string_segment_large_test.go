package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestStringSegmentLargeCapacity tests that string segment can handle many unique strings
func TestStringSegmentLargeCapacity(t *testing.T) {
	filename := "test_string_segment_large.bytedb"
	defer os.Remove(filename)

	// Create file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	// Add columns
	cf.AddColumn("id", DataTypeInt64, false)
	cf.AddColumn("name", DataTypeString, false)

	// Test with 5000 unique strings
	numRows := 5000
	t.Logf("Testing with %d unique strings", numRows)

	intData := make([]IntData, numRows)
	stringData := make([]StringData, numRows)

	for i := 0; i < numRows; i++ {
		intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
		// Create unique string for each row
		stringData[i] = StringData{Value: fmt.Sprintf("UniqueString_%05d", i), RowNum: uint64(i)}
	}

	// Load data
	if err := cf.LoadIntColumn("id", intData); err != nil {
		t.Fatal(err)
	}
	if err := cf.LoadStringColumn("name", stringData); err != nil {
		t.Fatal(err)
	}

	// Get stats
	pageCount := cf.pageManager.GetPageCount()
	t.Logf("Total pages used: %d", pageCount)

	cf.Close()

	// Reopen and verify data
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()

	// Test queries at various points
	testPoints := []int{0, 100, 250, 251, 500, 1000, 2500, 4999}

	for _, point := range testPoints {
		t.Run(fmt.Sprintf("Point_%d", point), func(t *testing.T) {
			// Test forward lookup
			expectedString := fmt.Sprintf("UniqueString_%05d", point)
			bitmap, err := cf2.QueryString("name", expectedString)
			if err != nil {
				t.Errorf("Failed to query string at point %d: %v", point, err)
				return
			}
			if bitmap.GetCardinality() != 1 {
				t.Errorf("Expected 1 result for string at point %d, got %d", point, bitmap.GetCardinality())
			}

			// Test reverse lookup (row to string)
			val, found, err := cf2.LookupStringByRow("name", uint64(point))
			if err != nil {
				t.Errorf("Failed to lookup row %d: %v", point, err)
				return
			}
			if !found {
				t.Errorf("Row %d not found", point)
				return
			}
			if val != expectedString {
				t.Errorf("Row %d: expected %s, got %s", point, expectedString, val)
			}
		})
	}

	// Test range query
	t.Run("RangeQuery", func(t *testing.T) {
		// Query for strings in a range
		bitmap, err := cf2.RangeQueryString("name", "UniqueString_01000", "UniqueString_01100")
		if err != nil {
			t.Fatal(err)
		}
		// Should get 101 results (1000-1100 inclusive)
		if bitmap.GetCardinality() != 101 {
			t.Errorf("Expected 101 results, got %d", bitmap.GetCardinality())
		}
	})
}

// TestStringSegmentMultipleDirectoryPages specifically tests the directory page overflow
func TestStringSegmentMultipleDirectoryPages(t *testing.T) {
	filename := "test_string_directory_pages.bytedb"
	defer os.Remove(filename)

	// Calculate how many entries fit per directory page
	entriesPerPage := (PageSize - PageHeaderSize - 8) / 16
	t.Logf("Entries per directory page: %d", entriesPerPage)

	// Test with enough strings to require multiple directory pages
	numStrings := entriesPerPage*3 + 50 // Three full pages plus some extra
	t.Logf("Testing with %d unique strings (requires %d directory pages)", numStrings, (numStrings+entriesPerPage-1)/entriesPerPage)

	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	cf.AddColumn("str", DataTypeString, false)

	stringData := make([]StringData, numStrings)
	for i := 0; i < numStrings; i++ {
		stringData[i] = StringData{
			Value:  fmt.Sprintf("TestString_%06d", i),
			RowNum: uint64(i),
		}
	}

	if err := cf.LoadStringColumn("str", stringData); err != nil {
		t.Fatal(err)
	}

	cf.Close()

	// Reopen and verify
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()

	// Test strings from each directory page
	testIndices := []int{
		0,                    // First entry
		entriesPerPage - 1,   // Last entry of first page
		entriesPerPage,       // First entry of second page
		entriesPerPage*2 - 1, // Last entry of second page
		entriesPerPage * 2,   // First entry of third page
		numStrings - 1,       // Last entry
	}

	for _, idx := range testIndices {
		expectedString := fmt.Sprintf("TestString_%06d", idx)
		
		// Test forward lookup
		bitmap, err := cf2.QueryString("str", expectedString)
		if err != nil {
			t.Errorf("Failed to query string at index %d: %v", idx, err)
			continue
		}
		if bitmap.GetCardinality() != 1 {
			t.Errorf("Index %d: expected 1 result, got %d", idx, bitmap.GetCardinality())
		}

		// Test reverse lookup
		val, found, err := cf2.LookupStringByRow("str", uint64(idx))
		if err != nil {
			t.Errorf("Failed to lookup row %d: %v", idx, err)
			continue
		}
		if !found {
			t.Errorf("Row %d not found", idx)
			continue
		}
		if val != expectedString {
			t.Errorf("Row %d: expected %s, got %s", idx, expectedString, val)
		}
	}

	t.Log("Multiple directory pages test passed!")
}