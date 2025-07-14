package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// Note: Correct typed reader interface names:
// - BooleanReader with ReadBooleans([]bool) method
// - Int32Reader with ReadInt32s([]int32) method
// - Int64Reader with ReadInt64s([]int64) method
// - FloatReader with ReadFloats([]float32) method
// - DoubleReader with ReadDoubles([]float64) method
// - ByteArrayReader with ReadByteArrays([]byte) method

// TestRow represents our test data structure
type TestRow2 struct {
	ID       int64   `parquet:"id"`
	Name     string  `parquet:"name"`
	Age      int32   `parquet:"age"`
	Salary   float64 `parquet:"salary"`
	Active   bool    `parquet:"active"`
	Optional *string `parquet:"optional,optional"`
}

// TestColumnSpecificReading demonstrates various ways to read specific columns
func TestColumnSpecificReading(t *testing.T) {
	// Create test data
	testData := []TestRow2{
		{ID: 1, Name: "Alice", Age: 30, Salary: 75000.50, Active: true, Optional: stringPtr("Manager")},
		{ID: 2, Name: "Bob", Age: 25, Salary: 60000.00, Active: true, Optional: nil},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 85000.75, Active: false, Optional: stringPtr("Senior")},
		{ID: 4, Name: "Diana", Age: 28, Salary: 70000.25, Active: true, Optional: stringPtr("Lead")},
		{ID: 5, Name: "Eve", Age: 32, Salary: 78000.00, Active: false, Optional: nil},
	}

	// Write test data to parquet buffer
	parquetData := createTestParquetFile(t, testData)

	// Open the parquet file
	reader := bytes.NewReader(parquetData)
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatalf("Failed to open parquet file: %v", err)
	}

	t.Run("ReadSpecificColumnByIndex", func(t *testing.T) {
		testReadColumnByIndex(t, file)
	})

	t.Run("ReadSpecificColumnByName", func(t *testing.T) {
		testReadColumnByName(t, file)
	})

	t.Run("ReadMultipleSpecificColumns", func(t *testing.T) {
		testReadMultipleColumns(t, file)
	})

	t.Run("ReadColumnWithBloomFilter", func(t *testing.T) {
		testReadColumnWithBloomFilter(t, file)
	})

	t.Run("ReadColumnUsingValueReader", func(t *testing.T) {
		testReadColumnUsingValueReader(t, file)
	})
}

// testReadColumnByIndex demonstrates reading a specific column by its index
func testReadColumnByIndex(t *testing.T, file *parquet.File) {
	fmt.Println("\n=== Reading Column by Index (Column 1: Name) ===")

	for rowGroupIdx, rowGroup := range file.RowGroups() {
		fmt.Printf("Row Group %d:\n", rowGroupIdx)

		// Access column by index (1 = Name column)
		nameColumn := rowGroup.ColumnChunks()[1]

		fmt.Printf("  Column Type: %v\n", nameColumn.Type())
		fmt.Printf("  Column Index: %d\n", nameColumn.Column())
		fmt.Printf("  Num Values: %d\n", nameColumn.NumValues())

		// Read all pages from this column
		pages := nameColumn.Pages()
		defer pages.Close()

		pageNum := 0
		for {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Error reading page: %v", err)
			}

			fmt.Printf("  Page %d: %d values, %d rows\n", pageNum, page.NumValues(), page.NumRows())

			// Read values from the page
			values := make([]parquet.Value, page.NumValues())
			valueReader := page.Values()
			n, err := valueReader.ReadValues(values)
			if err != nil && err != io.EOF {
				t.Fatalf("Error reading values: %v", err)
			}

			fmt.Printf("    Values read: %d\n", n)
			for i, val := range values[:n] {
				fmt.Printf("      [%d] %v (column: %d, null: %v)\n",
					i, val.String(), val.Column(), val.IsNull())
			}

			parquet.Release(page)
			pageNum++
		}
	}
}

// testReadColumnByName demonstrates reading a specific column by name
func testReadColumnByName(t *testing.T, file *parquet.File) {
	fmt.Println("\n=== Reading Column by Name (salary) ===")

	// Access column by name
	salaryColumn := file.Root().Column("salary")
	if salaryColumn == nil {
		t.Fatal("Could not find 'salary' column")
	}

	fmt.Printf("Column Name: %s\n", salaryColumn.Name())
	fmt.Printf("Column Type: %v\n", salaryColumn.Type())
	fmt.Printf("Column Index: %d\n", salaryColumn.Index())

	// Read pages from this specific column
	pages := salaryColumn.Pages()
	defer pages.Close()

	allValues := []float64{}
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading page: %v", err)
		}

		// Try to read as typed float64 values for better performance
		switch pageValues := page.Values().(type) {
		case parquet.DoubleReader:
			values := make([]float64, page.NumValues())
			n, err := pageValues.ReadDoubles(values)
			if err != nil && err != io.EOF {
				t.Fatalf("Error reading float64 values: %v", err)
			}
			allValues = append(allValues, values[:n]...)
			fmt.Printf("Read %d float64 values directly\n", n)
		default:
			// Fallback to generic value reading
			values := make([]parquet.Value, page.NumValues())
			n, err := pageValues.ReadValues(values)
			if err != nil && err != io.EOF {
				t.Fatalf("Error reading values: %v", err)
			}
			for _, val := range values[:n] {
				if !val.IsNull() {
					allValues = append(allValues, val.Double())
				}
			}
			fmt.Printf("Read %d values using generic reader\n", n)
		}

		parquet.Release(page)
	}

	fmt.Printf("All salary values: %v\n", allValues)
}

// testReadMultipleColumns demonstrates reading multiple specific columns
func testReadMultipleColumns(t *testing.T, file *parquet.File) {
	fmt.Println("\n=== Reading Multiple Specific Columns (ID and Age) ===")

	for rowGroupIdx, rowGroup := range file.RowGroups() {
		fmt.Printf("Row Group %d:\n", rowGroupIdx)

		// Read ID column (index 0) and Age column (index 2)
		targetColumns := []int{0, 2} // ID and Age
		columnNames := []string{"ID", "Age"}

		for i, colIdx := range targetColumns {
			columnChunk := rowGroup.ColumnChunks()[colIdx]
			fmt.Printf("  Reading %s column (index %d):\n", columnNames[i], colIdx)

			pages := columnChunk.Pages()
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error reading page: %v", err)
				}

				// Read based on column type
				switch colIdx {
				case 0: // ID column (int64)
					if int64Reader, ok := page.Values().(parquet.Int64Reader); ok {
						values := make([]int64, page.NumValues())
						n, _ := int64Reader.ReadInt64s(values)
						fmt.Printf("    ID values: %v\n", values[:n])
					}
				case 2: // Age column (int32)
					if int32Reader, ok := page.Values().(parquet.Int32Reader); ok {
						values := make([]int32, page.NumValues())
						n, _ := int32Reader.ReadInt32s(values)
						fmt.Printf("    Age values: %v\n", values[:n])
					}
				}

				parquet.Release(page)
			}
			pages.Close()
		}
	}
}

// testReadColumnWithBloomFilter demonstrates using bloom filters for efficient column access
func testReadColumnWithBloomFilter(t *testing.T, file *parquet.File) {
	fmt.Println("\n=== Reading Column with Bloom Filter Check ===")

	searchValue := parquet.ValueOf("Alice")

	for rowGroupIdx, rowGroup := range file.RowGroups() {
		fmt.Printf("Row Group %d:\n", rowGroupIdx)

		// Check name column (index 1) with bloom filter
		nameColumn := rowGroup.ColumnChunks()[1]
		bloomFilter := nameColumn.BloomFilter()

		if bloomFilter != nil {
			fmt.Printf("  Bloom filter available\n")

			// Check if the value might exist in this column chunk
			exists, err := bloomFilter.Check(searchValue)
			if err != nil {
				t.Fatalf("Error checking bloom filter: %v", err)
			}

			if exists {
				fmt.Printf("  Bloom filter indicates 'Alice' might exist - reading column\n")
				readColumnValues(t, nameColumn)
			} else {
				fmt.Printf("  Bloom filter indicates 'Alice' definitely does not exist - skipping\n")
			}
		} else {
			fmt.Printf("  No bloom filter available - reading column anyway\n")
			readColumnValues(t, nameColumn)
		}
	}
}

// testReadColumnUsingValueReader demonstrates using ColumnChunkValueReader
func testReadColumnUsingValueReader(t *testing.T, file *parquet.File) {
	fmt.Println("\n=== Reading Column using ColumnChunkValueReader ===")

	for rowGroupIdx, rowGroup := range file.RowGroups() {
		fmt.Printf("Row Group %d:\n", rowGroupIdx)

		// Use ColumnChunkValueReader for the boolean 'active' column (index 4)
		activeColumn := rowGroup.ColumnChunks()[4]
		valueReader := parquet.NewColumnChunkValueReader(activeColumn)
		defer valueReader.Close()

		fmt.Printf("  Reading 'active' column using ColumnChunkValueReader\n")

		buffer := make([]parquet.Value, 10)
		for {
			n, err := valueReader.ReadValues(buffer)
			if n == 0 && err == io.EOF {
				break
			}
			if err != nil && err != io.EOF {
				t.Fatalf("Error reading values: %v", err)
			}

			fmt.Printf("  Read %d values: ", n)
			for i := 0; i < n; i++ {
				fmt.Printf("%v ", buffer[i].Boolean())
			}
			fmt.Println()

			if err == io.EOF {
				break
			}
		}
	}
}

// Helper function to read and display column values
func readColumnValues(t *testing.T, columnChunk parquet.ColumnChunk) {
	pages := columnChunk.Pages()
	defer pages.Close()

	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading page: %v", err)
		}

		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			t.Fatalf("Error reading values: %v", err)
		}

		fmt.Printf("    Values: ")
		for i := 0; i < n; i++ {
			fmt.Printf("%v ", values[i].String())
		}
		fmt.Println()

		parquet.Release(page)
	}
}

// Helper function to create test parquet file
func createTestParquetFile(t *testing.T, data []TestRow2) []byte {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[TestRow2](&buf)

	_, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	return buf.Bytes()
}

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}

//// Example of how to run this test
//func main() {
//	// Create a test instance
//	t := &testing.T{}
//
//	// Run the test
//	TestColumnSpecificReading(t)
//
//	if t.Failed() {
//		log.Fatal("Test failed")
//	} else {
//		fmt.Println("\n=== All tests passed! ===")
//	}
//}
