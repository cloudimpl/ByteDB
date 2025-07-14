package main

import (
	"bytes"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"strings"
)

// TestRow represents our test data structure
type TestRow struct {
	ID       int64   `parquet:"id"`
	Name     string  `parquet:"name"`
	Score    float64 `parquet:"score"`
	Active   bool    `parquet:"active"`
	Category *string `parquet:"category,optional"` // Optional field to test nulls
}

func main() {
	ExampleReadColumnStatistics()
}

// Example function demonstrating the original code
func ExampleReadColumnStatistics() {
	// This would be your actual file reader and size
	// reader := ...
	// size := ...

	// For demo, we'll create a simple in-memory example
	testData := []TestRow{
		{ID: 1, Name: "Alice", Score: 85.5, Active: true},
		{ID: 2, Name: "Bob", Score: 92.3, Active: false},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[TestRow](&buf)
	writer.Write(testData)
	writer.Close()

	reader := bytes.NewReader(buf.Bytes())
	size := int64(buf.Len())

	// Open a parquet file
	file, err := parquet.OpenFile(reader, size)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	//defer file.Close()

	// Access row groups and column statistics
	for _, rowGroup := range file.RowGroups() {
		for idx, columnChunk := range rowGroup.ColumnChunks() {
			// Get the column metadata which contains statistics
			fileColumnChunk := columnChunk.(*parquet.FileColumnChunk)
			schema := file.Schema()
			columnName := getColumnName(schema, idx)
			println("Column Name:", columnName)
			min, max, hasMinMax := fileColumnChunk.Bounds()
			if hasMinMax {
				fmt.Println("kind:", min.Kind())
				fmt.Println("Min value:", min.String())
				fmt.Println("Max value:", max.String())
			}

			fmt.Println("Null count: ", fileColumnChunk.NullCount())

		}
	}
}

// Helper function to get column name from schema (improved version)
func getColumnName(schema *parquet.Schema, columnIndex int) string {
	columns := schema.Columns()
	if columnIndex < len(columns) {
		// Join the path with dots for nested columns (e.g., "address.street")
		return strings.Join(columns[columnIndex], ".")
	}
	return fmt.Sprintf("column_%d", columnIndex)
}
