package main

import (
	"fmt"
	"os"
	
	"bytedb/columnar"
)

func main() {
	// This example demonstrates the memory-efficient streaming merge
	// that can handle files larger than available memory
	
	fmt.Println("Creating test files...")
	
	// Create first file
	file1 := "customers_2023.bytedb"
	cf1, _ := columnar.CreateFile(file1)
	cf1.AddColumn("customer_id", columnar.DataTypeInt64, false)
	cf1.AddColumn("name", columnar.DataTypeString, false)
	cf1.AddColumn("revenue", columnar.DataTypeInt64, true)
	
	// Add data with global row numbers
	cf1.LoadIntColumn("customer_id", []columnar.IntData{
		columnar.NewIntData(1001, 0),
		columnar.NewIntData(1002, 1),
		columnar.NewIntData(1003, 2),
	})
	cf1.LoadStringColumn("name", []columnar.StringData{
		columnar.NewStringData("Alice Corp", 0),
		columnar.NewStringData("Bob Inc", 1),
		columnar.NewStringData("Charlie LLC", 2),
	})
	cf1.LoadIntColumn("revenue", []columnar.IntData{
		columnar.NewIntData(50000, 0),
		columnar.NewNullIntData(1),
		columnar.NewIntData(75000, 2),
	})
	cf1.Close()
	
	// Create second file
	file2 := "customers_2024.bytedb"
	cf2, _ := columnar.CreateFile(file2)
	cf2.AddColumn("customer_id", columnar.DataTypeInt64, false)
	cf2.AddColumn("name", columnar.DataTypeString, false)
	cf2.AddColumn("revenue", columnar.DataTypeInt64, true)
	
	// Add data with different global row numbers
	cf2.LoadIntColumn("customer_id", []columnar.IntData{
		columnar.NewIntData(1002, 100), // Same customer, updated
		columnar.NewIntData(1004, 101),
		columnar.NewIntData(1005, 102),
	})
	cf2.LoadStringColumn("name", []columnar.StringData{
		columnar.NewStringData("Bob Inc (Updated)", 100),
		columnar.NewStringData("David Co", 101),
		columnar.NewStringData("Eve Systems", 102),
	})
	cf2.LoadIntColumn("revenue", []columnar.IntData{
		columnar.NewIntData(60000, 100),
		columnar.NewIntData(45000, 101),
		columnar.NewIntData(90000, 102),
	})
	
	// Mark a customer as deleted
	cf2.DeleteRow(0) // Delete Alice Corp
	cf2.Close()
	
	// Perform streaming merge
	fmt.Println("\nPerforming streaming merge...")
	outputFile := "customers_merged.bytedb"
	
	options := &columnar.MergeOptions{
		// Deleted rows are always excluded during merge
		DeduplicateKeys:   true,
		UseStreamingMerge: true, // Enable memory-efficient streaming
	}
	
	err := columnar.MergeFiles(outputFile, []string{file1, file2}, options)
	if err != nil {
		panic(err)
	}
	
	// Read and display merged results
	fmt.Println("\nMerged customer data:")
	merged, _ := columnar.OpenFile(outputFile)
	defer merged.Close()
	
	// Create iterator to read all customers
	iter, _ := merged.NewIterator("customer_id")
	defer iter.Close()
	
	fmt.Println("ID\tName\t\t\tRevenue")
	fmt.Println("--\t----\t\t\t-------")
	
	for iter.Next() {
		customerID := iter.Key().(int64)
		rows := iter.Rows()
		
		// Get the first (and only due to deduplication) row
		rowIter := rows.Iterator()
		if rowIter.HasNext() {
			rowNum := uint64(rowIter.Next())
			
			// Look up name (would need LookupByRow implementation)
			// For now, just show the customer ID and row number
			fmt.Printf("%d\t(row %d)\n", customerID, rowNum)
		}
	}
	
	// Show statistics
	fmt.Println("\nMerge Statistics:")
	fmt.Printf("- Used streaming merge: %t\n", options.UseStreamingMerge)
	fmt.Printf("- Deleted rows excluded: always true\n")
	fmt.Printf("- Duplicate keys deduplicated: %t\n", options.DeduplicateKeys)
	
	// Query specific customer
	fmt.Println("\nQuerying customer 1002:")
	bitmap, _ := merged.QueryInt("customer_id", 1002)
	fmt.Printf("Found %d occurrence(s)\n", bitmap.GetCardinality())
	
	// Clean up
	os.Remove(file1)
	os.Remove(file2)
	os.Remove(outputFile)
	
	fmt.Println("\nStreaming merge example completed!")
	fmt.Println("Note: Streaming merge can handle files larger than RAM by")
	fmt.Println("processing data incrementally without loading everything into memory.")
}