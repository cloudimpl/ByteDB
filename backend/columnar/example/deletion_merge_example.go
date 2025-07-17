package main

import (
	"fmt"
	"os"
	
	"bytedb/columnar"
)

func main() {
	fmt.Println("=== Deletion and Merge Example ===\n")
	
	// Cleanup files on exit
	defer cleanup()
	
	// Step 1: Create first file with some data
	fmt.Println("1. Creating first file with initial data...")
	file1 := "sales_2024_q1.bytedb"
	cf1, err := columnar.CreateFile(file1)
	if err != nil {
		panic(err)
	}
	
	cf1.AddColumn("order_id", columnar.DataTypeInt64, false)
	cf1.AddColumn("customer", columnar.DataTypeString, false)
	cf1.AddColumn("amount", columnar.DataTypeInt32, false)
	cf1.AddColumn("status", columnar.DataTypeString, false)
	
	// Q1 orders
	orderData1 := []columnar.IntData{
		columnar.NewIntData(1001, 0),
		columnar.NewIntData(1002, 1),
		columnar.NewIntData(1003, 2),
		columnar.NewIntData(1004, 3),
		columnar.NewIntData(1005, 4),
	}
	cf1.LoadIntColumn("order_id", orderData1)
	
	customerData1 := []columnar.StringData{
		columnar.NewStringData("Alice", 0),
		columnar.NewStringData("Bob", 1),
		columnar.NewStringData("Charlie", 2),
		columnar.NewStringData("Alice", 3),
		columnar.NewStringData("David", 4),
	}
	cf1.LoadStringColumn("customer", customerData1)
	
	amountData1 := []columnar.IntData{
		columnar.NewIntData(100, 0),
		columnar.NewIntData(200, 1),
		columnar.NewIntData(150, 2),
		columnar.NewIntData(300, 3),
		columnar.NewIntData(250, 4),
	}
	cf1.LoadIntColumn("amount", amountData1)
	
	statusData1 := []columnar.StringData{
		columnar.NewStringData("completed", 0),
		columnar.NewStringData("cancelled", 1), // Will be marked for deletion
		columnar.NewStringData("completed", 2),
		columnar.NewStringData("refunded", 3),  // Will be marked for deletion
		columnar.NewStringData("completed", 4),
	}
	cf1.LoadStringColumn("status", statusData1)
	
	fmt.Printf("Created %s with 5 orders\n", file1)
	
	// Step 2: Mark cancelled and refunded orders for deletion
	fmt.Println("\n2. Marking cancelled and refunded orders for deletion...")
	
	// Find cancelled orders
	cancelledBitmap, _ := cf1.QueryString("status", "cancelled")
	refundedBitmap, _ := cf1.QueryString("status", "refunded")
	
	// Mark them for deletion
	cf1.DeleteRows(cancelledBitmap)
	cf1.DeleteRows(refundedBitmap)
	
	fmt.Printf("Marked %d orders for deletion\n", cf1.GetDeletedCount())
	
	cf1.Close()
	
	// Step 3: Create second file with Q2 data
	fmt.Println("\n3. Creating second file with Q2 data...")
	file2 := "sales_2024_q2.bytedb"
	cf2, err := columnar.CreateFile(file2)
	if err != nil {
		panic(err)
	}
	
	cf2.AddColumn("order_id", columnar.DataTypeInt64, false)
	cf2.AddColumn("customer", columnar.DataTypeString, false)
	cf2.AddColumn("amount", columnar.DataTypeInt32, false)
	cf2.AddColumn("status", columnar.DataTypeString, false)
	
	// Q2 orders
	orderData2 := []columnar.IntData{
		columnar.NewIntData(2001, 0),
		columnar.NewIntData(2002, 1),
		columnar.NewIntData(2003, 2),
		columnar.NewIntData(2004, 3),
	}
	cf2.LoadIntColumn("order_id", orderData2)
	
	customerData2 := []columnar.StringData{
		columnar.NewStringData("Eve", 0),
		columnar.NewStringData("Alice", 1),
		columnar.NewStringData("Bob", 2),
		columnar.NewStringData("Frank", 3),
	}
	cf2.LoadStringColumn("customer", customerData2)
	
	amountData2 := []columnar.IntData{
		columnar.NewIntData(400, 0),
		columnar.NewIntData(350, 1),
		columnar.NewIntData(600, 2),
		columnar.NewIntData(450, 3),
	}
	cf2.LoadIntColumn("amount", amountData2)
	
	statusData2 := []columnar.StringData{
		columnar.NewStringData("completed", 0),
		columnar.NewStringData("completed", 1),
		columnar.NewStringData("pending", 2),    // Will be marked for deletion
		columnar.NewStringData("completed", 3),
	}
	cf2.LoadStringColumn("status", statusData2)
	
	// Mark pending order for deletion
	pendingBitmap, _ := cf2.QueryString("status", "pending")
	cf2.DeleteRows(pendingBitmap)
	
	fmt.Printf("Created %s with 4 orders (1 marked for deletion)\n", file2)
	
	cf2.Close()
	
	// Step 4: Merge files, excluding deleted orders
	fmt.Println("\n4. Merging files and excluding deleted orders...")
	mergedFile := "sales_2024_h1.bytedb"
	
	mergeOptions := &columnar.MergeOptions{
		DeletedRowHandling: columnar.ExcludeDeleted, // Exclude deleted rows
		DeduplicateKeys:    true,                     // Remove duplicate order IDs
	}
	
	err = columnar.MergeFiles(mergedFile, []string{file1, file2}, mergeOptions)
	if err != nil {
		panic(err)
	}
	
	fmt.Printf("Created merged file: %s\n", mergedFile)
	
	// Step 5: Verify merged results
	fmt.Println("\n5. Verifying merged results...")
	merged, err := columnar.OpenFile(mergedFile)
	if err != nil {
		panic(err)
	}
	defer merged.Close()
	
	// Count completed orders
	completedBitmap, _ := merged.QueryString("status", "completed")
	fmt.Printf("Completed orders in merged file: %d\n", completedBitmap.GetCardinality())
	
	// Show customer order counts
	fmt.Println("\nCustomer order summary:")
	customers := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"}
	for _, customer := range customers {
		bitmap, _ := merged.QueryString("customer", customer)
		if bitmap.GetCardinality() > 0 {
			fmt.Printf("  %s: %d orders\n", customer, bitmap.GetCardinality())
		}
	}
	
	// Calculate total revenue using iterator
	fmt.Println("\nRevenue analysis:")
	totalRevenue := int64(0)
	orderCount := uint64(0)
	
	iter, err := merged.NewIterator("amount")
	if err == nil {
		defer iter.Close()
		for iter.Next() {
			amount := iter.Key().(int32)
			rows := iter.Rows()
			count := rows.GetCardinality()
			totalRevenue += int64(amount) * int64(count)
			orderCount += count
		}
	}
	
	fmt.Printf("Total orders in merged file: %d\n", orderCount)
	fmt.Printf("Total revenue: $%d\n", totalRevenue)
	
	// Show that no deleted orders exist in merged file
	fmt.Printf("\nDeleted orders in merged file: %d\n", merged.GetDeletedCount())
	
	// Verify excluded orders don't exist
	excludedOrders := []int64{1002, 1004, 2003} // Cancelled, refunded, and pending
	fmt.Println("\nVerifying excluded orders are not in merged file:")
	for _, orderID := range excludedOrders {
		bitmap, _ := merged.QueryInt("order_id", orderID)
		if bitmap.GetCardinality() == 0 {
			fmt.Printf("  Order %d: correctly excluded\n", orderID)
		}
	}
}

func cleanup() {
	// Remove example files
	files := []string{
		"sales_2024_q1.bytedb",
		"sales_2024_q2.bytedb",
		"sales_2024_h1.bytedb",
	}
	for _, file := range files {
		os.Remove(file)
	}
}