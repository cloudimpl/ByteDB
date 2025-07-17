package main

import (
	"fmt"
	"os"
	
	"bytedb/columnar"
	roaring "github.com/RoaringBitmap/roaring/v2"
)

func main() {
	fmt.Println("=== Global Row Numbers and Cross-File Deletion Example ===\n")
	
	// Cleanup files on exit
	defer cleanup()
	
	// Simulate a system with global row number management
	// In a real system, this would be managed by the application
	var globalRowCounter uint64 = 1000
	
	// Step 1: Create first batch of data (Day 1)
	fmt.Println("1. Creating Day 1 data file...")
	file1 := "transactions_day1.bytedb"
	cf1, err := columnar.CreateFile(file1)
	if err != nil {
		panic(err)
	}
	
	cf1.AddColumn("tx_id", columnar.DataTypeInt64, false)
	cf1.AddColumn("user_id", columnar.DataTypeInt64, false)
	cf1.AddColumn("amount", columnar.DataTypeInt32, false)
	
	// Day 1 transactions using global row numbers 1000-1004
	txData1 := []columnar.IntData{
		columnar.NewIntData(5001, globalRowCounter),
		columnar.NewIntData(5002, globalRowCounter+1),
		columnar.NewIntData(5003, globalRowCounter+2),
		columnar.NewIntData(5004, globalRowCounter+3),
		columnar.NewIntData(5005, globalRowCounter+4),
	}
	cf1.LoadIntColumn("tx_id", txData1)
	
	userData1 := []columnar.IntData{
		columnar.NewIntData(101, globalRowCounter),
		columnar.NewIntData(102, globalRowCounter+1),
		columnar.NewIntData(103, globalRowCounter+2),
		columnar.NewIntData(101, globalRowCounter+3), // Same user, different tx
		columnar.NewIntData(104, globalRowCounter+4),
	}
	cf1.LoadIntColumn("user_id", userData1)
	
	amountData1 := []columnar.IntData{
		columnar.NewIntData(100, globalRowCounter),
		columnar.NewIntData(200, globalRowCounter+1),
		columnar.NewIntData(150, globalRowCounter+2),
		columnar.NewIntData(300, globalRowCounter+3),
		columnar.NewIntData(250, globalRowCounter+4),
	}
	cf1.LoadIntColumn("amount", amountData1)
	
	fmt.Printf("Created %s with transactions using global rows %d-%d\n", 
		file1, globalRowCounter, globalRowCounter+4)
	
	// Update global counter
	day1Rows := []uint64{globalRowCounter, globalRowCounter+1, globalRowCounter+2, 
		globalRowCounter+3, globalRowCounter+4}
	globalRowCounter += 5
	
	cf1.Close()
	
	// Step 2: Create second batch of data (Day 2)
	fmt.Println("\n2. Creating Day 2 data file...")
	file2 := "transactions_day2.bytedb"
	cf2, err := columnar.CreateFile(file2)
	if err != nil {
		panic(err)
	}
	
	cf2.AddColumn("tx_id", columnar.DataTypeInt64, false)
	cf2.AddColumn("user_id", columnar.DataTypeInt64, false)
	cf2.AddColumn("amount", columnar.DataTypeInt32, false)
	
	// Day 2 transactions using global row numbers 1005-1008
	txData2 := []columnar.IntData{
		columnar.NewIntData(5006, globalRowCounter),
		columnar.NewIntData(5007, globalRowCounter+1),
		columnar.NewIntData(5008, globalRowCounter+2),
		columnar.NewIntData(5009, globalRowCounter+3),
	}
	cf2.LoadIntColumn("tx_id", txData2)
	
	userData2 := []columnar.IntData{
		columnar.NewIntData(105, globalRowCounter),
		columnar.NewIntData(101, globalRowCounter+1), // User 101 again
		columnar.NewIntData(106, globalRowCounter+2),
		columnar.NewIntData(102, globalRowCounter+3), // User 102 again
	}
	cf2.LoadIntColumn("user_id", userData2)
	
	amountData2 := []columnar.IntData{
		columnar.NewIntData(400, globalRowCounter),
		columnar.NewIntData(350, globalRowCounter+1),
		columnar.NewIntData(600, globalRowCounter+2),
		columnar.NewIntData(450, globalRowCounter+3),
	}
	cf2.LoadIntColumn("amount", amountData2)
	
	// Mark some transactions from Day 1 as deleted (e.g., chargebacks)
	fmt.Println("\n3. Marking Day 1 chargebacks in Day 2 file...")
	cf2.DeleteRow(day1Rows[1]) // Transaction 5002 (row 1001)
	cf2.DeleteRow(day1Rows[3]) // Transaction 5004 (row 1003)
	fmt.Printf("Marked transactions from Day 1 (rows %d and %d) as deleted\n", 
		day1Rows[1], day1Rows[3])
	
	fmt.Printf("Created %s with transactions using global rows %d-%d\n", 
		file2, globalRowCounter, globalRowCounter+3)
	
	// Update global counter
	day2Rows := []uint64{globalRowCounter, globalRowCounter+1, globalRowCounter+2, 
		globalRowCounter+3}
	globalRowCounter += 4
	
	cf2.Close()
	
	// Step 3: Create third batch of data (Day 3)
	fmt.Println("\n4. Creating Day 3 data file...")
	file3 := "transactions_day3.bytedb"
	cf3, err := columnar.CreateFile(file3)
	if err != nil {
		panic(err)
	}
	
	cf3.AddColumn("tx_id", columnar.DataTypeInt64, false)
	cf3.AddColumn("user_id", columnar.DataTypeInt64, false)
	cf3.AddColumn("amount", columnar.DataTypeInt32, false)
	
	// Day 3 transactions using global row numbers 1009-1011
	txData3 := []columnar.IntData{
		columnar.NewIntData(5010, globalRowCounter),
		columnar.NewIntData(5011, globalRowCounter+1),
		columnar.NewIntData(5012, globalRowCounter+2),
	}
	cf3.LoadIntColumn("tx_id", txData3)
	
	userData3 := []columnar.IntData{
		columnar.NewIntData(107, globalRowCounter),
		columnar.NewIntData(108, globalRowCounter+1),
		columnar.NewIntData(101, globalRowCounter+2), // User 101 again
	}
	cf3.LoadIntColumn("user_id", userData3)
	
	amountData3 := []columnar.IntData{
		columnar.NewIntData(500, globalRowCounter),
		columnar.NewIntData(700, globalRowCounter+1),
		columnar.NewIntData(800, globalRowCounter+2),
	}
	cf3.LoadIntColumn("amount", amountData3)
	
	// Mark more transactions as deleted (from both previous days)
	fmt.Println("\n5. Marking more deletions in Day 3 file...")
	cf3.DeleteRow(day1Rows[2])  // Transaction 5003 from Day 1 (row 1002)
	cf3.DeleteRow(day2Rows[1])  // Transaction 5007 from Day 2 (row 1006)
	cf3.DeleteRow(day2Rows[3])  // Transaction 5009 from Day 2 (row 1008)
	fmt.Printf("Marked transactions from Day 1 (row %d) and Day 2 (rows %d, %d) as deleted\n", 
		day1Rows[2], day2Rows[1], day2Rows[3])
	
	cf3.Close()
	
	// Step 4: Demonstrate how deleted bitmaps accumulate
	fmt.Println("\n6. Showing accumulated deletions across files...")
	
	// Open files to check deleted bitmaps
	cf1, _ = columnar.OpenFileReadOnly(file1)
	cf2, _ = columnar.OpenFileReadOnly(file2)
	cf3, _ = columnar.OpenFileReadOnly(file3)
	
	fmt.Printf("Day 1 file deleted count: %d\n", cf1.GetDeletedCount())
	fmt.Printf("Day 2 file deleted count: %d (references Day 1 rows)\n", cf2.GetDeletedCount())
	fmt.Printf("Day 3 file deleted count: %d (references Day 1 & 2 rows)\n", cf3.GetDeletedCount())
	
	cf1.Close()
	cf2.Close()
	cf3.Close()
	
	// Step 5: Merge all files to consolidate data
	fmt.Println("\n7. Merging all files with global deletion handling...")
	mergedFile := "transactions_consolidated.bytedb"
	
	options := &columnar.MergeOptions{
		DeletedRowHandling: columnar.ExcludeDeleted,
		DeduplicateKeys:    false, // Preserve all transactions
	}
	
	err = columnar.MergeFiles(mergedFile, []string{file1, file2, file3}, options)
	if err != nil {
		panic(err)
	}
	
	// Step 6: Verify results
	fmt.Println("\n8. Verifying consolidated results...")
	merged, err := columnar.OpenFileReadOnly(mergedFile)
	if err != nil {
		panic(err)
	}
	defer merged.Close()
	
	// Count remaining transactions
	allTxBitmap := roaring.New()
	for tx := int64(5001); tx <= 5012; tx++ {
		bitmap, _ := merged.QueryInt("tx_id", tx)
		allTxBitmap.Or(bitmap)
	}
	
	fmt.Printf("Total transactions after merge: %d\n", allTxBitmap.GetCardinality())
	fmt.Println("Deleted transactions were excluded:")
	fmt.Println("  - Transaction 5002 (marked deleted in Day 2 file)")
	fmt.Println("  - Transaction 5003 (marked deleted in Day 3 file)")
	fmt.Println("  - Transaction 5004 (marked deleted in Day 2 file)")
	fmt.Println("  - Transaction 5007 (marked deleted in Day 3 file)")
	fmt.Println("  - Transaction 5009 (marked deleted in Day 3 file)")
	
	// Show per-user transaction count
	fmt.Println("\nTransactions per user after consolidation:")
	users := []int64{101, 102, 103, 104, 105, 106, 107, 108}
	for _, userID := range users {
		bitmap, _ := merged.QueryInt("user_id", userID)
		if bitmap.GetCardinality() > 0 {
			fmt.Printf("  User %d: %d transactions\n", userID, bitmap.GetCardinality())
		}
	}
	
	fmt.Println("\nKey Takeaways:")
	fmt.Println("- Row numbers are globally unique across all files")
	fmt.Println("- Later files can mark rows from earlier files as deleted")
	fmt.Println("- During merge, all deleted bitmaps are collected for a global view")
	fmt.Println("- This enables efficient deletion without modifying immutable files")
}

func cleanup() {
	// Remove example files
	files := []string{
		"transactions_day1.bytedb",
		"transactions_day2.bytedb",
		"transactions_day3.bytedb",
		"transactions_consolidated.bytedb",
	}
	for _, file := range files {
		os.Remove(file)
	}
}