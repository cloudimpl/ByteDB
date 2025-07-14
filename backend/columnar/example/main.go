package main

import (
	"fmt"
	"log"
	
	"bytedb/columnar"
)

func main() {
	// Create a new columnar file
	cf, err := columnar.CreateFile("example.bytedb")
	if err != nil {
		log.Fatal("Failed to create file:", err)
	}
	defer cf.Close()
	
	// Add columns
	if err := cf.AddColumn("id", columnar.DataTypeInt64, false); err != nil {
		log.Fatal("Failed to add id column:", err)
	}
	
	if err := cf.AddColumn("name", columnar.DataTypeString, false); err != nil {
		log.Fatal("Failed to add name column:", err)
	}
	
	if err := cf.AddColumn("age", columnar.DataTypeInt64, false); err != nil {
		log.Fatal("Failed to add age column:", err)
	}
	
	// Load some sample data
	fmt.Println("Loading data...")
	
	// ID data
	idData := []columnar.IntData{
		columnar.NewIntData(1, 0), columnar.NewIntData(2, 1), columnar.NewIntData(3, 2), 
		columnar.NewIntData(4, 3), columnar.NewIntData(5, 4),
		columnar.NewIntData(1, 5), // Duplicate ID
		columnar.NewIntData(6, 6), columnar.NewIntData(7, 7), columnar.NewIntData(8, 8), 
		columnar.NewIntData(9, 9),
	}
	if err := cf.LoadIntColumn("id", idData); err != nil {
		log.Fatal("Failed to load id data:", err)
	}
	
	// Name data
	nameData := []columnar.StringData{
		columnar.NewStringData("Alice", 0), columnar.NewStringData("Bob", 1), 
		columnar.NewStringData("Charlie", 2), columnar.NewStringData("David", 3), 
		columnar.NewStringData("Eve", 4),
		columnar.NewStringData("Alice", 5), // Duplicate name
		columnar.NewStringData("Frank", 6), columnar.NewStringData("Grace", 7), 
		columnar.NewStringData("Henry", 8), columnar.NewStringData("Iris", 9),
	}
	if err := cf.LoadStringColumn("name", nameData); err != nil {
		log.Fatal("Failed to load name data:", err)
	}
	
	// Age data
	ageData := []columnar.IntData{
		columnar.NewIntData(25, 0), columnar.NewIntData(30, 1), columnar.NewIntData(35, 2), 
		columnar.NewIntData(28, 3), columnar.NewIntData(32, 4),
		columnar.NewIntData(25, 5), columnar.NewIntData(40, 6), columnar.NewIntData(45, 7), 
		columnar.NewIntData(38, 8), columnar.NewIntData(29, 9),
	}
	if err := cf.LoadIntColumn("age", ageData); err != nil {
		log.Fatal("Failed to load age data:", err)
	}
	
	fmt.Println("Data loaded successfully!")
	
	// Query examples
	fmt.Println("\n=== Query Examples ===")
	
	// 1. Find all rows with id = 1
	fmt.Println("\n1. Find rows with id = 1:")
	bitmap, err := cf.QueryInt("id", 1)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	rows := columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// 2. Find all rows with name = "Alice"
	fmt.Println("\n2. Find rows with name = 'Alice':")
	bitmap, err = cf.QueryString("name", "Alice")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// 3. Range query on age (25-35)
	fmt.Println("\n3. Find rows with age between 25 and 35:")
	bitmap, err = cf.RangeQueryInt("age", 25, 35)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// 4. String range query
	fmt.Println("\n4. Find names between 'Bob' and 'Frank':")
	bitmap, err = cf.RangeQueryString("name", "Bob", "Frank")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// 5. Demonstrate bitmap operations
	fmt.Println("\n=== Advanced Bitmap Operations ===")
	
	// Find people with age > 30 AND name < "Grace"
	fmt.Println("\n5. Complex query: age > 30 AND name < 'Grace':")
	ageBitmap, _ := cf.QueryGreaterThan("age", int64(30))
	nameBitmap, _ := cf.QueryLessThan("name", "Grace")
	andResult := cf.QueryAnd(ageBitmap, nameBitmap)
	rows = columnar.BitmapToSlice(andResult)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// Find people with age < 30 OR age > 40
	fmt.Println("\n6. OR query: age < 30 OR age > 40:")
	youngBitmap, _ := cf.QueryLessThan("age", int64(30))
	oldBitmap, _ := cf.QueryGreaterThan("age", int64(40))
	orResult := cf.QueryOr(youngBitmap, oldBitmap)
	rows = columnar.BitmapToSlice(orResult)
	fmt.Printf("   Found %d rows: %v\n", len(rows), rows)
	
	// Display statistics
	fmt.Println("\n=== Column Statistics ===")
	for _, colName := range cf.GetColumns() {
		stats, err := cf.GetStats(colName)
		if err != nil {
			continue
		}
		fmt.Printf("\nColumn '%s':\n", colName)
		for k, v := range stats {
			fmt.Printf("  %s: %v\n", k, v)
		}
	}
	
	fmt.Println("\nFile created successfully: example.bytedb")
}