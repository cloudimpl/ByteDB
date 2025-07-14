package main

import (
	"fmt"
	"log"
	"os"
	
	"bytedb/columnar"
)

func main() {
	// Clean up any existing file
	os.Remove("null_example.bytedb")
	
	// Create a new columnar file
	cf, err := columnar.CreateFile("null_example.bytedb")
	if err != nil {
		log.Fatal("Failed to create file:", err)
	}
	defer cf.Close()
	
	// Add columns (some nullable, some not)
	if err := cf.AddColumn("id", columnar.DataTypeInt64, false); err != nil {
		log.Fatal("Failed to add id column:", err)
	}
	
	if err := cf.AddColumn("name", columnar.DataTypeString, true); err != nil { // Nullable
		log.Fatal("Failed to add name column:", err)
	}
	
	if err := cf.AddColumn("age", columnar.DataTypeInt64, true); err != nil { // Nullable
		log.Fatal("Failed to add age column:", err)
	}
	
	// Load sample data with null values
	fmt.Println("Loading data with NULL values...")
	
	// ID data (non-nullable)
	idData := []columnar.IntData{
		columnar.NewIntData(1, 0),
		columnar.NewIntData(2, 1),
		columnar.NewIntData(3, 2),
		columnar.NewIntData(4, 3),
		columnar.NewIntData(5, 4),
	}
	if err := cf.LoadIntColumn("id", idData); err != nil {
		log.Fatal("Failed to load id data:", err)
	}
	
	// Name data (with nulls)
	nameData := []columnar.StringData{
		columnar.NewStringData("Alice", 0),   // Non-null
		columnar.NewNullStringData(1),        // NULL
		columnar.NewStringData("Charlie", 2), // Non-null
		columnar.NewNullStringData(3),        // NULL
		columnar.NewStringData("Eve", 4),     // Non-null
	}
	if err := cf.LoadStringColumn("name", nameData); err != nil {
		log.Fatal("Failed to load name data:", err)
	}
	
	// Age data (with nulls)
	ageData := []columnar.IntData{
		columnar.NewIntData(25, 0),     // Non-null
		columnar.NewIntData(30, 1),     // Non-null
		columnar.NewNullIntData(2),     // NULL
		columnar.NewIntData(28, 3),     // Non-null
		columnar.NewNullIntData(4),     // NULL
	}
	if err := cf.LoadIntColumn("age", ageData); err != nil {
		log.Fatal("Failed to load age data:", err)
	}
	
	fmt.Println("Data loaded successfully!")
	
	// Demonstrate null queries
	fmt.Println("\n=== NULL Query Examples ===")
	
	// 1. Find rows where name IS NULL
	fmt.Println("\n1. Find rows where name IS NULL:")
	bitmap, err := cf.QueryNull("name")
	if err != nil {
		log.Fatal("QueryNull failed:", err)
	}
	rows := columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows with NULL names: %v\n", len(rows), rows)
	
	// 2. Find rows where name IS NOT NULL
	fmt.Println("\n2. Find rows where name IS NOT NULL:")
	bitmap, err = cf.QueryNotNull("name")
	if err != nil {
		log.Fatal("QueryNotNull failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows with non-NULL names: %v\n", len(rows), rows)
	
	// 3. Find rows where age IS NULL
	fmt.Println("\n3. Find rows where age IS NULL:")
	bitmap, err = cf.QueryNull("age")
	if err != nil {
		log.Fatal("QueryNull failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows with NULL ages: %v\n", len(rows), rows)
	
	// 4. Regular queries automatically exclude nulls
	fmt.Println("\n4. Regular string query (automatically excludes NULLs):")
	bitmap, err = cf.QueryString("name", "Alice")
	if err != nil {
		log.Fatal("QueryString failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows with name = 'Alice': %v\n", len(rows), rows)
	
	// 5. Range queries exclude nulls
	fmt.Println("\n5. Age range query (automatically excludes NULLs):")
	bitmap, err = cf.RangeQueryInt("age", 25, 35)
	if err != nil {
		log.Fatal("RangeQueryInt failed:", err)
	}
	rows = columnar.BitmapToSlice(bitmap)
	fmt.Printf("   Found %d rows with age between 25-35: %v\n", len(rows), rows)
	
	// 6. Complex queries with null handling
	fmt.Println("\n=== Complex NULL Operations ===")
	
	// Find rows where both name and age are NOT NULL
	fmt.Println("\n6. Find rows where both name AND age are NOT NULL:")
	nameNotNull, _ := cf.QueryNotNull("name")
	ageNotNull, _ := cf.QueryNotNull("age")
	bothNotNull := cf.QueryAnd(nameNotNull, ageNotNull)
	rows = columnar.BitmapToSlice(bothNotNull)
	fmt.Printf("   Found %d rows with both non-NULL: %v\n", len(rows), rows)
	
	// Find rows where either name OR age is NULL
	fmt.Println("\n7. Find rows where name OR age is NULL:")
	nameNull, _ := cf.QueryNull("name")
	ageNull, _ := cf.QueryNull("age")
	eitherNull := cf.QueryOr(nameNull, ageNull)
	rows = columnar.BitmapToSlice(eitherNull)
	fmt.Printf("   Found %d rows with at least one NULL: %v\n", len(rows), rows)
	
	// Display statistics including null counts
	fmt.Println("\n=== Column Statistics (including NULL counts) ===")
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
	
	// Demonstrate validation: try to load null into non-nullable column
	fmt.Println("\n=== Validation Demo: NULL in Non-Nullable Column ===")
	
	// Try to create a non-nullable column and load null data (this should fail)
	if err := cf.AddColumn("required_id", columnar.DataTypeInt64, false); err != nil {
		log.Fatal("Failed to add required_id column:", err)
	}
	
	// Attempt to load null data into non-nullable column
	invalidData := []columnar.IntData{
		columnar.NewIntData(1000, 0),  // Valid
		columnar.NewNullIntData(1),    // Invalid - should cause error
	}
	
	fmt.Println("8. Attempting to load NULL into non-nullable column...")
	err = cf.LoadIntColumn("required_id", invalidData)
	if err != nil {
		fmt.Printf("   ✅ Validation worked! Error: %v\n", err)
	} else {
		fmt.Printf("   ❌ Validation failed - no error was returned!\n")
	}
	
	fmt.Println("\nNull example completed successfully!")
}