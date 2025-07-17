package main

import (
	"fmt"
	"os"

	"bytedb/columnar"
)

func main() {
	filename := "iterator_example.bytedb"
	defer os.Remove(filename)

	// Create a new columnar file
	cf, err := columnar.CreateFile(filename)
	if err != nil {
		panic(err)
	}

	// Add columns
	cf.AddColumn("product_id", columnar.DataTypeInt64, false)
	cf.AddColumn("category", columnar.DataTypeString, false)
	cf.AddColumn("price", columnar.DataTypeInt32, false)
	cf.AddColumn("in_stock", columnar.DataTypeBool, false)

	// Load test data
	productData := []columnar.IntData{
		columnar.NewIntData(1001, 0),
		columnar.NewIntData(1002, 1),
		columnar.NewIntData(1003, 2),
		columnar.NewIntData(1004, 3),
		columnar.NewIntData(1005, 4),
		columnar.NewIntData(1006, 5),
		columnar.NewIntData(1007, 6),
		columnar.NewIntData(1008, 7),
		columnar.NewIntData(1009, 8),
		columnar.NewIntData(1010, 9),
	}
	cf.LoadIntColumn("product_id", productData)

	// Categories with duplicates
	categoryData := []columnar.StringData{
		columnar.NewStringData("Electronics", 0),
		columnar.NewStringData("Books", 1),
		columnar.NewStringData("Electronics", 2),
		columnar.NewStringData("Clothing", 3),
		columnar.NewStringData("Books", 4),
		columnar.NewStringData("Electronics", 5),
		columnar.NewStringData("Food", 6),
		columnar.NewStringData("Books", 7),
		columnar.NewStringData("Clothing", 8),
		columnar.NewStringData("Electronics", 9),
	}
	cf.LoadStringColumn("category", categoryData)

	// Prices
	priceData := []columnar.IntData{
		columnar.NewIntData(299, 0),   // Electronics
		columnar.NewIntData(15, 1),    // Books
		columnar.NewIntData(599, 2),   // Electronics
		columnar.NewIntData(49, 3),    // Clothing
		columnar.NewIntData(25, 4),    // Books
		columnar.NewIntData(899, 5),   // Electronics
		columnar.NewIntData(10, 6),    // Food
		columnar.NewIntData(30, 7),    // Books
		columnar.NewIntData(79, 8),    // Clothing
		columnar.NewIntData(399, 9),   // Electronics
	}
	cf.LoadIntColumn("price", priceData)

	// Stock status
	stockData := []columnar.IntData{
		columnar.NewIntData(1, 0), // true
		columnar.NewIntData(1, 1), // true
		columnar.NewIntData(0, 2), // false
		columnar.NewIntData(1, 3), // true
		columnar.NewIntData(0, 4), // false
		columnar.NewIntData(1, 5), // true
		columnar.NewIntData(1, 6), // true
		columnar.NewIntData(1, 7), // true
		columnar.NewIntData(0, 8), // false
		columnar.NewIntData(1, 9), // true
	}
	cf.LoadIntColumn("in_stock", stockData)

	cf.Close()

	// Reopen for querying
	cf, err = columnar.OpenFile(filename)
	if err != nil {
		panic(err)
	}
	defer cf.Close()

	fmt.Println("=== Iterator Examples ===\n")

	// Example 1: Basic forward iteration
	fmt.Println("1. Forward Iteration - All Products:")
	iter, err := cf.NewIterator("product_id")
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	for iter.Next() {
		productId := iter.Key().(int64)
		rows := iter.Rows()
		fmt.Printf("  Product ID %d: %d rows\n", productId, rows.GetCardinality())
	}

	// Example 2: Reverse iteration
	fmt.Println("\n2. Reverse Iteration - Last 5 Products:")
	iter2, err := cf.NewIterator("product_id")
	if err != nil {
		panic(err)
	}
	defer iter2.Close()

	iter2.SeekLast()
	count := 0
	for count < 5 && iter2.Valid() {
		productId := iter2.Key().(int64)
		fmt.Printf("  Product ID: %d\n", productId)
		iter2.Prev()
		count++
	}

	// Example 3: Range iteration
	fmt.Println("\n3. Range Iteration - Products 1003-1007:")
	rangeIter, err := cf.NewRangeIterator("product_id", int64(1003), int64(1007))
	if err != nil {
		panic(err)
	}
	defer rangeIter.Close()

	for rangeIter.Next() {
		productId := rangeIter.Key().(int64)
		fmt.Printf("  Product ID: %d\n", productId)
	}

	// Example 4: Iterating over categories (with duplicates)
	fmt.Println("\n4. Category Distribution:")
	catIter, err := cf.NewIterator("category")
	if err != nil {
		panic(err)
	}
	defer catIter.Close()

	for catIter.Next() {
		category := catIter.Key().(string)
		rowBitmap := catIter.Rows()
		fmt.Printf("  %s: %d products (rows: %v)\n", 
			category, 
			rowBitmap.GetCardinality(),
			columnar.BitmapToSlice(rowBitmap))
	}

	// Example 5: Price range analysis
	fmt.Println("\n5. Products in Price Range $25-$100:")
	priceIter, err := cf.NewRangeIterator("price", int32(25), int32(100))
	if err != nil {
		panic(err)
	}
	defer priceIter.Close()

	for priceIter.Next() {
		price := priceIter.Key().(int32)
		rows := priceIter.Rows()
		
		// Get product IDs for this price
		rowSlice := columnar.BitmapToSlice(rows)
		fmt.Printf("  Price $%d: ", price)
		for _, row := range rowSlice {
			// Look up product ID for this row
			productBitmap, _ := cf.QueryInt("product_id", int64(1001+row))
			if productBitmap.GetCardinality() > 0 {
				fmt.Printf("Product %d ", 1001+row)
			}
		}
		fmt.Println()
	}

	// Example 6: Seek functionality
	fmt.Println("\n6. Seek to Specific Product:")
	seekIter, err := cf.NewIterator("product_id")
	if err != nil {
		panic(err)
	}
	defer seekIter.Close()

	if seekIter.Seek(int64(1005)) {
		fmt.Printf("  Found product: %d\n", seekIter.Key().(int64))
		
		// Continue from there
		fmt.Println("  Next 3 products:")
		for i := 0; i < 3 && seekIter.Next(); i++ {
			fmt.Printf("    Product ID: %d\n", seekIter.Key().(int64))
		}
	}

	// Example 7: Boolean iteration
	fmt.Println("\n7. Stock Status Distribution:")
	stockIter, err := cf.NewIterator("in_stock")
	if err != nil {
		panic(err)
	}
	defer stockIter.Close()

	for stockIter.Next() {
		inStock := stockIter.Key().(bool)
		rows := stockIter.Rows()
		status := "Out of Stock"
		if inStock {
			status = "In Stock"
		}
		fmt.Printf("  %s: %d products\n", status, rows.GetCardinality())
	}

	// Example 8: Complex iteration with filtering
	fmt.Println("\n8. In-Stock Electronics Under $500:")
	
	// First get electronics
	electronicsIter, err := cf.NewIterator("category")
	if err != nil {
		panic(err)
	}
	defer electronicsIter.Close()

	for electronicsIter.Next() {
		if electronicsIter.Key().(string) == "Electronics" {
			electronicsRows := electronicsIter.Rows()
			
			// Get in-stock items
			inStockBitmap, _ := cf.QueryInt("in_stock", 1)
			
			// Get items under $500
			underPriceBitmap, _ := cf.QueryLessThan("price", int32(500))
			
			// Combine all conditions
			result := cf.QueryAnd(electronicsRows, inStockBitmap, underPriceBitmap)
			
			fmt.Printf("  Found %d in-stock electronics under $500\n", result.GetCardinality())
			fmt.Printf("  Product rows: %v\n", columnar.BitmapToSlice(result))
			break
		}
	}
}