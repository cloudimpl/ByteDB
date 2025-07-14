package main

import (
	"fmt"
	"log"

	"bytedb/core"
)

func main() {
	// Use current directory as data path
	dataPath := "."

	// Create query engine
	engine := core.NewQueryEngine(dataPath)
	defer engine.Close()

	fmt.Println("ByteDB - DuckDB-style Table Creation Example")
	fmt.Println("============================================")

	// Example 1: Query parquet file directly
	fmt.Println("\n1. Query parquet file directly using read_parquet():")
	fmt.Println("   SELECT COUNT(*) FROM read_parquet('data/employees.parquet')")

	result, err := engine.Execute("SELECT COUNT(*) as total FROM read_parquet('data/employees.parquet')")
	if err != nil {
		log.Printf("Error: %v", err)
	} else if result.Error != "" {
		log.Printf("Query Error: %s", result.Error)
	} else {
		fmt.Printf("   Result: %d employees found\n", result.Rows[0]["total"])
	}

	// Example 2: Create table from parquet file
	fmt.Println("\n2. Create table from parquet file:")
	fmt.Println("   CREATE TABLE employees AS SELECT * FROM read_parquet('data/employees.parquet')")

	result, err = engine.Execute("CREATE TABLE employees AS SELECT * FROM read_parquet('data/employees.parquet')")
	if err != nil {
		log.Printf("Error: %v", err)
	} else if result.Error != "" {
		log.Printf("Query Error: %s", result.Error)
	} else {
		fmt.Printf("   %s\n", result.Rows[0]["status"])
	}

	// Example 3: Query the created table
	fmt.Println("\n3. Query the created table:")
	fmt.Println("   SELECT department, COUNT(*) as count FROM employees GROUP BY department")

	result, err = engine.Execute("SELECT department, COUNT(*) as count FROM employees GROUP BY department")
	if err != nil {
		log.Printf("Error: %v", err)
	} else if result.Error != "" {
		log.Printf("Query Error: %s", result.Error)
	} else {
		fmt.Println("   Results:")
		for _, row := range result.Rows {
			fmt.Printf("   - %s: %d employees\n", row["department"], row["count"])
		}
	}

	// Example 4: Create table with IF NOT EXISTS
	fmt.Println("\n4. Create table with IF NOT EXISTS (should not error):")
	fmt.Println("   CREATE TABLE IF NOT EXISTS employees AS SELECT * FROM read_parquet('data/employees.parquet')")

	result, err = engine.Execute("CREATE TABLE IF NOT EXISTS employees AS SELECT * FROM read_parquet('data/employees.parquet')")
	if err != nil {
		log.Printf("Error: %v", err)
	} else if result.Error != "" {
		log.Printf("Query Error: %s", result.Error)
	} else {
		fmt.Println("   Success: Table already exists, no error thrown")
	}
}
