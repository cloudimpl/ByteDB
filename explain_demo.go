package main

import (
	"fmt"
	"log"
)

func DemoExplainCommand() {
	fmt.Println("🔍 ByteDB Query Optimization Demo")
	fmt.Println("=====================================")
	fmt.Println()

	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Demo 1: Simple query plan
	fmt.Println("1. Simple SELECT Query Plan:")
	fmt.Println("----------------------------")
	query := "EXPLAIN SELECT name, salary FROM employees WHERE department = 'Engineering'"
	result, err := engine.Execute(query)
	if err != nil {
		log.Printf("Error: %v\n", err)
	} else if result.Error != "" {
		log.Printf("Query error: %s\n", result.Error)
	} else {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 2: Aggregate query plan
	fmt.Println("\n2. Aggregate Query Plan:")
	fmt.Println("------------------------")
	query = "EXPLAIN SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department"
	result, err = engine.Execute(query)
	if err == nil && result.Error == "" {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 3: JOIN query plan
	fmt.Println("\n3. JOIN Query Plan:")
	fmt.Println("-------------------")
	query = "EXPLAIN SELECT e.name, d.budget FROM employees e JOIN departments d ON e.department = d.name"
	result, err = engine.Execute(query)
	if err == nil && result.Error == "" {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 4: Complex query with CTE
	fmt.Println("\n4. CTE Query Plan:")
	fmt.Println("------------------")
	query = `EXPLAIN WITH high_earners AS (
		SELECT name, salary, department FROM employees WHERE salary > 70000
	)
	SELECT department, COUNT(*) FROM high_earners GROUP BY department`
	result, err = engine.Execute(query)
	if err == nil && result.Error == "" {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 5: EXPLAIN ANALYZE
	fmt.Println("\n5. EXPLAIN ANALYZE (with actual execution):")
	fmt.Println("-------------------------------------------")
	query = "EXPLAIN ANALYZE SELECT name FROM employees WHERE salary > 75000"
	result, err = engine.Execute(query)
	if err == nil && result.Error == "" {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 6: JSON format
	fmt.Println("\n6. EXPLAIN with JSON format:")
	fmt.Println("----------------------------")
	query = "EXPLAIN (FORMAT JSON) SELECT * FROM employees LIMIT 5"
	result, err = engine.Execute(query)
	if err == nil && result.Error == "" {
		fmt.Println(result.Rows[0]["QUERY PLAN"])
	}

	// Demo 7: Statistics collection
	fmt.Println("\n7. Table Statistics:")
	fmt.Println("--------------------")
	stats, err := engine.planner.statsCollector.CollectTableStats("employees")
	if err == nil {
		fmt.Printf("Table: %s\n", stats.TableName)
		fmt.Printf("Row Count: %d\n", stats.RowCount)
		fmt.Printf("Size: %d bytes\n", stats.SizeBytes)
		fmt.Printf("Columns: %d\n", len(stats.ColumnStats))
		
		if salaryStats, ok := stats.ColumnStats["salary"]; ok {
			fmt.Printf("\nSalary Column Stats:\n")
			fmt.Printf("  Distinct Values: %d\n", salaryStats.DistinctCount)
			fmt.Printf("  Min: %v\n", salaryStats.MinValue)
			fmt.Printf("  Max: %v\n", salaryStats.MaxValue)
			fmt.Printf("  Nulls: %d\n", salaryStats.NullCount)
		}
	}

	fmt.Println("\n✅ Query optimization features demonstrated successfully!")
}

// Run this demo with: go run . -demo-explain