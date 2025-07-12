package main

import (
	"fmt"
	"os"
	"testing"

	"bytedb/core"
)

func TestSubqueries(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data
	generateSampleData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test IN subquery
	t.Run("IN Subquery", func(t *testing.T) {
		query := "SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 200000);"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute IN subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("IN subquery returned error: %s", result.Error)
		}

		fmt.Printf("IN Subquery Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// All departments have budget > 200000, so all employees should be returned
		expectedCount := 10 // All employees
		if len(result.Rows) != expectedCount {
			t.Errorf("Expected %d rows from IN subquery, got %d", expectedCount, len(result.Rows))
		}

		for i, row := range result.Rows {
			if i < 3 { // Show first 3 rows
				fmt.Printf("Row %d: ", i+1)
				for col, val := range row {
					fmt.Printf("%s=%v ", col, val)
				}
				fmt.Printf("\n")
			}
		}
	})

	// Test NOT IN subquery
	t.Run("NOT IN Subquery", func(t *testing.T) {
		query := "SELECT name FROM employees WHERE department NOT IN (SELECT name FROM departments WHERE budget > 300000);"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute NOT IN subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("NOT IN subquery returned error: %s", result.Error)
		}

		fmt.Printf("\nNOT IN Subquery Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// Should exclude employees in Engineering department (budget 500000)
		if len(result.Rows) == 0 {
			t.Error("Expected some results from NOT IN subquery, got 0 rows")
		}
	})

	// Test EXISTS subquery (non-correlated for now)
	t.Run("EXISTS Subquery", func(t *testing.T) {
		query := "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees LIMIT 1);"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute EXISTS subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("EXISTS subquery returned error: %s", result.Error)
		}

		fmt.Printf("\nEXISTS Subquery Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// Should return all departments since employees table has data
		expectedCount := 6 // All departments
		if len(result.Rows) != expectedCount {
			t.Errorf("Expected %d departments (all), got %d", expectedCount, len(result.Rows))
		}
	})

	// Test scalar subquery (simplified)
	t.Run("Scalar Subquery", func(t *testing.T) {
		query := "SELECT name FROM employees WHERE salary > (SELECT 50000);"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute scalar subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Scalar subquery returned error: %s", result.Error)
		}

		fmt.Printf("\nScalar Subquery Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// Should return employees with salary > 50000 (should be several employees)
		if len(result.Rows) == 0 {
			t.Error("Expected some results from scalar subquery, got 0 rows")
		}
	})

	// Test nested subqueries
	t.Run("Nested Subqueries", func(t *testing.T) {
		query := "SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > (SELECT AVG(salary) FROM employees WHERE department = 'Engineering'));"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute nested subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Nested subquery returned error: %s", result.Error)
		}

		fmt.Printf("\nNested Subquery Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// Should work without errors
		if len(result.Rows) == 0 {
			t.Error("Expected some results from nested subquery, got 0 rows")
		}
	})

	// Test subquery edge cases
	t.Run("Subquery Edge Cases", func(t *testing.T) {
		// Empty result subquery
		query := "SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 1000000);"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute empty subquery: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Empty subquery returned error: %s", result.Error)
		}

		fmt.Printf("\nEmpty Subquery Results:\n")
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// Should return 0 rows
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows from empty subquery, got %d", len(result.Rows))
		}
	})
}
