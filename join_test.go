package main

import (
	"fmt"
	"os"
	"testing"

	"bytedb/core"
)

func TestJoinExecution(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data including departments
	generateSampleData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test simple INNER JOIN
	t.Run("INNER JOIN", func(t *testing.T) {
		query := "SELECT e.name, d.manager FROM employees e JOIN departments d ON e.department = d.name;"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute INNER JOIN query: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("INNER JOIN query returned error: %s", result.Error)
		}

		fmt.Printf("INNER JOIN Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		for i, row := range result.Rows {
			if i < 5 { // Show first 5 rows
				fmt.Printf("Row %d: ", i+1)
				for col, val := range row {
					fmt.Printf("%s=%v ", col, val)
				}
				fmt.Printf("\n")
			}
		}

		// Basic validation - should have some results since we have matching data
		if len(result.Rows) == 0 {
			t.Error("Expected some results from INNER JOIN, got 0 rows")
		}
	})

	// Test LEFT JOIN
	t.Run("LEFT JOIN", func(t *testing.T) {
		query := "SELECT e.name, d.budget FROM employees e LEFT JOIN departments d ON e.department = d.name;"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute LEFT JOIN query: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("LEFT JOIN query returned error: %s", result.Error)
		}

		fmt.Printf("\nLEFT JOIN Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// LEFT JOIN should return all employees (10 rows)
		if len(result.Rows) != 10 {
			t.Errorf("Expected 10 rows from LEFT JOIN (all employees), got %d", len(result.Rows))
		}
	})

	// Test qualified column selection
	t.Run("Qualified Column Selection", func(t *testing.T) {
		query := "SELECT e.name, e.salary, d.budget FROM employees e JOIN departments d ON e.department = d.name;"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute qualified column query: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Qualified column query returned error: %s", result.Error)
		}

		fmt.Printf("\nQualified Column Selection Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)

		// Check that we have the right column names
		expectedColumns := []string{"name", "salary", "budget"}
		if len(result.Columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
		}

		for _, expectedCol := range expectedColumns {
			found := false
			for _, actualCol := range result.Columns {
				if actualCol == expectedCol {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected column %s not found in result", expectedCol)
			}
		}
	})

	// Test RIGHT JOIN
	t.Run("RIGHT JOIN", func(t *testing.T) {
		query := "SELECT e.name, d.budget FROM employees e RIGHT JOIN departments d ON e.department = d.name;"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute RIGHT JOIN query: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("RIGHT JOIN query returned error: %s", result.Error)
		}

		fmt.Printf("\nRIGHT JOIN Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// RIGHT JOIN should return all departments
		// We have 6 departments, but only 5 have employees, so expect 10 matching rows + 1 unmatched department = 11 rows
		if len(result.Rows) != 11 {
			t.Errorf("Expected 11 rows from RIGHT JOIN (10 employee matches + 1 unmatched department), got %d", len(result.Rows))
		}
	})

	// Test FULL OUTER JOIN
	t.Run("FULL OUTER JOIN", func(t *testing.T) {
		query := "SELECT e.name, d.budget FROM employees e FULL OUTER JOIN departments d ON e.department = d.name;"

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute FULL OUTER JOIN query: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("FULL OUTER JOIN query returned error: %s", result.Error)
		}

		fmt.Printf("\nFULL OUTER JOIN Results:\n")
		fmt.Printf("Columns: %v\n", result.Columns)
		fmt.Printf("Row count: %d\n", len(result.Rows))

		// FULL OUTER JOIN should return all employees + any unmatched departments
		// We have 10 employees matching 5 departments + 1 unmatched department = 11 rows
		if len(result.Rows) != 11 {
			t.Errorf("Expected 11 rows from FULL OUTER JOIN (10 employee matches + 1 unmatched department), got %d", len(result.Rows))
		}
	})

	// Test table aliases work correctly
	t.Run("Table Aliases", func(t *testing.T) {
		// Test that table aliases are working in the parser
		parser := core.NewSQLParser()
		parsedQuery, err := parser.Parse("SELECT e.name FROM employees e JOIN departments d ON e.department = d.name;")
		if err != nil {
			t.Fatalf("Failed to parse query with aliases: %v", err)
		}

		if parsedQuery.TableName != "employees" {
			t.Errorf("Expected main table 'employees', got '%s'", parsedQuery.TableName)
		}

		if parsedQuery.TableAlias != "e" {
			t.Errorf("Expected main table alias 'e', got '%s'", parsedQuery.TableAlias)
		}

		if len(parsedQuery.Joins) != 1 {
			t.Errorf("Expected 1 JOIN clause, got %d", len(parsedQuery.Joins))
		}

		if len(parsedQuery.Joins) > 0 {
			join := parsedQuery.Joins[0]
			if join.TableName != "departments" {
				t.Errorf("Expected join table 'departments', got '%s'", join.TableName)
			}
			if join.TableAlias != "d" {
				t.Errorf("Expected join table alias 'd', got '%s'", join.TableAlias)
			}
		}
	})
}
