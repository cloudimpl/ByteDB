package main

import (
	"testing"
)

// Integration tests that verify the entire system works end-to-end
func TestEndToEndQueries(t *testing.T) {
	// Generate test data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	tests := []struct {
		name         string
		sql          string
		expectedRows int
		shouldError  bool
	}{
		{
			name:         "Basic SELECT with LIMIT",
			sql:          "SELECT * FROM employees LIMIT 2;",
			expectedRows: 2,
			shouldError:  false,
		},
		{
			name:         "Numeric WHERE clause",
			sql:          "SELECT name FROM employees WHERE salary > 80000;",
			expectedRows: 1, // Only Lisa Davis (85000) - Mike Johnson is exactly 80000
			shouldError:  false,
		},
		{
			name:         "String WHERE clause",
			sql:          "SELECT name FROM employees WHERE department = 'Engineering';",
			expectedRows: 4,
			shouldError:  false,
		},
		{
			name:         "Column selection",
			sql:          "SELECT name, salary FROM employees LIMIT 1;",
			expectedRows: 1,
			shouldError:  false,
		},
		{
			name:         "Greater than operator",
			sql:          "SELECT name FROM products WHERE price > 100;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "Less than operator",
			sql:          "SELECT name FROM products WHERE price < 30;",
			expectedRows: 5,
			shouldError:  false,
		},
		{
			name:         "Greater than or equal operator",
			sql:          "SELECT name FROM products WHERE price >= 199.99;",
			expectedRows: 3, // Laptop (999.99), Monitor (299.99), Desk Chair (199.99)
			shouldError:  false,
		},
		{
			name:         "Empty result set",
			sql:          "SELECT * FROM products WHERE price > 10000;",
			expectedRows: 0,
			shouldError:  false,
		},
		{
			name:         "Type coercion - int comparison",
			sql:          "SELECT name FROM products WHERE price > 199;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "Type coercion - float comparison",
			sql:          "SELECT name FROM products WHERE price > 199.99;",
			expectedRows: 2, // Laptop (999.99), Monitor (299.99) - Desk Chair is exactly 199.99
			shouldError:  false,
		},
		{
			name:        "Invalid table name",
			sql:         "SELECT * FROM invalid_table;",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.sql)
			
			if err != nil {
				t.Errorf("Unexpected error from Execute: %v", err)
				return
			}

			if tt.shouldError {
				if result.Error == "" {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if result.Error != "" {
				t.Errorf("Unexpected error in result: %s", result.Error)
				return
			}

			if result.Count != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, result.Count)
				t.Logf("SQL: %s", tt.sql)
				t.Logf("Result: %+v", result)
			}
		})
	}
}

// Test that verifies specific query results are correct
func TestQueryResultAccuracy(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test engineering employees
	result, err := engine.Execute("SELECT name FROM employees WHERE department = 'Engineering';")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	expectedEngineers := map[string]bool{
		"John Doe":       true,
		"Mike Johnson":   true,
		"Lisa Davis":     true,
		"Chris Anderson": true,
	}

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 engineering employees, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		name, exists := row["name"]
		if !exists {
			t.Errorf("Row missing name column: %+v", row)
			continue
		}
		
		nameStr, ok := name.(string)
		if !ok {
			t.Errorf("Name is not a string: %v (%T)", name, name)
			continue
		}
		
		if !expectedEngineers[nameStr] {
			t.Errorf("Unexpected engineer: %s", nameStr)
		}
	}

	// Test high-priced products
	result, err = engine.Execute("SELECT name, price FROM products WHERE price > 100;")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	expectedExpensive := map[string]float64{
		"Laptop":     999.99,
		"Monitor":    299.99,
		"Desk Chair": 199.99,
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 expensive products, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		name := row["name"].(string)
		price := row["price"].(float64)
		
		expectedPrice, exists := expectedExpensive[name]
		if !exists {
			t.Errorf("Unexpected expensive product: %s", name)
			continue
		}
		
		if price != expectedPrice {
			t.Errorf("Price mismatch for %s: expected %.2f, got %.2f", name, expectedPrice, price)
		}
		
		if price <= 100 {
			t.Errorf("Product %s should have price > 100, got %.2f", name, price)
		}
	}
}