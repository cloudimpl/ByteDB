package main

import (
	"testing"

	"bytedb/core"
)

// TestBasicCaseExpressions tests core CASE expression functionality
func TestBasicCaseExpressions(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	t.Run("Simple CASE with string results", func(t *testing.T) {
		query := "SELECT name, salary, CASE WHEN salary > 75000 THEN 'High' ELSE 'Low' END as tier FROM employees WHERE name = 'Lisa Davis'"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		row := result.Rows[0]
		// Debug: print the full row
		for k, v := range row {
			t.Logf("Row data: %s = %v (%T)", k, v, v)
		}

		if tier, exists := row["tier"]; !exists {
			t.Errorf("tier column not found in result")
		} else if tier != "High" {
			t.Errorf("Expected tier='High', got '%v' (type: %T)", tier, tier)
		}
	})

	t.Run("CASE with numeric results", func(t *testing.T) {
		query := "SELECT name, department, CASE WHEN department = 'Engineering' THEN 1 ELSE 0 END as is_engineer FROM employees WHERE name = 'John Doe'"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		row := result.Rows[0]
		if isEngineer, exists := row["is_engineer"]; !exists || isEngineer != int32(1) {
			t.Errorf("Expected is_engineer=1, got %v (type: %T)", isEngineer, isEngineer)
		}
	})

	t.Run("CASE without ELSE returns NULL", func(t *testing.T) {
		query := "SELECT name, salary, CASE WHEN salary > 100000 THEN 'Very High' END as high_earner FROM employees WHERE name = 'John Doe'"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		row := result.Rows[0]
		if highEarner, exists := row["high_earner"]; !exists || highEarner != nil {
			t.Errorf("Expected high_earner=nil, got %v", highEarner)
		}
	})

	t.Run("Multiple CASE expressions", func(t *testing.T) {
		query := "SELECT name, salary, department, CASE WHEN salary > 75000 THEN 'High' ELSE 'Low' END as salary_tier, CASE WHEN department = 'Engineering' THEN 'Tech' ELSE 'Other' END as dept_type FROM employees WHERE name = 'Lisa Davis'"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		row := result.Rows[0]
		if tier, exists := row["salary_tier"]; !exists || tier != "High" {
			t.Errorf("Expected salary_tier='High', got %v", tier)
		}
		if deptType, exists := row["dept_type"]; !exists || deptType != "Tech" {
			t.Errorf("Expected dept_type='Tech', got %v", deptType)
		}
	})

	t.Run("Nested CASE expressions", func(t *testing.T) {
		query := "SELECT name, salary, department, CASE WHEN department = 'Engineering' THEN CASE WHEN salary > 80000 THEN 'Senior' ELSE 'Junior' END ELSE 'Other' END as level FROM employees WHERE name IN ('Lisa Davis', 'John Doe')"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
		}

		// Check results for both employees
		levelCounts := make(map[string]int)
		for _, row := range result.Rows {
			if level, exists := row["level"]; exists {
				levelCounts[level.(string)]++
			}
		}

		if levelCounts["Senior"] != 1 || levelCounts["Junior"] != 1 {
			t.Errorf("Expected 1 Senior and 1 Junior, got %v", levelCounts)
		}
	})
}

// TestCaseExpressionDataTypes tests CASE with different data types
func TestCaseExpressionDataTypes(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "String result",
			query:    "SELECT name, salary, CASE WHEN salary > 0 THEN 'success' ELSE 'failure' END as result FROM employees WHERE name = 'John Doe'",
			expected: "success",
		},
		{
			name:     "Integer result",
			query:    "SELECT name, salary, CASE WHEN salary > 0 THEN 42 ELSE 0 END as result FROM employees WHERE name = 'John Doe'",
			expected: int32(42),
		},
		{
			name:     "NULL result",
			query:    "SELECT name, salary, CASE WHEN salary < 0 THEN 'never' END as result FROM employees WHERE name = 'John Doe'",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Error != "" {
				t.Fatalf("Query error: %s", result.Error)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			row := result.Rows[0]
			if actual, exists := row["result"]; !exists {
				t.Errorf("Result column not found")
			} else if actual != tt.expected {
				t.Errorf("Expected %v (type %T), got %v (type %T)", tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

// TestCaseExpressionErrors tests error conditions
func TestCaseExpressionErrors(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// This test is mainly to ensure the parser doesn't crash on malformed CASE expressions
	// The exact error handling behavior may vary
	t.Run("Malformed CASE should not crash", func(t *testing.T) {
		queries := []string{
			"SELECT CASE FROM employees",                      // No WHEN clause
			"SELECT CASE WHEN THEN 'test' END FROM employees", // No condition
		}

		for _, query := range queries {
			// We don't expect these to succeed, but they shouldn't crash
			_, err := engine.Execute(query)
			// Just ensure we get some kind of response, whether error or result
			if err == nil {
				t.Logf("Query '%s' unexpectedly succeeded", query)
			} else {
				t.Logf("Query '%s' failed as expected: %v", query, err)
			}
		}
	})
}
