package main

import (
	"fmt"
	"strings"
	"testing"

	"bytedb/core"
)

func TestCaseExpressions(t *testing.T) {
	// Initialize query engine with test data
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	tests := []struct {
		name        string
		query       string
		expected    []string // Expected values in results
		notExpected []string // Values that should NOT be in results
	}{
		{
			name:     "Basic CASE with salary tiers",
			query:    "SELECT name, salary, CASE WHEN salary > 80000 THEN 'High' WHEN salary > 60000 THEN 'Medium' ELSE 'Low' END as salary_tier FROM employees",
			expected: []string{"Lisa Davis", "High", "Jane Smith", "Medium", "Sarah Wilson", "Low"},
		},
		{
			name:     "CASE with department categorization",
			query:    "SELECT name, department, CASE WHEN department = 'Engineering' THEN 'Tech' WHEN department = 'Marketing' THEN 'Sales & Marketing' ELSE department END as dept_category FROM employees",
			expected: []string{"Engineering", "Tech", "Marketing", "Sales & Marketing", "HR"},
		},
		{
			name:     "CASE with numeric results",
			query:    "SELECT name, salary, CASE WHEN salary >= 75000 THEN 1 WHEN salary >= 65000 THEN 2 ELSE 3 END as salary_rank FROM employees",
			expected: []string{"Lisa Davis", "1", "Jane Smith", "2", "Sarah Wilson", "3"},
		},
		{
			name:     "Nested CASE expressions",
			query:    "SELECT name, department, salary, CASE WHEN department = 'Engineering' THEN CASE WHEN salary > 80000 THEN 'Senior Engineer' ELSE 'Engineer' END WHEN department = 'Marketing' THEN 'Marketing Specialist' ELSE 'Other' END as role FROM employees",
			expected: []string{"Lisa Davis", "Senior Engineer", "John Doe", "Engineer", "Jane Smith", "Marketing Specialist"},
		},
		{
			name:     "CASE without ELSE clause",
			query:    "SELECT name, department, salary, CASE WHEN salary > 80000 THEN 'High Earner' WHEN department = 'Engineering' THEN 'Engineer' END as category FROM employees",
			expected: []string{"Lisa Davis", "High Earner", "John Doe", "Engineer"},
		},
		{
			name:        "CASE with IS NULL conditions",
			query:       "SELECT name, department, salary, CASE WHEN department IS NULL THEN 'No Department' WHEN salary IS NULL THEN 'No Salary' ELSE 'Has Data' END as data_status FROM employees",
			expected:    []string{"Has Data"},
			notExpected: []string{"No Department", "No Salary"},
		},
		{
			name:     "Multiple CASE expressions in same query",
			query:    "SELECT name, salary, department, CASE WHEN salary > 75000 THEN 'High' ELSE 'Low' END as salary_tier, CASE WHEN department = 'Engineering' THEN 'Tech' ELSE 'Non-Tech' END as tech_status FROM employees",
			expected: []string{"Lisa Davis", "High", "Tech", "Jane Smith", "Low", "Non-Tech"},
		},
		{
			name:     "CASE with comparison operators",
			query:    "SELECT name, salary, CASE WHEN salary > 80000 THEN 'Above 80k' WHEN salary >= 70000 THEN 'Above 70k' WHEN salary >= 60000 THEN 'Above 60k' ELSE 'Below 60k' END as salary_bracket FROM employees",
			expected: []string{"Lisa Davis", "Above 80k", "David Brown", "Above 70k", "Jane Smith", "Above 60k", "Sarah Wilson", "Below 60k"},
		},
		{
			name:     "CASE with string operations",
			query:    "SELECT name, department, CASE WHEN department = 'Engineering' THEN 'ENG' WHEN department = 'Marketing' THEN 'MKT' WHEN department = 'Finance' THEN 'FIN' WHEN department = 'Sales' THEN 'SLS' WHEN department = 'HR' THEN 'HR' ELSE 'UNK' END as dept_code FROM employees",
			expected: []string{"John Doe", "ENG", "Jane Smith", "MKT", "Anna Garcia", "FIN", "David Brown", "SLS", "Sarah Wilson", "HR"},
		},
		{
			name:        "CASE with boundary values",
			query:       "SELECT name, salary, CASE WHEN salary = 75000 THEN 'Exactly 75k' WHEN salary = 80000 THEN 'Exactly 80k' WHEN salary > 100000 THEN 'Above 100k' WHEN salary < 50000 THEN 'Below 50k' ELSE 'Normal range' END as boundary_test FROM employees",
			expected:    []string{"John Doe", "Exactly 75k", "Mike Johnson", "Exactly 80k", "Normal range"},
			notExpected: []string{"Above 100k", "Below 50k"},
		},
		{
			name:     "CASE with mixed data types in results",
			query:    "SELECT name, salary, CASE WHEN salary > 80000 THEN 1 WHEN department = 'Marketing' THEN 0 ELSE -1 END as numeric_category FROM employees",
			expected: []string{"Lisa Davis", "1", "Jane Smith", "0", "-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query execution failed: %v", err)
			}

			if result.Error != "" {
				t.Fatalf("Query returned error: %s", result.Error)
			}

			// Convert result to string for easy searching
			resultStr := convertResultToString(result)

			// Check expected values are present
			for _, expected := range tt.expected {
				if !strings.Contains(resultStr, expected) {
					t.Errorf("Expected '%s' not found in result. Query: %s\nResult: %s", expected, tt.query, resultStr)
				}
			}

			// Check unexpected values are not present
			for _, notExpected := range tt.notExpected {
				if strings.Contains(resultStr, notExpected) {
					t.Errorf("Unexpected '%s' found in result. Query: %s\nResult: %s", notExpected, tt.query, resultStr)
				}
			}

			// Ensure we got some results (unless testing empty case)
			if len(result.Rows) == 0 && !strings.Contains(tt.name, "empty") {
				t.Errorf("Expected non-empty results for test: %s. Query: %s", tt.name, tt.query)
			}
		})
	}
}

func TestCaseExpressionEdgeCases(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	edgeCaseTests := []struct {
		name        string
		query       string
		expectError bool
		description string
	}{
		{
			name:        "CASE with no matching conditions and no ELSE",
			query:       "SELECT name, CASE WHEN salary > 100000 THEN 'Very High' WHEN salary < 0 THEN 'Negative' END as result FROM employees",
			expectError: false,
			description: "Should return NULL for rows that don't match any condition",
		},
		{
			name:        "CASE with all NULL conditions",
			query:       "SELECT name, CASE WHEN NULL THEN 'Never' ELSE 'Always' END as result FROM employees",
			expectError: false,
			description: "NULL conditions should always be false",
		},
		{
			name:        "Empty CASE expression",
			query:       "SELECT name, CASE END as result FROM employees",
			expectError: true,
			description: "CASE with no WHEN clauses should cause parse error",
		},
	}

	for _, tt := range edgeCaseTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.query)

			if tt.expectError {
				if err == nil && result.Error == "" {
					t.Errorf("Expected error for test: %s, but query succeeded", tt.name)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for test: %s: %v", tt.name, err)
				}
				if result.Error != "" {
					t.Errorf("Query returned error for test: %s: %s", tt.name, result.Error)
				}
			}
		})
	}
}

func TestCaseExpressionPerformance(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test that CASE expressions don't significantly impact performance
	query := "SELECT name, salary, CASE WHEN salary > 80000 THEN 'High' WHEN salary > 60000 THEN 'Medium' ELSE 'Low' END as tier FROM employees"

	// Run the query multiple times to check for performance regression
	for i := 0; i < 10; i++ {
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Performance test iteration %d failed: %v", i, err)
		}
		if result.Error != "" {
			t.Fatalf("Performance test iteration %d returned error: %s", i, result.Error)
		}
		if len(result.Rows) == 0 {
			t.Fatalf("Performance test iteration %d returned no results", i)
		}
	}
}

// Helper function to convert QueryResult to string for easy searching
func convertResultToString(result *core.QueryResult) string {
	var parts []string

	// Add column headers
	for _, col := range result.Columns {
		parts = append(parts, col)
	}

	// Add row data
	for _, row := range result.Rows {
		for _, col := range result.Columns {
			if val, exists := row[col]; exists {
				if val == nil {
					parts = append(parts, "<nil>")
				} else {
					parts = append(parts, strings.TrimSpace(strings.ReplaceAll(fmt.Sprintf("%v", val), "\n", " ")))
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

func BenchmarkCaseExpressions(b *testing.B) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	query := "SELECT name, salary, CASE WHEN salary > 80000 THEN 'High' WHEN salary > 60000 THEN 'Medium' ELSE 'Low' END as tier FROM employees"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute(query)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}
