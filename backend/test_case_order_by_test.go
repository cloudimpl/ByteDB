package main

import (
	"testing"
	"bytedb/core"
)

func TestCASEExpressionOrderBy(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test ORDER BY with CASE expression alias
	result, err := engine.Execute(`
		SELECT name, salary,
		CASE 
			WHEN salary > 80000 THEN 'High' 
			WHEN salary > 60000 THEN 'Medium' 
			ELSE 'Low' 
		END as salary_grade 
		FROM employees 
		ORDER BY salary_grade DESC
	`)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.Count != 10 {
		t.Errorf("Expected 10 rows, got %d", result.Count)
	}

	// Check that results are properly ordered by salary_grade DESC
	// In DESC order: Medium -> Low -> High (alphabetically)
	expectedOrder := []struct {
		name  string
		grade string
	}{
		{"John Doe", "Medium"},
		{"Jane Smith", "Medium"},
		{"Mike Johnson", "Medium"},
		{"David Brown", "Medium"},
		{"Tom Miller", "Medium"},
		{"Anna Garcia", "Medium"},
		{"Chris Anderson", "Medium"},
		{"Maria Rodriguez", "Medium"},
		{"Sarah Wilson", "Low"},
		{"Lisa Davis", "High"},
	}

	for i, expected := range expectedOrder {
		if i >= len(result.Rows) {
			break
		}
		row := result.Rows[i]
		name := row["name"].(string)
		grade := row["salary_grade"].(string)
		
		if name != expected.name || grade != expected.grade {
			t.Errorf("Row %d: expected %s/%s, got %s/%s", 
				i, expected.name, expected.grade, name, grade)
		}
	}

	// Test ORDER BY with CASE expression ASC
	result2, err := engine.Execute(`
		SELECT name,
		CASE 
			WHEN salary > 80000 THEN 'High' 
			WHEN salary > 60000 THEN 'Medium' 
			ELSE 'Low' 
		END as salary_grade 
		FROM employees 
		ORDER BY salary_grade ASC
		LIMIT 3
	`)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// In ASC order: High -> Low -> Medium (alphabetically)
	if result2.Rows[0]["salary_grade"] != "High" {
		t.Errorf("First row should be 'High', got %v", result2.Rows[0]["salary_grade"])
	}
}