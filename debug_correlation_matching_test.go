package main

import (
	"fmt"
	"testing"
)

func TestDebugCorrelationMatching(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	
	// Test each department individually
	departments := []string{"Engineering", "Marketing", "HR", "Sales"}
	
	for _, dept := range departments {
		sql := fmt.Sprintf(`SELECT COUNT(*) as count FROM employees WHERE department = '%s'`, dept)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Error for %s: %v", dept, err)
			continue
		}
		
		if len(result.Rows) > 0 {
			count := result.Rows[0]["count"]
			fmt.Printf("Department %s: %v employees\n", dept, count)
		}
	}
	
	fmt.Printf("\nTesting correlated subquery:\n")
	
	// Test the correlated query with detailed output
	sql := `SELECT name, department, 
	       (SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) as dept_count 
	FROM employees e WHERE department IN ('Engineering', 'Marketing') LIMIT 4`
	
	fmt.Printf("SQL: %s\n", sql)
	
	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Results: %d rows\n", result.Count)
	for i, row := range result.Rows {
		if i >= 4 {
			break
		}
		dept := row["department"]
		count := row["dept_count"]
		name := row["name"]
		fmt.Printf("Row %d: %s (%s) -> count: %v\n", i+1, name, dept, count)
	}
}