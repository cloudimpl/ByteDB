package main

import (
	"fmt"
	"testing"
)

func TestDebugExistsSubquery(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	
	// Test the failing EXISTS query
	sql := `SELECT d.name, d.budget 
	        FROM departments d 
	        WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department = d.name AND e.salary > 70000)`
	
	fmt.Printf("Testing EXISTS query: %s\n", sql)
	
	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Results: %d rows\n", result.Count)
	if result.Count == 0 {
		fmt.Printf("ERROR: Expected some departments but got 0 results\n")
	}
	
	// Let's test what employees have salary > 70000 and what departments they're in
	fmt.Printf("\nFor comparison, employees with salary > 70000:\n")
	checkSQL := `SELECT name, department, salary FROM employees WHERE salary > 70000`
	checkResult, err := engine.Execute(checkSQL)
	if err != nil {
		t.Errorf("Check query error: %v", err)
		return
	}
	
	fmt.Printf("High earners: %d employees\n", checkResult.Count)
	for i, row := range checkResult.Rows {
		if i >= 5 {
			break
		}
		name := row["name"]
		dept := row["department"]
		salary := row["salary"]
		fmt.Printf("  %s (%s): %v\n", name, dept, salary)
	}
}