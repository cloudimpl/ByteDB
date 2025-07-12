package main

import (
	"fmt"
	"testing"
)

func TestDebugCorrelationExecution(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	
	// Test a simplified version of the correlated query
	sql := `SELECT name, department, 
	       (SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) as dept_count 
	FROM employees e LIMIT 3`
	
	fmt.Printf("Testing correlated execution: %s\n", sql)
	
	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Result error: %s\n", result.Error)
	fmt.Printf("Results: %d rows\n", result.Count)
	
	for i, row := range result.Rows {
		if i >= 3 { // Only show first 3 rows
			break
		}
		fmt.Printf("Row %d: ", i+1)
		for column, value := range row {
			fmt.Printf("%s=%v ", column, value)
		}
		fmt.Printf("\n")
	}
}