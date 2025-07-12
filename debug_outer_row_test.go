package main

import (
	"fmt"
	"testing"
)

func TestDebugOuterRow(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	
	// First, let's see what a regular query returns
	regularSQL := `SELECT name, department FROM employees e LIMIT 3`
	
	fmt.Printf("Regular query: %s\n", regularSQL)
	
	result, err := engine.Execute(regularSQL)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Regular results: %d rows\n", result.Count)
	for i, row := range result.Rows {
		if i >= 3 {
			break
		}
		fmt.Printf("Row %d keys: ", i+1)
		for key, value := range row {
			fmt.Printf("'%s'=%v ", key, value)
		}
		fmt.Printf("\n")
	}
	
	// Now let's test a simple non-correlated subquery to ensure basic functionality
	nonCorrelatedSQL := `SELECT name, (SELECT COUNT(*) FROM employees) as total_count FROM employees LIMIT 3`
	
	fmt.Printf("\nNon-correlated subquery: %s\n", nonCorrelatedSQL)
	
	result2, err := engine.Execute(nonCorrelatedSQL)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Non-correlated results: %d rows\n", result2.Count)
	for i, row := range result2.Rows {
		if i >= 3 {
			break
		}
		fmt.Printf("Row %d: ", i+1)
		for key, value := range row {
			fmt.Printf("%s=%v ", key, value)
		}
		fmt.Printf("\n")
	}
}