package main

import (
	"fmt"
	"testing"
)

func TestDebugSmallCorrelation(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	
	// Test with only 2 rows to see the pattern clearly
	sql := `SELECT name, department, 
	       (SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) as dept_count 
	FROM employees e LIMIT 2`
	
	fmt.Printf("Testing small correlation: %s\n", sql)
	
	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}
	
	fmt.Printf("Results: %d rows\n", result.Count)
	for i, row := range result.Rows {
		name := row["name"]
		dept := row["department"]
		count := row["dept_count"]
		fmt.Printf("Row %d: %s (%s) -> count: %v\n", i+1, name, dept, count)
	}
}