package main

import (
	"fmt"
	"testing"

	"bytedb/core"
)

func TestFinalCorrelation(t *testing.T) {
	generateSampleData()
	engine := core.NewQueryEngine("./data")

	// Test COUNT first (simpler than AVG)
	sql := `SELECT name, department, 
	       (SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) as dept_count 
	FROM employees e LIMIT 5`

	fmt.Printf("Testing final correlation with COUNT: %s\n", sql)

	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}

	fmt.Printf("Results: %d rows\n", result.Count)
	for i, row := range result.Rows {
		if i >= 5 {
			break
		}
		name := row["name"]
		dept := row["department"]
		count := row["dept_count"]
		fmt.Printf("Row %d: %s (%s) -> count: %v\n", i+1, name, dept, count)
	}

	// Now test AVG
	fmt.Printf("\nTesting final correlation with AVG:\n")

	avgSQL := `SELECT name, department, salary,
	          (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department = e.department) as avg_dept_salary 
	      FROM employees e LIMIT 3`

	result2, err := engine.Execute(avgSQL)
	if err != nil {
		t.Errorf("AVG Execution error: %v", err)
		return
	}

	fmt.Printf("AVG Results: %d rows\n", result2.Count)
	for i, row := range result2.Rows {
		if i >= 3 {
			break
		}
		name := row["name"]
		dept := row["department"]
		salary := row["salary"]
		avgSalary := row["avg_dept_salary"]
		fmt.Printf("Row %d: %s (%s) salary=%v -> avg: %v\n", i+1, name, dept, salary, avgSalary)
	}
}
