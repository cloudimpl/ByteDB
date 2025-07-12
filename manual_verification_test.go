package main

import (
	"fmt"
	"os"
	"testing"
)

func TestManualVerification(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	fmt.Println("=== VERIFYING SUBQUERY CORRECTNESS ===")
	
	// 1. Check our test data
	fmt.Println("\n--- TEST DATA ---")
	
	employees, _ := engine.Execute("SELECT name, department, salary FROM employees;")
	fmt.Printf("Employees (%d):\n", len(employees.Rows))
	for _, row := range employees.Rows {
		fmt.Printf("  %s: %s ($%v)\n", row["name"], row["department"], row["salary"])
	}
	
	departments, _ := engine.Execute("SELECT name, budget FROM departments;")
	fmt.Printf("\nDepartments (%d):\n", len(departments.Rows))
	for _, row := range departments.Rows {
		fmt.Printf("  %s: $%v\n", row["name"], row["budget"])
	}
	
	// 2. Verify IN subquery step by step
	fmt.Println("\n--- IN SUBQUERY VERIFICATION ---")
	fmt.Println("Query: SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 200000);")
	
	// Step 1: Which departments have budget > 200000?
	deptQuery, _ := engine.Execute("SELECT name, budget FROM departments WHERE budget > 200000;")
	fmt.Printf("\nDepartments with budget > 200000:\n")
	for _, row := range deptQuery.Rows {
		fmt.Printf("  %s ($%v)\n", row["name"], row["budget"])
	}
	
	// Step 2: Manual count of employees in those departments
	inDeptCount := 0
	for _, empRow := range employees.Rows {
		empDept := empRow["department"].(string)
		for _, deptRow := range deptQuery.Rows {
			if empDept == deptRow["name"].(string) {
				inDeptCount++
				break
			}
		}
	}
	
	// Step 3: Compare with actual result
	inResult, _ := engine.Execute("SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 200000);")
	fmt.Printf("\nExpected employees in high-budget departments: %d\n", inDeptCount)
	fmt.Printf("Actual IN subquery result: %d\n", len(inResult.Rows))
	fmt.Printf("✓ IN subquery correct: %v\n", inDeptCount == len(inResult.Rows))
	
	// 3. Verify NOT IN subquery
	fmt.Println("\n--- NOT IN SUBQUERY VERIFICATION ---")
	fmt.Println("Query: SELECT name FROM employees WHERE department NOT IN (SELECT name FROM departments WHERE budget > 300000);")
	
	// Which departments have budget > 300000?
	highDeptQuery, _ := engine.Execute("SELECT name, budget FROM departments WHERE budget > 300000;")
	fmt.Printf("\nDepartments with budget > 300000:\n")
	for _, row := range highDeptQuery.Rows {
		fmt.Printf("  %s ($%v)\n", row["name"], row["budget"])
	}
	
	// Manual count of employees NOT in those departments
	notInDeptCount := 0
	for _, empRow := range employees.Rows {
		empDept := empRow["department"].(string)
		isInHighDept := false
		for _, deptRow := range highDeptQuery.Rows {
			if empDept == deptRow["name"].(string) {
				isInHighDept = true
				break
			}
		}
		if !isInHighDept {
			notInDeptCount++
		}
	}
	
	notInResult, _ := engine.Execute("SELECT name FROM employees WHERE department NOT IN (SELECT name FROM departments WHERE budget > 300000);")
	fmt.Printf("\nExpected employees NOT in very high-budget departments: %d\n", notInDeptCount)
	fmt.Printf("Actual NOT IN subquery result: %d\n", len(notInResult.Rows))
	fmt.Printf("✓ NOT IN subquery correct: %v\n", notInDeptCount == len(notInResult.Rows))
	
	// 4. Verify scalar subquery
	fmt.Println("\n--- SCALAR SUBQUERY VERIFICATION ---")
	fmt.Println("Query: SELECT name FROM employees WHERE salary > (SELECT 50000);")
	
	// Manual count of employees with salary > 50000
	highSalaryCount := 0
	for _, empRow := range employees.Rows {
		salary := empRow["salary"].(float64)
		if salary > 50000 {
			highSalaryCount++
		}
	}
	
	scalarResult, _ := engine.Execute("SELECT name FROM employees WHERE salary > (SELECT 50000);")
	fmt.Printf("\nExpected employees with salary > 50000: %d\n", highSalaryCount)
	fmt.Printf("Actual scalar subquery result: %d\n", len(scalarResult.Rows))
	fmt.Printf("✓ Scalar subquery correct: %v\n", highSalaryCount == len(scalarResult.Rows))
	
	// 5. Verify EXISTS subquery
	fmt.Println("\n--- EXISTS SUBQUERY VERIFICATION ---")
	fmt.Println("Query: SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees LIMIT 1);")
	
	// Since employees table has data, EXISTS should return all departments
	existsResult, _ := engine.Execute("SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees LIMIT 1);")
	fmt.Printf("\nTotal departments: %d\n", len(departments.Rows))
	fmt.Printf("EXISTS subquery result: %d\n", len(existsResult.Rows))
	fmt.Printf("✓ EXISTS subquery correct: %v\n", len(departments.Rows) == len(existsResult.Rows))
	
	fmt.Println("\n=== VERIFICATION COMPLETE ===")
}