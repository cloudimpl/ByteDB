package main

import (
	"fmt"
	"os"
	"testing"
)

func TestVerifySubqueryCorrectness(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// First, let's examine our test data to understand what results should be
	t.Run("Examine Test Data", func(t *testing.T) {
		fmt.Println("=== EXAMINING TEST DATA ===")
		
		// Check all employees
		result, _ := engine.Execute("SELECT name, department, salary FROM employees;")
		fmt.Printf("\nEmployees (%d total):\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("  %s: %s (salary: %v)\n", row["name"], row["department"], row["salary"])
		}
		
		// Check all departments
		result, _ = engine.Execute("SELECT name, budget FROM departments;")
		fmt.Printf("\nDepartments (%d total):\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("  %s: budget %v\n", row["name"], row["budget"])
		}
	})

	// Verify IN subquery step by step
	t.Run("Verify IN Subquery Correctness", func(t *testing.T) {
		fmt.Println("\n=== VERIFYING IN SUBQUERY ===")
		
		// Step 1: Check which departments have budget > 200000
		subquery := "SELECT name FROM departments WHERE budget > 200000;"
		result, _ := engine.Execute(subquery)
		fmt.Printf("Departments with budget > 200000:\n")
		var deptNames []string
		for _, row := range result.Rows {
			deptName := row["name"].(string)
			deptNames = append(deptNames, deptName)
			fmt.Printf("  %s\n", deptName)
		}
		
		// Step 2: Check which employees are in those departments
		fmt.Printf("\nEmployees in those departments:\n")
		employeeCount := 0
		allEmployees, _ := engine.Execute("SELECT name, department FROM employees;")
		for _, empRow := range allEmployees.Rows {
			empDept := empRow["department"].(string)
			for _, dept := range deptNames {
				if empDept == dept {
					fmt.Printf("  %s (%s)\n", empRow["name"], empDept)
					employeeCount++
					break
				}
			}
		}
		fmt.Printf("Expected IN subquery result: %d employees\n", employeeCount)
		
		// Step 3: Run the actual IN subquery
		mainQuery := "SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 200000);"
		result, _ = engine.Execute(mainQuery)
		fmt.Printf("Actual IN subquery result: %d employees\n", len(result.Rows))
		
		if len(result.Rows) != employeeCount {
			t.Errorf("IN subquery mismatch: expected %d, got %d", employeeCount, len(result.Rows))
		}
	})

	// Verify EXISTS subquery
	t.Run("Verify EXISTS Subquery Correctness", func(t *testing.T) {
		fmt.Println("\n=== VERIFYING EXISTS SUBQUERY ===")
		
		// Check if employees table has any data
		result, _ := engine.Execute("SELECT COUNT(*) as count FROM employees;")
		empCount := result.Rows[0]["count"]
		fmt.Printf("Employee count: %v\n", empCount)
		
		// Check total departments
		result, _ = engine.Execute("SELECT COUNT(*) as count FROM departments;")
		deptCount := result.Rows[0]["count"]
		fmt.Printf("Department count: %v\n", deptCount)
		
		// Since employees table has data, EXISTS should return all departments
		existsQuery := "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees LIMIT 1);"
		result, _ = engine.Execute(existsQuery)
		fmt.Printf("EXISTS result: %d departments\n", len(result.Rows))
		
		// This should equal total department count since employees table has data
		expectedCount := int(deptCount.(float64))
		if len(result.Rows) != expectedCount {
			t.Errorf("EXISTS subquery mismatch: expected %d, got %d", expectedCount, len(result.Rows))
		}
	})

	// Verify scalar subquery
	t.Run("Verify Scalar Subquery Correctness", func(t *testing.T) {
		fmt.Println("\n=== VERIFYING SCALAR SUBQUERY ===")
		
		// Check which employees have salary > 50000
		directQuery := "SELECT name, salary FROM employees WHERE salary > 50000;"
		result, _ := engine.Execute(directQuery)
		fmt.Printf("Employees with salary > 50000 (direct query):\n")
		for _, row := range result.Rows {
			fmt.Printf("  %s: %v\n", row["name"], row["salary"])
		}
		expectedCount := len(result.Rows)
		
		// Run the scalar subquery
		scalarQuery := "SELECT name FROM employees WHERE salary > (SELECT 50000);"
		result, _ = engine.Execute(scalarQuery)
		fmt.Printf("Scalar subquery result: %d employees\n", len(result.Rows))
		
		if len(result.Rows) != expectedCount {
			t.Errorf("Scalar subquery mismatch: expected %d, got %d", expectedCount, len(result.Rows))
		}
	})

	// Verify NOT IN subquery
	t.Run("Verify NOT IN Subquery Correctness", func(t *testing.T) {
		fmt.Println("\n=== VERIFYING NOT IN SUBQUERY ===")
		
		// Check which departments have budget > 300000
		subquery := "SELECT name FROM departments WHERE budget > 300000;"
		result, _ := engine.Execute(subquery)
		fmt.Printf("Departments with budget > 300000:\n")
		var deptNames []string
		for _, row := range result.Rows {
			deptName := row["name"].(string)
			deptNames = append(deptNames, deptName)
			fmt.Printf("  %s\n", deptName)
		}
		
		// Check which employees are NOT in those departments
		fmt.Printf("\nEmployees NOT in those departments:\n")
		employeeCount := 0
		allEmployees, _ := engine.Execute("SELECT name, department FROM employees;")
		for _, empRow := range allEmployees.Rows {
			empDept := empRow["department"].(string)
			isInDept := false
			for _, dept := range deptNames {
				if empDept == dept {
					isInDept = true
					break
				}
			}
			if !isInDept {
				fmt.Printf("  %s (%s)\n", empRow["name"], empDept)
				employeeCount++
			}
		}
		fmt.Printf("Expected NOT IN result: %d employees\n", employeeCount)
		
		// Run the actual NOT IN subquery
		notInQuery := "SELECT name FROM employees WHERE department NOT IN (SELECT name FROM departments WHERE budget > 300000);"
		result, _ = engine.Execute(notInQuery)
		fmt.Printf("Actual NOT IN result: %d employees\n", len(result.Rows))
		
		if len(result.Rows) != employeeCount {
			t.Errorf("NOT IN subquery mismatch: expected %d, got %d", employeeCount, len(result.Rows))
		}
	})
}