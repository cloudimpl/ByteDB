package main

import (
	"fmt"
	"testing"

	"bytedb/core"
)

func TestQualifiedColumnParsing(t *testing.T) {
	parser := core.NewSQLParser()

	// Simple test with qualified columns in WHERE
	sql := `SELECT * FROM employees e2 WHERE e2.department = 'Engineering'`

	fmt.Printf("Testing qualified column parsing: %s\n", sql)

	parsed, err := parser.Parse(sql)
	if err != nil {
		t.Errorf("Parse error: %v", err)
		return
	}

	fmt.Printf("WHERE conditions: %d\n", len(parsed.Where))
	if len(parsed.Where) > 0 {
		condition := parsed.Where[0]
		fmt.Printf("  Table: '%s'\n", condition.TableName)
		fmt.Printf("  Column: '%s'\n", condition.Column)
		fmt.Printf("  Operator: '%s'\n", condition.Operator)
		fmt.Printf("  Value: %v\n", condition.Value)
		fmt.Printf("  IsComplex: %v\n", condition.IsComplex)
	}

	// Test execution
	generateSampleData()
	engine := core.NewQueryEngine("./data")
	result, err := engine.Execute(sql)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}

	fmt.Printf("Results: %d rows\n", result.Count)
}
