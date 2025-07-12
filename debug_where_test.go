package main

import (
	"fmt"
	"testing"
)

func TestDebugWhereParsing(t *testing.T) {
	parser := NewSQLParser()
	
	// Test just the subquery part
	subquerySQL := `SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department = e.department`
	
	fmt.Printf("Testing subquery WHERE parsing: %s\n", subquerySQL)
	
	parsed, err := parser.Parse(subquerySQL)
	if err != nil {
		t.Errorf("Parse error: %v", err)
		return
	}
	
	fmt.Printf("Table: %s, Alias: %s\n", parsed.TableName, parsed.TableAlias)
	fmt.Printf("WHERE conditions: %d\n", len(parsed.Where))
	
	for i, where := range parsed.Where {
		fmt.Printf("  WHERE %d:\n", i)
		fmt.Printf("    TableName: '%s'\n", where.TableName)
		fmt.Printf("    Column: '%s'\n", where.Column)
		fmt.Printf("    Operator: '%s'\n", where.Operator)
		fmt.Printf("    Value: %v (type: %T)\n", where.Value, where.Value)
		fmt.Printf("    ValueColumn: '%s'\n", where.ValueColumn)
		fmt.Printf("    ValueTableName: '%s'\n", where.ValueTableName)
		fmt.Printf("    IsComplex: %v\n", where.IsComplex)
	}
}