package main

import (
	"fmt"
	"reflect"
	"testing"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestWindowStructure(t *testing.T) {
	sql := "SELECT name, ROW_NUMBER() OVER (ORDER BY salary) FROM employees"
	
	result, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	if len(result.Stmts) == 0 {
		t.Fatalf("No statements found")
	}

	stmt := result.Stmts[0].Stmt
	if selectStmt := stmt.GetSelectStmt(); selectStmt != nil {
		if selectStmt.TargetList != nil {
			for i, target := range selectStmt.TargetList {
				if resTarget := target.GetResTarget(); resTarget != nil {
					fmt.Printf("Target %d:\n", i)
					
					if windowFunc := resTarget.Val.GetWindowFunc(); windowFunc != nil {
						fmt.Printf("  Found WindowFunc!\n")
						
						// Use reflection to see available fields
						v := reflect.ValueOf(windowFunc).Elem()
						t := v.Type()
						
						for i := 0; i < v.NumField(); i++ {
							field := t.Field(i)
							value := v.Field(i)
							fmt.Printf("    %s: %v (type: %s)\n", field.Name, value.Interface(), field.Type)
						}
					}
				}
			}
		}
	}
}