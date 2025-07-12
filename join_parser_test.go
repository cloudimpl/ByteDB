package main

import (
	"fmt"
	"testing"
)

func TestJoinParser(t *testing.T) {
	parser := NewSQLParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
		expectJoins bool
		tableName   string
		tableAlias  string
	}{
		{
			name:        "Simple table with alias",
			sql:         "SELECT name FROM employees e;",
			expectError: false,
			expectJoins: false,
			tableName:   "employees",
			tableAlias:  "e",
		},
		{
			name:        "INNER JOIN with aliases",
			sql:         "SELECT e.name, d.department_name FROM employees e JOIN departments d ON e.department = d.name;",
			expectError: false,
			expectJoins: true,
			tableName:   "employees",
			tableAlias:  "e",
		},
		{
			name:        "LEFT JOIN",
			sql:         "SELECT e.name, d.budget FROM employees e LEFT JOIN departments d ON e.department = d.name;",
			expectError: false,
			expectJoins: true,
			tableName:   "employees",
			tableAlias:  "e",
		},
		{
			name:        "Qualified column names",
			sql:         "SELECT e.name, e.salary, d.budget FROM employees e JOIN departments d ON e.department = d.name;",
			expectError: false,
			expectJoins: true,
			tableName:   "employees",
			tableAlias:  "e",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := parser.Parse(test.sql)
			
			if test.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			
			if !test.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if test.expectError {
				return // Skip further tests for error cases
			}
			
			// Check basic table info
			if query.TableName != test.tableName {
				t.Errorf("Expected table name %s, got %s", test.tableName, query.TableName)
			}
			
			if query.TableAlias != test.tableAlias {
				t.Errorf("Expected table alias %s, got %s", test.tableAlias, query.TableAlias)
			}
			
			// Check JOIN presence
			if test.expectJoins != query.HasJoins {
				t.Errorf("Expected HasJoins=%v, got %v", test.expectJoins, query.HasJoins)
			}
			
			if test.expectJoins {
				if len(query.Joins) == 0 {
					t.Errorf("Expected JOIN clauses but found none")
				} else {
					// Check first JOIN
					join := query.Joins[0]
					fmt.Printf("JOIN: %s %s ON %s.%s = %s.%s\n", 
						joinTypeToString(join.Type), join.TableName, 
						join.Condition.LeftTable, join.Condition.LeftColumn,
						join.Condition.RightTable, join.Condition.RightColumn)
				}
			}
			
			// Check qualified column parsing
			for _, col := range query.Columns {
				if col.TableName != "" {
					fmt.Printf("Qualified column: %s.%s (alias: %s)\n", col.TableName, col.Name, col.Alias)
				} else {
					fmt.Printf("Unqualified column: %s (alias: %s)\n", col.Name, col.Alias)
				}
			}
			
			// Print full query structure for debugging
			fmt.Printf("Parsed query structure:\n%s\n", query.String())
		})
	}
}

func joinTypeToString(jt JoinType) string {
	switch jt {
	case INNER_JOIN:
		return "INNER JOIN"
	case LEFT_JOIN:
		return "LEFT JOIN"
	case RIGHT_JOIN:
		return "RIGHT JOIN"
	case FULL_OUTER_JOIN:
		return "FULL OUTER JOIN"
	default:
		return "UNKNOWN JOIN"
	}
}