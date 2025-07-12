package main

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type QueryType int

const (
	SELECT QueryType = iota
	INSERT
	UPDATE
	DELETE
	UNSUPPORTED
)

type Column struct {
	Name      string
	Alias     string
	TableName string       // For qualified columns like "e.name"
	Subquery  *ParsedQuery // For subquery columns like (SELECT COUNT(*) FROM ...)
}

type WhereCondition struct {
	Column    string
	Operator  string
	Value     interface{}   // Single value for =, !=, <, <=, >, >=, LIKE
	ValueList []interface{} // List of values for IN operator
	TableName string        // For qualified columns like "e.department"
	Subquery  *ParsedQuery  // For subquery conditions like IN (SELECT ...), EXISTS (SELECT ...)
	
	// For BETWEEN operator
	ValueFrom interface{} // Start value for BETWEEN
	ValueTo   interface{} // End value for BETWEEN
	
	// For complex logical operations
	LogicalOp string              // "AND", "OR", ""
	Left      *WhereCondition     // Left side of logical operation
	Right     *WhereCondition     // Right side of logical operation
	IsComplex bool                // True if this is a compound condition
}

type JoinType int

const (
	INNER_JOIN JoinType = iota
	LEFT_JOIN
	RIGHT_JOIN
	FULL_OUTER_JOIN
)

type JoinCondition struct {
	LeftColumn  string // Left table column
	RightColumn string // Right table column
	LeftTable   string // Left table alias/name
	RightTable  string // Right table alias/name
	Operator    string // Usually "=" but could be others
}

type JoinClause struct {
	Type       JoinType
	TableName  string
	TableAlias string
	Condition  JoinCondition
}

type TableReference struct {
	Name  string
	Alias string
}

type OrderByColumn struct {
	Column    string
	Direction string // "ASC" or "DESC"
}

type AggregateFunction struct {
	Function string // "COUNT", "SUM", "AVG", "MIN", "MAX"
	Column   string // "*" for COUNT(*), or specific column name
	Alias    string // Optional alias for the result
}

type ParsedQuery struct {
	Type        QueryType
	TableName   string
	TableAlias  string
	Columns     []Column
	Aggregates  []AggregateFunction
	GroupBy     []string
	Where       []WhereCondition
	OrderBy     []OrderByColumn
	Joins       []JoinClause
	Limit       int
	RawSQL      string
	IsAggregate bool // True if query contains aggregate functions
	HasJoins    bool // True if query contains JOIN clauses
	IsCorrelated bool // True if subquery references outer query columns
	CorrelatedColumns []string // List of outer query columns referenced
}

type SQLParser struct{}

func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

func (p *SQLParser) Parse(sql string) (*ParsedQuery, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found in SQL")
	}

	stmt := result.Stmts[0].Stmt
	query := &ParsedQuery{RawSQL: sql}

	if selectStmt := stmt.GetSelectStmt(); selectStmt != nil {
		return p.parseSelect(selectStmt, query)
	}
	
	query.Type = UNSUPPORTED
	return query, fmt.Errorf("unsupported statement type")
}

func (p *SQLParser) parseFromClause(fromClause []*pg_query.Node, query *ParsedQuery) error {
	if len(fromClause) == 0 {
		return fmt.Errorf("empty FROM clause")
	}

	fromNode := fromClause[0]
	
	// Check if it's a simple table reference or a JOIN
	if rangeVar := fromNode.GetRangeVar(); rangeVar != nil {
		// Simple table reference: FROM table_name [alias]
		query.TableName = rangeVar.Relname
		if rangeVar.Alias != nil {
			query.TableAlias = rangeVar.Alias.Aliasname
		}
		return nil
	}
	
	// Check if it's a JOIN expression
	if joinExpr := fromNode.GetJoinExpr(); joinExpr != nil {
		return p.parseJoinExpression(joinExpr, query)
	}
	
	return fmt.Errorf("unsupported FROM clause type")
}

func (p *SQLParser) parseJoinExpression(joinExpr *pg_query.JoinExpr, query *ParsedQuery) error {
	query.HasJoins = true
	
	// Parse left side (main table)
	if leftNode := joinExpr.Larg; leftNode != nil {
		if rangeVar := leftNode.GetRangeVar(); rangeVar != nil {
			query.TableName = rangeVar.Relname
			if rangeVar.Alias != nil {
				query.TableAlias = rangeVar.Alias.Aliasname
			}
		}
	}
	
	// Parse right side (joined table)
	var joinClause JoinClause
	if rightNode := joinExpr.Rarg; rightNode != nil {
		if rangeVar := rightNode.GetRangeVar(); rangeVar != nil {
			joinClause.TableName = rangeVar.Relname
			if rangeVar.Alias != nil {
				joinClause.TableAlias = rangeVar.Alias.Aliasname
			}
		}
	}
	
	// Parse JOIN type
	switch joinExpr.Jointype {
	case pg_query.JoinType_JOIN_INNER:
		joinClause.Type = INNER_JOIN
	case pg_query.JoinType_JOIN_LEFT:
		joinClause.Type = LEFT_JOIN
	case pg_query.JoinType_JOIN_RIGHT:
		joinClause.Type = RIGHT_JOIN
	case pg_query.JoinType_JOIN_FULL:
		joinClause.Type = FULL_OUTER_JOIN
	default:
		joinClause.Type = INNER_JOIN // Default to INNER JOIN
	}
	
	// Parse ON condition
	if joinExpr.Quals != nil {
		err := p.parseJoinCondition(joinExpr.Quals, &joinClause, query)
		if err != nil {
			return fmt.Errorf("failed to parse JOIN condition: %w", err)
		}
	}
	
	query.Joins = append(query.Joins, joinClause)
	return nil
}

func (p *SQLParser) parseJoinCondition(quals *pg_query.Node, joinClause *JoinClause, query *ParsedQuery) error {
	if aExpr := quals.GetAExpr(); aExpr != nil {
		// Parse the join condition (e.g., e.department = d.name)
		condition := JoinCondition{
			Operator: "=", // Default to equality
		}
		
		// Get operator
		if len(aExpr.Name) > 0 {
			if str := aExpr.Name[0].GetString_(); str != nil {
				condition.Operator = str.Sval
			}
		}
		
		// Parse left expression (e.g., e.department)
		if lexpr := aExpr.Lexpr; lexpr != nil {
			if columnRef := lexpr.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.LeftTable = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.LeftColumn = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if columnStr := columnRef.Fields[0].GetString_(); columnStr != nil {
						condition.LeftColumn = columnStr.Sval
					}
				}
			}
		}
		
		// Parse right expression (e.g., d.name)
		if rexpr := aExpr.Rexpr; rexpr != nil {
			if columnRef := rexpr.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.RightTable = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.RightColumn = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if columnStr := columnRef.Fields[0].GetString_(); columnStr != nil {
						condition.RightColumn = columnStr.Sval
					}
				}
			}
		}
		
		joinClause.Condition = condition
		return nil
	}
	
	return fmt.Errorf("unsupported JOIN condition type")
}

func (p *SQLParser) parseSelect(stmt *pg_query.SelectStmt, query *ParsedQuery) (*ParsedQuery, error) {
	query.Type = SELECT

	if stmt.FromClause != nil && len(stmt.FromClause) > 0 {
		err := p.parseFromClause(stmt.FromClause, query)
		if err != nil {
			return nil, fmt.Errorf("failed to parse FROM clause: %w", err)
		}
	}

	if stmt.TargetList != nil {
		for _, target := range stmt.TargetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				p.parseColumnTarget(resTarget, query)
			}
		}
	}

	if stmt.WhereClause != nil {
		p.parseWhere(stmt.WhereClause, query)
	}

	if stmt.GroupClause != nil {
		p.parseGroupBy(stmt.GroupClause, query)
	}

	if stmt.SortClause != nil {
		p.parseOrderBy(stmt.SortClause, query)
	}

	if stmt.LimitCount != nil {
		if aConst := stmt.LimitCount.GetAConst(); aConst != nil {
			if ival := aConst.GetIval(); ival != nil {
				query.Limit = int(ival.Ival)
			}
		}
	}

	// Post-process to detect correlated subqueries
	p.detectAllCorrelations(query)

	return query, nil
}

func (p *SQLParser) parseWhere(node *pg_query.Node, query *ParsedQuery) {
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		// Handle AND/OR expressions
		if boolExpr.Boolop == pg_query.BoolExprType_AND_EXPR || boolExpr.Boolop == pg_query.BoolExprType_OR_EXPR {
			if len(boolExpr.Args) >= 2 {
				condition := WhereCondition{
					IsComplex: true,
				}
				
				if boolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
					condition.LogicalOp = "AND"
				} else {
					condition.LogicalOp = "OR"
				}
				
				// Parse left and right conditions recursively
				condition.Left = p.parseWhereCondition(boolExpr.Args[0])
				condition.Right = p.parseWhereCondition(boolExpr.Args[1])
				
				// For more than 2 args, chain them with the same operator
				if len(boolExpr.Args) > 2 {
					for i := 2; i < len(boolExpr.Args); i++ {
						// Create a copy of the current condition to avoid circular reference
						leftCondition := condition
						condition = WhereCondition{
							IsComplex: true,
							LogicalOp: condition.LogicalOp,
							Left:      &leftCondition,
							Right:     p.parseWhereCondition(boolExpr.Args[i]),
						}
					}
				}
				
				query.Where = append(query.Where, condition)
				return
			}
		} else if boolExpr.Boolop == pg_query.BoolExprType_NOT_EXPR && len(boolExpr.Args) > 0 {
			// Handle NOT expressions (e.g., NOT IN)
			// Parse the inner expression and negate it
			firstArg := boolExpr.Args[0]
			if subLink := firstArg.GetSubLink(); subLink != nil {
				condition := WhereCondition{}
				if subLink.SubLinkType == pg_query.SubLinkType_ANY_SUBLINK {
					condition.Operator = "NOT IN"
					// Extract column from testexpr
					if subLink.Testexpr != nil {
						if columnRef := subLink.Testexpr.GetColumnRef(); columnRef != nil {
							if len(columnRef.Fields) > 0 {
								if str := columnRef.Fields[0].GetString_(); str != nil {
									condition.Column = str.Sval
								}
							}
						}
					}
					if subquery := p.parseSubquery(subLink); subquery != nil {
						condition.Subquery = subquery
					}
					if condition.Subquery != nil {
						query.Where = append(query.Where, condition)
					}
				}
			}
		}
	} else if aExpr := node.GetAExpr(); aExpr != nil {
		condition := WhereCondition{}
		
		// Check if this is a BETWEEN expression
		if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_BETWEEN || aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN {
			if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_BETWEEN {
				condition.Operator = "BETWEEN"
			} else {
				condition.Operator = "NOT BETWEEN"
			}
			
			// Get column name from left expression
			if lexpr := aExpr.Lexpr; lexpr != nil {
				if columnRef := lexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			
			// Get BETWEEN values from right expression (should be a list)
			if rexpr := aExpr.Rexpr; rexpr != nil {
				if aList := rexpr.GetList(); aList != nil && len(aList.Items) >= 2 {
					// Parse first value (FROM)
					if aConst := aList.Items[0].GetAConst(); aConst != nil {
						condition.ValueFrom = p.parseConstantNode(aConst)
					}
					// Parse second value (TO) 
					if aConst := aList.Items[1].GetAConst(); aConst != nil {
						condition.ValueTo = p.parseConstantNode(aConst)
					}
				}
			}
		} else if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_IN {
			condition.Operator = "IN"
			
			// Get column name from left expression
			if lexpr := aExpr.Lexpr; lexpr != nil {
				if columnRef := lexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			
			// Get list of values from right expression
			if rexpr := aExpr.Rexpr; rexpr != nil {
				if aList := rexpr.GetList(); aList != nil {
					p.parseValueList(aList.Items, &condition)
				} else if subLink := rexpr.GetSubLink(); subLink != nil {
					// Handle IN subquery
					if subquery := p.parseSubquery(subLink); subquery != nil {
						condition.Subquery = subquery
						condition.ValueList = nil // Clear value list for subquery
					}
				}
			}
		} else {
			// Regular operators (=, !=, <, <=, >, >=, LIKE)
			if len(aExpr.Name) > 0 {
				if str := aExpr.Name[0].GetString_(); str != nil {
					operator := str.Sval
					// PostgreSQL represents LIKE as "~~" operator
					if operator == "~~" {
						condition.Operator = "LIKE"
					} else {
						condition.Operator = operator
					}
				}
			}
			
			if lexpr := aExpr.Lexpr; lexpr != nil {
				if columnRef := lexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			
			if rexpr := aExpr.Rexpr; rexpr != nil {
				if aConst := rexpr.GetAConst(); aConst != nil {
					if ival := aConst.GetIval(); ival != nil {
						condition.Value = ival.Ival
					} else if sval := aConst.GetSval(); sval != nil {
						condition.Value = sval.Sval
					} else if fval := aConst.GetFval(); fval != nil {
						condition.Value = fval.Fval
					} else if bval := aConst.GetBoolval(); bval != nil {
						condition.Value = bval.Boolval
					}
				} else if subLink := rexpr.GetSubLink(); subLink != nil {
					// Handle scalar subquery (e.g., col = (SELECT ...))
					if subquery := p.parseSubquery(subLink); subquery != nil {
						condition.Subquery = subquery
					}
				}
			}
		}
		
		query.Where = append(query.Where, condition)
	} else if subLink := node.GetSubLink(); subLink != nil {
		// Handle EXISTS subqueries
		condition := WhereCondition{}
		if subLink.SubLinkType == pg_query.SubLinkType_EXISTS_SUBLINK {
			condition.Operator = "EXISTS"
			if subquery := p.parseSubquery(subLink); subquery != nil {
				condition.Subquery = subquery
			}
		} else if subLink.SubLinkType == pg_query.SubLinkType_ANY_SUBLINK {
			condition.Operator = "IN"
			// For IN subqueries, we need to extract the column from the testexpr
			if subLink.Testexpr != nil {
				if columnRef := subLink.Testexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			if subquery := p.parseSubquery(subLink); subquery != nil {
				condition.Subquery = subquery
			}
		}
		
		if condition.Subquery != nil {
			query.Where = append(query.Where, condition)
		}
	}
}

// parseWhereCondition parses a single WHERE condition node
func (p *SQLParser) parseWhereCondition(node *pg_query.Node) *WhereCondition {
	condition := &WhereCondition{}
	
	// Handle nested BoolExpr (for parenthesized expressions)
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		// Handle AND/OR expressions
		if boolExpr.Boolop == pg_query.BoolExprType_AND_EXPR || boolExpr.Boolop == pg_query.BoolExprType_OR_EXPR {
			if len(boolExpr.Args) >= 2 {
				condition.IsComplex = true
				
				if boolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
					condition.LogicalOp = "AND"
				} else {
					condition.LogicalOp = "OR"
				}
				
				// Parse left and right conditions recursively
				condition.Left = p.parseWhereCondition(boolExpr.Args[0])
				condition.Right = p.parseWhereCondition(boolExpr.Args[1])
				
				// For more than 2 args, chain them with the same operator
				if len(boolExpr.Args) > 2 {
					for i := 2; i < len(boolExpr.Args); i++ {
						// Create a copy of the current condition to avoid circular reference
						leftCondition := *condition
						condition = &WhereCondition{
							IsComplex: true,
							LogicalOp: condition.LogicalOp,
							Left:      &leftCondition,
							Right:     p.parseWhereCondition(boolExpr.Args[i]),
						}
					}
				}
				
				return condition
			}
		}
		return condition
	}
	
	if aExpr := node.GetAExpr(); aExpr != nil {
		// Handle BETWEEN expressions
		if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_BETWEEN || aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN {
			if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_BETWEEN {
				condition.Operator = "BETWEEN"
			} else {
				condition.Operator = "NOT BETWEEN"
			}
			
			// Get column name from left expression
			if lexpr := aExpr.Lexpr; lexpr != nil {
				if columnRef := lexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			
			// Get BETWEEN values from right expression (should be a list)
			if rexpr := aExpr.Rexpr; rexpr != nil {
				if aList := rexpr.GetList(); aList != nil && len(aList.Items) >= 2 {
					// Parse first value (FROM)
					if aConst := aList.Items[0].GetAConst(); aConst != nil {
						condition.ValueFrom = p.parseConstantNode(aConst)
					}
					// Parse second value (TO) 
					if aConst := aList.Items[1].GetAConst(); aConst != nil {
						condition.ValueTo = p.parseConstantNode(aConst)
					}
				}
			}
			return condition
		}
		
		// Handle IN expressions
		if aExpr.Kind == pg_query.A_Expr_Kind_AEXPR_IN {
			condition.Operator = "IN"
			
			// Get column name from left expression
			if lexpr := aExpr.Lexpr; lexpr != nil {
				if columnRef := lexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			
			// Get list of values from right expression
			if rexpr := aExpr.Rexpr; rexpr != nil {
				if aList := rexpr.GetList(); aList != nil {
					for _, item := range aList.Items {
						if aConst := item.GetAConst(); aConst != nil {
							condition.ValueList = append(condition.ValueList, p.parseConstantNode(aConst))
						}
					}
				} else if subLink := rexpr.GetSubLink(); subLink != nil {
					// Handle IN subquery
					if subquery := p.parseSubquery(subLink); subquery != nil {
						condition.Subquery = subquery
						condition.ValueList = nil // Clear value list for subquery
					}
				}
			}
			return condition
		}
		
		// Handle NULL checks
		if len(aExpr.Name) > 0 {
			if str := aExpr.Name[0].GetString_(); str != nil {
				operator := str.Sval
				if operator == "IS" {
					// Check if this is IS NULL or IS NOT NULL
					if rexpr := aExpr.Rexpr; rexpr != nil {
						if nullTest := rexpr.GetNullTest(); nullTest != nil {
							condition.Operator = "IS NULL"
							if nullTest.Nulltesttype == pg_query.NullTestType_IS_NOT_NULL {
								condition.Operator = "IS NOT NULL"
							}
							
							// Get column from left expression
							if lexpr := aExpr.Lexpr; lexpr != nil {
								if columnRef := lexpr.GetColumnRef(); columnRef != nil {
									if len(columnRef.Fields) > 0 {
										if str := columnRef.Fields[0].GetString_(); str != nil {
											condition.Column = str.Sval
										}
									}
								}
							}
							return condition
						}
					}
				}
				
				// Regular operators (=, !=, <, <=, >, >=, LIKE)
				if operator == "~~" {
					condition.Operator = "LIKE"
				} else {
					condition.Operator = operator
				}
			}
		}
		
		// Get column name from left expression
		if lexpr := aExpr.Lexpr; lexpr != nil {
			if columnRef := lexpr.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) > 0 {
					if str := columnRef.Fields[0].GetString_(); str != nil {
						condition.Column = str.Sval
					}
				}
			}
		}
		
		// Get value from right expression
		if rexpr := aExpr.Rexpr; rexpr != nil {
			if aConst := rexpr.GetAConst(); aConst != nil {
				condition.Value = p.parseConstantNode(aConst)
			} else if subLink := rexpr.GetSubLink(); subLink != nil {
				// Handle scalar subquery (e.g., col = (SELECT ...))
				if subquery := p.parseSubquery(subLink); subquery != nil {
					condition.Subquery = subquery
				}
			}
		}
		
		return condition
	} else if subLink := node.GetSubLink(); subLink != nil {
		// Handle EXISTS subqueries
		if subLink.SubLinkType == pg_query.SubLinkType_EXISTS_SUBLINK {
			condition.Operator = "EXISTS"
			if subquery := p.parseSubquery(subLink); subquery != nil {
				condition.Subquery = subquery
			}
		} else if subLink.SubLinkType == pg_query.SubLinkType_ANY_SUBLINK {
			condition.Operator = "IN"
			// For IN subqueries, we need to extract the column from the testexpr
			if subLink.Testexpr != nil {
				if columnRef := subLink.Testexpr.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				}
			}
			if subquery := p.parseSubquery(subLink); subquery != nil {
				condition.Subquery = subquery
			}
		}
		return condition
	}
	
	return condition
}

// parseConstantNode extracts value from AConst node
func (p *SQLParser) parseConstantNode(aConst *pg_query.A_Const) interface{} {
	if ival := aConst.GetIval(); ival != nil {
		return ival.Ival
	} else if sval := aConst.GetSval(); sval != nil {
		return sval.Sval
	} else if fval := aConst.GetFval(); fval != nil {
		return fval.Fval
	} else if bval := aConst.GetBoolval(); bval != nil {
		return bval.Boolval
	}
	return nil
}

func (p *SQLParser) parseSubquery(subLink *pg_query.SubLink) *ParsedQuery {
	if subLink.Subselect == nil {
		return nil
	}
	
	// Parse the subquery directly from the AST node
	if selectStmt := subLink.Subselect.GetSelectStmt(); selectStmt != nil {
		subquery := &ParsedQuery{
			Type:        SELECT,
			IsAggregate: false,
			HasJoins:    false,
			IsCorrelated: false,
		}
		
		// Parse the subquery components
		p.parseSelectStatement(selectStmt, subquery)
		return subquery
	}
	
	return nil
}

// parseSubqueryWithOuter parses a subquery and detects correlation with outer query
func (p *SQLParser) parseSubqueryWithOuter(subLink *pg_query.SubLink, outerQuery *ParsedQuery) *ParsedQuery {
	subquery := p.parseSubquery(subLink)
	if subquery == nil {
		return nil
	}
	
	// Detect correlation by checking if subquery references outer query tables
	p.detectCorrelation(subquery, outerQuery)
	return subquery
}

// detectCorrelation checks if subquery references outer query tables/aliases
func (p *SQLParser) detectCorrelation(subquery *ParsedQuery, outerQuery *ParsedQuery) {
	outerTables := make(map[string]bool)
	
	// Add outer query table names and aliases
	if outerQuery.TableName != "" {
		outerTables[outerQuery.TableName] = true
	}
	if outerQuery.TableAlias != "" {
		outerTables[outerQuery.TableAlias] = true
	}
	
	// Add JOIN table names and aliases
	for _, join := range outerQuery.Joins {
		if join.TableName != "" {
			outerTables[join.TableName] = true
		}
		if join.TableAlias != "" {
			outerTables[join.TableAlias] = true
		}
	}
	
	// Check subquery WHERE conditions for references to outer tables
	for _, condition := range subquery.Where {
		if condition.TableName != "" && outerTables[condition.TableName] {
			subquery.IsCorrelated = true
			subquery.CorrelatedColumns = append(subquery.CorrelatedColumns, condition.TableName+"."+condition.Column)
		}
	}
	
	// Check subquery columns for references to outer tables
	for _, column := range subquery.Columns {
		if column.TableName != "" && outerTables[column.TableName] {
			subquery.IsCorrelated = true
			subquery.CorrelatedColumns = append(subquery.CorrelatedColumns, column.TableName+"."+column.Name)
		}
	}
}

func (p *SQLParser) parseSelectStatement(selectStmt *pg_query.SelectStmt, query *ParsedQuery) {
	// Parse target list (SELECT columns)
	for _, target := range selectStmt.TargetList {
		if resTarget := target.GetResTarget(); resTarget != nil {
			p.parseColumnTarget(resTarget, query)
		}
	}
	
	// Parse FROM clause
	if len(selectStmt.FromClause) > 0 {
		p.parseFromClause([]*pg_query.Node{selectStmt.FromClause[0]}, query)
	}
	
	// Parse WHERE clause
	if selectStmt.WhereClause != nil {
		p.parseWhere(selectStmt.WhereClause, query)
	}
	
	// Parse GROUP BY
	if len(selectStmt.GroupClause) > 0 {
		p.parseGroupBy(selectStmt.GroupClause, query)
	}
	
	// Parse ORDER BY
	if len(selectStmt.SortClause) > 0 {
		p.parseOrderBy(selectStmt.SortClause, query)
	}
	
	// Parse LIMIT
	if selectStmt.LimitCount != nil {
		if aConst := selectStmt.LimitCount.GetAConst(); aConst != nil {
			if ival := aConst.GetIval(); ival != nil {
				query.Limit = int(ival.Ival)
			}
		}
	}
}

func (p *SQLParser) parseColumnTarget(resTarget *pg_query.ResTarget, query *ParsedQuery) {
	// Check if this is an aggregate function call
	if funcCall := resTarget.Val.GetFuncCall(); funcCall != nil {
		agg := p.parseAggregateFunction(funcCall, resTarget.Name)
		if agg != nil {
			query.Aggregates = append(query.Aggregates, *agg)
			query.IsAggregate = true
			return
		}
	}
	
	// Regular column
	column := Column{}
	
	// Get column name
	if columnRef := resTarget.Val.GetColumnRef(); columnRef != nil {
		if len(columnRef.Fields) > 0 {
			if str := columnRef.Fields[0].GetString_(); str != nil {
				column.Name = str.Sval
			} else if columnRef.Fields[0].GetAStar() != nil {
				column.Name = "*"
			}
		}
		
		// Handle qualified column names (table.column)
		if len(columnRef.Fields) > 1 {
			if str := columnRef.Fields[1].GetString_(); str != nil {
				column.TableName = column.Name
				column.Name = str.Sval
			}
		}
	} else if aConst := resTarget.Val.GetAConst(); aConst != nil {
		// Handle constants like SELECT 1, SELECT 'hello', etc.
		if ival := aConst.GetIval(); ival != nil {
			column.Name = fmt.Sprintf("%d", ival.Ival)
		} else if sval := aConst.GetSval(); sval != nil {
			column.Name = sval.Sval
		} else if fval := aConst.GetFval(); fval != nil {
			column.Name = fval.Fval
		} else {
			column.Name = "const"
		}
	} else if subLink := resTarget.Val.GetSubLink(); subLink != nil {
		// Handle subqueries in SELECT clause like (SELECT COUNT(*) FROM ...)
		if subquery := p.parseSubquery(subLink); subquery != nil {
			column.Subquery = subquery
			if resTarget.Name != "" {
				column.Name = resTarget.Name
				column.Alias = resTarget.Name
			} else {
				column.Name = "subquery"
			}
		}
	}
	
	// Get alias
	if resTarget.Name != "" {
		column.Alias = resTarget.Name
	} else if column.Name == "" {
		column.Name = "column"
	}
	
	query.Columns = append(query.Columns, column)
}

func (p *SQLParser) parseInValues(node *pg_query.Node, condition *WhereCondition) {
	if aList := node.GetList(); aList != nil {
		p.parseValueList(aList.Items, condition)
	}
}

func (p *SQLParser) parseValueList(items []*pg_query.Node, condition *WhereCondition) {
	for _, item := range items {
		if aConst := item.GetAConst(); aConst != nil {
			if ival := aConst.GetIval(); ival != nil {
				condition.ValueList = append(condition.ValueList, ival.Ival)
			} else if sval := aConst.GetSval(); sval != nil {
				condition.ValueList = append(condition.ValueList, sval.Sval)
			} else if fval := aConst.GetFval(); fval != nil {
				condition.ValueList = append(condition.ValueList, fval.Fval)
			} else if bval := aConst.GetBoolval(); bval != nil {
				condition.ValueList = append(condition.ValueList, bval.Boolval)
			}
		}
	}
}

func (p *SQLParser) parseAggregateFunction(funcCall *pg_query.FuncCall, alias string) *AggregateFunction {
	if len(funcCall.Funcname) == 0 {
		return nil
	}
	
	funcName := ""
	if str := funcCall.Funcname[0].GetString_(); str != nil {
		funcName = strings.ToUpper(str.Sval)
	}
	
	// Check if it's a supported aggregate function
	supportedFuncs := map[string]bool{
		"COUNT": true,
		"SUM":   true,
		"AVG":   true,
		"MIN":   true,
		"MAX":   true,
	}
	
	if !supportedFuncs[funcName] {
		return nil
	}
	
	agg := &AggregateFunction{
		Function: funcName,
		Alias:    alias,
	}
	
	// Parse function arguments
	if len(funcCall.Args) == 0 {
		// Functions like COUNT() without arguments
		agg.Column = "*"
	} else {
		arg := funcCall.Args[0]
		if columnRef := arg.GetColumnRef(); columnRef != nil {
			if len(columnRef.Fields) > 0 {
				if str := columnRef.Fields[0].GetString_(); str != nil {
					agg.Column = str.Sval
				} else if columnRef.Fields[0].GetAStar() != nil {
					agg.Column = "*"
				}
			}
		} else if arg.GetAStar() != nil {
			agg.Column = "*"
		}
	}
	
	// If no alias provided, generate one
	if agg.Alias == "" {
		if agg.Column == "*" {
			agg.Alias = strings.ToLower(funcName)
		} else {
			agg.Alias = strings.ToLower(funcName) + "_" + agg.Column
		}
	}
	
	return agg
}

func (p *SQLParser) parseGroupBy(groupClause []*pg_query.Node, query *ParsedQuery) {
	for _, groupNode := range groupClause {
		if columnRef := groupNode.GetColumnRef(); columnRef != nil {
			if len(columnRef.Fields) > 0 {
				if str := columnRef.Fields[0].GetString_(); str != nil {
					query.GroupBy = append(query.GroupBy, str.Sval)
				}
			}
		}
	}
}

func (p *SQLParser) parseOrderBy(sortClause []*pg_query.Node, query *ParsedQuery) {
	for _, sortNode := range sortClause {
		if sortBy := sortNode.GetSortBy(); sortBy != nil {
			orderCol := OrderByColumn{
				Direction: "ASC", // Default direction
			}
			
			// Get column name
			if sortBy.Node != nil {
				if columnRef := sortBy.Node.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							orderCol.Column = str.Sval
						}
					}
				}
			}
			
			// Get sort direction
			if sortBy.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
				orderCol.Direction = "DESC"
			}
			
			if orderCol.Column != "" {
				query.OrderBy = append(query.OrderBy, orderCol)
			}
		}
	}
}

// GetRequiredColumns analyzes the query and returns all columns that need to be read
func (q *ParsedQuery) GetRequiredColumns() []string {
	requiredCols := make(map[string]bool)
	
	// Add columns from SELECT clause
	for _, col := range q.Columns {
		if col.Name == "*" {
			// If SELECT *, we need all columns - return empty slice to indicate this
			return []string{}
		}
		// Skip constant columns
		if !q.isConstantColumn(col.Name) {
			requiredCols[col.Name] = true
		}
	}
	
	// Add columns from aggregate functions
	for _, agg := range q.Aggregates {
		if agg.Column != "*" {
			requiredCols[agg.Column] = true
		}
	}
	
	// Add columns from WHERE clause
	for _, where := range q.Where {
		q.addWhereConditionColumns(where, requiredCols)
	}
	
	// Add columns from GROUP BY clause
	for _, groupCol := range q.GroupBy {
		requiredCols[groupCol] = true
	}
	
	// Add columns from ORDER BY clause
	for _, orderCol := range q.OrderBy {
		requiredCols[orderCol.Column] = true
	}
	
	// Convert map to slice
	var result []string
	for col := range requiredCols {
		result = append(result, col)
	}
	
	return result
}

// addWhereConditionColumns recursively adds all columns from a WHERE condition to the required columns map
func (q *ParsedQuery) addWhereConditionColumns(where WhereCondition, requiredCols map[string]bool) {
	// Handle complex conditions (AND/OR)
	if where.IsComplex {
		if where.Left != nil {
			q.addWhereConditionColumns(*where.Left, requiredCols)
		}
		if where.Right != nil {
			q.addWhereConditionColumns(*where.Right, requiredCols)
		}
		return
	}
	
	// Handle simple conditions
	if where.Column != "" {
		requiredCols[where.Column] = true
	}
}

func (q *ParsedQuery) isConstantColumn(name string) bool {
	// Check if this looks like a constant value
	if name == "const" || name == "column" {
		return true
	}
	// Check if it's a numeric constant
	if len(name) > 0 && (name[0] >= '0' && name[0] <= '9') {
		return true
	}
	// Check if it's a string constant (starts and ends with quotes)
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		return true
	}
	return false
}

func (q *ParsedQuery) String() string {
	result := fmt.Sprintf("Query Type: %v\n", q.Type)
	result += fmt.Sprintf("Table: %s\n", q.TableName)
	if len(q.Columns) > 0 {
		result += fmt.Sprintf("Columns: %v\n", q.Columns)
	}
	if len(q.Aggregates) > 0 {
		result += fmt.Sprintf("Aggregates: %v\n", q.Aggregates)
	}
	if len(q.GroupBy) > 0 {
		result += fmt.Sprintf("Group By: %v\n", q.GroupBy)
	}
	if len(q.Where) > 0 {
		result += fmt.Sprintf("Where: %v\n", q.Where)
	}
	if len(q.OrderBy) > 0 {
		result += fmt.Sprintf("Order By: %v\n", q.OrderBy)
	}
	if q.Limit > 0 {
		result += fmt.Sprintf("Limit: %d\n", q.Limit)
	}
	return result
}

// detectAllCorrelations scans the entire query to detect correlated subqueries
func (p *SQLParser) detectAllCorrelations(query *ParsedQuery) {
	// Check all WHERE conditions for subqueries
	for i := range query.Where {
		if query.Where[i].Subquery != nil {
			p.detectCorrelation(query.Where[i].Subquery, query)
		}
	}
	
	// Check column subqueries (for future SELECT clause subqueries)
	for i := range query.Columns {
		if query.Columns[i].Subquery != nil {
			p.detectCorrelation(query.Columns[i].Subquery, query)
		}
	}
}