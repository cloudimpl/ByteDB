package core

import (
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type QueryType int

const (
	SELECT QueryType = iota
	INSERT
	UPDATE
	DELETE
	EXPLAIN
	UNION
	UNSUPPORTED
)

// String returns the string representation of QueryType
func (qt QueryType) String() string {
	switch qt {
	case SELECT:
		return "SELECT"
	case INSERT:
		return "INSERT"
	case UPDATE:
		return "UPDATE"
	case DELETE:
		return "DELETE"
	case EXPLAIN:
		return "EXPLAIN"
	case UNION:
		return "UNION"
	case UNSUPPORTED:
		return "UNSUPPORTED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", qt)
	}
}

type Column struct {
	Name       string
	Alias      string
	TableName  string          // For qualified columns like "e.name"
	Subquery   *ParsedQuery    // For subquery columns like (SELECT COUNT(*) FROM ...)
	WindowFunc *WindowFunction // For window function columns like ROW_NUMBER() OVER (...)
	CaseExpr   *CaseExpression // For CASE expressions like CASE WHEN ... THEN ... END
	Function   *FunctionCall   // For function calls like UPPER(name), CONCAT(first, ' ', last)
}

type WhereCondition struct {
	Column    string
	Operator  string
	Value     interface{}   // Single value for =, !=, <, <=, >, >=, LIKE
	ValueList []interface{} // List of values for IN operator
	TableName string        // For qualified columns like "e.department"
	Subquery  *ParsedQuery  // For subquery conditions like IN (SELECT ...), EXISTS (SELECT ...)

	// For column-to-column comparisons (e.g., e2.department = e.department)
	ValueColumn    string // Column name on right side for column comparisons
	ValueTableName string // Table name for right side column in qualified comparisons

	// For BETWEEN operator
	ValueFrom interface{} // Start value for BETWEEN
	ValueTo   interface{} // End value for BETWEEN

	// For CASE expressions in WHERE clauses
	CaseExpr      *CaseExpression // For CASE expressions on left side
	ValueCaseExpr *CaseExpression // For CASE expressions on right side

	// For function calls in WHERE clauses
	Function      *FunctionCall // For function calls on left side
	ValueFunction *FunctionCall // For function calls on right side

	// For complex logical operations
	LogicalOp string          // "AND", "OR", ""
	Left      *WhereCondition // Left side of logical operation
	Right     *WhereCondition // Right side of logical operation
	IsComplex bool            // True if this is a compound condition
}

type JoinType int

const (
	INNER_JOIN JoinType = iota
	LEFT_JOIN
	RIGHT_JOIN
	FULL_OUTER_JOIN
)

// String returns the string representation of JoinType
func (jt JoinType) String() string {
	switch jt {
	case INNER_JOIN:
		return "INNER"
	case LEFT_JOIN:
		return "LEFT"
	case RIGHT_JOIN:
		return "RIGHT"
	case FULL_OUTER_JOIN:
		return "FULL OUTER"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", jt)
	}
}

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

type WindowFunction struct {
	Function   string              // "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", etc.
	Column     string              // Column name for LAG/LEAD, empty for ROW_NUMBER/RANK
	Arguments  []interface{}       // Arguments for functions like LAG(column, offset, default)
	Alias      string              // Optional alias for the result
	WindowSpec WindowSpecification // OVER clause specification
}

type WindowSpecification struct {
	PartitionBy []string        // PARTITION BY columns
	OrderBy     []OrderByColumn // ORDER BY columns within the window
	FrameStart  string          // Frame start specification (for future use)
	FrameEnd    string          // Frame end specification (for future use)
}

type CaseExpression struct {
	WhenClauses []WhenClause     // WHEN condition THEN result pairs
	ElseClause  *ExpressionValue // ELSE result (optional)
	Alias       string           // Optional alias for the CASE expression
}

type WhenClause struct {
	Condition WhereCondition  // WHEN condition
	Result    ExpressionValue // THEN result
}

type ExpressionValue struct {
	Type         string          // "literal", "column", "subquery", "case"
	LiteralValue interface{}     // For literal values (string, int, float, bool)
	ColumnName   string          // For column references
	TableName    string          // For qualified column references
	Subquery     *ParsedQuery    // For subquery expressions
	CaseExpr     *CaseExpression // For nested CASE expressions
}

type CTE struct {
	Name        string       // CTE name
	Query       *ParsedQuery // The CTE's query
	ColumnNames []string     // Optional column name list
	IsRecursive bool         // For RECURSIVE CTEs (future enhancement)
}

type UnionQuery struct {
	Query    *ParsedQuery // The query to union
	UnionAll bool         // True for UNION ALL, false for UNION
}

type ParsedQuery struct {
	Type              QueryType
	TableName         string
	TableAlias        string
	Columns           []Column
	Aggregates        []AggregateFunction
	WindowFuncs       []WindowFunction
	GroupBy           []string
	Where             []WhereCondition
	OrderBy           []OrderByColumn
	Joins             []JoinClause
	Limit             int
	RawSQL            string
	IsAggregate       bool     // True if query contains aggregate functions
	HasJoins          bool     // True if query contains JOIN clauses
	HasWindowFuncs    bool     // True if query contains window functions
	IsCorrelated      bool     // True if subquery references outer query columns
	CorrelatedColumns []string // List of outer query columns referenced
	CTEs              []CTE    // Common Table Expressions (WITH clauses)
	IsSubquery        bool     // True if this query is being executed as a subquery

	// EXPLAIN specific fields
	ExplainOptions *ExplainOptions // Options for EXPLAIN command
	ExplainQuery   *ParsedQuery    // The query being explained

	// UNION specific fields
	UnionQueries []UnionQuery // List of queries to union
	HasUnion     bool         // True if query contains UNION/UNION ALL
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

	if explainStmt := stmt.GetExplainStmt(); explainStmt != nil {
		return p.parseExplain(explainStmt, query)
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
		// Simple table reference: FROM [catalog.][schema.]table_name [alias]
		
		// Build fully qualified name if catalog/schema are specified
		var tableParts []string
		if rangeVar.Catalogname != "" {
			tableParts = append(tableParts, rangeVar.Catalogname)
		}
		if rangeVar.Schemaname != "" {
			tableParts = append(tableParts, rangeVar.Schemaname)
		}
		tableParts = append(tableParts, rangeVar.Relname)
		
		// Join parts with dots to create the table identifier
		query.TableName = strings.Join(tableParts, ".")
		
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
			// Build fully qualified name if catalog/schema are specified
			var tableParts []string
			if rangeVar.Catalogname != "" {
				tableParts = append(tableParts, rangeVar.Catalogname)
			}
			if rangeVar.Schemaname != "" {
				tableParts = append(tableParts, rangeVar.Schemaname)
			}
			tableParts = append(tableParts, rangeVar.Relname)
			
			query.TableName = strings.Join(tableParts, ".")
			if rangeVar.Alias != nil {
				query.TableAlias = rangeVar.Alias.Aliasname
			}
		}
	}

	// Parse right side (joined table)
	var joinClause JoinClause
	if rightNode := joinExpr.Rarg; rightNode != nil {
		if rangeVar := rightNode.GetRangeVar(); rangeVar != nil {
			// Build fully qualified name if catalog/schema are specified
			var tableParts []string
			if rangeVar.Catalogname != "" {
				tableParts = append(tableParts, rangeVar.Catalogname)
			}
			if rangeVar.Schemaname != "" {
				tableParts = append(tableParts, rangeVar.Schemaname)
			}
			tableParts = append(tableParts, rangeVar.Relname)
			
			joinClause.TableName = strings.Join(tableParts, ".")
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

func (p *SQLParser) parseWithClause(withClause *pg_query.WithClause, query *ParsedQuery) error {
	for _, cteNode := range withClause.Ctes {
		if commonTableExpr := cteNode.GetCommonTableExpr(); commonTableExpr != nil {
			cte := CTE{
				Name:        commonTableExpr.Ctename,
				IsRecursive: withClause.Recursive,
			}

			// Parse optional column names
			if commonTableExpr.Aliascolnames != nil {
				for _, colNode := range commonTableExpr.Aliascolnames {
					if str := colNode.GetString_(); str != nil {
						cte.ColumnNames = append(cte.ColumnNames, str.Sval)
					}
				}
			}

			// Parse the CTE query
			if commonTableExpr.Ctequery != nil {
				if selectStmt := commonTableExpr.Ctequery.GetSelectStmt(); selectStmt != nil {
					cteQuery := &ParsedQuery{
						Type: SELECT,
					}
					_, err := p.parseSelect(selectStmt, cteQuery)
					if err != nil {
						return fmt.Errorf("failed to parse CTE '%s' query: %w", cte.Name, err)
					}
					cte.Query = cteQuery
				}
			}

			query.CTEs = append(query.CTEs, cte)
		}
	}

	return nil
}

func (p *SQLParser) parseSelect(stmt *pg_query.SelectStmt, query *ParsedQuery) (*ParsedQuery, error) {
	query.Type = SELECT

	// Parse WITH clause (CTEs) if present
	if stmt.WithClause != nil {
		err := p.parseWithClause(stmt.WithClause, query)
		if err != nil {
			return nil, fmt.Errorf("failed to parse WITH clause: %w", err)
		}
	}

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
		// Ensure unique column names for aggregates
		p.EnsureUniqueAggregateAliases(query)
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

	// Check for UNION operations
	if stmt.Op != pg_query.SetOperation_SETOP_NONE {
		query.HasUnion = true
		query.Type = UNION

		// For UNION queries, we need to parse left and right separately
		var leftQuery *ParsedQuery
		var rightQuery *ParsedQuery

		// Parse left side
		if stmt.Larg != nil {
			leftQuery = &ParsedQuery{}
			_, err := p.parseSelect(stmt.Larg, leftQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to parse UNION left side: %w", err)
			}
		} else {
			// If no Larg, the current query data is the left side
			leftQuery = &ParsedQuery{
				Type:        SELECT,
				TableName:   query.TableName,
				TableAlias:  query.TableAlias,
				Columns:     query.Columns,
				Aggregates:  query.Aggregates,
				WindowFuncs: query.WindowFuncs,
				GroupBy:     query.GroupBy,
				Where:       query.Where,
				Joins:       query.Joins,
				CTEs:        query.CTEs,
			}
		}

		// Parse right side
		if stmt.Rarg != nil {
			rightQuery = &ParsedQuery{}
			_, err := p.parseSelect(stmt.Rarg, rightQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to parse UNION right side: %w", err)
			}
		}

		// Clear the current query and set it up as a UNION
		query.TableName = ""
		query.TableAlias = ""
		query.Columns = nil
		query.Aggregates = nil
		query.WindowFuncs = nil
		query.GroupBy = nil
		query.Where = nil
		query.Joins = nil

		// Add both sides to UnionQueries
		if leftQuery != nil {
			if leftQuery.HasUnion {
				// Left side is also a UNION, add all its queries
				query.UnionQueries = append(query.UnionQueries, leftQuery.UnionQueries...)
			} else {
				// Left side is a simple SELECT
				query.UnionQueries = append(query.UnionQueries, UnionQuery{
					Query:    leftQuery,
					UnionAll: stmt.All, // Use the UNION/UNION ALL setting from the statement
				})
			}
		}

		if rightQuery != nil {
			if rightQuery.HasUnion {
				// Right side is also a UNION, add all its queries
				for _, uq := range rightQuery.UnionQueries {
					uq.UnionAll = stmt.All // Apply the current UNION/UNION ALL setting
					query.UnionQueries = append(query.UnionQueries, uq)
				}
			} else {
				// Right side is a simple SELECT
				query.UnionQueries = append(query.UnionQueries, UnionQuery{
					Query:    rightQuery,
					UnionAll: stmt.All,
				})
			}
		}

		// ORDER BY and LIMIT apply to the entire UNION result
		// These are already parsed into query.OrderBy and query.Limit
	}

	// Post-process to detect correlated subqueries
	p.detectAllCorrelations(query)

	return query, nil
}

func (p *SQLParser) parseExplain(stmt *pg_query.ExplainStmt, query *ParsedQuery) (*ParsedQuery, error) {
	query.Type = EXPLAIN
	query.ExplainOptions = &ExplainOptions{
		Costs:  true,              // Default to showing costs
		Format: ExplainFormatText, // Default format
	}

	// Parse EXPLAIN options
	for _, option := range stmt.Options {
		if defElem := option.GetDefElem(); defElem != nil {
			optName := defElem.Defname
			switch strings.ToLower(optName) {
			case "analyze":
				query.ExplainOptions.Analyze = true
			case "verbose":
				query.ExplainOptions.Verbose = true
			case "costs":
				if defElem.Arg != nil {
					if boolVal := defElem.Arg.GetBoolean(); boolVal != nil {
						query.ExplainOptions.Costs = boolVal.Boolval
					} else if aConst := defElem.Arg.GetAConst(); aConst != nil {
						// Handle FALSE as a string
						if str := aConst.GetSval(); str != nil && strings.ToUpper(str.Sval) == "FALSE" {
							query.ExplainOptions.Costs = false
						}
					}
				}
			case "buffers":
				query.ExplainOptions.Buffers = true
			case "format":
				if strVal := defElem.Arg.GetString_(); strVal != nil {
					switch strings.ToUpper(strVal.Sval) {
					case "JSON":
						query.ExplainOptions.Format = ExplainFormatJSON
					case "YAML":
						query.ExplainOptions.Format = ExplainFormatYAML
					default:
						query.ExplainOptions.Format = ExplainFormatText
					}
				}
			}
		}
	}

	// Parse the query being explained
	if stmt.Query != nil {
		explainedQuery := &ParsedQuery{}
		if selectStmt := stmt.Query.GetSelectStmt(); selectStmt != nil {
			var err error
			explainedQuery, err = p.parseSelect(selectStmt, explainedQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to parse explained query: %w", err)
			}
		} else {
			return nil, fmt.Errorf("only SELECT statements can be explained")
		}
		query.ExplainQuery = explainedQuery
	}

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
							if len(columnRef.Fields) >= 2 {
								// Qualified column: table.column
								if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
									condition.TableName = tableStr.Sval
								}
								if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
									condition.Column = columnStr.Sval
								}
							} else if len(columnRef.Fields) == 1 {
								// Unqualified column
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
	} else if node.GetAExpr() != nil {
		// Use parseWhereCondition to handle all the logic including functions
		condition := p.parseWhereCondition(node)
		if condition != nil {
			query.Where = append(query.Where, *condition)
		}
	} else if node.GetSubLink() != nil {
		// Use parseWhereCondition to handle sublinks
		condition := p.parseWhereCondition(node)
		if condition != nil {
			query.Where = append(query.Where, *condition)
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
					if len(columnRef.Fields) >= 2 {
						// Qualified column: table.column
						if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
							condition.TableName = tableStr.Sval
						}
						if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
							condition.Column = columnStr.Sval
						}
					} else if len(columnRef.Fields) == 1 {
						// Unqualified column
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
					if len(columnRef.Fields) >= 2 {
						// Qualified column: table.column
						if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
							condition.TableName = tableStr.Sval
						}
						if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
							condition.Column = columnStr.Sval
						}
					} else if len(columnRef.Fields) == 1 {
						// Unqualified column
						if str := columnRef.Fields[0].GetString_(); str != nil {
							condition.Column = str.Sval
						}
					}
				} else if funcCall := lexpr.GetFuncCall(); funcCall != nil {
					// Handle function call on left side
					if fn := p.parseFunctionCall(funcCall); fn != nil {
						condition.Function = fn
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
									if len(columnRef.Fields) >= 2 {
										// Qualified column: table.column
										if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
											condition.TableName = tableStr.Sval
										}
										if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
											condition.Column = columnStr.Sval
										}
									} else if len(columnRef.Fields) == 1 {
										// Unqualified column
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

		// Get column name or function from left expression
		if lexpr := aExpr.Lexpr; lexpr != nil {
			if columnRef := lexpr.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.TableName = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.Column = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if str := columnRef.Fields[0].GetString_(); str != nil {
						condition.Column = str.Sval
					}
				}
			} else if funcCall := lexpr.GetFuncCall(); funcCall != nil {
				// Handle function call on left side
				if fn := p.parseFunctionCall(funcCall); fn != nil {
					condition.Function = fn
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
			} else if columnRef := rexpr.GetColumnRef(); columnRef != nil {
				// Handle column reference on right side (e.g., e2.department = e.department)
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.ValueTableName = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.ValueColumn = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if str := columnRef.Fields[0].GetString_(); str != nil {
						condition.ValueColumn = str.Sval
					}
				}
			} else if funcCall := rexpr.GetFuncCall(); funcCall != nil {
				// Handle function call on right side
				if fn := p.parseFunctionCall(funcCall); fn != nil {
					condition.ValueFunction = fn
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
					if len(columnRef.Fields) >= 2 {
						// Qualified column: table.column
						if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
							condition.TableName = tableStr.Sval
						}
						if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
							condition.Column = columnStr.Sval
						}
					} else if len(columnRef.Fields) == 1 {
						// Unqualified column
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
			Type:         SELECT,
			IsAggregate:  false,
			HasJoins:     false,
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

	// Check subquery WHERE conditions for references to outer tables (including nested complex conditions)
	for _, condition := range subquery.Where {
		p.checkConditionForCorrelation(condition, outerTables, subquery)
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
	// Check if this is a function call (which might include window functions)
	if funcCall := resTarget.Val.GetFuncCall(); funcCall != nil {
		// First check if it's a window function (has OVER clause)
		if funcCall.Over != nil {
			winFunc := p.parseWindowFunctionFromFuncCall(funcCall, resTarget.Name)
			if winFunc != nil {
				// Add to both WindowFuncs collection and as a Column with WindowFunc
				query.WindowFuncs = append(query.WindowFuncs, *winFunc)
				query.HasWindowFuncs = true

				// Also add as a column for result set handling
				column := Column{
					WindowFunc: winFunc,
					Alias:      resTarget.Name,
				}
				if column.Alias == "" {
					column.Name = winFunc.Function // Use function name as default name
				} else {
					column.Name = column.Alias
				}
				query.Columns = append(query.Columns, column)
				return
			}
		}

		// Otherwise check if it's an aggregate function
		agg := p.parseAggregateFunction(funcCall, resTarget.Name)
		if agg != nil {
			query.Aggregates = append(query.Aggregates, *agg)
			query.IsAggregate = true
			return
		}

		// Otherwise it's a regular function call (string/date/math functions)
		if fn := p.parseFunctionCall(funcCall); fn != nil {
			column := Column{
				Function: fn,
				Alias:    resTarget.Name,
			}
			if column.Alias == "" {
				column.Name = fn.Name // Use function name as default name
			} else {
				column.Name = column.Alias
			}
			query.Columns = append(query.Columns, column)
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

		// Handle qualified column names (table.column or table.*)
		if len(columnRef.Fields) > 1 {
			if str := columnRef.Fields[1].GetString_(); str != nil {
				column.TableName = column.Name
				column.Name = str.Sval
			} else if columnRef.Fields[1].GetAStar() != nil {
				column.TableName = column.Name
				column.Name = "*"
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
	} else if caseExpr := resTarget.Val.GetCaseExpr(); caseExpr != nil {
		// Handle CASE expressions
		if parsedCase := p.parseCaseExpression(caseExpr, resTarget.Name); parsedCase != nil {
			column.CaseExpr = parsedCase
			if resTarget.Name != "" {
				column.Name = resTarget.Name
				column.Alias = resTarget.Name
				parsedCase.Alias = resTarget.Name
			} else {
				column.Name = "case_expr"
			}
		}
	} else if aExpr := resTarget.Val.GetAExpr(); aExpr != nil {
		// Handle arithmetic expressions like salary * 1.1
		fn := p.parseArithmeticExpression(aExpr)
		if fn != nil {
			column := Column{
				Function: fn,
				Alias:    resTarget.Name,
			}
			if column.Alias == "" {
				column.Name = "expr" // Default name for expressions
			} else {
				column.Name = column.Alias
			}
			query.Columns = append(query.Columns, column)
			return
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

func (p *SQLParser) parseWindowFunctionFromFuncCall(funcCall *pg_query.FuncCall, alias string) *WindowFunction {
	if len(funcCall.Funcname) == 0 {
		return nil
	}

	funcName := ""
	if str := funcCall.Funcname[0].GetString_(); str != nil {
		funcName = strings.ToUpper(str.Sval)
	}

	// Check if it's a supported window function
	supportedFuncs := map[string]bool{
		"ROW_NUMBER": true,
		"RANK":       true,
		"DENSE_RANK": true,
		"LAG":        true,
		"LEAD":       true,
	}

	if !supportedFuncs[funcName] {
		return nil
	}

	winFunc := &WindowFunction{
		Function: funcName,
		Alias:    alias,
	}

	// Parse function arguments (for LAG/LEAD)
	if len(funcCall.Args) > 0 {
		for _, arg := range funcCall.Args {
			if columnRef := arg.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) > 0 {
					if str := columnRef.Fields[0].GetString_(); str != nil {
						winFunc.Column = str.Sval
					}
				}
			} else if aConst := arg.GetAConst(); aConst != nil {
				// Handle constant arguments (like offset in LAG/LEAD)
				if ival := aConst.GetIval(); ival != nil {
					winFunc.Arguments = append(winFunc.Arguments, ival.Ival)
				} else if sval := aConst.GetSval(); sval != nil {
					winFunc.Arguments = append(winFunc.Arguments, sval.Sval)
				} else if fval := aConst.GetFval(); fval != nil {
					winFunc.Arguments = append(winFunc.Arguments, fval.Fval)
				}
			}
		}
	}

	// Parse the OVER clause (window specification)
	if funcCall.Over != nil {
		winFunc.WindowSpec = p.parseWindowSpecificationFromOverClause(funcCall.Over)
	}

	// If no alias provided, generate one
	if winFunc.Alias == "" {
		winFunc.Alias = strings.ToLower(funcName)
	}

	return winFunc
}

func (p *SQLParser) parseWindowSpecificationFromOverClause(overClause interface{}) WindowSpecification {
	spec := WindowSpecification{}

	// Type assert to *pg_query.WindowDef
	if windowDef, ok := overClause.(*pg_query.WindowDef); ok {
		// Parse PARTITION BY clause
		if windowDef.PartitionClause != nil {
			for _, partitionNode := range windowDef.PartitionClause {
				if columnRef := partitionNode.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if strNode := columnRef.Fields[0].GetString_(); strNode != nil {
							spec.PartitionBy = append(spec.PartitionBy, strNode.Sval)
						}
					}
				}
			}
		}

		// Parse ORDER BY clause
		if windowDef.OrderClause != nil {
			for _, orderNode := range windowDef.OrderClause {
				if sortBy := orderNode.GetSortBy(); sortBy != nil {
					orderCol := OrderByColumn{}

					// Extract column name
					if columnRef := sortBy.Node.GetColumnRef(); columnRef != nil {
						if len(columnRef.Fields) > 0 {
							if strNode := columnRef.Fields[0].GetString_(); strNode != nil {
								orderCol.Column = strNode.Sval
							}
						}
					}

					// Extract sort direction
					switch sortBy.SortbyDir {
					case pg_query.SortByDir_SORTBY_ASC, pg_query.SortByDir_SORTBY_DEFAULT:
						orderCol.Direction = "ASC"
					case pg_query.SortByDir_SORTBY_DESC:
						orderCol.Direction = "DESC"
					}

					if orderCol.Column != "" {
						spec.OrderBy = append(spec.OrderBy, orderCol)
					}
				}
			}
		}
	}

	return spec
}

func (p *SQLParser) parseWindowSpecification(windef *pg_query.WindowDef) WindowSpecification {
	// TODO: Implement proper window specification parsing if needed
	spec := WindowSpecification{}
	return spec
}

func (p *SQLParser) parseFunctionCall(funcCall *pg_query.FuncCall) *FunctionCall {
	if len(funcCall.Funcname) == 0 {
		return nil
	}

	// Extract function name (might be schema-qualified like pg_catalog.btrim)
	funcName := ""
	for i, part := range funcCall.Funcname {
		if str := part.GetString_(); str != nil {
			if i > 0 {
				funcName += "."
			}
			funcName += str.Sval
		}
	}

	// For common functions, strip the schema prefix
	if strings.HasPrefix(strings.ToUpper(funcName), "PG_CATALOG.") {
		funcName = funcName[11:] // Remove "pg_catalog."
	}

	funcName = strings.ToUpper(funcName)

	// Handle function name aliases
	switch funcName {
	case "BTRIM":
		funcName = "TRIM"
	}

	fn := &FunctionCall{
		Name: funcName,
		Args: make([]interface{}, 0),
	}

	// Parse function arguments
	for _, arg := range funcCall.Args {
		if columnRef := arg.GetColumnRef(); columnRef != nil {
			// Column reference
			col := Column{}
			if len(columnRef.Fields) > 0 {
				if str := columnRef.Fields[0].GetString_(); str != nil {
					col.Name = str.Sval
				}
			}
			// Handle qualified column names
			if len(columnRef.Fields) > 1 {
				if str := columnRef.Fields[1].GetString_(); str != nil {
					col.TableName = col.Name
					col.Name = str.Sval
				}
			}
			fn.Args = append(fn.Args, col)
		} else if aConst := arg.GetAConst(); aConst != nil {
			// Constant value
			if ival := aConst.GetIval(); ival != nil {
				fn.Args = append(fn.Args, ival.Ival)
			} else if sval := aConst.GetSval(); sval != nil {
				fn.Args = append(fn.Args, sval.Sval)
			} else if fval := aConst.GetFval(); fval != nil {
				// Parse float string to float64
				if f, err := strconv.ParseFloat(fval.Fval, 64); err == nil {
					fn.Args = append(fn.Args, f)
				} else {
					fn.Args = append(fn.Args, fval.Fval)
				}
			}
		} else if nestedFunc := arg.GetFuncCall(); nestedFunc != nil {
			// Nested function call
			if nestedCall := p.parseFunctionCall(nestedFunc); nestedCall != nil {
				fn.Args = append(fn.Args, nestedCall)
			}
		}
	}

	return fn
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
		// Add columns referenced in functions
		if col.Function != nil {
			q.addFunctionColumns(col.Function, requiredCols)
		}
		// Add columns referenced in CASE expressions
		if col.CaseExpr != nil {
			q.addCaseExpressionColumns(col.CaseExpr, requiredCols)
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

	// Handle column comparisons
	if where.ValueColumn != "" {
		requiredCols[where.ValueColumn] = true
	}

	// Handle functions in WHERE conditions
	if where.Function != nil {
		q.addFunctionColumns(where.Function, requiredCols)
	}
	if where.ValueFunction != nil {
		q.addFunctionColumns(where.ValueFunction, requiredCols)
	}
}

// addFunctionColumns recursively adds all columns referenced in a function call
func (q *ParsedQuery) addFunctionColumns(fn *FunctionCall, requiredCols map[string]bool) {
	for _, arg := range fn.Args {
		switch v := arg.(type) {
		case Column:
			// Add the column referenced in the function
			if v.Name != "" && v.Name != "*" {
				requiredCols[v.Name] = true
			}
		case *FunctionCall:
			// Recursively process nested functions
			q.addFunctionColumns(v, requiredCols)
		}
	}
}

// addCaseExpressionColumns recursively adds all columns referenced in a CASE expression
func (q *ParsedQuery) addCaseExpressionColumns(caseExpr *CaseExpression, requiredCols map[string]bool) {
	// Process each WHEN clause
	for _, whenClause := range caseExpr.WhenClauses {
		// Add columns from the condition
		if whenClause.Condition.Column != "" {
			requiredCols[whenClause.Condition.Column] = true
		}
		if whenClause.Condition.ValueColumn != "" {
			requiredCols[whenClause.Condition.ValueColumn] = true
		}
		// Handle nested CASE expressions in conditions
		if whenClause.Condition.CaseExpr != nil {
			q.addCaseExpressionColumns(whenClause.Condition.CaseExpr, requiredCols)
		}
		if whenClause.Condition.ValueCaseExpr != nil {
			q.addCaseExpressionColumns(whenClause.Condition.ValueCaseExpr, requiredCols)
		}
		// Handle function calls in conditions
		if whenClause.Condition.Function != nil {
			q.addFunctionColumns(whenClause.Condition.Function, requiredCols)
		}
		if whenClause.Condition.ValueFunction != nil {
			q.addFunctionColumns(whenClause.Condition.ValueFunction, requiredCols)
		}
		
		// Add columns from the result expression
		if whenClause.Result.ColumnName != "" {
			requiredCols[whenClause.Result.ColumnName] = true
		}
		// Handle nested CASE expressions in results
		if whenClause.Result.CaseExpr != nil {
			q.addCaseExpressionColumns(whenClause.Result.CaseExpr, requiredCols)
		}
	}
	
	// Process ELSE clause if present
	if caseExpr.ElseClause != nil {
		if caseExpr.ElseClause.ColumnName != "" {
			requiredCols[caseExpr.ElseClause.ColumnName] = true
		}
		// Handle nested CASE expressions in ELSE clause
		if caseExpr.ElseClause.CaseExpr != nil {
			q.addCaseExpressionColumns(caseExpr.ElseClause.CaseExpr, requiredCols)
		}
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
	// Check all WHERE conditions for subqueries (including nested ones)
	for i := range query.Where {
		p.detectCorrelationInCondition(&query.Where[i], query)
	}

	// Check column subqueries (for future SELECT clause subqueries)
	for i := range query.Columns {
		if query.Columns[i].Subquery != nil {
			p.detectCorrelation(query.Columns[i].Subquery, query)
		}
	}
}

// detectCorrelationInCondition recursively checks all subqueries in a WHERE condition
func (p *SQLParser) detectCorrelationInCondition(condition *WhereCondition, outerQuery *ParsedQuery) {
	// Check direct subquery in this condition
	if condition.Subquery != nil {
		p.detectCorrelation(condition.Subquery, outerQuery)
	}

	// Check nested conditions (for complex AND/OR conditions)
	if condition.IsComplex {
		if condition.Left != nil {
			p.detectCorrelationInCondition(condition.Left, outerQuery)
		}
		if condition.Right != nil {
			p.detectCorrelationInCondition(condition.Right, outerQuery)
		}
	}
}

// checkConditionForCorrelation recursively checks a condition for correlation with outer tables
func (p *SQLParser) checkConditionForCorrelation(condition WhereCondition, outerTables map[string]bool, subquery *ParsedQuery) {
	// Check simple condition for correlation
	if condition.TableName != "" && outerTables[condition.TableName] {
		subquery.IsCorrelated = true
		subquery.CorrelatedColumns = append(subquery.CorrelatedColumns, condition.TableName+"."+condition.Column)
	}
	// Also check the right side of the condition for column references
	if condition.ValueTableName != "" && outerTables[condition.ValueTableName] {
		subquery.IsCorrelated = true
		subquery.CorrelatedColumns = append(subquery.CorrelatedColumns, condition.ValueTableName+"."+condition.ValueColumn)
	}

	// Check nested conditions (for complex AND/OR conditions)
	if condition.IsComplex {
		if condition.Left != nil {
			p.checkConditionForCorrelation(*condition.Left, outerTables, subquery)
		}
		if condition.Right != nil {
			p.checkConditionForCorrelation(*condition.Right, outerTables, subquery)
		}
	}
}

// parseCaseExpression parses a CASE expression from the PostgreSQL AST
func (p *SQLParser) parseCaseExpression(caseExpr *pg_query.CaseExpr, alias string) *CaseExpression {
	result := &CaseExpression{
		Alias: alias,
	}

	// Parse WHEN clauses
	for _, whenNode := range caseExpr.Args {
		if caseWhen := whenNode.GetCaseWhen(); caseWhen != nil {
			whenClause := WhenClause{}

			// Parse the WHEN condition
			if caseWhen.Expr != nil {
				whenClause.Condition = *p.parseExpressionAsCondition(caseWhen.Expr)
			}

			// Parse the THEN result
			if caseWhen.Result != nil {
				whenClause.Result = *p.parseExpressionValue(caseWhen.Result)
			}

			result.WhenClauses = append(result.WhenClauses, whenClause)
		}
	}

	// Parse ELSE clause (optional)
	if caseExpr.Defresult != nil {
		result.ElseClause = p.parseExpressionValue(caseExpr.Defresult)
	}

	return result
}

// parseExpressionAsCondition converts an expression node to a WhereCondition
func (p *SQLParser) parseExpressionAsCondition(expr *pg_query.Node) *WhereCondition {
	condition := &WhereCondition{}

	// Handle A_Expr nodes (binary expressions like column = value)
	if aExpr := expr.GetAExpr(); aExpr != nil {
		if len(aExpr.Name) > 0 {
			if str := aExpr.Name[0].GetString_(); str != nil {
				condition.Operator = str.Sval
			}
		}

		// Parse left side (column reference or CASE expression)
		if aExpr.Lexpr != nil {
			if columnRef := aExpr.Lexpr.GetColumnRef(); columnRef != nil {
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.TableName = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.Column = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if str := columnRef.Fields[0].GetString_(); str != nil {
						condition.Column = str.Sval
					}
				}
			} else if caseExpr := aExpr.Lexpr.GetCaseExpr(); caseExpr != nil {
				// Handle CASE expression on left side
				condition.CaseExpr = p.parseCaseExpression(caseExpr, "")
			}
		}

		// Parse right side (value, column reference, or CASE expression)
		if aExpr.Rexpr != nil {
			if aConst := aExpr.Rexpr.GetAConst(); aConst != nil {
				if ival := aConst.GetIval(); ival != nil {
					condition.Value = ival.Ival
				} else if sval := aConst.GetSval(); sval != nil {
					condition.Value = sval.Sval
				} else if fval := aConst.GetFval(); fval != nil {
					condition.Value = fval.Fval
				} else if bval := aConst.GetBoolval(); bval != nil {
					condition.Value = bval.Boolval
				}
			} else if columnRef := aExpr.Rexpr.GetColumnRef(); columnRef != nil {
				// Handle column reference on right side
				if len(columnRef.Fields) >= 2 {
					// Qualified column: table.column
					if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
						condition.ValueTableName = tableStr.Sval
					}
					if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
						condition.ValueColumn = columnStr.Sval
					}
				} else if len(columnRef.Fields) == 1 {
					// Unqualified column
					if str := columnRef.Fields[0].GetString_(); str != nil {
						condition.ValueColumn = str.Sval
					}
				}
			} else if caseExpr := aExpr.Rexpr.GetCaseExpr(); caseExpr != nil {
				// Handle CASE expression on right side
				condition.ValueCaseExpr = p.parseCaseExpression(caseExpr, "")
			}
		}
	}

	return condition
}

// parseArithmeticExpression converts an A_Expr to a FunctionCall representation
func (p *SQLParser) parseArithmeticExpression(aExpr *pg_query.A_Expr) *FunctionCall {
	if len(aExpr.Name) == 0 {
		return nil
	}
	
	// Get the operator
	var op string
	if str := aExpr.Name[0].GetString_(); str != nil {
		op = str.Sval
	}
	
	// Map operators to function names
	var funcName string
	switch op {
	case "+":
		funcName = "ADD"
	case "-":
		funcName = "SUBTRACT"
	case "*":
		funcName = "MULTIPLY"
	case "/":
		funcName = "DIVIDE"
	case "%":
		funcName = "MODULO"
	default:
		// For other operators, we might not support them yet
		return nil
	}
	
	fn := &FunctionCall{
		Name: funcName,
		Args: []interface{}{},
	}
	
	// Parse left operand
	if aExpr.Lexpr != nil {
		if columnRef := aExpr.Lexpr.GetColumnRef(); columnRef != nil && len(columnRef.Fields) > 0 {
			if str := columnRef.Fields[0].GetString_(); str != nil {
				// Create a Column struct for column references
				col := Column{Name: str.Sval}
				fn.Args = append(fn.Args, col)
			}
		} else if constVal := aExpr.Lexpr.GetAConst(); constVal != nil {
			fn.Args = append(fn.Args, p.parseConstantNode(constVal))
		}
	}
	
	// Parse right operand
	if aExpr.Rexpr != nil {
		if columnRef := aExpr.Rexpr.GetColumnRef(); columnRef != nil && len(columnRef.Fields) > 0 {
			if str := columnRef.Fields[0].GetString_(); str != nil {
				// Create a Column struct for column references
				col := Column{Name: str.Sval}
				fn.Args = append(fn.Args, col)
			}
		} else if constVal := aExpr.Rexpr.GetAConst(); constVal != nil {
			fn.Args = append(fn.Args, p.parseConstantNode(constVal))
		}
	}
	
	return fn
}

// parseExpressionValue converts an expression node to an ExpressionValue
func (p *SQLParser) parseExpressionValue(expr *pg_query.Node) *ExpressionValue {
	result := &ExpressionValue{}

	// Handle constants
	if aConst := expr.GetAConst(); aConst != nil {
		result.Type = "literal"
		if ival := aConst.GetIval(); ival != nil {
			result.LiteralValue = ival.Ival
		} else if sval := aConst.GetSval(); sval != nil {
			result.LiteralValue = sval.Sval
		} else if fval := aConst.GetFval(); fval != nil {
			result.LiteralValue = fval.Fval
		} else if bval := aConst.GetBoolval(); bval != nil {
			result.LiteralValue = bval.Boolval
		}
		return result
	}

	// Handle column references
	if columnRef := expr.GetColumnRef(); columnRef != nil {
		result.Type = "column"
		if len(columnRef.Fields) >= 2 {
			// Qualified column: table.column
			if tableStr := columnRef.Fields[0].GetString_(); tableStr != nil {
				result.TableName = tableStr.Sval
			}
			if columnStr := columnRef.Fields[1].GetString_(); columnStr != nil {
				result.ColumnName = columnStr.Sval
			}
		} else if len(columnRef.Fields) == 1 {
			// Unqualified column
			if str := columnRef.Fields[0].GetString_(); str != nil {
				result.ColumnName = str.Sval
			}
		}
		return result
	}

	// Handle nested CASE expressions
	if caseExpr := expr.GetCaseExpr(); caseExpr != nil {
		result.Type = "case"
		result.CaseExpr = p.parseCaseExpression(caseExpr, "")
		return result
	}

	// Handle subqueries
	if subLink := expr.GetSubLink(); subLink != nil {
		result.Type = "subquery"
		result.Subquery = p.parseSubquery(subLink)
		return result
	}

	// Default to literal with nil value
	result.Type = "literal"
	result.LiteralValue = nil
	return result
}

// EnsureUniqueAggregateAliases ensures all aggregate functions have unique column aliases
func (p *SQLParser) EnsureUniqueAggregateAliases(query *ParsedQuery) {
	if len(query.Aggregates) == 0 {
		return
	}

	// Track aliases and their occurrence counters
	aliasCounters := make(map[string]int)

	// Process each aggregate and make aliases unique
	for i := range query.Aggregates {
		baseAlias := query.Aggregates[i].Alias
		if baseAlias == "" {
			continue
		}

		// Check if we've seen this base alias before
		if count, exists := aliasCounters[baseAlias]; exists {
			// This is a duplicate, increment counter and create unique alias
			count++
			aliasCounters[baseAlias] = count
			query.Aggregates[i].Alias = fmt.Sprintf("%s_%d", baseAlias, count)
		} else {
			// First occurrence of this alias
			aliasCounters[baseAlias] = 1
			// Keep the original alias unchanged
		}
	}
}
