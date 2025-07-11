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
	Name  string
	Alias string
}

type WhereCondition struct {
	Column    string
	Operator  string
	Value     interface{}   // Single value for =, !=, <, <=, >, >=, LIKE
	ValueList []interface{} // List of values for IN operator
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
	Columns     []Column
	Aggregates  []AggregateFunction
	GroupBy     []string
	Where       []WhereCondition
	OrderBy     []OrderByColumn
	Limit       int
	RawSQL      string
	IsAggregate bool // True if query contains aggregate functions
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

func (p *SQLParser) parseSelect(stmt *pg_query.SelectStmt, query *ParsedQuery) (*ParsedQuery, error) {
	query.Type = SELECT

	if stmt.FromClause != nil && len(stmt.FromClause) > 0 {
		fromNode := stmt.FromClause[0]
		if rangeVar := fromNode.GetRangeVar(); rangeVar != nil {
			query.TableName = rangeVar.Relname
		}
	}

	if stmt.TargetList != nil {
		for _, target := range stmt.TargetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				// Check if this is an aggregate function call
				if funcCall := resTarget.Val.GetFuncCall(); funcCall != nil {
					agg := p.parseAggregateFunction(funcCall, resTarget.Name)
					if agg != nil {
						query.Aggregates = append(query.Aggregates, *agg)
						query.IsAggregate = true
						continue
					}
				}
				
				// Regular column parsing
				col := Column{}
				
				if resTarget.Name != "" {
					col.Alias = resTarget.Name
				}
				
				if columnRef := resTarget.Val.GetColumnRef(); columnRef != nil {
					if len(columnRef.Fields) > 0 {
						if str := columnRef.Fields[0].GetString_(); str != nil {
							col.Name = str.Sval
						} else if columnRef.Fields[0].GetAStar() != nil {
							col.Name = "*"
						}
					}
				} else if aStar := resTarget.Val.GetAStar(); aStar != nil {
					col.Name = "*"
				}
				
				if col.Name == "" && col.Alias == "" {
					col.Name = "unknown"
				}
				
				query.Columns = append(query.Columns, col)
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

	return query, nil
}

func (p *SQLParser) parseWhere(node *pg_query.Node, query *ParsedQuery) {
	if aExpr := node.GetAExpr(); aExpr != nil {
		condition := WhereCondition{}
		
		// Check if this is an IN expression
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
					p.parseValueList(aList.Items, &condition)
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
				}
			}
		}
		
		query.Where = append(query.Where, condition)
	}
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
		requiredCols[col.Name] = true
	}
	
	// Add columns from aggregate functions
	for _, agg := range q.Aggregates {
		if agg.Column != "*" {
			requiredCols[agg.Column] = true
		}
	}
	
	// Add columns from WHERE clause
	for _, where := range q.Where {
		requiredCols[where.Column] = true
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