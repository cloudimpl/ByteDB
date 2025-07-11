package main

import (
	"fmt"

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
	Column   string
	Operator string
	Value    interface{}
}

type ParsedQuery struct {
	Type       QueryType
	TableName  string
	Columns    []Column
	Where      []WhereCondition
	OrderBy    []string
	Limit      int
	RawSQL     string
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
		
		if len(aExpr.Name) > 0 {
			if str := aExpr.Name[0].GetString_(); str != nil {
				condition.Operator = str.Sval
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
				}
			}
		}
		
		query.Where = append(query.Where, condition)
	}
}

func (q *ParsedQuery) String() string {
	result := fmt.Sprintf("Query Type: %v\n", q.Type)
	result += fmt.Sprintf("Table: %s\n", q.TableName)
	result += fmt.Sprintf("Columns: %v\n", q.Columns)
	if len(q.Where) > 0 {
		result += fmt.Sprintf("Where: %v\n", q.Where)
	}
	if q.Limit > 0 {
		result += fmt.Sprintf("Limit: %d\n", q.Limit)
	}
	return result
}