package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type QueryEngine struct {
	parser      *SQLParser
	dataPath    string
	openReaders map[string]*ParquetReader
}

type QueryResult struct {
	Columns []string      `json:"columns"`
	Rows    []Row         `json:"rows"`
	Count   int           `json:"count"`
	Query   string        `json:"query"`
	Error   string        `json:"error,omitempty"`
}

func NewQueryEngine(dataPath string) *QueryEngine {
	return &QueryEngine{
		parser:      NewSQLParser(),
		dataPath:    dataPath,
		openReaders: make(map[string]*ParquetReader),
	}
}

func (qe *QueryEngine) Close() {
	for _, reader := range qe.openReaders {
		reader.Close()
	}
}

func (qe *QueryEngine) Execute(sql string) (*QueryResult, error) {
	parsedQuery, err := qe.parser.Parse(sql)
	if err != nil {
		return &QueryResult{
			Query: sql,
			Error: err.Error(),
		}, nil
	}

	switch parsedQuery.Type {
	case SELECT:
		return qe.executeSelect(parsedQuery)
	default:
		return &QueryResult{
			Query: sql,
			Error: "unsupported query type",
		}, nil
	}
}

func (qe *QueryEngine) executeSelect(query *ParsedQuery) (*QueryResult, error) {
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: err.Error(),
		}, nil
	}

	// Always read all data first, then apply LIMIT after filtering and sorting
	var rows []Row
	rows, err = reader.ReadAll()
	
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: err.Error(),
		}, nil
	}

	// Apply WHERE clause filtering first
	rows = reader.FilterRows(rows, query.Where)
	
	// Handle aggregate queries
	if query.IsAggregate {
		return qe.executeAggregate(query, rows, reader)
	}
	
	// Regular non-aggregate query processing
	rows = reader.SelectColumns(rows, query.Columns)
	rows = qe.sortRows(rows, query.OrderBy, reader)

	// Apply LIMIT after filtering and sorting
	if query.Limit > 0 && len(rows) > query.Limit {
		rows = rows[:query.Limit]
	}

	columns := qe.getResultColumns(rows, query.Columns)

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
		Count:   len(rows),
		Query:   query.RawSQL,
	}, nil
}

func (qe *QueryEngine) getReader(tableName string) (*ParquetReader, error) {
	if reader, exists := qe.openReaders[tableName]; exists {
		return reader, nil
	}

	filePath := filepath.Join(qe.dataPath, tableName+".parquet")
	reader, err := NewParquetReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", tableName, err)
	}

	qe.openReaders[tableName] = reader
	return reader, nil
}

func (qe *QueryEngine) executeAggregate(query *ParsedQuery, rows []Row, reader *ParquetReader) (*QueryResult, error) {
	if len(query.GroupBy) > 0 {
		return qe.executeGroupedAggregate(query, rows, reader)
	} else {
		return qe.executeSimpleAggregate(query, rows, reader)
	}
}

func (qe *QueryEngine) executeSimpleAggregate(query *ParsedQuery, rows []Row, reader *ParquetReader) (*QueryResult, error) {
	// Simple aggregate without GROUP BY - single result row
	resultRow := make(Row)
	var columns []string
	
	for _, agg := range query.Aggregates {
		value, err := qe.calculateAggregate(agg, rows)
		if err != nil {
			return &QueryResult{
				Query: query.RawSQL,
				Error: err.Error(),
			}, nil
		}
		resultRow[agg.Alias] = value
		columns = append(columns, agg.Alias)
	}
	
	// Add non-aggregate columns (these should be constants in valid SQL)
	for _, col := range query.Columns {
		if col.Name != "*" {
			columns = append(columns, col.Name)
			// For now, we'll just use the first row's value
			if len(rows) > 0 {
				if val, exists := rows[0][col.Name]; exists {
					resultRow[col.Name] = val
				}
			}
		}
	}
	
	result := []Row{resultRow}
	
	// Apply ORDER BY and LIMIT
	result = qe.sortRows(result, query.OrderBy, reader)
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	
	return &QueryResult{
		Columns: columns,
		Rows:    result,
		Count:   len(result),
		Query:   query.RawSQL,
	}, nil
}

func (qe *QueryEngine) executeGroupedAggregate(query *ParsedQuery, rows []Row, reader *ParquetReader) (*QueryResult, error) {
	// Group rows by GROUP BY columns
	groups := make(map[string][]Row)
	var columns []string
	
	// Add GROUP BY columns to result columns
	for _, groupCol := range query.GroupBy {
		columns = append(columns, groupCol)
	}
	
	// Add aggregate columns to result columns
	for _, agg := range query.Aggregates {
		columns = append(columns, agg.Alias)
	}
	
	// Group the rows
	for _, row := range rows {
		groupKey := qe.buildGroupKey(row, query.GroupBy)
		groups[groupKey] = append(groups[groupKey], row)
	}
	
	// Calculate aggregates for each group
	var result []Row
	for _, groupRows := range groups {
		resultRow := make(Row)
		
		// Add GROUP BY column values
		if len(groupRows) > 0 {
			for _, groupCol := range query.GroupBy {
				if val, exists := groupRows[0][groupCol]; exists {
					resultRow[groupCol] = val
				}
			}
		}
		
		// Calculate aggregates for this group
		for _, agg := range query.Aggregates {
			value, err := qe.calculateAggregate(agg, groupRows)
			if err != nil {
				return &QueryResult{
					Query: query.RawSQL,
					Error: err.Error(),
				}, nil
			}
			resultRow[agg.Alias] = value
		}
		
		result = append(result, resultRow)
	}
	
	// Apply ORDER BY and LIMIT
	result = qe.sortRows(result, query.OrderBy, reader)
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	
	return &QueryResult{
		Columns: columns,
		Rows:    result,
		Count:   len(result),
		Query:   query.RawSQL,
	}, nil
}

func (qe *QueryEngine) buildGroupKey(row Row, groupByCols []string) string {
	var keyParts []string
	for _, col := range groupByCols {
		if val, exists := row[col]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", val))
		} else {
			keyParts = append(keyParts, "NULL")
		}
	}
	return strings.Join(keyParts, "|")
}

func (qe *QueryEngine) calculateAggregate(agg AggregateFunction, rows []Row) (interface{}, error) {
	switch agg.Function {
	case "COUNT":
		if agg.Column == "*" {
			return float64(len(rows)), nil
		} else {
			count := 0
			for _, row := range rows {
				if _, exists := row[agg.Column]; exists {
					count++
				}
			}
			return float64(count), nil
		}
		
	case "SUM":
		if agg.Column == "*" {
			return nil, fmt.Errorf("SUM(*) is not supported")
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			if val, exists := row[agg.Column]; exists {
				if numVal, err := qe.toFloat64(val); err == nil {
					sum += numVal
					count++
				}
			}
		}
		if count == 0 {
			return nil, nil // SQL standard: SUM of empty set is NULL
		}
		return sum, nil
		
	case "AVG":
		if agg.Column == "*" {
			return nil, fmt.Errorf("AVG(*) is not supported")
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			if val, exists := row[agg.Column]; exists {
				if numVal, err := qe.toFloat64(val); err == nil {
					sum += numVal
					count++
				}
			}
		}
		if count == 0 {
			return nil, nil // SQL standard: AVG of empty set is NULL
		}
		return sum / float64(count), nil
		
	case "MIN":
		if agg.Column == "*" {
			return nil, fmt.Errorf("MIN(*) is not supported")
		}
		var min interface{}
		for _, row := range rows {
			if val, exists := row[agg.Column]; exists {
				if min == nil {
					min = val
				} else {
					// Use existing comparison logic from ParquetReader
					// We need to create a dummy reader for comparison
					dummyReader := &ParquetReader{}
					if dummyReader.CompareValues(val, min) < 0 {
						min = val
					}
				}
			}
		}
		return min, nil
		
	case "MAX":
		if agg.Column == "*" {
			return nil, fmt.Errorf("MAX(*) is not supported")
		}
		var max interface{}
		for _, row := range rows {
			if val, exists := row[agg.Column]; exists {
				if max == nil {
					max = val
				} else {
					// Use existing comparison logic from ParquetReader
					dummyReader := &ParquetReader{}
					if dummyReader.CompareValues(val, max) > 0 {
						max = val
					}
				}
			}
		}
		return max, nil
		
	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", agg.Function)
	}
}

func (qe *QueryEngine) toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func (qe *QueryEngine) sortRows(rows []Row, orderBy []OrderByColumn, reader *ParquetReader) []Row {
	if len(orderBy) == 0 {
		return rows
	}

	// Create a copy of rows to avoid modifying the original slice
	sortedRows := make([]Row, len(rows))
	copy(sortedRows, rows)

	sort.Slice(sortedRows, func(i, j int) bool {
		return qe.compareRows(sortedRows[i], sortedRows[j], orderBy, reader)
	})

	return sortedRows
}

func (qe *QueryEngine) compareRows(row1, row2 Row, orderBy []OrderByColumn, reader *ParquetReader) bool {
	for _, orderCol := range orderBy {
		val1, exists1 := row1[orderCol.Column]
		val2, exists2 := row2[orderCol.Column]

		// Handle missing columns
		if !exists1 && !exists2 {
			continue // Equal, check next column
		}
		if !exists1 {
			return orderCol.Direction == "ASC" // NULL values sort first in ASC
		}
		if !exists2 {
			return orderCol.Direction == "DESC" // NULL values sort last in DESC
		}

		// Compare values using the same logic as WHERE clause filtering
		comparison := reader.CompareValues(val1, val2)
		
		if comparison != 0 {
			if orderCol.Direction == "DESC" {
				return comparison > 0
			} else { // ASC is default
				return comparison < 0
			}
		}
		// If values are equal, continue to next ORDER BY column
	}
	
	// All ORDER BY columns are equal
	return false
}

func (qe *QueryEngine) getResultColumns(rows []Row, queryColumns []Column) []string {
	if len(rows) == 0 {
		return []string{}
	}

	columnSet := make(map[string]bool)
	var columns []string

	for key := range rows[0] {
		if !columnSet[key] {
			columns = append(columns, key)
			columnSet[key] = true
		}
	}

	return columns
}

func (qe *QueryEngine) ExecuteToJSON(sql string) (string, error) {
	result, err := qe.Execute(sql)
	if err != nil {
		return "", err
	}

	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal result to JSON: %w", err)
	}

	return string(jsonData), nil
}

func (qe *QueryEngine) ExecuteToTable(sql string) (string, error) {
	result, err := qe.Execute(sql)
	if err != nil {
		return "", err
	}

	if result.Error != "" {
		return fmt.Sprintf("Error: %s", result.Error), nil
	}

	if len(result.Rows) == 0 {
		return "No results found", nil
	}

	var output strings.Builder
	
	output.WriteString(strings.Join(result.Columns, "\t"))
	output.WriteString("\n")
	
	for _, row := range result.Rows {
		var values []string
		for _, col := range result.Columns {
			if val, exists := row[col]; exists {
				values = append(values, fmt.Sprintf("%v", val))
			} else {
				values = append(values, "")
			}
		}
		output.WriteString(strings.Join(values, "\t"))
		output.WriteString("\n")
	}
	
	output.WriteString(fmt.Sprintf("\n(%d rows)", result.Count))
	
	return output.String(), nil
}

func (qe *QueryEngine) GetTableInfo(tableName string) (string, error) {
	reader, err := qe.getReader(tableName)
	if err != nil {
		return "", err
	}

	schema := reader.GetSchema()
	var info strings.Builder
	
	info.WriteString(fmt.Sprintf("Table: %s\n", tableName))
	info.WriteString("Columns:\n")
	
	for _, field := range schema.Fields() {
		info.WriteString(fmt.Sprintf("  - %s (%s)\n", field.Name(), field.Type()))
	}
	
	return info.String(), nil
}

func (qe *QueryEngine) ListTables() ([]string, error) {
	return []string{}, fmt.Errorf("table listing not implemented yet")
}