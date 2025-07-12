package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type QueryEngine struct {
	parser      *SQLParser
	dataPath    string
	openReaders map[string]*ParquetReader
	cache       *QueryCache
}

type QueryResult struct {
	Columns []string      `json:"columns"`
	Rows    []Row         `json:"rows"`
	Count   int           `json:"count"`
	Query   string        `json:"query"`
	Error   string        `json:"error,omitempty"`
}

func NewQueryEngine(dataPath string) *QueryEngine {
	// Default cache configuration
	cacheConfig := CacheConfig{
		MaxMemoryMB: 100,                  // 100MB default cache size
		DefaultTTL:  5 * time.Minute,     // 5 minute default TTL
		Enabled:     true,                 // Enable caching by default
	}
	
	return &QueryEngine{
		parser:      NewSQLParser(),
		dataPath:    dataPath,
		openReaders: make(map[string]*ParquetReader),
		cache:       NewQueryCache(cacheConfig),
	}
}

func (qe *QueryEngine) Close() {
	for _, reader := range qe.openReaders {
		reader.Close()
	}
}

func (qe *QueryEngine) Execute(sql string) (*QueryResult, error) {
	// Check cache first
	if cachedResult, found := qe.cache.Get(sql); found {
		return cachedResult, nil
	}
	
	parsedQuery, err := qe.parser.Parse(sql)
	if err != nil {
		return &QueryResult{
			Query: sql,
			Error: err.Error(),
		}, nil
	}

	var result *QueryResult
	switch parsedQuery.Type {
	case SELECT:
		// Handle constant-only queries (no FROM clause)
		if parsedQuery.TableName == "" && qe.hasOnlyConstantColumns(parsedQuery) {
			result, err = qe.executeConstantQuery(parsedQuery)
		} else {
			result, err = qe.executeSelect(parsedQuery)
		}
	default:
		result = &QueryResult{
			Query: sql,
			Error: "unsupported query type",
		}
	}
	
	// Cache successful results (not errors)
	if result != nil && result.Error == "" {
		qe.cache.Put(sql, result)
	}
	
	return result, err
}

func (qe *QueryEngine) executeSelect(query *ParsedQuery) (*QueryResult, error) {
	// Check if this is a JOIN query
	if query.HasJoins {
		return qe.executeJoinQuery(query)
	}
	
	// Single table query (existing logic)
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: err.Error(),
		}, nil
	}

	// Determine which columns are required for this query
	requiredColumns := query.GetRequiredColumns()
	
	// Read data with column pruning optimization
	var rows []Row
	if len(requiredColumns) > 0 {
		rows, err = reader.ReadAllWithColumns(requiredColumns)
	} else {
		// Check if this is a constants-only query (like SELECT 1 FROM table)
		if qe.hasOnlyConstantColumns(query) {
			// For constants-only queries, we just need to know the row count
			// Read a minimal column to get the correct number of rows
			allColumns := reader.GetColumnNames()
			if len(allColumns) > 0 {
				rows, err = reader.ReadAllWithColumns([]string{allColumns[0]})
			} else {
				rows, err = reader.ReadAll()
			}
		} else {
			// SELECT * or aggregates that need all columns
			rows, err = reader.ReadAll()
		}
	}
	
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: err.Error(),
		}, nil
	}

	// Apply WHERE clause filtering first
	rows = reader.FilterRowsWithEngine(rows, query.Where, qe)
	
	// Handle aggregate queries
	if query.IsAggregate {
		return qe.executeAggregate(query, rows, reader)
	}
	
	// Regular non-aggregate query processing
	if reader.hasColumnSubqueries(query.Columns) {
		rows = reader.SelectColumnsWithEngine(rows, query.Columns, qe)
	} else {
		rows = reader.SelectColumns(rows, query.Columns)
	}
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

// executeJoinQuery handles queries with JOIN clauses
func (qe *QueryEngine) executeJoinQuery(query *ParsedQuery) (*QueryResult, error) {
	// Read data from the main (left) table
	leftReader, err := qe.getReader(query.TableName)
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: fmt.Sprintf("Failed to read left table %s: %v", query.TableName, err),
		}, nil
	}
	
	leftRows, err := leftReader.ReadAll()
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: fmt.Sprintf("Failed to read data from %s: %v", query.TableName, err),
		}, nil
	}
	
	// Add table prefixes to left table rows
	leftRows = qe.prefixRowColumns(leftRows, query.TableAlias, query.TableName)
	
	// Process each JOIN sequentially
	joinedRows := leftRows
	for _, joinClause := range query.Joins {
		rightReader, err := qe.getReader(joinClause.TableName)
		if err != nil {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("Failed to read right table %s: %v", joinClause.TableName, err),
			}, nil
		}
		
		rightRows, err := rightReader.ReadAll()
		if err != nil {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("Failed to read data from %s: %v", joinClause.TableName, err),
			}, nil
		}
		
		// Add table prefixes to right table rows
		rightRows = qe.prefixRowColumns(rightRows, joinClause.TableAlias, joinClause.TableName)
		
		// Perform the JOIN
		joinedRows = qe.performJoin(joinedRows, rightRows, joinClause)
	}
	
	// Apply WHERE clause filtering
	if len(query.Where) > 0 {
		joinedRows = qe.filterJoinedRowsWithEngine(joinedRows, query.Where)
	}
	
	// Handle aggregate queries
	if query.IsAggregate {
		// For now, aggregates with JOINs are not fully supported
		return &QueryResult{
			Query: query.RawSQL,
			Error: "Aggregate functions with JOINs are not yet supported",
		}, nil
	}
	
	// Select specific columns and apply aliases
	resultRows := qe.selectJoinColumns(joinedRows, query.Columns)
	
	// Apply ORDER BY (complex due to qualified column names)
	// For now, basic ordering without table qualification
	if len(query.OrderBy) > 0 {
		resultRows = qe.sortJoinedRows(resultRows, query.OrderBy)
	}
	
	// Apply LIMIT
	if query.Limit > 0 && len(resultRows) > query.Limit {
		resultRows = resultRows[:query.Limit]
	}
	
	// Get result column names
	columns := qe.getJoinResultColumns(resultRows, query.Columns)
	
	return &QueryResult{
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
		Query:   query.RawSQL,
	}, nil
}

// prefixRowColumns adds table prefixes to column names for disambiguation
func (qe *QueryEngine) prefixRowColumns(rows []Row, tableAlias, tableName string) []Row {
	if len(rows) == 0 {
		return rows
	}
	
	prefix := tableAlias
	if prefix == "" {
		prefix = tableName
	}
	
	prefixedRows := make([]Row, len(rows))
	for i, row := range rows {
		prefixedRow := make(Row)
		for colName, value := range row {
			prefixedColName := prefix + "." + colName
			prefixedRow[prefixedColName] = value
		}
		prefixedRows[i] = prefixedRow
	}
	
	return prefixedRows
}

// performJoin executes the actual JOIN operation using nested loop algorithm
func (qe *QueryEngine) performJoin(leftRows, rightRows []Row, joinClause JoinClause) []Row {
	var result []Row
	
	switch joinClause.Type {
	case INNER_JOIN:
		result = qe.performInnerJoin(leftRows, rightRows, joinClause.Condition)
	case LEFT_JOIN:
		result = qe.performLeftJoin(leftRows, rightRows, joinClause.Condition)
	case RIGHT_JOIN:
		result = qe.performRightJoin(leftRows, rightRows, joinClause.Condition)
	case FULL_OUTER_JOIN:
		result = qe.performFullOuterJoin(leftRows, rightRows, joinClause.Condition)
	default:
		result = qe.performInnerJoin(leftRows, rightRows, joinClause.Condition)
	}
	
	return result
}

// performInnerJoin implements INNER JOIN logic
func (qe *QueryEngine) performInnerJoin(leftRows, rightRows []Row, condition JoinCondition) []Row {
	var result []Row
	
	leftColName := condition.LeftTable + "." + condition.LeftColumn
	rightColName := condition.RightTable + "." + condition.RightColumn
	
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			leftValue, leftExists := leftRow[leftColName]
			rightValue, rightExists := rightRow[rightColName]
			
			if leftExists && rightExists && qe.valuesMatch(leftValue, rightValue, condition.Operator) {
				// Combine rows
				combinedRow := make(Row)
				for k, v := range leftRow {
					combinedRow[k] = v
				}
				for k, v := range rightRow {
					combinedRow[k] = v
				}
				result = append(result, combinedRow)
			}
		}
	}
	
	return result
}

// performLeftJoin implements LEFT JOIN logic  
func (qe *QueryEngine) performLeftJoin(leftRows, rightRows []Row, condition JoinCondition) []Row {
	var result []Row
	
	leftColName := condition.LeftTable + "." + condition.LeftColumn
	rightColName := condition.RightTable + "." + condition.RightColumn
	
	for _, leftRow := range leftRows {
		matched := false
		
		for _, rightRow := range rightRows {
			leftValue, leftExists := leftRow[leftColName]
			rightValue, rightExists := rightRow[rightColName]
			
			if leftExists && rightExists && qe.valuesMatch(leftValue, rightValue, condition.Operator) {
				// Combine rows
				combinedRow := make(Row)
				for k, v := range leftRow {
					combinedRow[k] = v
				}
				for k, v := range rightRow {
					combinedRow[k] = v
				}
				result = append(result, combinedRow)
				matched = true
			}
		}
		
		if !matched {
			// Include left row with NULL values for right table columns
			combinedRow := make(Row)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			// Add NULL values for right table columns
			for _, rightRow := range rightRows {
				for k := range rightRow {
					combinedRow[k] = nil
				}
				break // Just need the column names from one row
			}
			result = append(result, combinedRow)
		}
	}
	
	return result
}

// performRightJoin implements RIGHT JOIN logic
func (qe *QueryEngine) performRightJoin(leftRows, rightRows []Row, condition JoinCondition) []Row {
	var result []Row
	
	leftColName := condition.LeftTable + "." + condition.LeftColumn
	rightColName := condition.RightTable + "." + condition.RightColumn
	
	for _, rightRow := range rightRows {
		matched := false
		
		for _, leftRow := range leftRows {
			leftValue, leftExists := leftRow[leftColName]
			rightValue, rightExists := rightRow[rightColName]
			
			if leftExists && rightExists && qe.valuesMatch(leftValue, rightValue, condition.Operator) {
				// Combine rows
				combinedRow := make(Row)
				for k, v := range leftRow {
					combinedRow[k] = v
				}
				for k, v := range rightRow {
					combinedRow[k] = v
				}
				result = append(result, combinedRow)
				matched = true
			}
		}
		
		if !matched {
			// Include right row with NULL values for left table columns
			combinedRow := make(Row)
			// Add NULL values for left table columns
			for _, leftRow := range leftRows {
				for k := range leftRow {
					combinedRow[k] = nil
				}
				break // Just need the column names from one row
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}
			result = append(result, combinedRow)
		}
	}
	
	return result
}

// performFullOuterJoin implements FULL OUTER JOIN logic
func (qe *QueryEngine) performFullOuterJoin(leftRows, rightRows []Row, condition JoinCondition) []Row {
	var result []Row
	leftColName := condition.LeftTable + "." + condition.LeftColumn
	rightColName := condition.RightTable + "." + condition.RightColumn
	
	// Track which right rows have been matched to avoid duplicates
	rightMatched := make(map[int]bool)
	
	// First pass: process all left rows (like LEFT JOIN)
	for _, leftRow := range leftRows {
		leftMatched := false
		
		for rightIdx, rightRow := range rightRows {
			leftValue, leftExists := leftRow[leftColName]
			rightValue, rightExists := rightRow[rightColName]
			
			if leftExists && rightExists && qe.valuesMatch(leftValue, rightValue, condition.Operator) {
				// Combine rows
				combinedRow := make(Row)
				for k, v := range leftRow {
					combinedRow[k] = v
				}
				for k, v := range rightRow {
					combinedRow[k] = v
				}
				result = append(result, combinedRow)
				leftMatched = true
				rightMatched[rightIdx] = true
			}
		}
		
		if !leftMatched {
			// Include left row with NULL values for right table columns
			combinedRow := make(Row)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			// Add NULL values for right table columns
			if len(rightRows) > 0 {
				for k := range rightRows[0] {
					combinedRow[k] = nil
				}
			}
			result = append(result, combinedRow)
		}
	}
	
	// Second pass: add unmatched right rows
	for rightIdx, rightRow := range rightRows {
		if !rightMatched[rightIdx] {
			// Include right row with NULL values for left table columns
			combinedRow := make(Row)
			// Add NULL values for left table columns
			if len(leftRows) > 0 {
				for k := range leftRows[0] {
					combinedRow[k] = nil
				}
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}
			result = append(result, combinedRow)
		}
	}
	
	return result
}

// valuesMatch checks if two values match according to the given operator
func (qe *QueryEngine) valuesMatch(leftValue, rightValue interface{}, operator string) bool {
	// Create a dummy reader for comparison logic
	dummyReader := &ParquetReader{}
	
	switch operator {
	case "=":
		return dummyReader.CompareValues(leftValue, rightValue) == 0
	case "!=", "<>":
		return dummyReader.CompareValues(leftValue, rightValue) != 0
	case "<":
		return dummyReader.CompareValues(leftValue, rightValue) < 0
	case "<=":
		return dummyReader.CompareValues(leftValue, rightValue) <= 0
	case ">":
		return dummyReader.CompareValues(leftValue, rightValue) > 0
	case ">=":
		return dummyReader.CompareValues(leftValue, rightValue) >= 0
	default:
		return dummyReader.CompareValues(leftValue, rightValue) == 0
	}
}

// filterJoinedRows applies WHERE conditions to joined rows
func (qe *QueryEngine) filterJoinedRows(rows []Row, conditions []WhereCondition) []Row {
	return qe.filterJoinedRowsWithEngine(rows, conditions)
}

func (qe *QueryEngine) filterJoinedRowsWithEngine(rows []Row, conditions []WhereCondition) []Row {
	if len(conditions) == 0 {
		return rows
	}
	
	var filtered []Row
	for _, row := range rows {
		if qe.rowMatchesConditionsWithEngine(row, conditions) {
			filtered = append(filtered, row)
		}
	}
	
	return filtered
}

// rowMatchesConditions checks if a row matches all WHERE conditions
func (qe *QueryEngine) rowMatchesConditions(row Row, conditions []WhereCondition) bool {
	return qe.rowMatchesConditionsWithEngine(row, conditions)
}

func (qe *QueryEngine) rowMatchesConditionsWithEngine(row Row, conditions []WhereCondition) bool {
	dummyReader := &ParquetReader{}
	
	for _, condition := range conditions {
		columnName := condition.Column
		if condition.TableName != "" {
			columnName = condition.TableName + "." + condition.Column
		}
		
		_, exists := row[columnName]
		if !exists && condition.Subquery == nil {
			return false
		}
		
		if !dummyReader.matchesConditionWithEngine(row, WhereCondition{
			Column:    columnName,
			Operator:  condition.Operator,
			Value:     condition.Value,
			ValueList: condition.ValueList,
			Subquery:  condition.Subquery,
		}, qe) {
			return false
		}
	}
	
	return true
}

// selectJoinColumns selects and renames columns from joined rows
func (qe *QueryEngine) selectJoinColumns(rows []Row, columns []Column) []Row {
	if len(rows) == 0 {
		return rows
	}
	
	// Check if we need to select all columns
	hasWildcard := false
	for _, col := range columns {
		if col.Name == "*" {
			hasWildcard = true
			break
		}
	}
	
	if hasWildcard {
		return rows // Return all columns
	}
	
	// Select specific columns
	var result []Row
	for _, row := range rows {
		newRow := make(Row)
		
		for _, col := range columns {
			var sourceColumnName string
			if col.TableName != "" {
				sourceColumnName = col.TableName + "." + col.Name
			} else {
				// Try to find the column without table qualification
				sourceColumnName = col.Name
				for rowCol := range row {
					if strings.HasSuffix(rowCol, "."+col.Name) {
						sourceColumnName = rowCol
						break
					}
				}
			}
			
			if value, exists := row[sourceColumnName]; exists {
				resultColumnName := col.Name
				if col.Alias != "" {
					resultColumnName = col.Alias
				}
				newRow[resultColumnName] = value
			}
		}
		
		result = append(result, newRow)
	}
	
	return result
}

// sortJoinedRows applies ORDER BY to joined rows
func (qe *QueryEngine) sortJoinedRows(rows []Row, orderBy []OrderByColumn) []Row {
	// This is a simplified implementation
	// In practice, you'd need to handle qualified column names properly
	return rows // For now, skip sorting in JOINs
}

// getJoinResultColumns extracts column names from the result
func (qe *QueryEngine) getJoinResultColumns(rows []Row, queryColumns []Column) []string {
	if len(rows) == 0 {
		return []string{}
	}
	
	var columns []string
	for key := range rows[0] {
		columns = append(columns, key)
	}
	
	return columns
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

// NewQueryEngineWithCache creates a QueryEngine with custom cache configuration
func NewQueryEngineWithCache(dataPath string, cacheConfig CacheConfig) *QueryEngine {
	return &QueryEngine{
		parser:      NewSQLParser(),
		dataPath:    dataPath,
		openReaders: make(map[string]*ParquetReader),
		cache:       NewQueryCache(cacheConfig),
	}
}

// GetCacheStats returns current cache statistics
func (qe *QueryEngine) GetCacheStats() CacheStats {
	return qe.cache.GetStats()
}

// ClearCache removes all cached entries
func (qe *QueryEngine) ClearCache() {
	qe.cache.Clear()
}

// SetCacheEnabled enables or disables query result caching
func (qe *QueryEngine) SetCacheEnabled(enabled bool) {
	qe.cache.config.Enabled = enabled
}

// hasOnlyConstantColumns checks if the query only selects constant values
func (qe *QueryEngine) hasOnlyConstantColumns(query *ParsedQuery) bool {
	if len(query.Columns) == 0 {
		return false
	}
	
	for _, col := range query.Columns {
		if col.Name == "*" {
			return false // Wildcard is not a constant
		}
		if !qe.isConstantColumnName(col.Name) {
			return false
		}
	}
	
	return true
}

// isConstantColumnName checks if a column name represents a constant value
func (qe *QueryEngine) isConstantColumnName(name string) bool {
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

// ExecuteSubquery implements the SubqueryExecutor interface
func (qe *QueryEngine) ExecuteSubquery(query *ParsedQuery) (*QueryResult, error) {
	// Execute subquery without caching to avoid cache pollution
	// and infinite recursion issues
	switch query.Type {
	case SELECT:
		// Handle constant-only queries (no FROM clause)
		if query.TableName == "" && qe.hasOnlyConstantColumns(query) {
			return qe.executeConstantQuery(query)
		}
		
		if query.HasJoins {
			return qe.executeJoinQuery(query)
		} else {
			return qe.executeSelect(query)
		}
	default:
		return &QueryResult{
			Query: "subquery",
			Error: "unsupported subquery type",
		}, nil
	}
}

// ExecuteCorrelatedSubquery implements the SubqueryExecutor interface for correlated subqueries
func (qe *QueryEngine) ExecuteCorrelatedSubquery(query *ParsedQuery, outerRow Row) (*QueryResult, error) {
	
	if !query.IsCorrelated {
		// If not correlated, use regular execution
		return qe.ExecuteSubquery(query)
	}
	
	// Execute correlated subquery with outer row context
	switch query.Type {
	case SELECT:
		// Handle constant-only queries (no FROM clause)
		if query.TableName == "" && qe.hasOnlyConstantColumns(query) {
			return qe.executeConstantQuery(query)
		}
		
		if query.HasJoins {
			return qe.executeCorrelatedJoinQuery(query, outerRow)
		} else {
			return qe.executeCorrelatedSelect(query, outerRow)
		}
	default:
		return &QueryResult{
			Query: "correlated subquery",
			Error: "unsupported correlated subquery type",
		}, nil
	}
}

// executeConstantQuery handles queries with only constants (no table access needed)
func (qe *QueryEngine) executeConstantQuery(query *ParsedQuery) (*QueryResult, error) {
	// Create a single row with constant values
	row := make(Row)
	var columns []string
	
	for _, col := range query.Columns {
		// Parse the constant value
		value := qe.parseConstantValue(col.Name)
		
		columnName := col.Name
		if col.Alias != "" {
			columnName = col.Alias
		}
		
		row[columnName] = value
		columns = append(columns, columnName)
	}
	
	return &QueryResult{
		Columns: columns,
		Rows:    []Row{row},
		Count:   1,
		Query:   "constant query",
	}, nil
}

// parseConstantValue converts a constant string to its appropriate type
func (qe *QueryEngine) parseConstantValue(name string) interface{} {
	// For numeric constants
	if len(name) > 0 && (name[0] >= '0' && name[0] <= '9') {
		// Try to parse as integer first
		if val, err := strconv.Atoi(name); err == nil {
			return val
		}
		// Try to parse as float
		if val, err := strconv.ParseFloat(name, 64); err == nil {
			return val
		}
	}
	// For string constants
	if len(name) >= 2 && name[0] == '\'' && name[len(name)-1] == '\'' {
		return name[1 : len(name)-1] // Remove quotes
	}
	// Default to the name itself
	return name
}

// executeCorrelatedSelect executes a correlated SELECT query with outer row context
func (qe *QueryEngine) executeCorrelatedSelect(query *ParsedQuery, outerRow Row) (*QueryResult, error) {
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return &QueryResult{
			Query: "correlated select",
			Error: fmt.Sprintf("failed to open file: %v", err),
		}, nil
	}
	defer reader.Close()

	// Read all rows and filter with correlation context
	allRows, err := reader.ReadAll()
	if err != nil {
		return &QueryResult{
			Query: "correlated select",
			Error: fmt.Sprintf("failed to read data: %v", err),
		}, nil
	}

	// Filter rows using WHERE conditions with outer row context
	var filteredRows []Row
	for _, row := range allRows {
		if qe.matchesCorrelatedWhereConditions(row, query.Where, outerRow) {
			filteredRows = append(filteredRows, row)
		}
	}

	// Select specific columns
	var result []Row
	if reader.hasColumnSubqueries(query.Columns) {
		result = reader.SelectColumnsWithEngine(filteredRows, query.Columns, qe)
	} else {
		result = reader.SelectColumns(filteredRows, query.Columns)
	}
	
	// Apply aggregations if needed (delegate to regular executeSelect for now)
	if query.IsAggregate {
		return qe.executeAggregate(query, result, reader)
	}
	
	// Apply ordering
	if len(query.OrderBy) > 0 {
		result = qe.sortRows(result, query.OrderBy, reader)
	}
	
	// Apply limit
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}

	return &QueryResult{
		Columns: qe.getResultColumns(result, query.Columns),
		Rows:    result,
		Count:   len(result),
		Query:   "correlated select",
	}, nil
}

// executeCorrelatedJoinQuery executes a correlated JOIN query with outer row context
func (qe *QueryEngine) executeCorrelatedJoinQuery(query *ParsedQuery, outerRow Row) (*QueryResult, error) {
	// For simplicity, delegate to regular JOIN for now
	// In a full implementation, we'd pass outer context through JOIN processing
	return qe.executeJoinQuery(query)
}

// matchesCorrelatedWhereConditions checks if a row matches WHERE conditions with outer row context
func (qe *QueryEngine) matchesCorrelatedWhereConditions(row Row, conditions []WhereCondition, outerRow Row) bool {
	
	for _, condition := range conditions {
		if !qe.matchesCorrelatedCondition(row, condition, outerRow) {
			return false
		}
	}
	return true
}

// matchesCorrelatedCondition checks if a row matches a single WHERE condition with outer row context  
func (qe *QueryEngine) matchesCorrelatedCondition(row Row, condition WhereCondition, outerRow Row) bool {
	// Handle complex logical conditions (AND/OR)
	if condition.IsComplex {
		switch condition.LogicalOp {
		case "AND":
			return qe.matchesCorrelatedCondition(row, *condition.Left, outerRow) && 
				   qe.matchesCorrelatedCondition(row, *condition.Right, outerRow)
		case "OR":
			return qe.matchesCorrelatedCondition(row, *condition.Left, outerRow) || 
				   qe.matchesCorrelatedCondition(row, *condition.Right, outerRow)
		default:
			return false
		}
	}
	
	var leftValue interface{}
	
	// Determine the left value - could be from current row or outer row
	if condition.TableName != "" {
		// This is a qualified column reference - need to determine if it refers to outer or inner table
		// For correlated subqueries, we need to be more careful about which table the column refers to
		
		// Check if this table alias refers to the outer query (we need outer query table info for this)
		// For now, use a simple heuristic: if the qualified key exists in outer row, use it
		qualifiedKey := condition.TableName + "." + condition.Column
		if outerValue, exists := outerRow[qualifiedKey]; exists {
			// Table alias found in outer row - use outer context
			leftValue = outerValue
		} else {
			// Table alias not in outer row - must be inner table reference, use inner row
			leftValue = row[condition.Column]
		}
	} else {
		leftValue = row[condition.Column]
	}
	
	// Handle subquery conditions
	if condition.Subquery != nil {
		return qe.matchesCorrelatedSubqueryCondition(leftValue, condition, outerRow)
	}
	
	// Create a dummy reader for comparison operations
	dummyReader := &ParquetReader{}
	
	// Determine the right value - could be a constant or column reference
	var rightValue interface{}
	if condition.ValueColumn != "" {
		// This is a column-to-column comparison (e.g., e2.department = e.department)
		if condition.ValueTableName != "" {
			// Qualified column reference - resolve from appropriate context
			qualifiedKey := condition.ValueTableName + "." + condition.ValueColumn
			if outerValue, exists := outerRow[qualifiedKey]; exists {
				rightValue = outerValue
			} else if outerValue, exists := outerRow[condition.ValueColumn]; exists {
				rightValue = outerValue
			} else {
				rightValue = row[condition.ValueColumn]
			}
		} else {
			// Unqualified column reference
			rightValue = row[condition.ValueColumn]
		}
	} else {
		// Regular constant value
		rightValue = condition.Value
	}
	
	// Handle regular conditions
	switch condition.Operator {
	case "=":
		return leftValue == rightValue
	case "!=":
		return leftValue != rightValue
	case "<":
		return dummyReader.CompareValues(leftValue, rightValue) < 0
	case "<=":
		return dummyReader.CompareValues(leftValue, rightValue) <= 0
	case ">":
		return dummyReader.CompareValues(leftValue, rightValue) > 0
	case ">=":
		return dummyReader.CompareValues(leftValue, rightValue) >= 0
	case "IN":
		for _, val := range condition.ValueList {
			if leftValue == val {
				return true
			}
		}
		return false
	case "LIKE":
		return dummyReader.matchesLike(leftValue, rightValue)
	}
	
	return false
}

// matchesCorrelatedSubqueryCondition handles subquery conditions with outer row context
func (qe *QueryEngine) matchesCorrelatedSubqueryCondition(leftValue interface{}, condition WhereCondition, outerRow Row) bool {
	// Execute subquery with correlation context
	var result *QueryResult
	var err error
	
	if condition.Subquery.IsCorrelated {
		result, err = qe.ExecuteCorrelatedSubquery(condition.Subquery, outerRow)
	} else {
		result, err = qe.ExecuteSubquery(condition.Subquery)
	}
	
	if err != nil || result.Error != "" {
		return false
	}
	
	switch condition.Operator {
	case "IN":
		for _, row := range result.Rows {
			for _, col := range result.Columns {
				if leftValue == row[col] {
					return true
				}
			}
		}
		return false
	case "NOT IN":
		for _, row := range result.Rows {
			for _, col := range result.Columns {
				if leftValue == row[col] {
					return false
				}
			}
		}
		return true
	case "EXISTS":
		return len(result.Rows) > 0
	case "=":
		if len(result.Rows) == 1 {
			for _, col := range result.Columns {
				return leftValue == result.Rows[0][col]
			}
		}
		return false
	}
	
	return false
}

// Helper function for debug output
func getRowKeys(row Row) []string {
	var keys []string
	for k, v := range row {
		keys = append(keys, fmt.Sprintf("%s=%v", k, v))
	}
	return keys
}
