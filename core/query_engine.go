package core

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueryEngine struct {
	parser      *SQLParser
	dataPath    string
	httpTables  map[string]string // Map table names to HTTP URLs
	openReaders map[string]*ParquetReader
	readersMu   sync.RWMutex
	cache       *QueryCache
	planner     *QueryPlanner
	optimizer   *QueryOptimizer
	functions   *FunctionRegistry
}

type QueryResult struct {
	Columns           []string               `json:"columns"`
	Rows              []Row                  `json:"rows"`
	Count             int                    `json:"count"`
	Query             string                 `json:"query"`
	Error             string                 `json:"error,omitempty"`
	OptimizationStats map[string]interface{} `json:"optimization_stats,omitempty"`
}

func NewQueryEngine(dataPath string) *QueryEngine {
	// Default cache configuration
	cacheConfig := CacheConfig{
		MaxMemoryMB: 100,             // 100MB default cache size
		DefaultTTL:  5 * time.Minute, // 5 minute default TTL
		Enabled:     true,            // Enable caching by default
	}

	planner := NewQueryPlanner()
	planner.statsCollector.Initialize(dataPath)
	optimizer := NewQueryOptimizer(planner)

	return &QueryEngine{
		parser:      NewSQLParser(),
		dataPath:    dataPath,
		httpTables:  make(map[string]string),
		openReaders: make(map[string]*ParquetReader),
		cache:       NewQueryCache(cacheConfig),
		planner:     planner,
		optimizer:   optimizer,
		functions:   NewFunctionRegistry(),
	}
}

func (qe *QueryEngine) Close() {
	qe.readersMu.Lock()
	defer qe.readersMu.Unlock()

	for _, reader := range qe.openReaders {
		reader.Close()
	}
}

// EvaluateFunction implements the SubqueryExecutor interface
func (qe *QueryEngine) EvaluateFunction(fn *FunctionCall, row Row) (interface{}, error) {
	return qe.functions.EvaluateFunction(fn, row)
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
		// Handle queries without FROM clause
		if parsedQuery.TableName == "" {
			if qe.hasOnlyConstantColumns(parsedQuery) {
				result, err = qe.executeConstantQuery(parsedQuery)
			} else {
				// Has functions or other expressions without FROM clause
				result, err = qe.executeNoTableQuery(parsedQuery)
			}
		} else {
			result, err = qe.executeSelect(parsedQuery)
		}
	case UNION:
		result, err = qe.executeUnion(parsedQuery)
	case EXPLAIN:
		result, err = qe.executeExplain(parsedQuery)
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

func (qe *QueryEngine) executeCTEQuery(query *ParsedQuery, cteRows []Row) (*QueryResult, error) {
	// The CTE rows are our base data
	rows := cteRows

	// Apply WHERE clause filtering if present
	if len(query.Where) > 0 {
		var filteredRows []Row
		for _, row := range rows {
			// Create a dummy reader for condition evaluation
			// This is needed because our filter functions expect a ParquetReader
			if qe.evaluateCTEConditions(query.Where, row) {
				filteredRows = append(filteredRows, row)
			}
		}
		rows = filteredRows
	}

	// Handle aggregation
	if query.IsAggregate {
		// For CTE queries, we need to handle aggregation differently
		// since we don't have a ParquetReader
		result, err := qe.executeAggregate(query, rows, nil)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Apply ORDER BY
	if len(query.OrderBy) > 0 {
		rows = qe.sortRows(rows, query.OrderBy, nil)
	}

	// Apply LIMIT
	if query.Limit > 0 && len(rows) > query.Limit {
		rows = rows[:query.Limit]
	}

	// Select only requested columns
	// For CTE results, we need to handle column selection manually
	selectedRows := qe.selectCTEColumns(rows, query.Columns)

	return &QueryResult{
		Columns: qe.getResultColumns(selectedRows, query.Columns),
		Rows:    selectedRows,
		Count:   len(selectedRows),
		Query:   query.RawSQL,
	}, nil
}

func (qe *QueryEngine) evaluateCTEConditions(conditions []WhereCondition, row Row) bool {
	// For simplicity, we'll implement basic condition evaluation for CTEs
	// This is a simplified version that doesn't support all features
	for _, condition := range conditions {
		if !qe.evaluateCTECondition(condition, row) {
			return false
		}
	}
	return true
}

func (qe *QueryEngine) evaluateCTECondition(condition WhereCondition, row Row) bool {
	// Handle complex conditions (AND/OR)
	if condition.IsComplex {
		switch condition.LogicalOp {
		case "AND":
			return qe.evaluateCTECondition(*condition.Left, row) &&
				qe.evaluateCTECondition(*condition.Right, row)
		case "OR":
			return qe.evaluateCTECondition(*condition.Left, row) ||
				qe.evaluateCTECondition(*condition.Right, row)
		}
	}

	// Get left side value (column or function)
	var leftValue interface{}
	var exists bool

	if condition.Function != nil {
		// Evaluate function on left side
		var err error
		leftValue, err = qe.functions.EvaluateFunction(condition.Function, row)
		if err != nil {
			return false
		}
		exists = true
	} else if condition.Column != "" {
		// Get column value
		leftValue, exists = row[condition.Column]
		if !exists {
			return false
		}
	} else {
		return false
	}

	// Get right side value (literal, column, or function)
	var rightValue interface{}
	if condition.ValueFunction != nil {
		// Evaluate function on right side
		var err error
		rightValue, err = qe.functions.EvaluateFunction(condition.ValueFunction, row)
		if err != nil {
			return false
		}
	} else if condition.ValueColumn != "" {
		// Column comparison
		rightValue, _ = row[condition.ValueColumn]
	} else {
		// Literal value
		rightValue = condition.Value
	}

	// Simple comparison operators
	switch condition.Operator {
	case "=":
		return qe.compareValues(leftValue, rightValue) == 0
	case "!=", "<>":
		return qe.compareValues(leftValue, rightValue) != 0
	case ">":
		return qe.compareValues(leftValue, rightValue) > 0
	case ">=":
		return qe.compareValues(leftValue, rightValue) >= 0
	case "<":
		return qe.compareValues(leftValue, rightValue) < 0
	case "<=":
		return qe.compareValues(leftValue, rightValue) <= 0
	case "IS NULL":
		return leftValue == nil
	case "IS NOT NULL":
		return leftValue != nil
	case "LIKE":
		// Simple LIKE implementation
		if leftStr, ok := leftValue.(string); ok {
			if pattern, ok := rightValue.(string); ok {
				// Convert SQL LIKE pattern to Go regex
				regexPattern := strings.ReplaceAll(pattern, "%", ".*")
				regexPattern = strings.ReplaceAll(regexPattern, "_", ".")
				regexPattern = "^" + regexPattern + "$"
				matched, _ := regexp.MatchString(regexPattern, leftStr)
				return matched
			}
		}
		return false
	}

	return true
}

func (qe *QueryEngine) selectCTEColumns(rows []Row, columns []Column) []Row {
	// If no specific columns requested or "*", return all columns
	if len(columns) == 0 || (len(columns) == 1 && columns[0].Name == "*") {
		return rows
	}

	// Select specific columns
	selectedRows := make([]Row, len(rows))
	for i, row := range rows {
		selectedRow := make(Row)
		for _, col := range columns {
			var value interface{}
			var err error

			// Handle different column types
			if col.Function != nil {
				// Evaluate function
				value, err = qe.functions.EvaluateFunction(col.Function, row)
				if err != nil {
					// If function evaluation fails, use nil
					value = nil
				}
			} else if col.Subquery != nil {
				// Handle subquery columns (existing logic would go here)
				value = nil // Placeholder
			} else if col.CaseExpr != nil {
				// Handle CASE expressions (existing logic would go here)
				value = nil // Placeholder
			} else {
				// Regular column
				if val, exists := row[col.Name]; exists {
					value = val
				}
			}

			// Use alias if provided, otherwise use column name
			key := col.Name
			if col.Alias != "" {
				key = col.Alias
			}
			if value != nil {
				selectedRow[key] = value
			}
		}
		selectedRows[i] = selectedRow
	}

	return selectedRows
}

func (qe *QueryEngine) applyCTEColumnAliases(rows []Row, originalColumns []string, aliasNames []string) []Row {
	// If no aliases or mismatched counts, return as-is
	if len(aliasNames) == 0 || len(aliasNames) != len(originalColumns) {
		return rows
	}

	// Create mapping from original to alias names
	columnMapping := make(map[string]string)
	for i, originalCol := range originalColumns {
		if i < len(aliasNames) {
			columnMapping[originalCol] = aliasNames[i]
		}
	}

	// Apply the mapping to all rows
	aliasedRows := make([]Row, len(rows))
	for i, row := range rows {
		aliasedRow := make(Row)
		for originalCol, value := range row {
			// Use alias if mapping exists, otherwise keep original
			newCol := originalCol
			if alias, exists := columnMapping[originalCol]; exists {
				newCol = alias
			}
			aliasedRow[newCol] = value
		}
		aliasedRows[i] = aliasedRow
	}

	return aliasedRows
}

func (qe *QueryEngine) executeSelect(query *ParsedQuery) (*QueryResult, error) {
	return qe.executeSelectWithCTEs(query, nil)
}

func (qe *QueryEngine) executeSelectWithCTEs(query *ParsedQuery, parentCTEs map[string][]Row) (*QueryResult, error) {
	// Initialize CTE results with parent CTEs if provided
	cteResults := make(map[string][]Row)
	if parentCTEs != nil {
		for name, rows := range parentCTEs {
			cteResults[name] = rows
		}
	}

	// Process CTEs if present
	if len(query.CTEs) > 0 {
		for _, cte := range query.CTEs {
			// Execute each CTE and store the results
			// Pass existing CTEs so they can reference each other
			cteResult, err := qe.executeSelectWithCTEs(cte.Query, cteResults)
			if err != nil {
				return nil, fmt.Errorf("failed to execute CTE '%s': %w", cte.Name, err)
			}

			// If the CTE has column aliases, apply them
			rows := cteResult.Rows
			if len(cte.ColumnNames) > 0 {
				rows = qe.applyCTEColumnAliases(rows, cteResult.Columns, cte.ColumnNames)
			}

			// Store the CTE results with aliased columns
			cteResults[cte.Name] = rows
		}
	}

	// Check if the main query references a CTE
	if cteRows, isCTE := cteResults[query.TableName]; isCTE {
		return qe.executeCTEQuery(query, cteRows)
	}

	// Check if this is a JOIN query
	if query.HasJoins {
		return qe.executeJoinQuery(query)
	}

	// Try optimized execution first
	if optimizedResult := qe.executeOptimized(query); optimizedResult != nil {
		return optimizedResult, nil
	}

	// Fallback to original execution logic
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

	// Handle window functions
	if query.HasWindowFuncs {
		rows = qe.executeWindowFunctions(query, rows, reader)
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

	// Evaluate function calls and compute column values
	rows = qe.evaluateQueryColumns(rows, query.Columns)

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
	// Try to get existing reader with read lock
	qe.readersMu.RLock()
	if reader, exists := qe.openReaders[tableName]; exists {
		qe.readersMu.RUnlock()
		return reader, nil
	}
	qe.readersMu.RUnlock()

	// Need to create new reader - use write lock
	qe.readersMu.Lock()
	defer qe.readersMu.Unlock()

	// Double-check pattern - another goroutine might have created it
	if reader, exists := qe.openReaders[tableName]; exists {
		return reader, nil
	}

	var filePath string

	// Check if this table is registered as an HTTP table
	if httpURL, exists := qe.httpTables[tableName]; exists {
		filePath = httpURL
	} else {
		filePath = filepath.Join(qe.dataPath, tableName+".parquet")
	}

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

// evaluateQueryColumns processes query columns and evaluates function calls
func (qe *QueryEngine) evaluateQueryColumns(rows []Row, queryColumns []Column) []Row {
	if len(queryColumns) == 0 {
		return rows
	}

	result := make([]Row, len(rows))

	for i, row := range rows {
		newRow := make(Row)

		for _, col := range queryColumns {
			var value interface{}
			var err error

			// Check if this is a function call
			if col.Function != nil {
				value, err = qe.functions.EvaluateFunction(col.Function, row)
				if err != nil {
					// If function evaluation fails, set to nil
					value = nil
				}
			} else if col.CaseExpr != nil {
				// Handle CASE expressions
				reader := &ParquetReader{} // Create a temporary reader for evaluation
				value = reader.evaluateCaseExpression(col.CaseExpr, row, qe)
			} else if col.Subquery != nil {
				// Handle subqueries in SELECT clause
				reader := &ParquetReader{} // Create a temporary reader for evaluation
				value = reader.executeColumnSubquery(col.Subquery, row, qe)
			} else if col.Name == "*" {
				// For wildcard, copy all original columns
				for k, v := range row {
					newRow[k] = v
				}
				continue
			} else {
				// Regular column - get value from row
				if col.TableName != "" {
					value = row[col.TableName+"."+col.Name]
				} else {
					value = row[col.Name]
				}
			}

			// Set the value with appropriate key
			if col.Alias != "" {
				newRow[col.Alias] = value
			} else if col.Function != nil {
				// For functions without alias, use function name + args as key
				newRow[col.Function.Name] = value
			} else if col.CaseExpr != nil {
				// For CASE expressions without alias, use "case" as key
				newRow["case"] = value
			} else if col.Subquery != nil {
				// For subqueries without alias, use "subquery" as key
				newRow["subquery"] = value
			} else {
				newRow[col.Name] = value
			}
		}

		result[i] = newRow
	}

	return result
}

func (qe *QueryEngine) getResultColumns(rows []Row, queryColumns []Column) []string {
	// If there are query columns specified, use those to maintain order
	if len(queryColumns) > 0 {
		var columns []string
		for _, col := range queryColumns {
			if col.Alias != "" {
				columns = append(columns, col.Alias)
			} else {
				columns = append(columns, col.Name)
			}
		}
		return columns
	}

	if len(rows) == 0 {
		return []string{}
	}

	// When no query columns are specified, we need to maintain a consistent order
	// Sort the keys to ensure deterministic column ordering
	columnSet := make(map[string]bool)
	var columns []string

	// First collect all unique columns
	for key := range rows[0] {
		if !columnSet[key] {
			columns = append(columns, key)
			columnSet[key] = true
		}
	}

	// Sort columns to ensure consistent ordering
	sort.Strings(columns)

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
	planner := NewQueryPlanner()
	planner.statsCollector.Initialize(dataPath)
	optimizer := NewQueryOptimizer(planner)

	return &QueryEngine{
		parser:      NewSQLParser(),
		dataPath:    dataPath,
		httpTables:  make(map[string]string),
		openReaders: make(map[string]*ParquetReader),
		cache:       NewQueryCache(cacheConfig),
		planner:     planner,
		optimizer:   optimizer,
		functions:   NewFunctionRegistry(),
	}
}

// RegisterHTTPTable registers an HTTP URL for a table name
func (qe *QueryEngine) RegisterHTTPTable(tableName, url string) {
	qe.readersMu.Lock()
	defer qe.readersMu.Unlock()
	qe.httpTables[tableName] = url
}

// UnregisterHTTPTable removes an HTTP table registration
func (qe *QueryEngine) UnregisterHTTPTable(tableName string) {
	qe.readersMu.Lock()
	defer qe.readersMu.Unlock()
	delete(qe.httpTables, tableName)
	// Also close and remove any open reader for this table
	if reader, exists := qe.openReaders[tableName]; exists {
		reader.Close()
		delete(qe.openReaders, tableName)
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
		// Check if column has function, subquery, or other non-constant features
		if col.Function != nil || col.Subquery != nil || col.CaseExpr != nil || col.WindowFunc != nil {
			return false
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
	// Mark as subquery to avoid optimization issues
	query.IsSubquery = true

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

// executeNoTableQuery handles queries without FROM clause that have functions or expressions
func (qe *QueryEngine) executeNoTableQuery(query *ParsedQuery) (*QueryResult, error) {
	// Create a single empty row for function evaluation
	row := make(Row)

	// Process columns using selectCTEColumns which handles functions
	rows := []Row{row}
	rows = qe.selectCTEColumns(rows, query.Columns)

	// Extract column names
	var columns []string
	if len(rows) > 0 {
		for key := range rows[0] {
			columns = append(columns, key)
		}
		sort.Strings(columns)
	}

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
		Count:   len(rows),
		Query:   query.RawSQL,
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

// executeWindowFunctions processes window functions and adds their results to rows
func (qe *QueryEngine) executeWindowFunctions(query *ParsedQuery, rows []Row, reader *ParquetReader) []Row {
	if len(query.WindowFuncs) == 0 {
		return rows
	}

	// Process each window function
	for _, winFunc := range query.WindowFuncs {
		rows = qe.executeWindowFunction(winFunc, rows, reader)
	}

	return rows
}

// executeWindowFunction processes a single window function
func (qe *QueryEngine) executeWindowFunction(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	switch winFunc.Function {
	case "ROW_NUMBER":
		return qe.executeRowNumber(winFunc, rows, reader)
	case "RANK":
		return qe.executeRank(winFunc, rows, reader)
	case "DENSE_RANK":
		return qe.executeDenseRank(winFunc, rows, reader)
	case "LAG":
		return qe.executeLag(winFunc, rows, reader)
	case "LEAD":
		return qe.executeLead(winFunc, rows, reader)
	default:
		// Unknown window function, return rows unchanged
		return rows
	}
}

// executeRowNumber implements ROW_NUMBER() window function
func (qe *QueryEngine) executeRowNumber(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	// Group rows by partition if PARTITION BY is specified
	partitions := qe.partitionRows(rows, winFunc.WindowSpec.PartitionBy)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		if len(winFunc.WindowSpec.OrderBy) > 0 {
			partition = qe.sortRows(partition, winFunc.WindowSpec.OrderBy, reader)
		}

		// Assign row numbers within this partition
		for i, row := range partition {
			row[winFunc.Alias] = i + 1 // ROW_NUMBER starts at 1
		}
	}

	return rows
}

// partitionRows groups rows by the specified partition columns
func (qe *QueryEngine) partitionRows(rows []Row, partitionBy []string) [][]Row {
	if len(partitionBy) == 0 {
		// No partitioning, return all rows as a single partition
		return [][]Row{rows}
	}

	partitionMap := make(map[string][]Row)

	for _, row := range rows {
		// Create a key from the partition columns
		var keyParts []string
		for _, col := range partitionBy {
			if val, exists := row[col]; exists {
				keyParts = append(keyParts, fmt.Sprintf("%v", val))
			} else {
				keyParts = append(keyParts, "NULL")
			}
		}
		key := strings.Join(keyParts, "|")

		partitionMap[key] = append(partitionMap[key], row)
	}

	// Convert map to slice
	var partitions [][]Row
	for _, partition := range partitionMap {
		partitions = append(partitions, partition)
	}

	return partitions
}

// compareRowsForRanking compares two rows based on the specified ORDER BY columns
// Returns: -1 if row1 < row2, 0 if row1 == row2, 1 if row1 > row2
func (qe *QueryEngine) compareRowsForRanking(row1, row2 Row, orderBy []OrderByColumn) int {
	for _, col := range orderBy {
		val1, exists1 := row1[col.Column]
		val2, exists2 := row2[col.Column]

		// Handle missing values
		if !exists1 && !exists2 {
			continue
		}
		if !exists1 {
			if col.Direction == "DESC" {
				return 1
			} else {
				return -1
			}
		}
		if !exists2 {
			if col.Direction == "DESC" {
				return -1
			} else {
				return 1
			}
		}

		// Compare values based on type
		cmp := qe.compareValues(val1, val2)
		if cmp != 0 {
			if col.Direction == "DESC" {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

// compareValues compares two interface{} values
func (qe *QueryEngine) compareValues(val1, val2 interface{}) int {
	// Handle nil values
	if val1 == nil && val2 == nil {
		return 0
	}
	if val1 == nil {
		return -1
	}
	if val2 == nil {
		return 1
	}

	// Convert to comparable types
	switch v1 := val1.(type) {
	case int:
		if v2, ok := val2.(int); ok {
			if v1 < v2 {
				return -1
			}
			if v1 > v2 {
				return 1
			}
			return 0
		}
	case int64:
		if v2, ok := val2.(int64); ok {
			if v1 < v2 {
				return -1
			}
			if v1 > v2 {
				return 1
			}
			return 0
		}
	case float64:
		if v2, ok := val2.(float64); ok {
			if v1 < v2 {
				return -1
			}
			if v1 > v2 {
				return 1
			}
			return 0
		}
	case string:
		if v2, ok := val2.(string); ok {
			if v1 < v2 {
				return -1
			}
			if v1 > v2 {
				return 1
			}
			return 0
		}
	}

	// Fallback to string comparison
	str1 := fmt.Sprintf("%v", val1)
	str2 := fmt.Sprintf("%v", val2)
	if str1 < str2 {
		return -1
	}
	if str1 > str2 {
		return 1
	}
	return 0
}

// executeRank implements RANK() window function
func (qe *QueryEngine) executeRank(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	// Group rows by partition if PARTITION BY is specified
	partitions := qe.partitionRows(rows, winFunc.WindowSpec.PartitionBy)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		if len(winFunc.WindowSpec.OrderBy) > 0 {
			partition = qe.sortRows(partition, winFunc.WindowSpec.OrderBy, reader)
		}

		// Assign ranks within this partition
		currentRank := 1
		for i, row := range partition {
			if i > 0 && qe.compareRowsForRanking(partition[i-1], row, winFunc.WindowSpec.OrderBy) != 0 {
				// Different values, update rank to current position + 1
				currentRank = i + 1
			}
			// Same values keep the same rank
			row[winFunc.Alias] = currentRank
		}
	}

	return rows
}

// executeDenseRank implements DENSE_RANK() window function
func (qe *QueryEngine) executeDenseRank(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	// Group rows by partition if PARTITION BY is specified
	partitions := qe.partitionRows(rows, winFunc.WindowSpec.PartitionBy)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		if len(winFunc.WindowSpec.OrderBy) > 0 {
			partition = qe.sortRows(partition, winFunc.WindowSpec.OrderBy, reader)
		}

		// Assign dense ranks within this partition
		currentRank := 1
		for i, row := range partition {
			if i > 0 && qe.compareRowsForRanking(partition[i-1], row, winFunc.WindowSpec.OrderBy) != 0 {
				// Different values, increment rank by 1 (dense rank)
				currentRank++
			}
			// Same values keep the same rank
			row[winFunc.Alias] = currentRank
		}
	}

	return rows
}

// executeLag implements LAG() window function
// LAG(column, offset, default) accesses value from previous row
func (qe *QueryEngine) executeLag(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	// Parse arguments: column, offset (default 1), default value (default nil)
	column := winFunc.Column
	offset := 1
	var defaultValue interface{}

	if len(winFunc.Arguments) > 0 {
		if offsetVal, ok := winFunc.Arguments[0].(int); ok {
			offset = offsetVal
		}
	}
	if len(winFunc.Arguments) > 1 {
		defaultValue = winFunc.Arguments[1]
	}

	// Group rows by partition if PARTITION BY is specified
	partitions := qe.partitionRows(rows, winFunc.WindowSpec.PartitionBy)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		if len(winFunc.WindowSpec.OrderBy) > 0 {
			partition = qe.sortRows(partition, winFunc.WindowSpec.OrderBy, reader)
		}

		// Apply LAG within this partition
		for i, row := range partition {
			prevIndex := i - offset
			if prevIndex >= 0 && prevIndex < len(partition) {
				// Get value from previous row
				if val, exists := partition[prevIndex][column]; exists {
					row[winFunc.Alias] = val
				} else {
					row[winFunc.Alias] = defaultValue
				}
			} else {
				// No previous row, use default value
				row[winFunc.Alias] = defaultValue
			}
		}
	}

	return rows
}

// executeLead implements LEAD() window function
// LEAD(column, offset, default) accesses value from next row
func (qe *QueryEngine) executeLead(winFunc WindowFunction, rows []Row, reader *ParquetReader) []Row {
	// Parse arguments: column, offset (default 1), default value (default nil)
	column := winFunc.Column
	offset := 1
	var defaultValue interface{}

	if len(winFunc.Arguments) > 0 {
		if offsetVal, ok := winFunc.Arguments[0].(int); ok {
			offset = offsetVal
		}
	}
	if len(winFunc.Arguments) > 1 {
		defaultValue = winFunc.Arguments[1]
	}

	// Group rows by partition if PARTITION BY is specified
	partitions := qe.partitionRows(rows, winFunc.WindowSpec.PartitionBy)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		if len(winFunc.WindowSpec.OrderBy) > 0 {
			partition = qe.sortRows(partition, winFunc.WindowSpec.OrderBy, reader)
		}

		// Apply LEAD within this partition
		for i, row := range partition {
			nextIndex := i + offset
			if nextIndex >= 0 && nextIndex < len(partition) {
				// Get value from next row
				if val, exists := partition[nextIndex][column]; exists {
					row[winFunc.Alias] = val
				} else {
					row[winFunc.Alias] = defaultValue
				}
			} else {
				// No next row, use default value
				row[winFunc.Alias] = defaultValue
			}
		}
	}

	return rows
}

// executeExplain handles EXPLAIN queries
func (qe *QueryEngine) executeExplain(query *ParsedQuery) (*QueryResult, error) {
	if query.ExplainQuery == nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: "no query to explain",
		}, nil
	}

	// Create query plan
	plan, err := qe.planner.CreatePlan(query.ExplainQuery, qe)
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: fmt.Sprintf("failed to create query plan: %v", err),
		}, nil
	}

	// If ANALYZE is requested, execute the query and collect actual statistics
	if query.ExplainOptions.Analyze {
		// Execute the query and measure time
		startTime := time.Now()
		result, err := qe.executeSelect(query.ExplainQuery)
		elapsed := time.Since(startTime).Milliseconds()

		if err != nil {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("failed to analyze query: %v", err),
			}, nil
		}

		// Update plan with actual statistics
		if plan.Root != nil {
			plan.Root.ActualRows = int64(result.Count)
			plan.Root.ActualTime = float64(elapsed)
		}
	}

	// Format the output based on options
	var output string
	switch query.ExplainOptions.Format {
	case ExplainFormatJSON:
		output = qe.formatPlanAsJSON(plan, query.ExplainOptions)
	case ExplainFormatYAML:
		output = qe.formatPlanAsYAML(plan, query.ExplainOptions)
	default:
		output = qe.formatPlanAsText(plan, query.ExplainOptions)
	}

	// Return the plan as a single row result
	return &QueryResult{
		Columns: []string{"QUERY PLAN"},
		Rows: []Row{
			{"QUERY PLAN": output},
		},
		Count: 1,
		Query: query.RawSQL,
	}, nil
}

// formatPlanAsText formats the query plan as text
func (qe *QueryEngine) formatPlanAsText(plan *QueryPlan, options *ExplainOptions) string {
	if plan == nil || plan.Root == nil {
		return "No query plan available"
	}

	var result strings.Builder
	result.WriteString("Query Plan:\n")
	result.WriteString(plan.Root.StringWithOptions(0, options.Costs))

	if options.Verbose {
		result.WriteString("\nNote: Verbose output not fully implemented yet\n")
	}

	return result.String()
}

// formatPlanAsJSON formats the query plan as JSON
func (qe *QueryEngine) formatPlanAsJSON(plan *QueryPlan, options *ExplainOptions) string {
	if plan == nil || plan.Root == nil {
		return "{\"error\": \"No query plan available\"}"
	}

	planMap := qe.planNodeToMap(plan.Root, options)
	jsonBytes, err := json.MarshalIndent(planMap, "", "  ")
	if err != nil {
		return fmt.Sprintf("{\"error\": \"Failed to marshal plan: %v\"}", err)
	}

	return string(jsonBytes)
}

// formatPlanAsYAML formats the query plan as YAML (simplified for now)
func (qe *QueryEngine) formatPlanAsYAML(plan *QueryPlan, options *ExplainOptions) string {
	// For now, return a simple text representation
	// A full implementation would use a YAML library
	return "YAML format not implemented yet\n" + qe.formatPlanAsText(plan, options)
}

// planNodeToMap converts a plan node to a map for JSON serialization
func (qe *QueryEngine) planNodeToMap(node *PlanNode, options *ExplainOptions) map[string]interface{} {
	result := make(map[string]interface{})

	result["node_type"] = string(node.Type)

	if options.Costs {
		result["cost"] = node.Cost
		result["rows"] = node.Rows
		result["width"] = node.Width
	}

	if node.ActualRows > 0 || node.ActualTime > 0 {
		result["actual_rows"] = node.ActualRows
		result["actual_time_ms"] = node.ActualTime
	}

	// Add operation-specific fields
	switch node.Type {
	case PlanNodeScan:
		result["table"] = node.TableName
		if len(node.Columns) > 0 {
			result["columns"] = node.Columns
		}
	case PlanNodeFilter:
		result["conditions"] = len(node.Filter)
	case PlanNodeProject:
		result["columns"] = node.Columns
	case PlanNodeSort:
		result["sort_keys"] = node.OrderBy
	case PlanNodeLimit:
		result["limit"] = node.LimitCount
	case PlanNodeJoin:
		result["join_type"] = getJoinTypeName(node.JoinType)
	case PlanNodeAggregate:
		if len(node.GroupBy) > 0 {
			result["group_by"] = node.GroupBy
		}
	case PlanNodeCTE:
		result["cte_name"] = node.CTEName
	}

	// Add children
	if len(node.Children) > 0 {
		var children []map[string]interface{}
		for _, child := range node.Children {
			children = append(children, qe.planNodeToMap(child, options))
		}
		result["children"] = children
	}

	return result
}

// executeUnion handles UNION and UNION ALL queries
func (qe *QueryEngine) executeUnion(query *ParsedQuery) (*QueryResult, error) {
	return qe.executeUnionWithCTEs(query, nil)
}

func (qe *QueryEngine) executeUnionWithCTEs(query *ParsedQuery, parentCTEs map[string][]Row) (*QueryResult, error) {
	if len(query.UnionQueries) == 0 {
		return &QueryResult{
			Query: query.RawSQL,
			Error: "UNION query has no queries to union",
		}, nil
	}

	// Initialize CTE results with parent CTEs if provided
	cteResults := make(map[string][]Row)
	if parentCTEs != nil {
		for name, rows := range parentCTEs {
			cteResults[name] = rows
		}
	}

	// Process CTEs if present
	if len(query.CTEs) > 0 {
		for _, cte := range query.CTEs {
			// Execute each CTE and store the results
			cteResult, err := qe.executeSelectWithCTEs(cte.Query, cteResults)
			if err != nil {
				return nil, fmt.Errorf("failed to execute CTE '%s': %w", cte.Name, err)
			}

			// If the CTE has column aliases, apply them
			rows := cteResult.Rows
			if len(cte.ColumnNames) > 0 {
				rows = qe.applyCTEColumnAliases(rows, cteResult.Columns, cte.ColumnNames)
			}

			// Store the CTE results with aliased columns
			cteResults[cte.Name] = rows
		}
	}

	// Execute all queries in the UNION
	var allResults []*QueryResult
	var allRows []Row
	var commonColumns []string

	// Execute each query
	for i, unionQuery := range query.UnionQueries {
		var result *QueryResult
		var err error

		// Pass CTEs to each union query
		unionQuery.Query.CTEs = query.CTEs

		// Execute the query
		if unionQuery.Query.Type == SELECT {
			result, err = qe.executeSelectWithCTEs(unionQuery.Query, cteResults)
		} else {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("UNION query %d is not a SELECT", i+1),
			}, nil
		}

		if err != nil {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("failed to execute UNION query %d: %v", i+1, err),
			}, nil
		}

		if result.Error != "" {
			return &QueryResult{
				Query: query.RawSQL,
				Error: fmt.Sprintf("UNION query %d error: %s", i+1, result.Error),
			}, nil
		}

		// Check column compatibility
		if i == 0 {
			commonColumns = result.Columns
			if len(commonColumns) == 0 {
				// If no columns returned, there might be an issue
				return &QueryResult{
					Query: query.RawSQL,
					Error: fmt.Sprintf("UNION query 1 returned no columns"),
				}, nil
			}
		} else {
			if len(result.Columns) != len(commonColumns) {
				return &QueryResult{
					Query: query.RawSQL,
					Error: fmt.Sprintf("UNION queries must have the same number of columns: query 1 has %d, query %d has %d",
						len(commonColumns), i+1, len(result.Columns)),
				}, nil
			}
		}

		allResults = append(allResults, result)
		allRows = append(allRows, result.Rows...)
	}

	// Handle UNION vs UNION ALL
	var finalRows []Row
	// Check if any UNION (not ALL) exists - if so, we need to deduplicate
	needsDedupe := false
	for _, uq := range query.UnionQueries {
		if !uq.UnionAll {
			needsDedupe = true
			break
		}
	}

	if needsDedupe {
		// UNION - remove duplicates
		finalRows = qe.removeDuplicateRows(allRows, commonColumns)
	} else {
		// UNION ALL - keep all rows including duplicates
		finalRows = allRows
	}

	// Apply final ORDER BY if present
	if len(query.OrderBy) > 0 {
		finalRows = qe.sortUnionRows(finalRows, query.OrderBy, commonColumns)
	}

	// Apply final LIMIT if present
	if query.Limit > 0 && len(finalRows) > query.Limit {
		finalRows = finalRows[:query.Limit]
	}

	return &QueryResult{
		Columns: commonColumns,
		Rows:    finalRows,
		Count:   len(finalRows),
		Query:   query.RawSQL,
	}, nil
}

// hasUnionAll checks if all unions should preserve duplicates
func (qe *QueryEngine) hasUnionAll(query *ParsedQuery) bool {
	// For now, if any query uses UNION (not ALL), we need to deduplicate
	for _, unionQuery := range query.UnionQueries {
		if !unionQuery.UnionAll {
			return false
		}
	}
	return true
}

// removeDuplicateRows removes duplicate rows for UNION (not UNION ALL)
func (qe *QueryEngine) removeDuplicateRows(rows []Row, columns []string) []Row {
	seen := make(map[string]bool)
	var uniqueRows []Row

	for _, row := range rows {
		// Create a key from all column values
		key := qe.createRowKey(row, columns)
		if !seen[key] {
			seen[key] = true
			uniqueRows = append(uniqueRows, row)
		}
	}

	return uniqueRows
}

// createRowKey creates a unique key for a row based on column values
func (qe *QueryEngine) createRowKey(row Row, columns []string) string {
	var parts []string
	for _, col := range columns {
		val := row[col]
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}

// sortUnionRows sorts rows from a UNION query
func (qe *QueryEngine) sortUnionRows(rows []Row, orderBy []OrderByColumn, columns []string) []Row {
	sort.Slice(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			// Find the column in the row
			colName := ob.Column

			// Get values from both rows
			val1 := rows[i][colName]
			val2 := rows[j][colName]

			cmp := qe.compareValues(val1, val2)

			if cmp != 0 {
				if ob.Direction == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return rows
}
