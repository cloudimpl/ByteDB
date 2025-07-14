package core

import (
	"container/heap"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"bytedb/common"
	"github.com/parquet-go/parquet-go"
	"howett.net/ranger"
)

type ParquetReader struct {
	filePath   string
	schema     *parquet.Schema
	reader     *parquet.File
	httpReader io.ReaderAt
	closer     io.Closer
	
	// Multi-file support
	multiFileReader *MultiFileParquetReader
}

type Row map[string]interface{}

// SubqueryExecutor interface for executing subqueries
type SubqueryExecutor interface {
	ExecuteSubquery(query *ParsedQuery) (*QueryResult, error)
	ExecuteCorrelatedSubquery(query *ParsedQuery, outerRow Row) (*QueryResult, error)
	EvaluateFunction(fn *FunctionCall, row Row) (interface{}, error)
}

// Define the structs for our sample data
type Employee struct {
	ID         int32   `parquet:"id"`
	Name       string  `parquet:"name"`
	Department string  `parquet:"department"`
	Salary     float64 `parquet:"salary"`
	Age        int32   `parquet:"age"`
	HireDate   string  `parquet:"hire_date"`
}

type Product struct {
	ID          int32   `parquet:"id"`
	Name        string  `parquet:"name"`
	Category    string  `parquet:"category"`
	Price       float64 `parquet:"price"`
	InStock     bool    `parquet:"in_stock"`
	Description string  `parquet:"description"`
}

type Department struct {
	Name          string  `parquet:"name"`
	Manager       string  `parquet:"manager"`
	Budget        float64 `parquet:"budget"`
	Location      string  `parquet:"location"`
	EmployeeCount int32   `parquet:"employee_count"`
}

func NewParquetReader(filePath string) (*ParquetReader, error) {
	// Check if filePath is a URL
	if IsHTTPURL(filePath) {
		return newHTTPParquetReader(filePath)
	}
	return newLocalParquetReader(filePath)
}

func IsHTTPURL(path string) bool {
	u, err := url.Parse(path)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func newLocalParquetReader(filePath string) (*ParquetReader, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	tracer.Debug(TraceComponentExecution, "Opening local Parquet file", TraceContext("file", filePath))
	
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	
	// Get file size
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	
	tracer.Debug(TraceComponentExecution, "File opened", TraceContext(
		"file", filePath,
		"size_bytes", fileSize,
		"size_mb", float64(fileSize)/(1024*1024),
		"elapsed_ms", time.Since(startTime).Milliseconds(),
	))

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	parquetOpenStart := time.Now()
	reader, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	
	// Get row groups and total row count
	rowGroupCount := len(reader.RowGroups())
	totalRows := int64(0)
	for _, rg := range reader.RowGroups() {
		totalRows += rg.NumRows()
	}
	
	tracer.Info(TraceComponentExecution, "Parquet reader initialized", TraceContext(
		"file", filePath,
		"parquet_open_ms", time.Since(parquetOpenStart).Milliseconds(),
		"total_elapsed_ms", time.Since(startTime).Milliseconds(),
		"schema_fields", len(reader.Schema().Fields()),
		"row_groups", rowGroupCount,
		"total_rows", totalRows,
	))

	return &ParquetReader{
		filePath: filePath,
		schema:   reader.Schema(),
		reader:   reader,
		closer:   file,
	}, nil
}

func newHTTPParquetReader(urlStr string) (*ParquetReader, error) {
	// Parse the URL
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Create HTTP ranger for range requests
	httpRanger := &ranger.HTTPRanger{URL: parsedURL}
	reader, err := ranger.NewReader(httpRanger)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP reader: %w", err)
	}

	// Get the content length
	length, err := reader.Length()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP content length: %w", err)
	}

	// Open parquet file using the HTTP reader
	parquetReader, err := parquet.OpenFile(reader, length)
	if err != nil {
		return nil, fmt.Errorf("failed to open remote parquet file: %w", err)
	}

	return &ParquetReader{
		filePath:   urlStr,
		schema:     parquetReader.Schema(),
		reader:     parquetReader,
		httpReader: reader,
	}, nil
}

func (pr *ParquetReader) Close() error {
	if pr.multiFileReader != nil {
		return pr.multiFileReader.Close()
	}
	if pr.closer != nil {
		return pr.closer.Close()
	}
	return nil
}

func (pr *ParquetReader) GetColumnNames() []string {
	if pr.multiFileReader != nil {
		return pr.multiFileReader.GetColumnNames()
	}
	var names []string
	for _, field := range pr.schema.Fields() {
		names = append(names, field.Name())
	}
	return names
}

func (pr *ParquetReader) GetRowCount() int {
	if pr.multiFileReader != nil {
		// Sum up row counts from all files
		total := 0
		for _, reader := range pr.multiFileReader.readers {
			total += reader.GetRowCount()
		}
		return total
	}
	return int(pr.reader.NumRows())
}

// GetSchema returns the schema as a slice of field information
func (pr *ParquetReader) GetSchema() []common.Field {
	if pr.multiFileReader != nil {
		return pr.multiFileReader.GetSchema()
	}
	
	var fields []common.Field
	for _, field := range pr.schema.Fields() {
		fields = append(fields, common.Field{
			Name:     field.Name(),
			Type:     field.GoType().String(),
			Required: field.Required(),
		})
	}
	return fields
}

// GetParquetSchema returns the underlying parquet schema
func (pr *ParquetReader) GetParquetSchema() *parquet.Schema {
	return pr.schema
}

// GetSchemaInfo returns detailed information about the schema
func (pr *ParquetReader) GetSchemaInfo() map[string]interface{} {
	fields := pr.schema.Fields()
	schemaInfo := map[string]interface{}{
		"field_count": len(fields),
		"fields":      make([]map[string]interface{}, len(fields)),
	}

	fieldInfos := schemaInfo["fields"].([]map[string]interface{})
	for i, field := range fields {
		fieldInfos[i] = map[string]interface{}{
			"name":     field.Name(),
			"type":     field.Type().String(),
			"optional": field.Optional(),
		}
	}

	return schemaInfo
}

func (pr *ParquetReader) ReadAll() ([]Row, error) {
	if pr.multiFileReader != nil {
		return pr.multiFileReader.ReadAll()
	}
	return pr.readRows(0, nil)
}

func (pr *ParquetReader) ReadWithLimit(limit int) ([]Row, error) {
	return pr.readRows(limit, nil)
}

// ReadAllWithColumns reads all rows but only the specified columns for performance
func (pr *ParquetReader) ReadAllWithColumns(requiredColumns []string) ([]Row, error) {
	if pr.multiFileReader != nil {
		return pr.multiFileReader.ReadAllWithColumns(requiredColumns)
	}
	return pr.readRows(0, requiredColumns)
}

// ReadWithLimitAndColumns reads rows with limit and only specified columns
func (pr *ParquetReader) ReadWithLimitAndColumns(limit int, requiredColumns []string) ([]Row, error) {
	return pr.readRows(limit, requiredColumns)
}

func (pr *ParquetReader) readRows(limit int, requiredColumns []string) ([]Row, error) {
	// Handle multi-file reader
	if pr.multiFileReader != nil {
		if len(requiredColumns) > 0 {
			rows, err := pr.multiFileReader.ReadAllWithColumns(requiredColumns)
			if err != nil {
				return nil, err
			}
			// Apply limit if specified
			if limit > 0 && len(rows) > limit {
				return rows[:limit], nil
			}
			return rows, nil
		} else {
			rows, err := pr.multiFileReader.ReadAll()
			if err != nil {
				return nil, err
			}
			// Apply limit if specified
			if limit > 0 && len(rows) > limit {
				return rows[:limit], nil
			}
			return rows, nil
		}
	}

	// Single file reader
	// If no specific columns requested or empty list, read all columns
	if len(requiredColumns) == 0 {
		return pr.readAllColumns(limit)
	}

	// Read only the required columns for performance
	return pr.readSpecificColumns(limit, requiredColumns)
}

func (pr *ParquetReader) readAllColumns(limit int) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	rows := make([]Row, 0)

	// Use generic reading with map interface
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	tracer.Info(TraceComponentExecution, "Starting Parquet file read", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"schema_fields", len(pr.schema.Fields()),
	))

	count := 0
	rowReadStart := time.Now()
	for {
		if limit > 0 && count >= limit {
			break
		}

		// Read into a generic map
		rowData := make(map[string]interface{})
		err := reader.Read(&rowData)
		if err != nil {
			break // End of file or error
		}

		// Convert map to our Row type
		row := Row(rowData)
		rows = append(rows, row)
		count++
		
		// Log progress every 1000 rows
		if count%1000 == 0 {
			tracer.Debug(TraceComponentExecution, "Read progress", TraceContext(
				"rows_read", count,
				"elapsed_ms", time.Since(rowReadStart).Milliseconds(),
			))
		}
	}
	
	elapsed := time.Since(startTime)
	tracer.Info(TraceComponentExecution, "Completed Parquet file read", TraceContext(
		"rows_read", count,
		"total_elapsed_ms", elapsed.Milliseconds(),
		"rows_per_second", float64(count)/elapsed.Seconds(),
	))

	return rows, nil
}

// Generic reading approach - parquet-go handles the conversion automatically

// readSpecificColumns reads only the specified columns from the Parquet file
// Current implementation: Reads all data and projects columns at Row level
// TODO: Implement true column projection at Parquet level when parquet-go supports it better
func (pr *ParquetReader) readSpecificColumns(limit int, requiredColumns []string) ([]Row, error) {
	tracer := GetTracer()
	tracer.Debug(TraceComponentOptimizer, "Reading specific columns", TraceContext(
		"columns", requiredColumns,
		"limit", limit,
	))
	
	rows := make([]Row, 0)

	// Get available columns from schema
	availableColumns := pr.GetColumnNames()

	// Validate that all required columns exist
	validColumns := make([]string, 0)
	for _, reqCol := range requiredColumns {
		for _, availCol := range availableColumns {
			if reqCol == availCol {
				validColumns = append(validColumns, reqCol)
				break
			}
		}
	}

	if len(validColumns) == 0 {
		return rows, nil // No valid columns to read
	}

	// IMPLEMENTATION NOTE: parquet-go library limitations
	// The current parquet-go library doesn't provide an easy way to read only specific columns
	// at the Parquet level. The library's API is designed to read entire rows.
	// 
	// Future optimization opportunities:
	// 1. Use row groups to read data in chunks
	// 2. Implement column chunk reading similar to knowledge_base/stats.go
	// 3. Consider using Apache Arrow for better columnar access
	//
	// For now, we implement column pruning at the Row level which still provides:
	// - Memory savings by not storing unnecessary columns in Row objects  
	// - Reduced data transfer between functions
	// - Preparation for future Parquet-level optimizations
	
	allRows, err := pr.readAllColumns(limit)
	if err != nil {
		return nil, err
	}

	// Project only the required columns to save memory
	for _, row := range allRows {
		projectedRow := make(Row)
		for _, col := range validColumns {
			if val, exists := row[col]; exists {
				projectedRow[col] = val
			}
		}
		rows = append(rows, projectedRow)
	}

	tracer.Debug(TraceComponentOptimizer, "Column projection complete", TraceContext(
		"input_rows", len(allRows),
		"output_columns", len(validColumns),
	))

	return rows, nil
}

// getRowGroupStatistics reads min/max statistics for a specific column across all row groups
// This is used for optimizing ORDER BY queries by skipping row groups that can't contain top values
func (pr *ParquetReader) getRowGroupStatistics(columnName string) ([]RowGroupStats, error) {
	tracer := GetTracer()
	tracer.Debug(TraceComponentOptimizer, "Reading row group statistics", TraceContext("column", columnName))
	
	var stats []RowGroupStats
	
	// Find column index
	columnIndex := -1
	for i, col := range pr.GetColumnNames() {
		if col == columnName {
			columnIndex = i
			break
		}
	}
	
	if columnIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Access row groups and extract statistics
	for rgIndex, rowGroup := range pr.reader.RowGroups() {
		if columnIndex >= len(rowGroup.ColumnChunks()) {
			continue
		}
		
		columnChunk := rowGroup.ColumnChunks()[columnIndex]
		fileColumnChunk, ok := columnChunk.(*parquet.FileColumnChunk)
		if !ok {
			continue
		}
		
		// Try to get enhanced statistics from ColumnIndex first
		var min, max parquet.Value
		var hasMinMax bool
		var nullCount, distinctCount int64
		
		// Try to access ColumnIndex for enhanced statistics
		if columnIdx, err := fileColumnChunk.ColumnIndex(); err == nil && columnIdx != nil {
			tracer.Debug(TraceComponentOptimizer, "Using ColumnIndex for enhanced statistics", 
				TraceContext("row_group", rgIndex, "column", columnName))
			
			// ColumnIndex provides page-level statistics
			// Note: The exact API may vary - this is a simplified version
			// In practice, you would iterate through pages and get their min/max values
			
			// For now, let's just note that ColumnIndex is available and could be used
			// for more granular page-level filtering in the future
			tracer.Debug(TraceComponentOptimizer, "ColumnIndex available for advanced filtering", 
				TraceContext("pages_available", "true"))
		}
		
		// Fallback to basic column chunk bounds if ColumnIndex not available
		if !hasMinMax {
			min, max, hasMinMax = fileColumnChunk.Bounds()
		}
		
		if hasMinMax {
			stats = append(stats, RowGroupStats{
				RowGroupIndex: rgIndex,
				MinValue:      min,
				MaxValue:      max,
				NumRows:       rowGroup.NumRows(),
				NullCount:     nullCount,
				DistinctCount: distinctCount,
			})
		}
	}
	
	tracer.Debug(TraceComponentOptimizer, "Row group statistics collected", TraceContext(
		"column", columnName,
		"row_groups_with_stats", len(stats),
		"enhanced_stats", "using ColumnIndex where available",
	))
	
	return stats, nil
}

// RowGroupStats holds statistics for a row group
type RowGroupStats struct {
	RowGroupIndex int
	MinValue      parquet.Value
	MaxValue      parquet.Value
	NumRows       int64
	NullCount     int64  // Number of null values (enhanced statistic)
	DistinctCount int64  // Approximate distinct count (for future use)
}

// Schema detection methods removed - now supports any schema generically

func (pr *ParquetReader) FilterRows(rows []Row, conditions []WhereCondition) []Row {
	return pr.FilterRowsWithEngine(rows, conditions, nil)
}

func (pr *ParquetReader) FilterRowsWithEngine(rows []Row, conditions []WhereCondition, engine SubqueryExecutor) []Row {
	if len(conditions) == 0 {
		return rows
	}

	filtered := make([]Row, 0)
	for _, row := range rows {
		if pr.matchesConditionsWithEngine(row, conditions, engine) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (pr *ParquetReader) matchesConditions(row Row, conditions []WhereCondition) bool {
	return pr.matchesConditionsWithEngine(row, conditions, nil)
}

func (pr *ParquetReader) matchesConditionsWithEngine(row Row, conditions []WhereCondition, engine SubqueryExecutor) bool {
	for _, condition := range conditions {
		if !pr.matchesConditionWithEngine(row, condition, engine) {
			return false
		}
	}
	return true
}

func (pr *ParquetReader) matchesCondition(row Row, condition WhereCondition) bool {
	return pr.matchesConditionWithEngine(row, condition, nil)
}

func (pr *ParquetReader) matchesConditionWithEngine(row Row, condition WhereCondition, engine SubqueryExecutor) bool {
	// Handle complex logical conditions (AND/OR)
	if condition.IsComplex {
		switch condition.LogicalOp {
		case "AND":
			return pr.matchesConditionWithEngine(row, *condition.Left, engine) &&
				pr.matchesConditionWithEngine(row, *condition.Right, engine)
		case "OR":
			return pr.matchesConditionWithEngine(row, *condition.Left, engine) ||
				pr.matchesConditionWithEngine(row, *condition.Right, engine)
		default:
			return false
		}
	}

	// Handle subquery-based conditions
	if condition.Subquery != nil {
		return pr.matchesSubqueryCondition(row, condition, engine)
	}

	// Handle IS NULL and IS NOT NULL
	if condition.Operator == "IS NULL" {
		value, exists := row[condition.Column]
		return !exists || value == nil
	}
	if condition.Operator == "IS NOT NULL" {
		value, exists := row[condition.Column]
		return exists && value != nil
	}

	// Get left-side value (column, function, or CASE expression)
	var leftValue interface{}
	var exists bool

	if condition.CaseExpr != nil {
		// Evaluate CASE expression on left side
		leftValue = pr.evaluateCaseExpression(condition.CaseExpr, row, engine)
		exists = true
	} else if condition.Function != nil && engine != nil {
		// Evaluate function on left side
		var err error
		leftValue, err = engine.EvaluateFunction(condition.Function, row)
		if err != nil {
			return false
		}
		exists = true
	} else if condition.Column != "" {
		// Regular column lookup
		leftValue, exists = row[condition.Column]
		if !exists {
			return false
		}
	} else {
		// No column or function specified
		return false
	}

	// Get right-side value (handling functions, CASE expressions, column references, or literal values)
	var rightValue interface{}
	if condition.ValueCaseExpr != nil {
		// Evaluate CASE expression on right side
		rightValue = pr.evaluateCaseExpression(condition.ValueCaseExpr, row, engine)
	} else if condition.ValueFunction != nil && engine != nil {
		// Evaluate function on right side
		var err error
		rightValue, err = engine.EvaluateFunction(condition.ValueFunction, row)
		if err != nil {
			rightValue = condition.Value // Fall back to literal value
		}
	} else if condition.ValueColumn != "" {
		// Column reference on right side
		if condition.ValueTableName != "" {
			qualifiedKey := condition.ValueTableName + "." + condition.ValueColumn
			if val, exists := row[qualifiedKey]; exists {
				rightValue = val
			} else {
				rightValue = row[condition.ValueColumn]
			}
		} else {
			rightValue = row[condition.ValueColumn]
		}
	} else {
		// Regular literal value
		rightValue = condition.Value
	}

	switch condition.Operator {
	case "BETWEEN":
		return pr.CompareValues(leftValue, condition.ValueFrom) >= 0 &&
			pr.CompareValues(leftValue, condition.ValueTo) <= 0
	case "NOT BETWEEN":
		return pr.CompareValues(leftValue, condition.ValueFrom) < 0 ||
			pr.CompareValues(leftValue, condition.ValueTo) > 0
	case "=":
		return pr.CompareValues(leftValue, rightValue) == 0
	case "!=", "<>":
		return pr.CompareValues(leftValue, rightValue) != 0
	case "<":
		return pr.CompareValues(leftValue, rightValue) < 0
	case "<=":
		return pr.CompareValues(leftValue, rightValue) <= 0
	case ">":
		return pr.CompareValues(leftValue, rightValue) > 0
	case ">=":
		return pr.CompareValues(leftValue, rightValue) >= 0
	case "LIKE":
		return pr.matchesLike(leftValue, rightValue)
	case "IN":
		return pr.matchesIn(leftValue, condition.ValueList)
	default:
		return false
	}
}

func (pr *ParquetReader) matchesSubqueryCondition(row Row, condition WhereCondition, engine SubqueryExecutor) bool {
	if engine == nil {
		return false // Cannot execute subquery without engine
	}

	switch condition.Operator {
	case "IN":
		return pr.matchesInSubquery(row, condition, engine)
	case "NOT IN":
		return !pr.matchesInSubquery(row, condition, engine)
	case "EXISTS":
		if condition.Subquery.IsCorrelated {
			return pr.matchesExistsSubqueryWithOuter(condition, engine, row)
		} else {
			return pr.matchesExistsSubquery(condition, engine)
		}
	case "NOT EXISTS":
		if condition.Subquery.IsCorrelated {
			return !pr.matchesExistsSubqueryWithOuter(condition, engine, row)
		} else {
			return !pr.matchesExistsSubquery(condition, engine)
		}
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return pr.matchesScalarSubquery(row, condition, engine)
	default:
		return false
	}
}

func (pr *ParquetReader) matchesInSubquery(row Row, condition WhereCondition, engine SubqueryExecutor) bool {
	value, exists := row[condition.Column]
	if !exists {
		return false
	}

	// Execute the subquery
	result, err := engine.ExecuteSubquery(condition.Subquery)
	if err != nil || result.Error != "" {
		return false
	}

	// Check if the value exists in the subquery results
	for _, subRow := range result.Rows {
		if len(result.Columns) > 0 {
			subValue, subExists := subRow[result.Columns[0]]
			if subExists && pr.CompareValues(value, subValue) == 0 {
				return true
			}
		}
	}

	return false
}

func (pr *ParquetReader) matchesExistsSubquery(condition WhereCondition, engine SubqueryExecutor) bool {
	// Execute the subquery
	result, err := engine.ExecuteSubquery(condition.Subquery)
	if err != nil || result.Error != "" {
		return false
	}

	// EXISTS returns true if subquery returns any rows
	return len(result.Rows) > 0
}

func (pr *ParquetReader) matchesScalarSubquery(row Row, condition WhereCondition, engine SubqueryExecutor) bool {
	value, exists := row[condition.Column]
	if !exists {
		return false
	}

	// Execute the subquery
	result, err := engine.ExecuteSubquery(condition.Subquery)
	if err != nil || result.Error != "" {
		return false
	}

	// Scalar subquery should return exactly one row and one column
	if len(result.Rows) != 1 || len(result.Columns) != 1 {
		return false
	}

	subValue, subExists := result.Rows[0][result.Columns[0]]
	if !subExists {
		return false
	}

	// Compare using the specified operator
	comparison := pr.CompareValues(value, subValue)
	switch condition.Operator {
	case "=":
		return comparison == 0
	case "!=", "<>":
		return comparison != 0
	case "<":
		return comparison < 0
	case "<=":
		return comparison <= 0
	case ">":
		return comparison > 0
	case ">=":
		return comparison >= 0
	default:
		return false
	}
}

func (pr *ParquetReader) CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Handle numeric type conversions (including string numbers)
	if pr.isNumericOrStringNumber(a) && pr.isNumericOrStringNumber(b) {
		aFloat := pr.toFloat64(a)
		bFloat := pr.toFloat64(b)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// Handle same types
	if aVal.Type() == bVal.Type() {
		return pr.compareSameTypes(aVal, bVal)
	}

	// Fall back to string comparison for different non-numeric types
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (pr *ParquetReader) isNumeric(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func (pr *ParquetReader) isNumericOrStringNumber(val interface{}) bool {
	v := reflect.ValueOf(val)
	if pr.isNumeric(v.Kind()) {
		return true
	}
	if v.Kind() == reflect.String {
		_, err := strconv.ParseFloat(v.String(), 64)
		return err == nil
	}
	return false
}

func (pr *ParquetReader) toFloat64(val interface{}) float64 {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.String:
		// Try to parse string as float
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return f
		}
	}
	return 0
}

func (pr *ParquetReader) compareSameTypes(aVal, bVal reflect.Value) int {

	switch aVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := aVal.Int()
		bInt := bVal.Int()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		aFloat := aVal.Float()
		bFloat := bVal.Float()
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case reflect.String:
		aStr := aVal.String()
		bStr := bVal.String()
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	default:
		aStr := fmt.Sprintf("%v", aVal.Interface())
		bStr := fmt.Sprintf("%v", bVal.Interface())
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}
}

func (pr *ParquetReader) matchesLike(value, pattern interface{}) bool {
	if value == nil || pattern == nil {
		return false
	}

	valueStr := fmt.Sprintf("%v", value)
	patternStr := fmt.Sprintf("%v", pattern)

	// Convert SQL LIKE pattern to regex pattern
	regexPattern := pr.convertLikeToRegex(patternStr)

	// Compile and match the regex
	matched, err := regexp.MatchString(regexPattern, valueStr)
	if err != nil {
		// If regex compilation fails, fall back to simple string comparison
		return valueStr == patternStr
	}

	return matched
}

// Case-insensitive LIKE matching (for future ILIKE support)
func (pr *ParquetReader) matchesILike(value, pattern interface{}) bool {
	if value == nil || pattern == nil {
		return false
	}

	valueStr := strings.ToLower(fmt.Sprintf("%v", value))
	patternStr := strings.ToLower(fmt.Sprintf("%v", pattern))

	// Convert SQL LIKE pattern to regex pattern
	regexPattern := pr.convertLikeToRegex(patternStr)

	// Compile and match the regex
	matched, err := regexp.MatchString(regexPattern, valueStr)
	if err != nil {
		// If regex compilation fails, fall back to simple string comparison
		return valueStr == patternStr
	}

	return matched
}

func (pr *ParquetReader) convertLikeToRegex(pattern string) string {
	// Handle escaped wildcards first (\\% and \\_)
	pattern = strings.ReplaceAll(pattern, "\\%", "\x00ESCAPED_PERCENT\x00")
	pattern = strings.ReplaceAll(pattern, "\\_", "\x00ESCAPED_UNDERSCORE\x00")

	// Replace SQL wildcards with placeholders to avoid conflicts
	pattern = strings.ReplaceAll(pattern, "%", "\x00PERCENT\x00")
	pattern = strings.ReplaceAll(pattern, "_", "\x00UNDERSCORE\x00")

	// Escape special regex characters
	pattern = regexp.QuoteMeta(pattern)

	// Now replace the placeholders with appropriate equivalents
	pattern = strings.ReplaceAll(pattern, "\x00PERCENT\x00", ".*")           // % matches any sequence of characters
	pattern = strings.ReplaceAll(pattern, "\x00UNDERSCORE\x00", ".")         // _ matches any single character
	pattern = strings.ReplaceAll(pattern, "\x00ESCAPED_PERCENT\x00", "%")    // \% becomes literal %
	pattern = strings.ReplaceAll(pattern, "\x00ESCAPED_UNDERSCORE\x00", "_") // \_ becomes literal _

	// Anchor the pattern to match the entire string
	return "^" + pattern + "$"
}

func (pr *ParquetReader) matchesIn(value interface{}, valueList []interface{}) bool {
	if len(valueList) == 0 {
		return false // Empty IN list matches nothing
	}

	for _, listValue := range valueList {
		if pr.CompareValues(value, listValue) == 0 {
			return true
		}
	}
	return false
}

func (pr *ParquetReader) SelectColumns(rows []Row, columns []Column) []Row {
	if len(columns) == 0 || (len(columns) == 1 && columns[0].Name == "*") {
		return rows
	}

	result := make([]Row, len(rows))
	for i, row := range rows {
		newRow := make(Row)
		for _, col := range columns {
			if col.Name == "*" {
				for k, v := range row {
					newRow[k] = v
				}
			} else {
				if value, exists := row[col.Name]; exists {
					key := col.Name
					if col.Alias != "" {
						key = col.Alias
					}
					newRow[key] = value
				} else if pr.isConstantColumn(col.Name) {
					// Handle constant columns like "1", "hello", etc.
					key := col.Name
					if col.Alias != "" {
						key = col.Alias
					}
					newRow[key] = pr.parseConstantValue(col.Name)
				}
			}
		}
		result[i] = newRow
	}
	return result
}

// SelectColumnsWithEngine handles column selection including subquery columns
func (pr *ParquetReader) SelectColumnsWithEngine(rows []Row, columns []Column, engine SubqueryExecutor) []Row {
	if len(columns) == 0 || (len(columns) == 1 && columns[0].Name == "*") {
		return rows
	}

	result := make([]Row, len(rows))
	for i, row := range rows {
		newRow := make(Row)
		for _, col := range columns {
			if col.Name == "*" {
				for k, v := range row {
					newRow[k] = v
				}
			} else if col.Subquery != nil {
				// Handle subquery columns
				key := col.Name
				if col.Alias != "" {
					key = col.Alias
				}

				// Execute subquery to get the value
				subqueryValue := pr.executeColumnSubquery(col.Subquery, row, engine)
				newRow[key] = subqueryValue
			} else if col.CaseExpr != nil {
				// Handle CASE expression columns
				key := col.Name
				if col.Alias != "" {
					key = col.Alias
				}

				// Evaluate CASE expression to get the value
				caseValue := pr.evaluateCaseExpression(col.CaseExpr, row, engine)
				newRow[key] = caseValue
				
			} else if col.Function != nil {
				// Handle function columns
				key := col.Name
				if col.Alias != "" {
					key = col.Alias
				}

				// Evaluate function to get the value
				funcValue, err := engine.EvaluateFunction(col.Function, row)
				if err != nil {
					funcValue = nil
				}
				newRow[key] = funcValue
			} else {
				if value, exists := row[col.Name]; exists {
					key := col.Name
					if col.Alias != "" {
						key = col.Alias
					}
					newRow[key] = value
				} else if pr.isConstantColumn(col.Name) {
					// Handle constant columns like "1", "hello", etc.
					key := col.Name
					if col.Alias != "" {
						key = col.Alias
					}
					newRow[key] = pr.parseConstantValue(col.Name)
				}
			}
		}
		result[i] = newRow
	}
	return result
}

func (pr *ParquetReader) isConstantColumn(name string) bool {
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

func (pr *ParquetReader) parseConstantValue(name string) interface{} {
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

// Correlated subquery support methods

func (pr *ParquetReader) matchesInSubqueryWithOuter(row Row, condition WhereCondition, engine SubqueryExecutor, outerRow Row) bool {
	value, exists := row[condition.Column]
	if !exists {
		return false
	}

	// Execute the subquery with correlation context
	var result *QueryResult
	var err error

	if condition.Subquery.IsCorrelated {
		result, err = engine.ExecuteCorrelatedSubquery(condition.Subquery, outerRow)
	} else {
		result, err = engine.ExecuteSubquery(condition.Subquery)
	}

	if err != nil || result.Error != "" {
		return false
	}

	// Check if the value exists in the subquery results
	for _, subRow := range result.Rows {
		if len(result.Columns) > 0 {
			subValue, subExists := subRow[result.Columns[0]]
			if subExists && pr.CompareValues(value, subValue) == 0 {
				return true
			}
		}
	}

	return false
}

func (pr *ParquetReader) matchesExistsSubqueryWithOuter(condition WhereCondition, engine SubqueryExecutor, outerRow Row) bool {
	// Execute the subquery with correlation context
	var result *QueryResult
	var err error

	if condition.Subquery.IsCorrelated {
		result, err = engine.ExecuteCorrelatedSubquery(condition.Subquery, outerRow)
	} else {
		result, err = engine.ExecuteSubquery(condition.Subquery)
	}

	if err != nil || result.Error != "" {
		return false
	}

	// EXISTS returns true if subquery returns any rows
	return len(result.Rows) > 0
}

func (pr *ParquetReader) matchesScalarSubqueryWithOuter(row Row, condition WhereCondition, engine SubqueryExecutor, outerRow Row) bool {
	value, exists := row[condition.Column]
	if !exists {
		return false
	}

	// Execute the subquery with correlation context
	var result *QueryResult
	var err error

	if condition.Subquery.IsCorrelated {
		result, err = engine.ExecuteCorrelatedSubquery(condition.Subquery, outerRow)
	} else {
		result, err = engine.ExecuteSubquery(condition.Subquery)
	}

	if err != nil || result.Error != "" {
		return false
	}

	// Scalar subquery should return exactly one row and one column
	if len(result.Rows) != 1 || len(result.Columns) != 1 {
		return false
	}

	subValue, subExists := result.Rows[0][result.Columns[0]]
	if !subExists {
		return false
	}

	// Compare values based on the operator
	switch condition.Operator {
	case "=":
		return pr.CompareValues(value, subValue) == 0
	case "!=", "<>":
		return pr.CompareValues(value, subValue) != 0
	case "<":
		return pr.CompareValues(value, subValue) < 0
	case "<=":
		return pr.CompareValues(value, subValue) <= 0
	case ">":
		return pr.CompareValues(value, subValue) > 0
	case ">=":
		return pr.CompareValues(value, subValue) >= 0
	default:
		return false
	}
}

// executeColumnSubquery executes a subquery in SELECT clause and returns the scalar result
func (pr *ParquetReader) executeColumnSubquery(subquery *ParsedQuery, currentRow Row, engine SubqueryExecutor) interface{} {
	if engine == nil {
		return nil
	}

	var result *QueryResult
	var err error

	// Execute subquery with correlation support
	if subquery.IsCorrelated {
		result, err = engine.ExecuteCorrelatedSubquery(subquery, currentRow)
	} else {
		result, err = engine.ExecuteSubquery(subquery)
	}

	if err != nil || result.Error != "" {
		return nil
	}

	// SELECT clause subqueries should return exactly one row and one column (scalar)
	if len(result.Rows) == 0 {
		return nil
	}

	if len(result.Rows) > 1 {
		// Multiple rows returned - take the first one for now
		// In a production system, this should be an error
	}

	if len(result.Columns) == 0 {
		return nil
	}

	// Get the first column value from the first row
	firstRow := result.Rows[0]
	firstColumn := result.Columns[0]

	if value, exists := firstRow[firstColumn]; exists {
		return value
	}

	return nil
}

// hasColumnSubqueries checks if any columns contain subqueries or CASE expressions
func (pr *ParquetReader) hasColumnSubqueries(columns []Column) bool {
	for _, col := range columns {
		if col.Subquery != nil || col.CaseExpr != nil || col.Function != nil {
			return true
		}
	}
	return false
}

// evaluateCaseExpression evaluates a CASE expression for a given row
func (pr *ParquetReader) evaluateCaseExpression(caseExpr *CaseExpression, row Row, engine SubqueryExecutor) interface{} {
	// Get tracer and log CASE expression evaluation
	tracer := GetTracer()
	tracer.Debug(TraceComponentCase, "Evaluating CASE expression", TraceContext("alias", caseExpr.Alias, "when_clauses", len(caseExpr.WhenClauses)))
	
	// Evaluate each WHEN clause in order
	for i, whenClause := range caseExpr.WhenClauses {
		tracer.Verbose(TraceComponentCase, "Evaluating WHEN clause", TraceContext("clause_index", i, "alias", caseExpr.Alias))
		if pr.evaluateCondition(whenClause.Condition, row, engine) {
			// Condition is true, return the THEN result
			result := pr.evaluateExpressionValue(whenClause.Result, row, engine)
			tracer.Debug(TraceComponentCase, "WHEN clause matched", TraceContext("clause_index", i, "result", result, "alias", caseExpr.Alias))
			return result
		}
	}

	// No WHEN clause matched, return ELSE value if present
	if caseExpr.ElseClause != nil {
		result := pr.evaluateExpressionValue(*caseExpr.ElseClause, row, engine)
		tracer.Debug(TraceComponentCase, "Using ELSE clause", TraceContext("result", result, "alias", caseExpr.Alias))
		return result
	}

	// No ELSE clause, return nil
	tracer.Debug(TraceComponentCase, "No WHEN/ELSE matched, returning nil", TraceContext("alias", caseExpr.Alias))
	return nil
}

// evaluateCondition evaluates a WHERE condition for CASE expressions
func (pr *ParquetReader) evaluateCondition(condition WhereCondition, row Row, engine SubqueryExecutor) bool {
	// Get the left value (column or function)
	var leftValue interface{}

	if condition.Function != nil {
		// Evaluate function on left side
		var err error
		leftValue, err = engine.EvaluateFunction(condition.Function, row)
		if err != nil {
			return false
		}
	} else if condition.Column != "" {
		// Get column value
		if condition.TableName != "" {
			qualifiedKey := condition.TableName + "." + condition.Column
			if val, exists := row[qualifiedKey]; exists {
				leftValue = val
			} else if val, exists := row[condition.Column]; exists {
				leftValue = val
			}
		} else {
			if val, exists := row[condition.Column]; exists {
				leftValue = val
			}
		}
	}

	// Get right side value (literal, column, or function)
	var rightValue interface{}
	if condition.ValueFunction != nil {
		// Evaluate function on right side
		var err error
		rightValue, err = engine.EvaluateFunction(condition.ValueFunction, row)
		if err != nil {
			return false
		}
	} else if condition.ValueColumn != "" {
		// Column comparison
		if condition.ValueTableName != "" {
			qualifiedKey := condition.ValueTableName + "." + condition.ValueColumn
			if val, exists := row[qualifiedKey]; exists {
				rightValue = val
			} else if val, exists := row[condition.ValueColumn]; exists {
				rightValue = val
			}
		} else {
			if val, exists := row[condition.ValueColumn]; exists {
				rightValue = val
			}
		}
	} else {
		// Literal value
		rightValue = condition.Value
	}

	// Compare with the right value based on operator
	switch condition.Operator {
	case "=":
		return pr.CompareValues(leftValue, rightValue) == 0
	case "!=", "<>":
		return pr.CompareValues(leftValue, rightValue) != 0
	case "<":
		return pr.CompareValues(leftValue, rightValue) < 0
	case "<=":
		return pr.CompareValues(leftValue, rightValue) <= 0
	case ">":
		return pr.CompareValues(leftValue, rightValue) > 0
	case ">=":
		return pr.CompareValues(leftValue, rightValue) >= 0
	case "LIKE":
		return pr.matchesLike(leftValue, rightValue)
	case "IN":
		return pr.matchesIn(leftValue, condition.ValueList)
	case "IS NULL":
		return leftValue == nil
	case "IS NOT NULL":
		return leftValue != nil
	}

	return false
}

// evaluateExpressionValue evaluates an expression value for CASE THEN/ELSE clauses
func (pr *ParquetReader) evaluateExpressionValue(expr ExpressionValue, row Row, engine SubqueryExecutor) interface{} {
	switch expr.Type {
	case "literal":
		return expr.LiteralValue
	case "column":
		// Get column value from row
		if expr.TableName != "" {
			qualifiedKey := expr.TableName + "." + expr.ColumnName
			if val, exists := row[qualifiedKey]; exists {
				return val
			}
		}
		if val, exists := row[expr.ColumnName]; exists {
			return val
		}
		return nil
	case "subquery":
		// Execute subquery
		if expr.Subquery != nil {
			return pr.executeColumnSubquery(expr.Subquery, row, engine)
		}
		return nil
	case "case":
		// Evaluate nested CASE expression
		if expr.CaseExpr != nil {
			return pr.evaluateCaseExpression(expr.CaseExpr, row, engine)
		}
		return nil
	}
	return nil
}

// ReadTopK reads rows and maintains only the top K rows based on orderBy criteria
func (pr *ParquetReader) ReadTopK(limit int, orderBy []OrderByColumn, whereConditions []WhereCondition, engine SubqueryExecutor) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	if pr.multiFileReader != nil {
		// For multi-file reader, delegate to it
		return pr.multiFileReader.ReadTopK(limit, orderBy, whereConditions, engine)
	}
	
	// Try to use row group statistics optimization if possible
	if len(orderBy) == 1 && len(whereConditions) == 0 && pr.canUseRowGroupStats(orderBy[0]) {
		// Single ORDER BY column and no WHERE clause - can use row group stats
		return pr.readTopKWithRowGroupFiltering(limit, orderBy[0], whereConditions, engine)
	}
	
	// Fall back to full row reading
	tracer.Info(TraceComponentExecution, "Starting Top-K optimized read (full rows)", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by", orderBy[0].Column,
		"order_direction", orderBy[0].Direction,
		"where_conditions", len(whereConditions),
	))
	
	// Create a TopK heap
	topK := &topKHeap{
		rows:     make([]Row, 0, limit),
		orderBy:  orderBy,
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topK)
	
	// Use parquet reader to stream rows
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	rowCount := 0
	processedCount := 0
	skippedByFilter := 0
	
	// Read rows one by one
	for {
		rowData := make(map[string]interface{})
		err := reader.Read(&rowData)
		if err != nil {
			break // End of file
		}
		
		rowCount++
		row := Row(rowData)
		
		// Apply WHERE filters if any
		if len(whereConditions) > 0 {
			if !pr.matchesConditionsWithEngine(row, whereConditions, engine) {
				skippedByFilter++
				continue
			}
		}
		
		processedCount++
		
		// Maintain top K
		if topK.Len() < limit {
			// Heap not full yet, just add
			heap.Push(topK, row)
		} else {
			// Compare with the worst row in heap (top of min-heap for DESC)
			if topK.shouldReplace(row) {
				heap.Pop(topK)
				heap.Push(topK, row)
			}
		}
		
		// Log progress
		if rowCount%100000 == 0 {
			tracer.Debug(TraceComponentExecution, "Top-K read progress", TraceContext(
				"rows_read", rowCount,
				"rows_processed", processedCount,
				"skipped_by_filter", skippedByFilter,
				"heap_size", topK.Len(),
				"elapsed_ms", time.Since(startTime).Milliseconds(),
			))
		}
	}
	
	// Extract results in sorted order
	result := make([]Row, topK.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(topK).(Row)
	}
	
	elapsed := time.Since(startTime)
	tracer.Info(TraceComponentExecution, "Completed Top-K optimized read", TraceContext(
		"rows_read", rowCount,
		"rows_processed", processedCount,
		"skipped_by_filter", skippedByFilter,
		"result_rows", len(result),
		"elapsed_ms", elapsed.Milliseconds(),
		"rows_per_second", float64(rowCount)/elapsed.Seconds(),
		"optimization_ratio", float64(rowCount)/float64(limit),
	))
	
	return result, nil
}

// readTopKWithStats uses row group statistics to optimize Top-K reading
// by skipping row groups that can't contain top values
func (pr *ParquetReader) readTopKWithStats(limit int, orderBy OrderByColumn) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	tracer.Info(TraceComponentOptimizer, "Starting Top-K read with row group statistics", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by", orderBy.Column,
		"direction", orderBy.Direction,
	))
	
	// Get row group statistics for the ORDER BY column
	stats, err := pr.getRowGroupStatistics(orderBy.Column)
	if err != nil {
		return nil, fmt.Errorf("failed to get row group statistics: %w", err)
	}
	
	if len(stats) == 0 {
		return nil, fmt.Errorf("no row group statistics available")
	}
	
	// Create a heap for column-based Top-K
	topKValues := &topKColumnHeap{
		values:   make([]columnValue, 0, limit),
		orderBy:  orderBy,
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topKValues)
	
	// Keep track of the current worst value in the heap
	var currentWorstValue interface{}
	
	// Process row groups in order of their potential to contain top values
	sort.Slice(stats, func(i, j int) bool {
		if orderBy.Direction == "DESC" {
			// For DESC, compare max values
			return pr.compareParquetValues(stats[i].MaxValue, stats[j].MaxValue) > 0
		} else {
			// For ASC, compare min values
			return pr.compareParquetValues(stats[i].MinValue, stats[j].MinValue) < 0
		}
	})
	
	rowGroupsProcessed := 0
	rowGroupsSkipped := 0
	totalValuesRead := 0
	
	// Process row groups
	for _, rgStats := range stats {
		// Check if we can skip this row group
		if topKValues.Len() >= limit && currentWorstValue != nil {
			canSkip := false
			if orderBy.Direction == "DESC" {
				// For DESC, skip if max value is worse than current worst
				maxVal := pr.parquetValueToInterface(rgStats.MaxValue)
				if pr.CompareValues(maxVal, currentWorstValue) <= 0 {
					canSkip = true
				}
			} else {
				// For ASC, skip if min value is worse than current worst
				minVal := pr.parquetValueToInterface(rgStats.MinValue)
				if pr.CompareValues(minVal, currentWorstValue) >= 0 {
					canSkip = true
				}
			}
			
			if canSkip {
				rowGroupsSkipped++
				tracer.Debug(TraceComponentOptimizer, "Skipping row group based on statistics", 
					TraceContext(
						"row_group", rgStats.RowGroupIndex,
						"min", rgStats.MinValue.String(),
						"max", rgStats.MaxValue.String(),
						"rows", rgStats.NumRows,
					))
				continue
			}
		}
		
		// Read only the ORDER BY column from this row group
		rowGroupsProcessed++
		columnValues, err := pr.readColumnFromRowGroup(rgStats.RowGroupIndex, orderBy.Column)
		if err != nil {
			tracer.Warn(TraceComponentOptimizer, "Failed to read column from row group", 
				TraceContext("row_group", rgStats.RowGroupIndex, "column", orderBy.Column, "error", err.Error()))
			continue
		}
		
		totalValuesRead += len(columnValues)
		
		// Calculate the starting row index for this row group
		startRowIdx := int64(0)
		rowGroups := pr.reader.RowGroups()
		for i := 0; i < rgStats.RowGroupIndex; i++ {
			startRowIdx += rowGroups[i].NumRows()
		}
		
		tracer.Debug(TraceComponentOptimizer, "Processing row group column values", 
			TraceContext(
				"row_group", rgStats.RowGroupIndex,
				"start_row_idx", startRowIdx,
				"num_values", len(columnValues),
				"row_group_num_rows", rgStats.NumRows,
			))
		
		// Process column values
		for i, val := range columnValues {
			if val == nil {
				continue // Skip null values
			}
			
			cv := columnValue{
				value:    val,
				rowIndex: startRowIdx + int64(i),
				rowGroup: rgStats.RowGroupIndex,
			}
			
			// Maintain top K
			if topKValues.Len() < limit {
				// Heap not full yet, just add
				heap.Push(topKValues, cv)
			} else {
				// Compare with the worst value in heap
				if topKValues.shouldReplace(cv) {
					heap.Pop(topKValues)
					heap.Push(topKValues, cv)
				}
			}
			
			// Update current worst value
			if topKValues.Len() >= limit {
				currentWorstValue = topKValues.values[0].value
			}
		}
	}
	
	// Now we have the top K row indices, read full rows
	// Extract row indices in sorted order
	topIndices := make([]columnValue, topKValues.Len())
	for i := len(topIndices) - 1; i >= 0; i-- {
		topIndices[i] = heap.Pop(topKValues).(columnValue)
	}
	
	// Read the full rows for these indices
	result, err := pr.readRowsByIndices(topIndices)
	if err != nil {
		return nil, fmt.Errorf("failed to read full rows: %w", err)
	}
	
	elapsed := time.Since(startTime)
	tracer.Info(TraceComponentOptimizer, "Completed Top-K read with row group statistics", TraceContext(
		"row_groups_total", len(stats),
		"row_groups_processed", rowGroupsProcessed,
		"row_groups_skipped", rowGroupsSkipped,
		"skip_ratio", float64(rowGroupsSkipped)/float64(len(stats)),
		"values_read", totalValuesRead,
		"result_rows", len(result),
		"elapsed_ms", elapsed.Milliseconds(),
	))
	
	return result, nil
}

// columnValue represents a column value with its row index
type columnValue struct {
	value    interface{}
	rowIndex int64
	rowGroup int
}

// topKColumnHeap is a min-heap for maintaining top K column values
type topKColumnHeap struct {
	values  []columnValue
	orderBy OrderByColumn
	reader  *ParquetReader
	limit   int
}

func (h topKColumnHeap) Len() int { return len(h.values) }

func (h topKColumnHeap) Less(i, j int) bool {
	// For DESC order, we want a min-heap (smallest at top)
	// For ASC order, we want a max-heap (largest at top)
	cmp := h.reader.CompareValues(h.values[i].value, h.values[j].value)
	if h.orderBy.Direction == "DESC" {
		return cmp < 0 // Min-heap for DESC
	}
	return cmp > 0 // Max-heap for ASC
}

func (h topKColumnHeap) Swap(i, j int) {
	h.values[i], h.values[j] = h.values[j], h.values[i]
}

func (h *topKColumnHeap) Push(x interface{}) {
	h.values = append(h.values, x.(columnValue))
}

func (h *topKColumnHeap) Pop() interface{} {
	old := h.values
	n := len(old)
	x := old[n-1]
	h.values = old[0 : n-1]
	return x
}

func (h *topKColumnHeap) shouldReplace(cv columnValue) bool {
	// Compare with the worst value (top of heap)
	cmp := h.reader.CompareValues(cv.value, h.values[0].value)
	if h.orderBy.Direction == "DESC" {
		return cmp > 0 // New value is better (larger) for DESC
	}
	return cmp < 0 // New value is better (smaller) for ASC
}

// readRowsByIndices reads full rows for the given row indices
func (pr *ParquetReader) readRowsByIndices(indices []columnValue) ([]Row, error) {
	tracer := GetTracer()
	
	// Group indices by row group for more efficient reading
	rowGroupMap := make(map[int][]columnValue)
	for _, cv := range indices {
		rowGroupMap[cv.rowGroup] = append(rowGroupMap[cv.rowGroup], cv)
	}
	
	// Create a map to store results with their original order
	resultMap := make(map[int64]Row)
	
	// Read rows from each row group
	for rgIndex, cvs := range rowGroupMap {
		// Create index map for this row group
		localIndexMap := make(map[int64]bool)
		for _, cv := range cvs {
			localIndexMap[cv.rowIndex] = true
		}
		
		// Read all rows from this row group
		rows, err := pr.readRowGroup(rgIndex)
		if err != nil {
			tracer.Warn(TraceComponentOptimizer, "Failed to read row group for full rows", 
				TraceContext("row_group", rgIndex, "error", err.Error()))
			continue
		}
		
		// Calculate starting row index for this row group
		startIdx := int64(0)
		rowGroups := pr.reader.RowGroups()
		for i := 0; i < rgIndex; i++ {
			startIdx += rowGroups[i].NumRows()
		}
		
		// Match rows with their global indices
		for localIdx, row := range rows {
			globalIdx := startIdx + int64(localIdx)
			if localIndexMap[globalIdx] {
				resultMap[globalIdx] = row
			}
		}
	}
	
	// Build result in the same order as indices
	result := make([]Row, 0, len(indices))
	for _, cv := range indices {
		if row, exists := resultMap[cv.rowIndex]; exists {
			result = append(result, row)
		} else {
			tracer.Warn(TraceComponentOptimizer, "Could not find row for index", 
				TraceContext("row_index", cv.rowIndex, "row_group", cv.rowGroup))
		}
	}
	
	return result, nil
}

// readRowGroup reads all rows from a specific row group
func (pr *ParquetReader) readRowGroup(rowGroupIndex int) ([]Row, error) {
	rowGroups := pr.reader.RowGroups()
	if rowGroupIndex >= len(rowGroups) {
		return nil, fmt.Errorf("row group index %d out of bounds", rowGroupIndex)
	}
	
	rowGroup := rowGroups[rowGroupIndex]
	rows := make([]Row, 0, rowGroup.NumRows())
	
	// Read rows from this specific row group
	// Note: parquet-go doesn't provide a direct API to read a specific row group,
	// so we need to seek to the right position
	// This is a simplified implementation - in production, we'd need more sophisticated handling
	
	// For now, we'll read all rows and filter by row group
	// This is not optimal but works with current parquet-go limitations
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	// Skip to the target row group
	// Calculate starting row index for this row group
	startRow := int64(0)
	for i := 0; i < rowGroupIndex; i++ {
		startRow += rowGroups[i].NumRows()
	}
	
	// Skip rows from previous row groups
	for i := int64(0); i < startRow; i++ {
		dummy := make(map[string]interface{})
		if err := reader.Read(&dummy); err != nil {
			return nil, err
		}
	}
	
	// Read rows from target row group
	for i := int64(0); i < rowGroup.NumRows(); i++ {
		rowData := make(map[string]interface{})
		if err := reader.Read(&rowData); err != nil {
			break
		}
		rows = append(rows, Row(rowData))
	}
	
	return rows, nil
}

// readColumnFromRowGroup reads specific column values from a row group
func (pr *ParquetReader) readColumnFromRowGroup(rowGroupIndex int, columnName string) ([]interface{}, error) {
	rowGroups := pr.reader.RowGroups()
	if rowGroupIndex >= len(rowGroups) {
		return nil, fmt.Errorf("row group index %d out of bounds", rowGroupIndex)
	}
	
	// Find column index by name
	columnIndex := -1
	for i, col := range pr.GetColumnNames() {
		if col == columnName {
			columnIndex = i
			break
		}
	}
	
	if columnIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	rowGroup := rowGroups[rowGroupIndex]
	columnChunk := rowGroup.ColumnChunks()[columnIndex]
	
	// Pre-allocate slice based on row count estimate for better performance
	values := make([]interface{}, 0, rowGroup.NumRows())
	
	// Read pages from this column with enhanced type handling
	pages := columnChunk.Pages()
	defer pages.Close()
	
	// Get column type for optimization
	columnType := columnChunk.Type()
	
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		
		// Use the most efficient reading strategy based on column type
		pageValues := page.Values()
		n := page.NumValues()
		
		// Optimize based on physical type for maximum performance
		switch columnType.Kind() {
		case parquet.Boolean:
			if boolReader, ok := pageValues.(parquet.BooleanReader); ok {
				boolValues := make([]bool, n)
				read, _ := boolReader.ReadBooleans(boolValues)
				for i := 0; i < read; i++ {
					values = append(values, boolValues[i])
				}
			} else {
				// Fallback to generic reading
				pr.readGenericPageValues(pageValues, n, &values)
			}
		case parquet.Int32:
			if int32Reader, ok := pageValues.(parquet.Int32Reader); ok {
				int32Values := make([]int32, n)
				read, _ := int32Reader.ReadInt32s(int32Values)
				for i := 0; i < read; i++ {
					values = append(values, int32Values[i])
				}
			} else {
				pr.readGenericPageValues(pageValues, n, &values)
			}
		case parquet.Int64:
			if int64Reader, ok := pageValues.(parquet.Int64Reader); ok {
				int64Values := make([]int64, n)
				read, _ := int64Reader.ReadInt64s(int64Values)
				for i := 0; i < read; i++ {
					values = append(values, int64Values[i])
				}
			} else {
				pr.readGenericPageValues(pageValues, n, &values)
			}
		case parquet.Float:
			if floatReader, ok := pageValues.(parquet.FloatReader); ok {
				floatValues := make([]float32, n)
				read, _ := floatReader.ReadFloats(floatValues)
				for i := 0; i < read; i++ {
					values = append(values, float64(floatValues[i]))
				}
			} else {
				pr.readGenericPageValues(pageValues, n, &values)
			}
		case parquet.Double:
			if doubleReader, ok := pageValues.(parquet.DoubleReader); ok {
				doubleValues := make([]float64, n)
				read, _ := doubleReader.ReadDoubles(doubleValues)
				for i := 0; i < read; i++ {
					values = append(values, doubleValues[i])
				}
			} else {
				pr.readGenericPageValues(pageValues, n, &values)
			}
		case parquet.ByteArray, parquet.FixedLenByteArray:
			// Handle string/byte array types - use generic reading for now
			// The ByteArrayReader API may have different signature
			pr.readGenericPageValues(pageValues, n, &values)
		default:
			// Fallback to generic value reading for unsupported types
			pr.readGenericPageValues(pageValues, n, &values)
		}
		
		parquet.Release(page)
	}
	
	return values, nil
}

// readColumnFromRowGroupOptimized reads a specific column with memory pooling and reduced allocations
func (pr *ParquetReader) readColumnFromRowGroupOptimized(rowGroupIndex int, columnName string, estimatedSize int) ([]interface{}, error) {
	rowGroups := pr.reader.RowGroups()
	if rowGroupIndex >= len(rowGroups) {
		return nil, fmt.Errorf("row group index %d out of bounds", rowGroupIndex)
	}
	
	// Find column index by name
	columnIndex := -1
	for i, col := range pr.GetColumnNames() {
		if col == columnName {
			columnIndex = i
			break
		}
	}
	
	if columnIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	rowGroup := rowGroups[rowGroupIndex]
	columnChunk := rowGroup.ColumnChunks()[columnIndex]
	
	// Use estimated size if provided, otherwise use row count
	capacity := estimatedSize
	if capacity <= 0 {
		capacity = int(rowGroup.NumRows())
	}
	
	// Pre-allocate with exact capacity to avoid reallocations
	values := make([]interface{}, 0, capacity)
	
	// Use ColumnChunkValueReader for more efficient reading
	valueReader := parquet.NewColumnChunkValueReader(columnChunk)
	defer valueReader.Close()
	
	// Get column type for optimized reading
	columnType := columnChunk.Type()
	
	// Use buffer pooling for better memory management
	const bufferSize = 1024 // Read in chunks of 1024 values
	valueBuffer := make([]parquet.Value, bufferSize)
	
	for {
		n, err := valueReader.ReadValues(valueBuffer)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading values: %w", err)
		}
		
		// Process the buffer based on column type for efficiency
		for i := 0; i < n; i++ {
			val := valueBuffer[i]
			if val.IsNull() {
				values = append(values, nil)
			} else {
				// Use type-specific conversion for better performance
				switch columnType.Kind() {
				case parquet.Boolean:
					values = append(values, val.Boolean())
				case parquet.Int32:
					values = append(values, val.Int32())
				case parquet.Int64:
					values = append(values, val.Int64())
				case parquet.Float:
					values = append(values, float64(val.Float()))
				case parquet.Double:
					values = append(values, val.Double())
				case parquet.ByteArray, parquet.FixedLenByteArray:
					values = append(values, string(val.ByteArray()))
				default:
					values = append(values, pr.parquetValueToInterface(val))
				}
			}
		}
		
		if err == io.EOF {
			break
		}
	}
	
	return values, nil
}

// readGenericPageValues reads page values using generic Value interface (fallback method)
func (pr *ParquetReader) readGenericPageValues(pageValues parquet.ValueReader, n int64, values *[]interface{}) {
	genericValues := make([]parquet.Value, n)
	read, _ := pageValues.ReadValues(genericValues)
	for i := 0; i < read; i++ {
		if !genericValues[i].IsNull() {
			*values = append(*values, pr.parquetValueToInterface(genericValues[i]))
		} else {
			*values = append(*values, nil)
		}
	}
}

// checkBloomFilterSkip checks if a row group can be skipped using bloom filters
func (pr *ParquetReader) checkBloomFilterSkip(rowGroupIndex int, whereConditions []WhereCondition) bool {
	tracer := GetTracer()
	
	rowGroups := pr.reader.RowGroups()
	if rowGroupIndex >= len(rowGroups) {
		return false
	}
	
	rowGroup := rowGroups[rowGroupIndex]
	
	// Check each WHERE condition that involves equality
	for _, condition := range whereConditions {
		if condition.Operator == "=" && condition.Value != nil {
			// Find column index
			columnIndex := -1
			for i, col := range pr.GetColumnNames() {
				if col == condition.Column {
					columnIndex = i
					break
				}
			}
			
			if columnIndex >= 0 && columnIndex < len(rowGroup.ColumnChunks()) {
				columnChunk := rowGroup.ColumnChunks()[columnIndex]
				if fileColumnChunk, ok := columnChunk.(*parquet.FileColumnChunk); ok {
					if bloomFilter := fileColumnChunk.BloomFilter(); bloomFilter != nil {
						// Convert condition value to parquet.Value for bloom filter check
						conditionValue := pr.interfaceToParquetValue(condition.Value)
						if !conditionValue.IsNull() {
							exists, err := bloomFilter.Check(conditionValue)
							if err == nil && !exists {
								tracer.Debug(TraceComponentOptimizer, "Bloom filter indicates value not present", 
									TraceContext(
										"row_group", rowGroupIndex,
										"column", condition.Column,
										"value", condition.Value,
									))
								return true // Skip this row group
							}
						}
					}
				}
			}
		}
	}
	
	return false // Cannot skip
}

// interfaceToParquetValue converts interface{} to parquet.Value
func (pr *ParquetReader) interfaceToParquetValue(val interface{}) parquet.Value {
	switch v := val.(type) {
	case bool:
		return parquet.BooleanValue(v)
	case int:
		return parquet.Int32Value(int32(v))
	case int32:
		return parquet.Int32Value(v)
	case int64:
		return parquet.Int64Value(v)
	case float32:
		return parquet.FloatValue(v)
	case float64:
		return parquet.DoubleValue(v)
	case string:
		return parquet.ByteArrayValue([]byte(v))
	case []byte:
		return parquet.ByteArrayValue(v)
	default:
		// Return null value for unsupported types
		return parquet.Value{}
	}
}

// compareParquetValues compares two parquet.Value objects using native parquet type system
func (pr *ParquetReader) compareParquetValues(a, b parquet.Value) int {
	// Handle null values first
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}
	
	// Use native parquet type comparison for better performance
	if a.Kind() != b.Kind() {
		// Different types - convert to common type or use interface comparison
		aVal := pr.parquetValueToInterface(a)
		bVal := pr.parquetValueToInterface(b)
		return pr.CompareValues(aVal, bVal)
	}
	
	// Same types - use efficient native comparison
	switch a.Kind() {
	case parquet.Boolean:
		aBool, bBool := a.Boolean(), b.Boolean()
		if aBool == bBool {
			return 0
		}
		if aBool {
			return 1
		}
		return -1
	case parquet.Int32:
		aInt, bInt := a.Int32(), b.Int32()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case parquet.Int64:
		aInt, bInt := a.Int64(), b.Int64()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case parquet.Float:
		aFloat, bFloat := a.Float(), b.Float()
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case parquet.Double:
		aDouble, bDouble := a.Double(), b.Double()
		if aDouble < bDouble {
			return -1
		} else if aDouble > bDouble {
			return 1
		}
		return 0
	case parquet.ByteArray, parquet.FixedLenByteArray:
		// Use efficient byte array comparison
		aBytes, bBytes := a.ByteArray(), b.ByteArray()
		if len(aBytes) == 0 && len(bBytes) == 0 {
			return 0
		}
		if len(aBytes) == 0 {
			return -1
		}
		if len(bBytes) == 0 {
			return 1
		}
		
		// Compare byte by byte for lexicographical order
		minLen := len(aBytes)
		if len(bBytes) < minLen {
			minLen = len(bBytes)
		}
		
		for i := 0; i < minLen; i++ {
			if aBytes[i] < bBytes[i] {
				return -1
			} else if aBytes[i] > bBytes[i] {
				return 1
			}
		}
		
		// All compared bytes are equal, check length
		if len(aBytes) < len(bBytes) {
			return -1
		} else if len(aBytes) > len(bBytes) {
			return 1
		}
		return 0
	default:
		// Fallback to interface comparison for unsupported types
		aVal := pr.parquetValueToInterface(a)
		bVal := pr.parquetValueToInterface(b)
		return pr.CompareValues(aVal, bVal)
	}
}

// parquetValueToInterface converts a parquet.Value to an interface{}
func (pr *ParquetReader) parquetValueToInterface(v parquet.Value) interface{} {
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return v.Int32()
	case parquet.Int64:
		return v.Int64()
	case parquet.Int96:
		return v.Int96()
	case parquet.Float:
		return v.Float()
	case parquet.Double:
		return v.Double()
	case parquet.ByteArray:
		return string(v.ByteArray())
	case parquet.FixedLenByteArray:
		// For fixed-length byte arrays, return as byte slice
		return v.ByteArray() // Use ByteArray() for fixed-length too
	default:
		return nil
	}
}

// readTopKWithStatsV2 is a simpler implementation that reads full rows from each row group
func (pr *ParquetReader) readTopKWithStatsV2(limit int, orderBy OrderByColumn) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	tracer.Info(TraceComponentOptimizer, "Starting Top-K read with row group statistics V2", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by", orderBy.Column,
		"direction", orderBy.Direction,
	))
	
	// Get row group statistics for the ORDER BY column
	stats, err := pr.getRowGroupStatistics(orderBy.Column)
	if err != nil {
		return nil, fmt.Errorf("failed to get row group statistics: %w", err)
	}
	
	if len(stats) == 0 {
		return nil, fmt.Errorf("no row group statistics available")
	}
	
	// Create a TopK heap for rows
	topK := &topKHeap{
		rows:     make([]Row, 0, limit),
		orderBy:  []OrderByColumn{orderBy},
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topK)
	
	// Keep track of the current worst value in the heap
	var currentWorstValue interface{}
	
	// Sort row groups by their potential to contain top values
	sort.Slice(stats, func(i, j int) bool {
		if orderBy.Direction == "DESC" {
			// For DESC, compare max values
			return pr.compareParquetValues(stats[i].MaxValue, stats[j].MaxValue) > 0
		} else {
			// For ASC, compare min values
			return pr.compareParquetValues(stats[i].MinValue, stats[j].MinValue) < 0
		}
	})
	
	rowGroupsProcessed := 0
	rowGroupsSkipped := 0
	totalRowsRead := 0
	
	// Process row groups
	for _, rgStats := range stats {
		// Check if we can skip this row group
		if topK.Len() >= limit && currentWorstValue != nil {
			canSkip := false
			if orderBy.Direction == "DESC" {
				// For DESC, skip if max value is worse than current worst
				maxVal := pr.parquetValueToInterface(rgStats.MaxValue)
				if pr.CompareValues(maxVal, currentWorstValue) <= 0 {
					canSkip = true
				}
			} else {
				// For ASC, skip if min value is worse than current worst
				minVal := pr.parquetValueToInterface(rgStats.MinValue)
				if pr.CompareValues(minVal, currentWorstValue) >= 0 {
					canSkip = true
				}
			}
			
			if canSkip {
				rowGroupsSkipped++
				tracer.Debug(TraceComponentOptimizer, "Skipping row group based on statistics", 
					TraceContext(
						"row_group", rgStats.RowGroupIndex,
						"min", rgStats.MinValue.String(),
						"max", rgStats.MaxValue.String(),
						"rows", rgStats.NumRows,
					))
				continue
			}
		}
		
		// Read this row group directly  
		rowGroupsProcessed++
		rowGroup := pr.reader.RowGroups()[rgStats.RowGroupIndex]
		
		// Create a reader for this specific row group
		// We'll read all rows from this row group and filter
		reader := parquet.NewReader(pr.reader)
		defer reader.Close()
		
		// Skip to this row group
		startRow := int64(0)
		for i := 0; i < rgStats.RowGroupIndex; i++ {
			startRow += pr.reader.RowGroups()[i].NumRows()
		}
		
		// Skip rows before this row group
		for i := int64(0); i < startRow; i++ {
			dummy := make(map[string]interface{})
			if err := reader.Read(&dummy); err != nil {
				break
			}
		}
		
		// Read rows from this row group
		rowsInGroup := 0
		for i := int64(0); i < rowGroup.NumRows(); i++ {
			rowData := make(map[string]interface{})
			if err := reader.Read(&rowData); err != nil {
				break
			}
			
			row := Row(rowData)
			rowsInGroup++
			totalRowsRead++
			
			// Check if this row has the ORDER BY column
			if val, exists := row[orderBy.Column]; !exists || val == nil {
				continue
			}
			
			// Maintain top K
			if topK.Len() < limit {
				// Heap not full yet, just add
				heap.Push(topK, row)
			} else {
				// Compare with the worst row in heap
				if topK.shouldReplace(row) {
					heap.Pop(topK)
					heap.Push(topK, row)
				}
			}
			
			// Update current worst value
			if topK.Len() >= limit {
				// Peek at the worst value (top of min-heap for DESC)
				worstRow := topK.rows[0]
				if val, exists := worstRow[orderBy.Column]; exists {
					currentWorstValue = val
				}
			}
		}
		
		tracer.Debug(TraceComponentOptimizer, "Processed row group", TraceContext(
			"row_group", rgStats.RowGroupIndex,
			"rows_read", rowsInGroup,
			"heap_size", topK.Len(),
		))
	}
	
	// Extract results in sorted order
	result := make([]Row, topK.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(topK).(Row)
	}
	
	elapsed := time.Since(startTime)
	tracer.Info(TraceComponentOptimizer, "Completed Top-K read with row group statistics V2", TraceContext(
		"row_groups_total", len(stats),
		"row_groups_processed", rowGroupsProcessed,
		"row_groups_skipped", rowGroupsSkipped,
		"skip_ratio", float64(rowGroupsSkipped)/float64(len(stats)),
		"rows_read", totalRowsRead,
		"result_rows", len(result),
		"elapsed_ms", elapsed.Milliseconds(),
	))
	
	return result, nil
}

// readTopKColumnOptimized uses column-specific reading to find Top-K without scanning full table
func (pr *ParquetReader) readTopKColumnOptimized(limit int, orderBy OrderByColumn) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	tracer.Info(TraceComponentOptimizer, "Starting column-optimized Top-K read", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by", orderBy.Column,
		"direction", orderBy.Direction,
	))
	
	// Get row group statistics
	stats, err := pr.getRowGroupStatistics(orderBy.Column)
	if err != nil {
		return nil, fmt.Errorf("failed to get row group statistics: %w", err)
	}
	
	// Find column index
	columnIndex := -1
	columnNames := pr.GetColumnNames()
	for i, col := range columnNames {
		if col == orderBy.Column {
			columnIndex = i
			break
		}
	}
	
	if columnIndex == -1 {
		return nil, fmt.Errorf("column %s not found", orderBy.Column)
	}
	
	// Create a heap for tracking top K values with their full row data
	topKHeap := &genericTopKHeap{
		items:    make([]valueWithRow, 0, limit),
		orderBy:  orderBy,
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topKHeap)
	
	// Keep track of current worst value
	var currentWorstValue interface{}
	
	// Sort row groups by potential to contain top values
	sort.Slice(stats, func(i, j int) bool {
		if orderBy.Direction == "DESC" {
			return pr.compareParquetValues(stats[i].MaxValue, stats[j].MaxValue) > 0
		} else {
			return pr.compareParquetValues(stats[i].MinValue, stats[j].MinValue) < 0
		}
	})
	
	rowGroupsProcessed := 0
	rowGroupsSkipped := 0
	totalRowsRead := 0
	
	// Process each row group
	for _, rgStats := range stats {
		// Check if we can skip this row group
		if topKHeap.Len() >= limit && currentWorstValue != nil {
			canSkip := false
			if orderBy.Direction == "DESC" {
				maxVal := pr.parquetValueToInterface(rgStats.MaxValue)
				if pr.CompareValues(maxVal, currentWorstValue) <= 0 {
					canSkip = true
				}
			} else {
				minVal := pr.parquetValueToInterface(rgStats.MinValue)
				if pr.CompareValues(minVal, currentWorstValue) >= 0 {
					canSkip = true
				}
			}
			
			if canSkip {
				rowGroupsSkipped++
				continue
			}
		}
		
		rowGroupsProcessed++
		
		// Read this row group efficiently
		rows, err := pr.readRowGroupEfficient(rgStats.RowGroupIndex, columnIndex, orderBy, topKHeap, &currentWorstValue, limit)
		if err != nil {
			tracer.Warn(TraceComponentOptimizer, "Failed to read row group", 
				TraceContext("row_group", rgStats.RowGroupIndex, "error", err.Error()))
			continue
		}
		
		totalRowsRead += len(rows)
	}
	
	// Extract final results
	result := make([]Row, topKHeap.Len())
	for i := len(result) - 1; i >= 0; i-- {
		item := heap.Pop(topKHeap).(valueWithRow)
		result[i] = item.row
	}
	
	elapsed := time.Since(startTime)
	tracer.Info(TraceComponentOptimizer, "Completed column-optimized Top-K read", TraceContext(
		"row_groups_total", len(stats),
		"row_groups_processed", rowGroupsProcessed,
		"row_groups_skipped", rowGroupsSkipped,
		"skip_ratio", float64(rowGroupsSkipped)/float64(len(stats)),
		"rows_read", totalRowsRead,
		"result_rows", len(result),
		"elapsed_ms", elapsed.Milliseconds(),
	))
	
	return result, nil
}

// readRowGroupEfficient reads a row group and maintains only top K rows
func (pr *ParquetReader) readRowGroupEfficient(rowGroupIndex int, columnIndex int, orderBy OrderByColumn, 
	topKHeap *genericTopKHeap, currentWorstValue *interface{}, limit int) ([]Row, error) {
	
	rowGroups := pr.reader.RowGroups()
	if rowGroupIndex >= len(rowGroups) {
		return nil, fmt.Errorf("row group index %d out of bounds", rowGroupIndex)
	}
	
	rowGroup := rowGroups[rowGroupIndex]
	
	// First, read the ORDER BY column values
	columnChunk := rowGroup.ColumnChunks()[columnIndex]
	pages := columnChunk.Pages()
	defer pages.Close()
	
	// Collect all values from this column
	columnValues := make([]interface{}, 0, rowGroup.NumRows())
	
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		
		// Read values using typed readers for efficiency
		pageValues := page.Values()
		n := page.NumValues()
		
		switch reader := pageValues.(type) {
		case parquet.Int32Reader:
			values := make([]int32, n)
			read, _ := reader.ReadInt32s(values)
			for i := 0; i < read; i++ {
				columnValues = append(columnValues, values[i])
			}
		case parquet.Int64Reader:
			values := make([]int64, n)
			read, _ := reader.ReadInt64s(values)
			for i := 0; i < read; i++ {
				columnValues = append(columnValues, values[i])
			}
		case parquet.FloatReader:
			values := make([]float32, n)
			read, _ := reader.ReadFloats(values)
			for i := 0; i < read; i++ {
				columnValues = append(columnValues, float64(values[i]))
			}
		case parquet.DoubleReader:
			values := make([]float64, n)
			read, _ := reader.ReadDoubles(values)
			for i := 0; i < read; i++ {
				columnValues = append(columnValues, values[i])
			}
		default:
			// Fallback to generic reading
			genericValues := make([]parquet.Value, n)
			read, _ := pageValues.ReadValues(genericValues)
			for i := 0; i < read; i++ {
				columnValues = append(columnValues, pr.parquetValueToInterface(genericValues[i]))
			}
		}
		
		parquet.Release(page)
	}
	
	// Find indices of potential top K values
	topIndices := make([]int, 0)
	for i, val := range columnValues {
		if val == nil {
			continue
		}
		
		// Check if this value could be in top K
		if topKHeap.Len() < limit {
			topIndices = append(topIndices, i)
		} else if *currentWorstValue != nil {
			cmp := pr.CompareValues(val, *currentWorstValue)
			if (orderBy.Direction == "DESC" && cmp > 0) || (orderBy.Direction == "ASC" && cmp < 0) {
				topIndices = append(topIndices, i)
			}
		}
	}
	
	// Now read only the rows at these indices
	if len(topIndices) == 0 {
		return []Row{}, nil
	}
	
	// Read full rows for the selected indices
	rows := make([]Row, 0, len(topIndices))
	
	// Create a reader positioned at this row group
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	// Skip to this row group
	skipCount := int64(0)
	for i := 0; i < rowGroupIndex; i++ {
		skipCount += rowGroups[i].NumRows()
	}
	
	// Skip previous row groups
	for i := int64(0); i < skipCount; i++ {
		dummy := make(map[string]interface{})
		reader.Read(&dummy)
	}
	
	// Read rows from this row group
	currentIndex := 0
	nextTargetIndex := 0
	
	for i := int64(0); i < rowGroup.NumRows() && nextTargetIndex < len(topIndices); i++ {
		if currentIndex == topIndices[nextTargetIndex] {
			// This is a row we want
			rowData := make(map[string]interface{})
			if err := reader.Read(&rowData); err != nil {
				break
			}
			
			row := Row(rowData)
			val := columnValues[currentIndex]
			
			// Add to heap
			vwr := valueWithRow{value: val, row: row}
			if topKHeap.Len() < limit {
				heap.Push(topKHeap, vwr)
			} else if topKHeap.shouldReplace(vwr) {
				heap.Pop(topKHeap)
				heap.Push(topKHeap, vwr)
			}
			
			// Update worst value
			if topKHeap.Len() >= limit {
				*currentWorstValue = topKHeap.items[0].value
			}
			
			rows = append(rows, row)
			nextTargetIndex++
		} else {
			// Skip this row
			dummy := make(map[string]interface{})
			reader.Read(&dummy)
		}
		currentIndex++
	}
	
	return rows, nil
}

// genericTopKHeap for maintaining top K values with their rows
type genericTopKHeap struct {
	items   []valueWithRow
	orderBy OrderByColumn
	reader  *ParquetReader
	limit   int
}

type valueWithRow struct {
	value interface{}
	row   Row
}

func (h genericTopKHeap) Len() int { return len(h.items) }

func (h genericTopKHeap) Less(i, j int) bool {
	cmp := h.reader.CompareValues(h.items[i].value, h.items[j].value)
	if h.orderBy.Direction == "DESC" {
		return cmp < 0 // Min-heap for DESC
	}
	return cmp > 0 // Max-heap for ASC
}

func (h genericTopKHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *genericTopKHeap) Push(x interface{}) {
	h.items = append(h.items, x.(valueWithRow))
}

func (h *genericTopKHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

func (h *genericTopKHeap) shouldReplace(vwr valueWithRow) bool {
	cmp := h.reader.CompareValues(vwr.value, h.items[0].value)
	if h.orderBy.Direction == "DESC" {
		return cmp > 0 // New value is better (larger) for DESC
	}
	return cmp < 0 // New value is better (smaller) for ASC
}

// canUseRowGroupStats checks if we can use row group statistics optimization
func (pr *ParquetReader) canUseRowGroupStats(orderBy OrderByColumn) bool {
	tracer := GetTracer()
	
	// Check if column exists and has statistics
	stats, err := pr.getRowGroupStatistics(orderBy.Column)
	hasStats := err == nil && len(stats) > 0
	
	tracer.Debug(TraceComponentOptimizer, "Checking row group statistics availability", TraceContext(
		"column", orderBy.Column,
		"has_stats", hasStats,
		"num_row_groups", len(stats),
		"error", err,
	))
	
	return hasStats
}

// readTopKWithRowGroupFiltering reads top K using row group filtering to avoid full table scan
func (pr *ParquetReader) readTopKWithRowGroupFiltering(limit int, orderBy OrderByColumn, whereConditions []WhereCondition, engine SubqueryExecutor) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	tracer.Info(TraceComponentOptimizer, "Starting Top-K with row group filtering", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by", orderBy.Column,
		"direction", orderBy.Direction,
		"where_conditions", len(whereConditions),
	))
	
	// Get row group statistics
	stats, err := pr.getRowGroupStatistics(orderBy.Column)
	if err != nil {
		return nil, err
	}
	
	// Log row group statistics
	tracer.Info(TraceComponentOptimizer, "Row group statistics retrieved", TraceContext(
		"num_row_groups", len(stats),
		"total_rows", pr.reader.NumRows(),
	))
	
	for i, stat := range stats {
		tracer.Debug(TraceComponentOptimizer, "Row group stats", TraceContext(
			"row_group", i,
			"min_value", stat.MinValue.String(),
			"max_value", stat.MaxValue.String(),
			"num_rows", stat.NumRows,
		))
	}
	
	// Create a TopK heap
	topK := &topKHeap{
		rows:     make([]Row, 0, limit),
		orderBy:  []OrderByColumn{orderBy},
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topK)
	
	tracer.Info(TraceComponentOptimizer, "Heap algorithm configuration", TraceContext(
		"heap_type", fmt.Sprintf("%s-heap for %s order", 
			map[bool]string{true: "min", false: "max"}[orderBy.Direction == "DESC"],
			orderBy.Direction),
		"capacity", limit,
		"comparison_strategy", "maintain worst value at heap root",
	))
	
	// Keep track of the current worst value
	var currentWorstValue interface{}
	
	// Instead of sorting, we'll process in natural order but use stats to skip
	// Create a map for quick lookup of stats by row group index
	statsMap := make(map[int]RowGroupStats)
	for _, stat := range stats {
		statsMap[stat.RowGroupIndex] = stat
	}
	
	rowGroupsProcessed := 0
	rowGroupsSkipped := 0
	totalRowsRead := 0
	
	// Create a single reader that we'll use throughout
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	// Process row groups in their natural order
	rowGroups := pr.reader.RowGroups()
	for rgIndex := 0; rgIndex < len(rowGroups); rgIndex++ {
		rgStats, hasStats := statsMap[rgIndex]
		if !hasStats {
			continue
		}
		
		// Check if we can use bloom filter for WHERE condition optimization
		if len(whereConditions) > 0 {
			canSkipWithBloom := pr.checkBloomFilterSkip(rgIndex, whereConditions)
			if canSkipWithBloom {
				rowGroupsSkipped++
				tracer.Debug(TraceComponentOptimizer, "Skipping row group using bloom filter", 
					TraceContext("row_group", rgIndex, "where_conditions", len(whereConditions)))
				// Skip all rows in this row group
				for i := int64(0); i < rgStats.NumRows; i++ {
					dummy := make(map[string]interface{})
					if err := reader.Read(&dummy); err != nil {
						break
					}
				}
				continue
			}
		}
		
		// Check if we can skip this row group
		if topK.Len() >= limit && currentWorstValue != nil {
			canSkip := false
			if orderBy.Direction == "DESC" {
				maxVal := pr.parquetValueToInterface(rgStats.MaxValue)
				if pr.CompareValues(maxVal, currentWorstValue) <= 0 {
					canSkip = true
				}
			} else {
				minVal := pr.parquetValueToInterface(rgStats.MinValue)
				if pr.CompareValues(minVal, currentWorstValue) >= 0 {
					canSkip = true
				}
			}
			
			if canSkip {
				rowGroupsSkipped++
				tracer.Debug(TraceComponentOptimizer, "Skipping row group based on statistics", 
					TraceContext(
						"row_group", rgIndex,
						"min", rgStats.MinValue.String(),
						"max", rgStats.MaxValue.String(),
						"num_rows", rgStats.NumRows,
					))
				// Skip all rows in this row group
				for i := int64(0); i < rgStats.NumRows; i++ {
					dummy := make(map[string]interface{})
					if err := reader.Read(&dummy); err != nil {
						break
					}
				}
				continue
			}
		}
		
		// Process this row group with column-based filtering
		rowGroupsProcessed++
		
		tracer.Debug(TraceComponentOptimizer, "Processing row group", TraceContext(
			"row_group", rgIndex,
			"min", rgStats.MinValue.String(),
			"max", rgStats.MaxValue.String(),
			"num_rows", rgStats.NumRows,
		))
		
		// First, read only the ORDER BY column to find candidate rows
		tracer.Debug(TraceComponentOptimizer, "Reading ORDER BY column for filtering", TraceContext(
			"row_group", rgIndex,
			"column", orderBy.Column,
			"strategy", "column-first selective reading",
		))
		
		// Use optimized column reader with estimated size based on current heap state
		estimatedCandidates := int(rgStats.NumRows)
		if topK.Len() >= limit && currentWorstValue != nil {
			// Estimate fewer candidates needed when heap is full
			estimatedCandidates = limit * 2 // Conservative estimate
		}
		
		columnValues, err := pr.readColumnFromRowGroupOptimized(rgIndex, orderBy.Column, estimatedCandidates)
		if err != nil {
			tracer.Warn(TraceComponentOptimizer, "Failed to read column from row group", 
				TraceContext("row_group", rgIndex, "error", err.Error()))
			// Fall back to reading all rows
			for i := int64(0); i < rgStats.NumRows; i++ {
				dummy := make(map[string]interface{})
				reader.Read(&dummy)
			}
			continue
		}
		
		// Find indices of rows that could be in top K
		candidateIndices := make([]int, 0)
		nullCount := 0
		for i, val := range columnValues {
			if val == nil {
				nullCount++
				continue
			}
			
			// Check if this value could be in top K
			if topK.Len() < limit {
				candidateIndices = append(candidateIndices, i)
			} else if currentWorstValue != nil {
				cmp := pr.CompareValues(val, currentWorstValue)
				if (orderBy.Direction == "DESC" && cmp > 0) || (orderBy.Direction == "ASC" && cmp < 0) {
					candidateIndices = append(candidateIndices, i)
				}
			}
		}
		
		selectivity := float64(len(candidateIndices)) / float64(len(columnValues) - nullCount) * 100
		tracer.Info(TraceComponentOptimizer, "Column-based filtering results", TraceContext(
			"row_group", rgIndex,
			"total_rows", len(columnValues),
			"null_values", nullCount,
			"candidate_rows", len(candidateIndices),
			"selectivity_pct", fmt.Sprintf("%.2f", selectivity),
			"current_worst", currentWorstValue,
		))
		
		// Read only the candidate rows
		rowsInGroup := 0
		currentIdx := 0
		nextCandidateIdx := 0
		
		for i := int64(0); i < rgStats.NumRows && nextCandidateIdx < len(candidateIndices); i++ {
			if currentIdx == candidateIndices[nextCandidateIdx] {
				// This is a candidate row - read it
				rowData := make(map[string]interface{})
				err := reader.Read(&rowData)
				if err != nil {
					break
				}
				
				row := Row(rowData)
				rowsInGroup++
				totalRowsRead++
				
				// Apply WHERE filters if any
				if len(whereConditions) > 0 {
					if !pr.matchesConditionsWithEngine(row, whereConditions, engine) {
						nextCandidateIdx++
						currentIdx++
						continue
					}
				}
				
				// Maintain top K
				if topK.Len() < limit {
					heap.Push(topK, row)
					// Update current worst value when heap becomes full
					if topK.Len() == limit {
						worstRow := topK.rows[0]
						if val, exists := worstRow[orderBy.Column]; exists {
							currentWorstValue = val
						}
					}
				} else {
					if topK.shouldReplace(row) {
						heap.Pop(topK)
						heap.Push(topK, row)
						// Update current worst value after replacement
						worstRow := topK.rows[0]
						if val, exists := worstRow[orderBy.Column]; exists {
							currentWorstValue = val
						}
					}
				}
				
				nextCandidateIdx++
			} else {
				// Skip this row
				dummy := make(map[string]interface{})
				reader.Read(&dummy)
			}
			currentIdx++
		}
		
		// Skip any remaining rows in this row group
		for i := int64(currentIdx); i < rgStats.NumRows; i++ {
			dummy := make(map[string]interface{})
			reader.Read(&dummy)
		}
		
		tracer.Debug(TraceComponentOptimizer, "Processed row group", TraceContext(
			"row_group", rgIndex,
			"rows_read", rowsInGroup,
			"heap_size", topK.Len(),
			"current_worst", currentWorstValue,
		))
		
		// Check if we can skip all remaining row groups
		if topK.Len() >= limit && currentWorstValue != nil {
			canSkipRemaining := true
			for nextRG := rgIndex + 1; nextRG < len(rowGroups); nextRG++ {
				if nextStats, hasStats := statsMap[nextRG]; hasStats {
					if orderBy.Direction == "DESC" {
						maxVal := pr.parquetValueToInterface(nextStats.MaxValue)
						if pr.CompareValues(maxVal, currentWorstValue) > 0 {
							canSkipRemaining = false
							break
						}
					} else {
						minVal := pr.parquetValueToInterface(nextStats.MinValue)
						if pr.CompareValues(minVal, currentWorstValue) < 0 {
							canSkipRemaining = false
							break
						}
					}
				}
			}
			
			if canSkipRemaining {
				tracer.Info(TraceComponentOptimizer, "Early termination - all remaining row groups can be skipped", 
					TraceContext("remaining_row_groups", len(rowGroups) - rgIndex - 1))
				rowGroupsSkipped += len(rowGroups) - rgIndex - 1
				break
			}
		}
	}
	
	// Extract results in sorted order
	result := make([]Row, topK.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(topK).(Row)
	}
	
	elapsed := time.Since(startTime)
	
	// Calculate optimization metrics
	totalPossibleRows := pr.reader.NumRows()
	rowsSkipped := totalPossibleRows - int64(totalRowsRead)
	reductionPct := float64(rowsSkipped) / float64(totalPossibleRows) * 100
	
	tracer.Info(TraceComponentOptimizer, "Completed Top-K with row group filtering", TraceContext(
		"row_groups_total", len(stats),
		"row_groups_processed", rowGroupsProcessed,
		"row_groups_skipped", rowGroupsSkipped,
		"skip_ratio", float64(rowGroupsSkipped)/float64(len(stats)),
		"rows_read", totalRowsRead,
		"rows_skipped", rowsSkipped,
		"reduction_pct", fmt.Sprintf("%.2f", reductionPct),
		"result_rows", len(result),
		"elapsed_ms", elapsed.Milliseconds(),
		"throughput_rows_per_sec", int64(float64(totalRowsRead)/elapsed.Seconds()),
	))
	
	// Log optimization summary
	tracer.Info(TraceComponentOptimizer, "Top-K optimization summary", TraceContext(
		"strategy", "row group filtering + column-based selection",
		"total_file_rows", totalPossibleRows,
		"rows_examined", totalRowsRead,
		"optimization_benefit", fmt.Sprintf("%.1fx reduction", float64(totalPossibleRows)/float64(totalRowsRead)),
		"column_projection", fmt.Sprintf("read 1/%d columns for filtering", len(pr.GetColumnNames())),
	))
	
	return result, nil
}

// indexedRow represents a row with its index for column-pruned Top-K
type indexedRow struct {
	index int64
	data  Row
}

// readTopKPruned reads only the ORDER BY columns first, then fetches full rows
func (pr *ParquetReader) readTopKPruned(limit int, orderBy []OrderByColumn) ([]Row, error) {
	tracer := GetTracer()
	startTime := time.Now()
	
	// Extract ORDER BY column names
	orderByColumns := make([]string, len(orderBy))
	for i, ob := range orderBy {
		orderByColumns[i] = ob.Column
	}
	
	tracer.Info(TraceComponentExecution, "Starting column-pruned Top-K read", TraceContext(
		"file", pr.filePath,
		"limit", limit,
		"order_by_columns", orderByColumns,
	))
	
	// Create a heap for indexed rows
	topKIndexed := &topKIndexedHeap{
		rows:     make([]indexedRow, 0, limit),
		orderBy:  orderBy,
		reader:   pr,
		limit:    limit,
	}
	heap.Init(topKIndexed)
	
	// Create a new reader for column-specific reading
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	// Configure reader to read only specific columns
	// Note: parquet-go doesn't support column pruning in the reader directly,
	// so we'll read all columns but only process the ones we need
	
	rowIndex := int64(0)
	
	// First pass: Read and process only ORDER BY columns
	for {
		rowData := make(map[string]interface{})
		err := reader.Read(&rowData)
		if err != nil {
			break // End of file
		}
		
		// Extract only ORDER BY columns
		prunedRow := make(Row)
		for _, col := range orderByColumns {
			if val, exists := rowData[col]; exists {
				prunedRow[col] = val
			}
		}
		
		idxRow := indexedRow{
			index: rowIndex,
			data:  prunedRow,
		}
		
		// Maintain top K
		if topKIndexed.Len() < limit {
			heap.Push(topKIndexed, idxRow)
		} else {
			if topKIndexed.shouldReplace(idxRow) {
				heap.Pop(topKIndexed)
				heap.Push(topKIndexed, idxRow)
			}
		}
		
		rowIndex++
		
		// Log progress
		if rowIndex%100000 == 0 {
			tracer.Debug(TraceComponentExecution, "Column-pruned scan progress", TraceContext(
				"rows_scanned", rowIndex,
				"heap_size", topKIndexed.Len(),
				"elapsed_ms", time.Since(startTime).Milliseconds(),
			))
		}
	}
	
	// Extract the top K row indices
	topIndices := make([]int64, topKIndexed.Len())
	for i := len(topIndices) - 1; i >= 0; i-- {
		idxRow := heap.Pop(topKIndexed).(indexedRow)
		topIndices[i] = idxRow.index
	}
	
	firstPassElapsed := time.Since(startTime)
	tracer.Info(TraceComponentExecution, "Completed first pass (column pruning)", TraceContext(
		"rows_scanned", rowIndex,
		"top_k_found", len(topIndices),
		"elapsed_ms", firstPassElapsed.Milliseconds(),
		"rows_per_second", float64(rowIndex)/firstPassElapsed.Seconds(),
	))
	
	// Second pass: Fetch full rows for the top K indices
	secondPassStart := time.Now()
	
	// Sort indices for sequential reading
	sort.Slice(topIndices, func(i, j int) bool {
		return topIndices[i] < topIndices[j]
	})
	
	// Read full rows for selected indices
	result := make([]Row, 0, len(topIndices))
	reader2 := parquet.NewReader(pr.reader)
	defer reader2.Close()
	
	currentIndex := int64(0)
	topIdxPos := 0
	
	for topIdxPos < len(topIndices) {
		targetIndex := topIndices[topIdxPos]
		
		// Skip to target row
		for currentIndex < targetIndex {
			var dummy interface{}
			err := reader2.Read(&dummy)
			if err != nil {
				break
			}
			currentIndex++
		}
		
		// Read the target row
		if currentIndex == targetIndex {
			rowData := make(map[string]interface{})
			err := reader2.Read(&rowData)
			if err == nil {
				result = append(result, Row(rowData))
			}
			currentIndex++
			topIdxPos++
		}
	}
	
	// Sort the final results according to ORDER BY
	SortRowsWithOrderBy(result, orderBy, pr)
	
	totalElapsed := time.Since(startTime)
	secondPassElapsed := time.Since(secondPassStart)
	
	tracer.Info(TraceComponentExecution, "Completed column-pruned Top-K read", TraceContext(
		"total_rows_scanned", rowIndex,
		"result_rows", len(result),
		"first_pass_ms", firstPassElapsed.Milliseconds(),
		"second_pass_ms", secondPassElapsed.Milliseconds(),
		"total_elapsed_ms", totalElapsed.Milliseconds(),
		"optimization_ratio", float64(rowIndex)/float64(limit),
	))
	
	return result, nil
}

// topKHeap implements heap.Interface for maintaining top K rows
type topKHeap struct {
	rows    []Row
	orderBy []OrderByColumn
	reader  *ParquetReader
	limit   int
}

func (h topKHeap) Len() int { return len(h.rows) }

func (h topKHeap) Less(i, j int) bool {
	// For a min-heap with DESC order, we want the smallest values at the top
	// so we can easily remove them when we find larger values
	cmp := h.compareRows(h.rows[i], h.rows[j])
	if h.orderBy[0].Direction == "DESC" {
		return cmp < 0 // Smallest at top for DESC
	}
	return cmp > 0 // Largest at top for ASC
}

func (h topKHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *topKHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(Row))
}

func (h *topKHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

// shouldReplace determines if a new row should replace the worst row in heap
func (h *topKHeap) shouldReplace(newRow Row) bool {
	if h.Len() == 0 {
		return true
	}
	
	// Compare with the top element (worst in our heap)
	cmp := h.compareRows(newRow, h.rows[0])
	
	if h.orderBy[0].Direction == "DESC" {
		// For DESC, we want larger values, so replace if new > top
		return cmp > 0
	} else {
		// For ASC, we want smaller values, so replace if new < top
		return cmp < 0
	}
}

// compareRows compares two rows based on all orderBy columns
func (h *topKHeap) compareRows(a, b Row) int {
	for _, orderCol := range h.orderBy {
		col := orderCol.Column
		aVal, aExists := a[col]
		bVal, bExists := b[col]
		
		// Handle NULL values
		if !aExists && !bExists {
			continue // Both NULL, check next column
		}
		if !aExists {
			return -1 // NULL is less than any value
		}
		if !bExists {
			return 1 // Any value is greater than NULL
		}
		
		// Compare non-NULL values
		cmp := h.reader.CompareValues(aVal, bVal)
		if cmp != 0 {
			return cmp
		}
		// If equal, continue to next order by column
	}
	
	return 0 // All columns are equal
}

// topKIndexedHeap is a heap for indexed rows (row index + partial data)
type topKIndexedHeap struct {
	rows    []indexedRow
	orderBy []OrderByColumn
	reader  *ParquetReader
	limit   int
}

func (h topKIndexedHeap) Len() int { return len(h.rows) }

func (h topKIndexedHeap) Less(i, j int) bool {
	cmp := h.compareIndexedRows(h.rows[i], h.rows[j])
	if h.orderBy[0].Direction == "DESC" {
		return cmp < 0 // Smallest at top for DESC
	}
	return cmp > 0 // Largest at top for ASC
}

func (h topKIndexedHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *topKIndexedHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(indexedRow))
}

func (h *topKIndexedHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

func (h *topKIndexedHeap) shouldReplace(newRow indexedRow) bool {
	if h.Len() == 0 {
		return true
	}
	
	cmp := h.compareIndexedRows(newRow, h.rows[0])
	
	if h.orderBy[0].Direction == "DESC" {
		return cmp > 0 // Replace if new > top
	} else {
		return cmp < 0 // Replace if new < top
	}
}

func (h *topKIndexedHeap) compareIndexedRows(a, b indexedRow) int {
	for _, orderCol := range h.orderBy {
		col := orderCol.Column
		aVal, aExists := a.data[col]
		bVal, bExists := b.data[col]
		
		if !aExists && !bExists {
			continue
		}
		if !aExists {
			return -1
		}
		if !bExists {
			return 1
		}
		
		cmp := h.reader.CompareValues(aVal, bVal)
		if cmp != 0 {
			return cmp
		}
	}
	
	return 0
}
