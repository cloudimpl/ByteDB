package main

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type ParquetReader struct {
	filePath string
	schema   *parquet.Schema
	reader   *parquet.File
}

type Row map[string]interface{}

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
	Name        string  `parquet:"name"`
	Manager     string  `parquet:"manager"`
	Budget      float64 `parquet:"budget"`
	Location    string  `parquet:"location"`
	EmployeeCount int32 `parquet:"employee_count"`
}

func NewParquetReader(filePath string) (*ParquetReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	reader, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &ParquetReader{
		filePath: filePath,
		schema:   reader.Schema(),
		reader:   reader,
	}, nil
}

func (pr *ParquetReader) Close() error {
	return nil
}

func (pr *ParquetReader) GetSchema() *parquet.Schema {
	return pr.schema
}

func (pr *ParquetReader) GetColumnNames() []string {
	var names []string
	for _, field := range pr.schema.Fields() {
		names = append(names, field.Name())
	}
	return names
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
	return pr.readRows(0, nil)
}

func (pr *ParquetReader) ReadWithLimit(limit int) ([]Row, error) {
	return pr.readRows(limit, nil)
}

// ReadAllWithColumns reads all rows but only the specified columns for performance
func (pr *ParquetReader) ReadAllWithColumns(requiredColumns []string) ([]Row, error) {
	return pr.readRows(0, requiredColumns)
}

// ReadWithLimitAndColumns reads rows with limit and only specified columns
func (pr *ParquetReader) ReadWithLimitAndColumns(limit int, requiredColumns []string) ([]Row, error) {
	return pr.readRows(limit, requiredColumns)
}

func (pr *ParquetReader) readRows(limit int, requiredColumns []string) ([]Row, error) {
	// If no specific columns requested or empty list, read all columns
	if len(requiredColumns) == 0 {
		return pr.readAllColumns(limit)
	}
	
	// Read only the required columns for performance
	return pr.readSpecificColumns(limit, requiredColumns)
}

func (pr *ParquetReader) readAllColumns(limit int) ([]Row, error) {
	rows := make([]Row, 0)
	
	// Use generic reading with map interface
	reader := parquet.NewReader(pr.reader)
	defer reader.Close()
	
	count := 0
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
	}
	
	return rows, nil
}

// Generic reading approach - parquet-go handles the conversion automatically

func (pr *ParquetReader) readSpecificColumns(limit int, requiredColumns []string) ([]Row, error) {
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
	
	// For now, we implement column pruning at the Row level rather than Parquet level
	// This still provides memory benefits by not storing unnecessary columns in Row objects
	// A full optimization would read only specific columns from Parquet, but that requires
	// more complex row group iteration with parquet-go
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
	
	return rows, nil
}

// Schema detection methods removed - now supports any schema generically

func (pr *ParquetReader) FilterRows(rows []Row, conditions []WhereCondition) []Row {
	if len(conditions) == 0 {
		return rows
	}

	filtered := make([]Row, 0)
	for _, row := range rows {
		if pr.matchesConditions(row, conditions) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (pr *ParquetReader) matchesConditions(row Row, conditions []WhereCondition) bool {
	for _, condition := range conditions {
		if !pr.matchesCondition(row, condition) {
			return false
		}
	}
	return true
}

func (pr *ParquetReader) matchesCondition(row Row, condition WhereCondition) bool {
	value, exists := row[condition.Column]
	if !exists {
		return false
	}

	switch condition.Operator {
	case "=":
		return pr.CompareValues(value, condition.Value) == 0
	case "!=", "<>":
		return pr.CompareValues(value, condition.Value) != 0
	case "<":
		return pr.CompareValues(value, condition.Value) < 0
	case "<=":
		return pr.CompareValues(value, condition.Value) <= 0
	case ">":
		return pr.CompareValues(value, condition.Value) > 0
	case ">=":
		return pr.CompareValues(value, condition.Value) >= 0
	case "LIKE":
		return pr.matchesLike(value, condition.Value)
	case "IN":
		return pr.matchesIn(value, condition.ValueList)
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
	pattern = strings.ReplaceAll(pattern, "\x00PERCENT\x00", ".*")    // % matches any sequence of characters
	pattern = strings.ReplaceAll(pattern, "\x00UNDERSCORE\x00", ".")  // _ matches any single character
	pattern = strings.ReplaceAll(pattern, "\x00ESCAPED_PERCENT\x00", "%")     // \% becomes literal %
	pattern = strings.ReplaceAll(pattern, "\x00ESCAPED_UNDERSCORE\x00", "_")  // \_ becomes literal _
	
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
				}
			}
		}
		result[i] = newRow
	}
	return result
}