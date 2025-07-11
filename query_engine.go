package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
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

	var rows []Row
	if query.Limit > 0 {
		rows, err = reader.ReadWithLimit(query.Limit)
	} else {
		rows, err = reader.ReadAll()
	}
	
	if err != nil {
		return &QueryResult{
			Query: query.RawSQL,
			Error: err.Error(),
		}, nil
	}

	rows = reader.FilterRows(rows, query.Where)
	rows = reader.SelectColumns(rows, query.Columns)

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