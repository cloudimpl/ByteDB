package core

import (
	"container/heap"
	"fmt"
	"strings"
	
	"bytedb/common"
)

// MultiFileParquetReader handles reading from multiple parquet files as a single table
type MultiFileParquetReader struct {
	filePaths []string
	readers   []*ParquetReader
	schema    []common.Field
}

// NewMultiFileParquetReader creates a new reader for multiple parquet files
func NewMultiFileParquetReader(filePaths []string) (*ParquetReader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no file paths provided")
	}
	
	// Create readers for all files
	readers := make([]*ParquetReader, 0, len(filePaths))
	var schema []common.Field
	
	for i, path := range filePaths {
		reader, err := NewParquetReader(path)
		if err != nil {
			// Close any opened readers
			for _, r := range readers {
				r.Close()
			}
			return nil, fmt.Errorf("failed to open file %s: %w", path, err)
		}
		
		// Verify schema compatibility
		if i == 0 {
			schema = reader.GetSchema()
		} else {
			// Simple schema comparison - just check column names and types match
			currentSchema := reader.GetSchema()
			if !schemasCompatible(schema, currentSchema) {
				// Close all readers
				for _, r := range readers {
					r.Close()
				}
				reader.Close()
				return nil, fmt.Errorf("schema mismatch in file %s", path)
			}
		}
		
		readers = append(readers, reader)
	}
	
	// Create a wrapper that delegates to multiple readers
	multiReader := &MultiFileParquetReader{
		filePaths: filePaths,
		readers:   readers,
		schema:    schema,
	}
	
	// Return a ParquetReader wrapper
	return &ParquetReader{
		filePath: strings.Join(filePaths, ";"), // Use semicolon-separated paths
		reader:   nil, // We don't use the single file reader
		multiFileReader: multiReader,
	}, nil
}

// schemasCompatible checks if two schemas are compatible
func schemasCompatible(schema1, schema2 []common.Field) bool {
	if len(schema1) != len(schema2) {
		return false
	}
	
	for i, field1 := range schema1 {
		field2 := schema2[i]
		if field1.Name != field2.Name || field1.Type != field2.Type {
			return false
		}
	}
	
	return true
}

// Extension methods for MultiFileParquetReader
// These would need to be integrated into the ParquetReader struct
// For now, we'll modify the ParquetReader to support multi-file operations

// ReadAllMultiFile reads all rows from multiple files
func (mfr *MultiFileParquetReader) ReadAll() ([]Row, error) {
	allRows := make([]Row, 0)
	
	for _, reader := range mfr.readers {
		rows, err := reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("failed to read from file: %w", err)
		}
		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// ReadAllWithColumnsMultiFile reads specific columns from multiple files
func (mfr *MultiFileParquetReader) ReadAllWithColumns(columns []string) ([]Row, error) {
	allRows := make([]Row, 0)
	
	for _, reader := range mfr.readers {
		rows, err := reader.ReadAllWithColumns(columns)
		if err != nil {
			return nil, fmt.Errorf("failed to read columns from file: %w", err)
		}
		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// CloseMultiFile closes all readers
func (mfr *MultiFileParquetReader) Close() error {
	var firstErr error
	for _, reader := range mfr.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetSchema returns the schema (from the first file)
func (mfr *MultiFileParquetReader) GetSchema() []common.Field {
	return mfr.schema
}

// GetColumnNamesMultiFile returns column names
func (mfr *MultiFileParquetReader) GetColumnNames() []string {
	names := make([]string, len(mfr.schema))
	for i, field := range mfr.schema {
		names[i] = field.Name
	}
	return names
}

// ReadTopK reads top K rows from multiple files using a heap-based approach
func (mfr *MultiFileParquetReader) ReadTopK(limit int, orderBy []OrderByColumn, whereConditions []WhereCondition, engine SubqueryExecutor) ([]Row, error) {
	// For multi-file reader, we need to merge results from all files
	// We'll use a simple approach: get top K from each file, then merge
	
	allTopRows := make([]Row, 0, limit*len(mfr.readers))
	
	// Get top K from each file
	for _, reader := range mfr.readers {
		topRows, err := reader.ReadTopK(limit, orderBy, whereConditions, engine)
		if err != nil {
			return nil, err
		}
		allTopRows = append(allTopRows, topRows...)
	}
	
	// If we have fewer rows than limit, just sort and return
	if len(allTopRows) <= limit {
		// Sort based on orderBy
		if len(mfr.readers) > 0 {
			SortRowsWithOrderBy(allTopRows, orderBy, mfr.readers[0])
		}
		return allTopRows, nil
	}
	
	// Otherwise, we need to find the top K from the merged results
	// Create a new heap for final top K
	topK := &topKHeap{
		rows:     make([]Row, 0, limit),
		orderBy:  orderBy,
		reader:   mfr.readers[0], // Use first reader for comparison
		limit:    limit,
	}
	heap.Init(topK)
	
	// Add all rows to heap
	for _, row := range allTopRows {
		if topK.Len() < limit {
			heap.Push(topK, row)
		} else if topK.shouldReplace(row) {
			heap.Pop(topK)
			heap.Push(topK, row)
		}
	}
	
	// Extract final results
	result := make([]Row, topK.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(topK).(Row)
	}
	
	return result, nil
}