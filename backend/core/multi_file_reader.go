package core

import (
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