package vectorized

import (
	"bytedb/core"
	"fmt"
	"strings"
)

// ParquetVectorDataSource adapts ByteDB's ParquetReader to vectorized execution
type ParquetVectorDataSource struct {
	reader       *core.ParquetReader
	schema       *Schema
	tableName    string
	batchSize    int
	currentBatch int
	totalRows    int
	columnMap    map[string]int
}

// NewParquetVectorDataSource creates a new Parquet-based vectorized data source
func NewParquetVectorDataSource(reader *core.ParquetReader, tableName string, batchSize int) (*ParquetVectorDataSource, error) {
	if reader == nil {
		return nil, fmt.Errorf("parquet reader cannot be nil")
	}
	
	// Convert Parquet schema to vectorized schema
	schema, err := convertParquetSchema(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to convert parquet schema: %w", err)
	}
	
	// Create column name to index mapping
	columnMap := make(map[string]int)
	for i, field := range schema.Fields {
		columnMap[field.Name] = i
	}
	
	return &ParquetVectorDataSource{
		reader:       reader,
		schema:       schema,
		tableName:    tableName,
		batchSize:    batchSize,
		currentBatch: 0,
		totalRows:    reader.GetRowCount(),
		columnMap:    columnMap,
	}, nil
}

// GetNextBatch reads the next batch of data from the Parquet file
func (pvds *ParquetVectorDataSource) GetNextBatch() (*VectorBatch, error) {
	// Calculate the range for this batch
	startRow := pvds.currentBatch * pvds.batchSize
	endRow := startRow + pvds.batchSize
	
	if startRow >= pvds.totalRows {
		return nil, nil // End of data
	}
	
	if endRow > pvds.totalRows {
		endRow = pvds.totalRows
	}
	
	actualBatchSize := endRow - startRow
	
	// Read rows from Parquet reader
	rows, err := pvds.readRowsRange(startRow, actualBatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read rows [%d, %d): %w", startRow, endRow, err)
	}
	
	// Convert rows to vector batch
	batch, err := pvds.convertRowsToVectorBatch(rows, actualBatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows to vector batch: %w", err)
	}
	
	pvds.currentBatch++
	return batch, nil
}

// GetSchema returns the vectorized schema
func (pvds *ParquetVectorDataSource) GetSchema() *Schema {
	return pvds.schema
}

// Close closes the underlying Parquet reader
func (pvds *ParquetVectorDataSource) Close() error {
	if pvds.reader != nil {
		return pvds.reader.Close()
	}
	return nil
}

// HasNext returns true if there are more batches to read
func (pvds *ParquetVectorDataSource) HasNext() bool {
	return (pvds.currentBatch * pvds.batchSize) < pvds.totalRows
}

// GetEstimatedRowCount returns the total number of rows
func (pvds *ParquetVectorDataSource) GetEstimatedRowCount() int {
	return pvds.totalRows
}

// Reset resets the data source to the beginning
func (pvds *ParquetVectorDataSource) Reset() error {
	pvds.currentBatch = 0
	return nil
}

// readRowsRange reads a specific range of rows from the Parquet file
func (pvds *ParquetVectorDataSource) readRowsRange(startRow, count int) ([]core.Row, error) {
	// For now, read all rows and slice the range
	// In a production implementation, this would use range queries
	allRows, err := pvds.reader.ReadAll()
	if err != nil {
		return nil, err
	}
	
	if startRow >= len(allRows) {
		return []core.Row{}, nil
	}
	
	endRow := startRow + count
	if endRow > len(allRows) {
		endRow = len(allRows)
	}
	
	return allRows[startRow:endRow], nil
}

// convertRowsToVectorBatch converts core.Row slice to VectorBatch
func (pvds *ParquetVectorDataSource) convertRowsToVectorBatch(rows []core.Row, batchSize int) (*VectorBatch, error) {
	if len(rows) == 0 {
		return NewVectorBatch(pvds.schema, 0), nil
	}
	
	// Create vector batch
	batch := NewVectorBatch(pvds.schema, batchSize)
	
	// Populate vectors column by column
	for colIdx, field := range pvds.schema.Fields {
		vector := batch.Columns[colIdx]
		
		// Extract values for this column from all rows
		for rowIdx, row := range rows {
			value, exists := row[field.Name]
			if !exists || value == nil {
				vector.SetNull(rowIdx)
			} else {
				err := pvds.setVectorValue(vector, rowIdx, value, field.DataType)
				if err != nil {
					return nil, fmt.Errorf("failed to set value for column %s at row %d: %w", field.Name, rowIdx, err)
				}
			}
		}
		
		vector.Length = len(rows)
	}
	
	batch.RowCount = len(rows)
	return batch, nil
}

// setVectorValue sets a value in a vector at the specified index
func (pvds *ParquetVectorDataSource) setVectorValue(vector *Vector, index int, value interface{}, dataType DataType) error {
	switch dataType {
	case INT32:
		switch v := value.(type) {
		case int32:
			vector.SetInt32(index, v)
		case int:
			vector.SetInt32(index, int32(v))
		case int64:
			vector.SetInt32(index, int32(v))
		default:
			return fmt.Errorf("cannot convert %T to int32", value)
		}
		
	case INT64:
		switch v := value.(type) {
		case int64:
			vector.SetInt64(index, v)
		case int:
			vector.SetInt64(index, int64(v))
		case int32:
			vector.SetInt64(index, int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}
		
	case FLOAT64:
		switch v := value.(type) {
		case float64:
			vector.SetFloat64(index, v)
		case float32:
			vector.SetFloat64(index, float64(v))
		case int:
			vector.SetFloat64(index, float64(v))
		case int32:
			vector.SetFloat64(index, float64(v))
		case int64:
			vector.SetFloat64(index, float64(v))
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}
		
	case STRING:
		switch v := value.(type) {
		case string:
			vector.SetString(index, v)
		default:
			// Convert any type to string
			vector.SetString(index, fmt.Sprintf("%v", value))
		}
		
	case BOOLEAN:
		switch v := value.(type) {
		case bool:
			vector.SetBoolean(index, v)
		case int:
			vector.SetBoolean(index, v != 0)
		case string:
			vector.SetBoolean(index, strings.ToLower(v) == "true")
		default:
			return fmt.Errorf("cannot convert %T to boolean", value)
		}
		
	default:
		return fmt.Errorf("unsupported data type: %v", dataType)
	}
	
	return nil
}

// convertParquetSchema converts a Parquet schema to vectorized schema
func convertParquetSchema(reader *core.ParquetReader) (*Schema, error) {
	parquetSchema := reader.GetSchema()
	if parquetSchema == nil {
		return nil, fmt.Errorf("parquet schema is nil")
	}
	
	fields := make([]*Field, len(parquetSchema.Fields()))
	
	for i, parquetField := range parquetSchema.Fields() {
		dataType, err := convertParquetType(parquetField.Type().String())
		if err != nil {
			return nil, fmt.Errorf("failed to convert type for field %s: %w", parquetField.Name(), err)
		}
		
		fields[i] = &Field{
			Name:     parquetField.Name(),
			DataType: dataType,
			Nullable: true, // Assume nullable for now
		}
	}
	
	return &Schema{Fields: fields}, nil
}

// convertParquetType converts Parquet type strings to vectorized DataType
func convertParquetType(parquetType string) (DataType, error) {
	// Normalize the type string
	typeStr := strings.ToLower(strings.TrimSpace(parquetType))
	
	switch {
	case strings.Contains(typeStr, "int32") || strings.Contains(typeStr, "int_32"):
		return INT32, nil
	case strings.Contains(typeStr, "int64") || strings.Contains(typeStr, "int_64"):
		return INT64, nil
	case strings.Contains(typeStr, "float") || strings.Contains(typeStr, "double"):
		return FLOAT64, nil
	case strings.Contains(typeStr, "string") || strings.Contains(typeStr, "byte_array"):
		return STRING, nil
	case strings.Contains(typeStr, "bool"):
		return BOOLEAN, nil
	default:
		// Default to string for unknown types
		return STRING, nil
	}
}

// ParquetVectorDataSourceFactory creates ParquetVectorDataSource instances
type ParquetVectorDataSourceFactory struct {
	engine *core.QueryEngine
}

// NewParquetVectorDataSourceFactory creates a new factory
func NewParquetVectorDataSourceFactory(engine *core.QueryEngine) *ParquetVectorDataSourceFactory {
	return &ParquetVectorDataSourceFactory{
		engine: engine,
	}
}

// CreateDataSource creates a vectorized data source for a table
func (factory *ParquetVectorDataSourceFactory) CreateDataSource(tableName string, batchSize int) (VectorizedDataSource, error) {
	// Get the ParquetReader from the engine
	// Note: This assumes the engine has a method to get readers
	// In practice, you might need to adapt this based on the actual QueryEngine API
	reader, err := factory.getParquetReader(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get parquet reader for table %s: %w", tableName, err)
	}
	
	return NewParquetVectorDataSource(reader, tableName, batchSize)
}

// getParquetReader gets a ParquetReader for the specified table
func (factory *ParquetVectorDataSourceFactory) getParquetReader(tableName string) (*core.ParquetReader, error) {
	// This is a simplified approach - in practice, you'd need access to the engine's internal methods
	// or create a public method in QueryEngine to get readers
	
	// For now, create a new reader directly
	// This assumes the table files follow the pattern: dataPath/tableName.parquet
	filePath := fmt.Sprintf("%s.parquet", tableName) // Simplified path
	return core.NewParquetReader(filePath)
}

// VectorizedDataSourceRegistry manages vectorized data sources
type VectorizedDataSourceRegistry struct {
	dataSources map[string]VectorizedDataSource
	factory     *ParquetVectorDataSourceFactory
}

// NewVectorizedDataSourceRegistry creates a new registry
func NewVectorizedDataSourceRegistry(engine *core.QueryEngine) *VectorizedDataSourceRegistry {
	return &VectorizedDataSourceRegistry{
		dataSources: make(map[string]VectorizedDataSource),
		factory:     NewParquetVectorDataSourceFactory(engine),
	}
}

// RegisterDataSource registers a data source for a table
func (registry *VectorizedDataSourceRegistry) RegisterDataSource(tableName string, dataSource VectorizedDataSource) {
	registry.dataSources[tableName] = dataSource
}

// GetDataSource gets a data source for a table, creating it if necessary
func (registry *VectorizedDataSourceRegistry) GetDataSource(tableName string) (VectorizedDataSource, error) {
	// Check if already registered
	if dataSource, exists := registry.dataSources[tableName]; exists {
		return dataSource, nil
	}
	
	// Create new data source using factory
	dataSource, err := registry.factory.CreateDataSource(tableName, DefaultBatchSize)
	if err != nil {
		return nil, err
	}
	
	// Register for future use
	registry.dataSources[tableName] = dataSource
	return dataSource, nil
}

// GetAllDataSources returns all registered data sources
func (registry *VectorizedDataSourceRegistry) GetAllDataSources() map[string]VectorizedDataSource {
	result := make(map[string]VectorizedDataSource)
	for name, ds := range registry.dataSources {
		result[name] = ds
	}
	return result
}