package vectorized

import (
	"fmt"
	"sort"
)

// Additional vectorized operators to complete the operator set

// VectorizedSortOperator implements sorting
type VectorizedSortOperator struct {
	Input      VectorizedOperator
	SortKeys   []*SortKey
	sortBuffer []*VectorBatch
}

// VectorizedLimitOperator implements row limiting
type VectorizedLimitOperator struct {
	Input       VectorizedOperator
	LimitCount  int
	currentRows int
}

// SortKey represents a sort key with column and direction
type SortKey struct {
	ColumnIndex int
	Ascending   bool
}

// Additional filter constructors and utilities

// NewVectorizedSortOperator creates a new sort operator
func NewVectorizedSortOperator(input VectorizedOperator, sortKeys []*SortKey) *VectorizedSortOperator {
	return &VectorizedSortOperator{
		Input:      input,
		SortKeys:   sortKeys,
		sortBuffer: make([]*VectorBatch, 0),
	}
}

// Execute executes the sort operation
func (op *VectorizedSortOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	// Collect all input batches for sorting
	for {
		batch, err := op.Input.Execute(input)
		if err != nil {
			return nil, err
		}
		
		if batch == nil {
			break // End of input
		}
		
		op.sortBuffer = append(op.sortBuffer, batch)
	}
	
	if len(op.sortBuffer) == 0 {
		return nil, nil
	}
	
	// Merge all batches into a single batch for sorting
	mergedBatch, err := op.mergeBatches(op.sortBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to merge batches for sorting: %w", err)
	}
	
	// Sort the merged batch
	err = op.sortBatch(mergedBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to sort batch: %w", err)
	}
	
	return mergedBatch, nil
}

// GetOutputSchema returns the output schema
func (op *VectorizedSortOperator) GetOutputSchema() *Schema {
	return op.Input.GetOutputSchema()
}

// GetEstimatedRowCount returns estimated row count
func (op *VectorizedSortOperator) GetEstimatedRowCount() int {
	return op.Input.GetEstimatedRowCount()
}

// mergeBatches merges multiple vector batches into one
func (op *VectorizedSortOperator) mergeBatches(batches []*VectorBatch) (*VectorBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}
	
	if len(batches) == 1 {
		return batches[0], nil
	}
	
	// Calculate total row count
	totalRows := 0
	for _, batch := range batches {
		totalRows += batch.RowCount
	}
	
	// Create merged batch
	schema := batches[0].Schema
	mergedBatch := NewVectorBatch(schema, totalRows)
	
	// Merge each column
	for colIdx := range schema.Fields {
		targetVector := mergedBatch.Columns[colIdx]
		targetIndex := 0
		
		for _, batch := range batches {
			sourceVector := batch.Columns[colIdx]
			
			// Copy values from source to target
			for i := 0; i < sourceVector.Length; i++ {
				if sourceVector.IsNull(i) {
					targetVector.SetNull(targetIndex)
				} else {
					err := op.copyVectorValue(sourceVector, i, targetVector, targetIndex)
					if err != nil {
						return nil, fmt.Errorf("failed to copy value: %w", err)
					}
				}
				targetIndex++
			}
		}
		
		targetVector.Length = totalRows
	}
	
	mergedBatch.RowCount = totalRows
	return mergedBatch, nil
}

// copyVectorValue copies a value from one vector to another
func (op *VectorizedSortOperator) copyVectorValue(source *Vector, sourceIndex int, target *Vector, targetIndex int) error {
	switch source.DataType {
	case INT32:
		if value, ok := source.GetInt32(sourceIndex); ok {
			target.SetInt32(targetIndex, value)
		}
	case INT64:
		if value, ok := source.GetInt64(sourceIndex); ok {
			target.SetInt64(targetIndex, value)
		}
	case FLOAT64:
		if value, ok := source.GetFloat64(sourceIndex); ok {
			target.SetFloat64(targetIndex, value)
		}
	case STRING:
		if value, ok := source.GetString(sourceIndex); ok {
			target.SetString(targetIndex, value)
		}
	case BOOLEAN:
		if value, ok := source.GetBoolean(sourceIndex); ok {
			target.SetBoolean(targetIndex, value)
		}
	default:
		return fmt.Errorf("unsupported data type for copying: %v", source.DataType)
	}
	
	return nil
}

// sortBatch sorts a vector batch according to sort keys
func (op *VectorizedSortOperator) sortBatch(batch *VectorBatch) error {
	if batch.RowCount <= 1 {
		return nil // Nothing to sort
	}
	
	// Create row indices for indirect sorting
	indices := make([]int, batch.RowCount)
	for i := range indices {
		indices[i] = i
	}
	
	// Sort indices based on sort keys
	sort.Slice(indices, func(i, j int) bool {
		return op.compareRows(batch, indices[i], indices[j])
	})
	
	// Reorder all columns based on sorted indices
	return op.reorderColumns(batch, indices)
}

// compareRows compares two rows according to sort keys
func (op *VectorizedSortOperator) compareRows(batch *VectorBatch, row1, row2 int) bool {
	for _, sortKey := range op.SortKeys {
		if sortKey.ColumnIndex >= len(batch.Columns) {
			continue
		}
		
		column := batch.Columns[sortKey.ColumnIndex]
		
		// Handle null values
		null1 := column.IsNull(row1)
		null2 := column.IsNull(row2)
		
		if null1 && null2 {
			continue // Both null, equal
		}
		if null1 {
			return !sortKey.Ascending // Nulls last for ASC, first for DESC
		}
		if null2 {
			return sortKey.Ascending // Nulls last for ASC, first for DESC
		}
		
		// Compare values
		cmp := op.compareValues(column, row1, row2)
		if cmp != 0 {
			if sortKey.Ascending {
				return cmp < 0
			} else {
				return cmp > 0
			}
		}
	}
	
	return false // Equal
}

// compareValues compares two values in a vector
func (op *VectorizedSortOperator) compareValues(vector *Vector, index1, index2 int) int {
	switch vector.DataType {
	case INT32:
		val1, _ := vector.GetInt32(index1)
		val2, _ := vector.GetInt32(index2)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
		
	case INT64:
		val1, _ := vector.GetInt64(index1)
		val2, _ := vector.GetInt64(index2)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
		
	case FLOAT64:
		val1, _ := vector.GetFloat64(index1)
		val2, _ := vector.GetFloat64(index2)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
		
	case STRING:
		val1, _ := vector.GetString(index1)
		val2, _ := vector.GetString(index2)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
		
	case BOOLEAN:
		val1, _ := vector.GetBoolean(index1)
		val2, _ := vector.GetBoolean(index2)
		if !val1 && val2 {
			return -1
		} else if val1 && !val2 {
			return 1
		}
		return 0
		
	default:
		return 0
	}
}

// reorderColumns reorders all columns in a batch according to the given indices
func (op *VectorizedSortOperator) reorderColumns(batch *VectorBatch, indices []int) error {
	for _, column := range batch.Columns {
		err := op.reorderVector(column, indices)
		if err != nil {
			return err
		}
	}
	return nil
}

// reorderVector reorders a single vector according to the given indices
func (op *VectorizedSortOperator) reorderVector(vector *Vector, indices []int) error {
	// Create temporary storage for reordered data
	switch vector.DataType {
	case INT32:
		data := vector.Data.([]int32)
		temp := make([]int32, len(data))
		for i, idx := range indices {
			temp[i] = data[idx]
		}
		copy(data, temp)
		
	case INT64:
		data := vector.Data.([]int64)
		temp := make([]int64, len(data))
		for i, idx := range indices {
			temp[i] = data[idx]
		}
		copy(data, temp)
		
	case FLOAT64:
		data := vector.Data.([]float64)
		temp := make([]float64, len(data))
		for i, idx := range indices {
			temp[i] = data[idx]
		}
		copy(data, temp)
		
	case STRING:
		data := vector.Data.([]string)
		temp := make([]string, len(data))
		for i, idx := range indices {
			temp[i] = data[idx]
		}
		copy(data, temp)
		
	case BOOLEAN:
		data := vector.Data.([]bool)
		temp := make([]bool, len(data))
		for i, idx := range indices {
			temp[i] = data[idx]
		}
		copy(data, temp)
		
	default:
		return fmt.Errorf("unsupported data type for reordering: %v", vector.DataType)
	}
	
	// Reorder null mask
	if vector.Nulls != nil {
		tempNulls := NewNullMask(vector.Length)
		for i, idx := range indices {
			if vector.Nulls.IsNull(idx) {
				tempNulls.SetNull(i)
			}
		}
		vector.Nulls = tempNulls
	}
	
	return nil
}

// NewVectorizedLimitOperator creates a new limit operator
func NewVectorizedLimitOperator(input VectorizedOperator, limitCount int) *VectorizedLimitOperator {
	return &VectorizedLimitOperator{
		Input:       input,
		LimitCount:  limitCount,
		currentRows: 0,
	}
}

// Execute executes the limit operation
func (op *VectorizedLimitOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	if op.currentRows >= op.LimitCount {
		return nil, nil // Already reached limit
	}
	
	// Get input batch
	inputBatch, err := op.Input.Execute(input)
	if err != nil {
		return nil, err
	}
	
	if inputBatch == nil || inputBatch.RowCount == 0 {
		return nil, nil
	}
	
	// Calculate how many rows to include
	remainingRows := op.LimitCount - op.currentRows
	rowsToInclude := inputBatch.RowCount
	
	if rowsToInclude > remainingRows {
		rowsToInclude = remainingRows
	}
	
	// If we need all rows, return as-is
	if rowsToInclude == inputBatch.RowCount {
		op.currentRows += rowsToInclude
		return inputBatch, nil
	}
	
	// Create limited batch
	limitedBatch := NewVectorBatch(inputBatch.Schema, rowsToInclude)
	limitedBatch.RowCount = rowsToInclude
	
	// Copy limited rows for each column
	for i, inputColumn := range inputBatch.Columns {
		outputColumn := limitedBatch.Columns[i]
		
		for row := 0; row < rowsToInclude; row++ {
			if inputColumn.IsNull(row) {
				outputColumn.SetNull(row)
			} else {
				err := copyVectorValueForLimit(inputColumn, row, outputColumn, row)
				if err != nil {
					return nil, fmt.Errorf("failed to copy limited value: %w", err)
				}
			}
		}
		
		outputColumn.Length = rowsToInclude
	}
	
	// Copy selection mask if present
	if inputBatch.SelectMask != nil && inputBatch.SelectMask.Length > 0 {
		limitedBatch.SelectMask = NewSelectionVector(rowsToInclude)
		copyCount := minInt(inputBatch.SelectMask.Length, rowsToInclude)
		for i := 0; i < copyCount; i++ {
			if inputBatch.SelectMask.Indices[i] < rowsToInclude {
				limitedBatch.SelectMask.Add(inputBatch.SelectMask.Indices[i])
			}
		}
	}
	
	op.currentRows += rowsToInclude
	return limitedBatch, nil
}

// GetOutputSchema returns the output schema
func (op *VectorizedLimitOperator) GetOutputSchema() *Schema {
	return op.Input.GetOutputSchema()
}

// GetEstimatedRowCount returns estimated row count (limited)
func (op *VectorizedLimitOperator) GetEstimatedRowCount() int {
	inputRows := op.Input.GetEstimatedRowCount()
	if inputRows < op.LimitCount {
		return inputRows
	}
	return op.LimitCount
}

// NewVectorEqualFilter creates an equality filter
func NewVectorEqualFilter(columnIndex int, value interface{}) *VectorizedFilter {
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    EQ,
		Value:       value,
	}
}

// NewVectorRangeFilter creates a range filter (greater than)
func NewVectorRangeFilter(columnIndex int, operator FilterOperator, value interface{}) *VectorizedFilter {
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    operator,
		Value:       value,
	}
}

// Helper function for additional operators
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// copyVectorValueForLimit copies a value from one vector to another for limit operations
func copyVectorValueForLimit(source *Vector, sourceIndex int, target *Vector, targetIndex int) error {
	switch source.DataType {
	case INT32:
		if value, ok := source.GetInt32(sourceIndex); ok {
			target.SetInt32(targetIndex, value)
		}
	case INT64:
		if value, ok := source.GetInt64(sourceIndex); ok {
			target.SetInt64(targetIndex, value)
		}
	case FLOAT64:
		if value, ok := source.GetFloat64(sourceIndex); ok {
			target.SetFloat64(targetIndex, value)
		}
	case STRING:
		if value, ok := source.GetString(sourceIndex); ok {
			target.SetString(targetIndex, value)
		}
	case BOOLEAN:
		if value, ok := source.GetBoolean(sourceIndex); ok {
			target.SetBoolean(targetIndex, value)
		}
	default:
		return fmt.Errorf("unsupported data type for copying: %v", source.DataType)
	}
	
	return nil
}