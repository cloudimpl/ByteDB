package vectorized

import (
	"fmt"
	"strings"
)

// VectorizedOperator defines the interface for all vectorized operators
type VectorizedOperator interface {
	Execute(input *VectorBatch) (*VectorBatch, error)
	GetOutputSchema() *Schema
	GetEstimatedRowCount() int
}

// VectorizedScanOperator reads data in vectorized batches
type VectorizedScanOperator struct {
	Schema           *Schema
	BatchSize        int
	DataSource       VectorizedDataSource
	PushedFilters    []*VectorizedFilter
	ProjectedColumns []int
	EstimatedRows    int
}

// VectorizedDataSource represents a data source that can produce vectorized batches
type VectorizedDataSource interface {
	GetNextBatch() (*VectorBatch, error)
	GetSchema() *Schema
	Close() error
	HasNext() bool
	GetEstimatedRowCount() int
}

// VectorizedFilterOperator applies filters using vectorized execution
type VectorizedFilterOperator struct {
	Input       VectorizedOperator
	Filters     []*VectorizedFilter
	OutputSchema *Schema
}

// VectorizedFilter represents a filter condition optimized for vectorized execution
type VectorizedFilter struct {
	ColumnIndex  int
	Operator     FilterOperator
	Value        interface{}
	Values       []interface{} // For IN operations
	ValueColumn  int           // For column-to-column comparisons
	IsColumnComp bool          // True if comparing two columns
}

// FilterOperator defines the types of filter operations
type FilterOperator int

const (
	EQ FilterOperator = iota // Equal
	NE                       // Not Equal
	LT                       // Less Than
	LE                       // Less Than or Equal
	GT                       // Greater Than
	GE                       // Greater Than or Equal
	IN                       // In list
	LIKE                     // String pattern matching
	IS_NULL                  // Is null check
	IS_NOT_NULL              // Is not null check
)

// VectorizedProjectOperator selects specific columns
type VectorizedProjectOperator struct {
	Input            VectorizedOperator
	ProjectedColumns []int
	OutputSchema     *Schema
	Expressions      []*VectorizedExpression
}

// VectorizedExpression represents expressions that can be evaluated vectorized
type VectorizedExpression struct {
	Type         ExpressionType
	ColumnIndex  int                     // For column references
	Value        interface{}             // For constants
	Function     string                  // For function calls
	Arguments    []*VectorizedExpression // For complex expressions
	OutputColumn int                     // Index in output
}

// NewVectorizedProjectOperator creates a new project operator
func NewVectorizedProjectOperator(input VectorizedOperator, projectedColumns []int) *VectorizedProjectOperator {
	inputSchema := input.GetOutputSchema()
	
	// Build output schema with projected columns
	outputFields := make([]*Field, len(projectedColumns))
	for i, colIndex := range projectedColumns {
		if colIndex >= 0 && colIndex < len(inputSchema.Fields) {
			outputFields[i] = inputSchema.Fields[colIndex]
		}
	}
	
	return &VectorizedProjectOperator{
		Input:            input,
		ProjectedColumns: projectedColumns,
		OutputSchema:     &Schema{Fields: outputFields},
		Expressions:      nil, // For simple column projection
	}
}

// Execute executes the projection operation
func (op *VectorizedProjectOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	// Get input batch
	inputBatch, err := op.Input.Execute(input)
	if err != nil {
		return nil, err
	}
	
	if inputBatch == nil || inputBatch.RowCount == 0 {
		return nil, nil
	}
	
	// Create output batch with projected columns
	outputBatch := NewVectorBatch(op.OutputSchema, inputBatch.RowCount)
	outputBatch.RowCount = inputBatch.RowCount
	
	// Copy projected columns
	for i, colIndex := range op.ProjectedColumns {
		if colIndex >= 0 && colIndex < len(inputBatch.Columns) {
			sourceColumn := inputBatch.Columns[colIndex]
			targetColumn := outputBatch.Columns[i]
			
			// Copy all data from source to target
			err := copyProjectedVector(sourceColumn, targetColumn)
			if err != nil {
				return nil, fmt.Errorf("failed to copy column %d: %w", colIndex, err)
			}
		}
	}
	
	// Copy selection mask if present
	if inputBatch.SelectMask != nil {
		outputBatch.SelectMask = inputBatch.SelectMask
	}
	
	return outputBatch, nil
}

// GetOutputSchema returns the output schema
func (op *VectorizedProjectOperator) GetOutputSchema() *Schema {
	return op.OutputSchema
}

// GetEstimatedRowCount returns estimated row count
func (op *VectorizedProjectOperator) GetEstimatedRowCount() int {
	return op.Input.GetEstimatedRowCount()
}

// copyProjectedVector copies all data from source to target vector
func copyProjectedVector(source, target *Vector) error {
	target.DataType = source.DataType
	target.Length = source.Length
	
	// Copy null mask
	if source.Nulls != nil {
		target.Nulls = NewNullMask(source.Length)
		for i := 0; i < source.Length; i++ {
			if source.Nulls.IsNull(i) {
				target.Nulls.SetNull(i)
			}
		}
	}
	
	// Copy data based on type
	switch source.DataType {
	case INT32:
		sourceData := source.Data.([]int32)
		targetData := make([]int32, len(sourceData))
		copy(targetData, sourceData)
		target.Data = targetData
		
	case INT64:
		sourceData := source.Data.([]int64)
		targetData := make([]int64, len(sourceData))
		copy(targetData, sourceData)
		target.Data = targetData
		
	case FLOAT64:
		sourceData := source.Data.([]float64)
		targetData := make([]float64, len(sourceData))
		copy(targetData, sourceData)
		target.Data = targetData
		
	case STRING:
		sourceData := source.Data.([]string)
		targetData := make([]string, len(sourceData))
		copy(targetData, sourceData)
		target.Data = targetData
		
	case BOOLEAN:
		sourceData := source.Data.([]bool)
		targetData := make([]bool, len(sourceData))
		copy(targetData, sourceData)
		target.Data = targetData
		
	default:
		return fmt.Errorf("unsupported data type for copying: %v", source.DataType)
	}
	
	return nil
}

// ExpressionType defines the types of expressions
type ExpressionType int

const (
	COLUMN_REF ExpressionType = iota
	CONSTANT
	FUNCTION_CALL
	ARITHMETIC_OP
	COMPARISON_OP
	LOGICAL_OP
)

// VectorizedAggregateOperator performs aggregations using vectorized execution
type VectorizedAggregateOperator struct {
	Input        VectorizedOperator
	GroupByColumns []int
	Aggregates   []*VectorizedAggregate
	OutputSchema *Schema
	HashTable    *VectorizedHashTable
}

// VectorizedAggregate represents an aggregate function
type VectorizedAggregate struct {
	Function     AggregateFunction
	InputColumn  int
	OutputColumn int
	OutputType   DataType
}

// AggregateFunction defines the types of aggregate functions
type AggregateFunction int

const (
	COUNT AggregateFunction = iota
	SUM
	AVG
	MIN
	MAX
	FIRST
	LAST
)

// VectorizedHashTable for efficient grouping operations
type VectorizedHashTable struct {
	Buckets      []*HashBucket
	BucketCount  int
	Size         int
	LoadFactor   float64
	KeyColumns   []int
	ValueColumns []int
}

// HashBucket represents a bucket in the hash table
type HashBucket struct {
	Keys      *VectorBatch
	Values    *VectorBatch
	Next      *HashBucket
	KeyCount  int
}

// VectorizedJoinOperator performs joins using vectorized execution
type VectorizedJoinOperator struct {
	LeftInput    VectorizedOperator
	RightInput   VectorizedOperator
	JoinType     JoinType
	JoinColumns  []*JoinColumn
	OutputSchema *Schema
	HashTable    *VectorizedHashTable
}

// JoinType defines the types of joins
type JoinType int

const (
	INNER_JOIN_VEC JoinType = iota
	LEFT_JOIN_VEC
	RIGHT_JOIN_VEC
	FULL_OUTER_JOIN_VEC
)

// JoinColumn represents a join condition
type JoinColumn struct {
	LeftColumn  int
	RightColumn int
	Operator    FilterOperator
}

// Implementation of VectorizedScanOperator

func NewVectorizedScanOperator(dataSource VectorizedDataSource, batchSize int) *VectorizedScanOperator {
	return &VectorizedScanOperator{
		Schema:        dataSource.GetSchema(),
		BatchSize:     batchSize,
		DataSource:    dataSource,
		PushedFilters: make([]*VectorizedFilter, 0),
		EstimatedRows: dataSource.GetEstimatedRowCount(),
	}
}

func (op *VectorizedScanOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	// Get next batch from data source
	batch, err := op.DataSource.GetNextBatch()
	if err != nil {
		return nil, err
	}
	
	if batch == nil {
		return nil, nil // End of data
	}
	
	// Apply pushed-down filters
	if len(op.PushedFilters) > 0 {
		batch, err = op.applyFilters(batch)
		if err != nil {
			return nil, err
		}
	}
	
	// Apply column projection
	if len(op.ProjectedColumns) > 0 {
		batch, err = op.applyProjection(batch)
		if err != nil {
			return nil, err
		}
	}
	
	return batch, nil
}

func (op *VectorizedScanOperator) GetOutputSchema() *Schema {
	if len(op.ProjectedColumns) > 0 {
		fields := make([]*Field, len(op.ProjectedColumns))
		for i, colIndex := range op.ProjectedColumns {
			fields[i] = op.Schema.Fields[colIndex]
		}
		return &Schema{Fields: fields}
	}
	return op.Schema
}

func (op *VectorizedScanOperator) GetEstimatedRowCount() int {
	return op.EstimatedRows
}

func (op *VectorizedScanOperator) applyFilters(batch *VectorBatch) (*VectorBatch, error) {
	// Apply filters in sequence, updating selection vector
	selection := batch.SelectMask
	selection.Clear()
	
	// Initialize with all rows selected
	for i := 0; i < batch.RowCount; i++ {
		selection.Add(i)
	}
	
	for _, filter := range op.PushedFilters {
		err := op.applyFilter(batch, filter, selection)
		if err != nil {
			return nil, err
		}
	}
	
	return batch, nil
}

func (op *VectorizedScanOperator) applyFilter(batch *VectorBatch, filter *VectorizedFilter, selection *SelectionVector) error {
	if filter.ColumnIndex >= len(batch.Columns) {
		return fmt.Errorf("invalid column index: %d", filter.ColumnIndex)
	}
	
	column := batch.Columns[filter.ColumnIndex]
	newSelection := NewSelectionVector(selection.Capacity)
	
	// Apply filter based on operator type
	switch filter.Operator {
	case EQ:
		op.applyEqualFilter(column, filter.Value, selection, newSelection)
	case NE:
		op.applyNotEqualFilter(column, filter.Value, selection, newSelection)
	case LT:
		op.applyLessThanFilter(column, filter.Value, selection, newSelection)
	case LE:
		op.applyLessThanOrEqualFilter(column, filter.Value, selection, newSelection)
	case GT:
		op.applyGreaterThanFilter(column, filter.Value, selection, newSelection)
	case GE:
		op.applyGreaterThanOrEqualFilter(column, filter.Value, selection, newSelection)
	case IN:
		op.applyInFilter(column, filter.Values, selection, newSelection)
	case IS_NULL:
		op.applyIsNullFilter(column, selection, newSelection)
	case IS_NOT_NULL:
		op.applyIsNotNullFilter(column, selection, newSelection)
	default:
		return fmt.Errorf("unsupported filter operator: %v", filter.Operator)
	}
	
	// Replace selection vector
	*selection = *newSelection
	return nil
}

// Vectorized filter implementations using SIMD-friendly patterns

func (op *VectorizedScanOperator) applyEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case STRING:
		targetValue := value.(string)
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case BOOLEAN:
		targetValue := value.(bool)
		data := column.Data.([]bool)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyNotEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] != targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] != targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] != targetValue {
				output.Add(idx)
			}
		}
	case STRING:
		targetValue := value.(string)
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] != targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyLessThanFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] < targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] < targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] < targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyLessThanOrEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] <= targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] <= targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] <= targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyGreaterThanFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyGreaterThanOrEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyInFilter(column *Vector, values []interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetSet := make(map[int32]bool)
		for _, v := range values {
			targetSet[v.(int32)] = true
		}
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && targetSet[data[idx]] {
				output.Add(idx)
			}
		}
	case INT64:
		targetSet := make(map[int64]bool)
		for _, v := range values {
			targetSet[v.(int64)] = true
		}
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && targetSet[data[idx]] {
				output.Add(idx)
			}
		}
	case STRING:
		targetSet := make(map[string]bool)
		for _, v := range values {
			targetSet[v.(string)] = true
		}
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && targetSet[data[idx]] {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedScanOperator) applyIsNullFilter(column *Vector, input *SelectionVector, output *SelectionVector) {
	for i := 0; i < input.Length; i++ {
		idx := input.Indices[i]
		if column.IsNull(idx) {
			output.Add(idx)
		}
	}
}

func (op *VectorizedScanOperator) applyIsNotNullFilter(column *Vector, input *SelectionVector, output *SelectionVector) {
	for i := 0; i < input.Length; i++ {
		idx := input.Indices[i]
		if !column.IsNull(idx) {
			output.Add(idx)
		}
	}
}

func (op *VectorizedScanOperator) applyProjection(batch *VectorBatch) (*VectorBatch, error) {
	// Create new batch with projected columns
	projectedSchema := &Schema{
		Fields: make([]*Field, len(op.ProjectedColumns)),
	}
	
	for i, colIndex := range op.ProjectedColumns {
		projectedSchema.Fields[i] = batch.Schema.Fields[colIndex]
	}
	
	projectedBatch := NewVectorBatch(projectedSchema, batch.Capacity)
	projectedBatch.RowCount = batch.RowCount
	
	// Copy projected columns
	for i, colIndex := range op.ProjectedColumns {
		projectedBatch.Columns[i] = batch.Columns[colIndex]
	}
	
	// Copy selection mask
	projectedBatch.SelectMask = batch.SelectMask
	
	return projectedBatch, nil
}

// Implementation of VectorizedFilterOperator

func NewVectorizedFilterOperator(input VectorizedOperator, filters []*VectorizedFilter) *VectorizedFilterOperator {
	return &VectorizedFilterOperator{
		Input:        input,
		Filters:      filters,
		OutputSchema: input.GetOutputSchema(),
	}
}

func (op *VectorizedFilterOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	// Get input batch
	batch, err := op.Input.Execute(input)
	if err != nil {
		return nil, err
	}
	
	if batch == nil {
		return nil, nil
	}
	
	// Apply filters
	for _, filter := range op.Filters {
		err = op.applyFilter(batch, filter)
		if err != nil {
			return nil, err
		}
	}
	
	return batch, nil
}

func (op *VectorizedFilterOperator) GetOutputSchema() *Schema {
	return op.OutputSchema
}

func (op *VectorizedFilterOperator) GetEstimatedRowCount() int {
	// Estimate filtering reduces rows by 50% on average
	return int(float64(op.Input.GetEstimatedRowCount()) * 0.5)
}

func (op *VectorizedFilterOperator) applyFilter(batch *VectorBatch, filter *VectorizedFilter) error {
	selection := batch.SelectMask
	newSelection := NewSelectionVector(selection.Capacity)
	
	column := batch.Columns[filter.ColumnIndex]
	
	// Apply filter based on operator
	switch filter.Operator {
	case EQ:
		op.applyEqualFilter(column, filter.Value, selection, newSelection)
	case NE:
		op.applyNotEqualFilter(column, filter.Value, selection, newSelection)
	case LT:
		op.applyLessThanFilter(column, filter.Value, selection, newSelection)
	case LE:
		op.applyLessThanOrEqualFilter(column, filter.Value, selection, newSelection)
	case GT:
		op.applyGreaterThanFilter(column, filter.Value, selection, newSelection)
	case GE:
		op.applyGreaterThanOrEqualFilter(column, filter.Value, selection, newSelection)
	case IN:
		op.applyInFilter(column, filter.Values, selection, newSelection)
	case LIKE:
		op.applyLikeFilter(column, filter.Value, selection, newSelection)
	case IS_NULL:
		op.applyIsNullFilter(column, selection, newSelection)
	case IS_NOT_NULL:
		op.applyIsNotNullFilter(column, selection, newSelection)
	default:
		return fmt.Errorf("unsupported filter operator: %v", filter.Operator)
	}
	
	*selection = *newSelection
	return nil
}

// Filter implementations (reusing scan operator implementations)
func (op *VectorizedFilterOperator) applyEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	case STRING:
		targetValue := value.(string)
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] == targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedFilterOperator) applyNotEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	// Similar implementation to scan operator
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] != targetValue {
				output.Add(idx)
			}
		}
	// ... other types similar to scan operator
	}
}

func (op *VectorizedFilterOperator) applyLessThanFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	// Implementation similar to scan operator
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] < targetValue {
				output.Add(idx)
			}
		}
	// ... other numeric types
	}
}

func (op *VectorizedFilterOperator) applyLessThanOrEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	// Similar to less than but with <= comparison
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] <= targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedFilterOperator) applyGreaterThanFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	// Similar to less than but with > comparison
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	case STRING:
		targetValue := value.(string)
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] > targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedFilterOperator) applyGreaterThanOrEqualFilter(column *Vector, value interface{}, input *SelectionVector, output *SelectionVector) {
	// Similar to greater than but with >= comparison
	switch column.DataType {
	case INT32:
		targetValue := value.(int32)
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	case INT64:
		targetValue := value.(int64)
		data := column.Data.([]int64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	case FLOAT64:
		targetValue := value.(float64)
		data := column.Data.([]float64)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	case STRING:
		targetValue := value.(string)
		data := column.Data.([]string)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && data[idx] >= targetValue {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedFilterOperator) applyInFilter(column *Vector, values []interface{}, input *SelectionVector, output *SelectionVector) {
	// Implementation similar to scan operator
	switch column.DataType {
	case INT32:
		targetSet := make(map[int32]bool)
		for _, v := range values {
			targetSet[v.(int32)] = true
		}
		data := column.Data.([]int32)
		for i := 0; i < input.Length; i++ {
			idx := input.Indices[i]
			if !column.IsNull(idx) && targetSet[data[idx]] {
				output.Add(idx)
			}
		}
	}
}

func (op *VectorizedFilterOperator) applyLikeFilter(column *Vector, pattern interface{}, input *SelectionVector, output *SelectionVector) {
	if column.DataType != STRING {
		return
	}
	
	patternStr := pattern.(string)
	data := column.Data.([]string)
	
	for i := 0; i < input.Length; i++ {
		idx := input.Indices[i]
		if !column.IsNull(idx) && matchesLikePattern(data[idx], patternStr) {
			output.Add(idx)
		}
	}
}

func (op *VectorizedFilterOperator) applyIsNullFilter(column *Vector, input *SelectionVector, output *SelectionVector) {
	for i := 0; i < input.Length; i++ {
		idx := input.Indices[i]
		if column.IsNull(idx) {
			output.Add(idx)
		}
	}
}

func (op *VectorizedFilterOperator) applyIsNotNullFilter(column *Vector, input *SelectionVector, output *SelectionVector) {
	for i := 0; i < input.Length; i++ {
		idx := input.Indices[i]
		if !column.IsNull(idx) {
			output.Add(idx)
		}
	}
}

// Helper function for LIKE pattern matching
func matchesLikePattern(text, pattern string) bool {
	// Simple LIKE implementation with % and _ wildcards
	// % matches any sequence of characters
	// _ matches any single character
	
	// Convert SQL LIKE pattern to regex-like matching
	if strings.Contains(pattern, "%") {
		parts := strings.Split(pattern, "%")
		if len(parts) == 1 {
			return text == pattern
		}
		
		// Check if text starts with first part
		if !strings.HasPrefix(text, parts[0]) {
			return false
		}
		
		// Check if text ends with last part
		if parts[len(parts)-1] != "" && !strings.HasSuffix(text, parts[len(parts)-1]) {
			return false
		}
		
		// Check middle parts
		currentPos := len(parts[0])
		for i := 1; i < len(parts)-1; i++ {
			if parts[i] == "" {
				continue
			}
			pos := strings.Index(text[currentPos:], parts[i])
			if pos == -1 {
				return false
			}
			currentPos += pos + len(parts[i])
		}
		
		return true
	}
	
	// Handle _ wildcard (single character)
	if strings.Contains(pattern, "_") {
		if len(text) != len(pattern) {
			return false
		}
		for i := 0; i < len(pattern); i++ {
			if pattern[i] != '_' && pattern[i] != text[i] {
				return false
			}
		}
		return true
	}
	
	// Exact match
	return text == pattern
}

// Utility functions for creating filters
func NewEqualFilter(columnIndex int, value interface{}) *VectorizedFilter {
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    EQ,
		Value:       value,
	}
}

func NewInFilter(columnIndex int, values []interface{}) *VectorizedFilter {
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    IN,
		Values:      values,
	}
}

func NewRangeFilter(columnIndex int, min, max interface{}) []*VectorizedFilter {
	return []*VectorizedFilter{
		{
			ColumnIndex: columnIndex,
			Operator:    GE,
			Value:       min,
		},
		{
			ColumnIndex: columnIndex,
			Operator:    LE,
			Value:       max,
		},
	}
}

func NewLikeFilter(columnIndex int, pattern string) *VectorizedFilter {
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    LIKE,
		Value:       pattern,
	}
}

func NewNullFilter(columnIndex int, isNull bool) *VectorizedFilter {
	operator := IS_NULL
	if !isNull {
		operator = IS_NOT_NULL
	}
	return &VectorizedFilter{
		ColumnIndex: columnIndex,
		Operator:    operator,
	}
}