package vectorized

import (
	"fmt"
	"runtime"
	"time"
)

// SIMDConfig holds configuration for SIMD operations
type SIMDConfig struct {
	EnableSIMD     bool
	ForceScalar    bool
	MinVectorSize  int
	PreferredAlign int
}

// DefaultSIMDConfig returns sensible defaults for SIMD operations
func DefaultSIMDConfig() *SIMDConfig {
	return &SIMDConfig{
		EnableSIMD:     true,
		ForceScalar:    false,
		MinVectorSize:  64,    // Only use SIMD for vectors >= 64 elements
		PreferredAlign: 32,    // AVX2 prefers 32-byte alignment
	}
}

// SIMDVectorizedFilterOperator replaces the standard filter with SIMD-optimized version
type SIMDVectorizedFilterOperator struct {
	Input       VectorizedOperator
	Filters     []*VectorizedFilter
	OutputSchema *Schema
	simdOps     *SIMDOperations
	config      *SIMDConfig
}

// NewSIMDVectorizedFilterOperator creates a SIMD-optimized filter operator
func NewSIMDVectorizedFilterOperator(input VectorizedOperator, filters []*VectorizedFilter, config *SIMDConfig) *SIMDVectorizedFilterOperator {
	if config == nil {
		config = DefaultSIMDConfig()
	}
	
	return &SIMDVectorizedFilterOperator{
		Input:       input,
		Filters:     filters,
		OutputSchema: input.GetOutputSchema(),
		simdOps:     NewSIMDOperations(),
		config:      config,
	}
}

// Execute performs SIMD-optimized filtering
func (op *SIMDVectorizedFilterOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	inputBatch, err := op.Input.Execute(input)
	if err != nil {
		return nil, err
	}

	if inputBatch == nil || inputBatch.RowCount == 0 {
		return nil, nil
	}

	// Apply each filter using SIMD when possible
	currentSelection := &SelectionVector{
		Indices:  make([]int, inputBatch.RowCount),
		Length:   inputBatch.RowCount,
		Capacity: inputBatch.RowCount,
	}
	
	// Initialize with all rows selected
	for i := 0; i < inputBatch.RowCount; i++ {
		currentSelection.Indices[i] = i
	}

	for _, filter := range op.Filters {
		if filter.ColumnIndex >= len(inputBatch.Columns) {
			continue
		}
		
		column := inputBatch.Columns[filter.ColumnIndex]
		newSelection := op.applySIMDFilter(column, filter, currentSelection)
		currentSelection = newSelection
		
		if currentSelection.Length == 0 {
			break // No rows pass the filter
		}
	}

	// Create output batch with filtered results
	outputBatch := NewVectorBatch(op.OutputSchema, currentSelection.Length)
	outputBatch.RowCount = currentSelection.Length
	outputBatch.SelectMask = currentSelection

	// Copy filtered data
	for colIdx, inputColumn := range inputBatch.Columns {
		outputColumn := outputBatch.Columns[colIdx]
		err := op.copyFilteredColumn(inputColumn, outputColumn, currentSelection)
		if err != nil {
			return nil, err
		}
	}

	return outputBatch, nil
}

// applySIMDFilter applies a single filter using SIMD operations when beneficial
func (op *SIMDVectorizedFilterOperator) applySIMDFilter(column *Vector, filter *VectorizedFilter, input *SelectionVector) *SelectionVector {
	// Check if we should use SIMD
	if !op.shouldUseSIMD(column, input.Length) {
		return op.applyScalarFilter(column, filter, input)
	}

	// For now, only optimize FLOAT64 GT/LT operations with SIMD
	if column.DataType == FLOAT64 && op.isSIMDOptimizable(filter.Operator) {
		return op.applySIMDFloat64Filter(column, filter, input)
	}

	// Fallback to scalar
	return op.applyScalarFilter(column, filter, input)
}

// applySIMDFloat64Filter performs SIMD-optimized filtering on FLOAT64 data
func (op *SIMDVectorizedFilterOperator) applySIMDFloat64Filter(column *Vector, filter *VectorizedFilter, input *SelectionVector) *SelectionVector {
	data := column.Data.([]float64)
	threshold := filter.Value.(float64)
	
	// Create temporary arrays for SIMD operations
	tempData := make([]float64, input.Length)
	tempSelection := make([]int, input.Length)
	
	// Copy selected data to contiguous array for SIMD efficiency
	for i := 0; i < input.Length; i++ {
		tempData[i] = data[input.Indices[i]]
	}
	
	// Perform SIMD filtering
	var resultCount int
	switch filter.Operator {
	case GT:
		resultCount = SIMDFilterGreaterThan(tempData, threshold, tempSelection)
	case LT:
		// Negate threshold and data, then use GT
		negThreshold := -threshold
		for i := range tempData {
			tempData[i] = -tempData[i]
		}
		resultCount = SIMDFilterGreaterThan(tempData, negThreshold, tempSelection)
	default:
		// Fallback for unsupported operators
		return op.applyScalarFilter(column, filter, input)
	}
	
	// Map back to original indices
	result := NewSelectionVector(resultCount)
	for i := 0; i < resultCount; i++ {
		result.Indices[i] = input.Indices[tempSelection[i]]
	}
	result.Length = resultCount
	
	return result
}

// shouldUseSIMD determines if SIMD should be used based on configuration and data size
func (op *SIMDVectorizedFilterOperator) shouldUseSIMD(column *Vector, length int) bool {
	if !op.config.EnableSIMD || op.config.ForceScalar {
		return false
	}
	
	if length < op.config.MinVectorSize {
		return false
	}
	
	// Only enable SIMD for supported data types
	return column.DataType == FLOAT64 || column.DataType == INT64
}

// isSIMDOptimizable checks if the filter operator can be SIMD-optimized
func (op *SIMDVectorizedFilterOperator) isSIMDOptimizable(operator FilterOperator) bool {
	switch operator {
	case GT, LT, GE, LE, EQ, NE:
		return true
	default:
		return false
	}
}

// applyScalarFilter fallback to regular scalar filtering
func (op *SIMDVectorizedFilterOperator) applyScalarFilter(column *Vector, filter *VectorizedFilter, input *SelectionVector) *SelectionVector {
	result := NewSelectionVector(input.Length)
	resultCount := 0
	
	for i := 0; i < input.Length; i++ {
		rowIndex := input.Indices[i]
		if op.evaluateFilter(column, filter, rowIndex) {
			result.Indices[resultCount] = rowIndex
			resultCount++
		}
	}
	
	result.Length = resultCount
	return result
}

// evaluateFilter evaluates a single filter condition
func (op *SIMDVectorizedFilterOperator) evaluateFilter(column *Vector, filter *VectorizedFilter, rowIndex int) bool {
	if column.IsNull(rowIndex) {
		return false
	}
	
	switch column.DataType {
	case FLOAT64:
		value, _ := column.GetFloat64(rowIndex)
		threshold := filter.Value.(float64)
		return op.compareFloat64(value, threshold, filter.Operator)
	case INT64:
		value, _ := column.GetInt64(rowIndex)
		threshold := filter.Value.(int64)
		return op.compareInt64(value, threshold, filter.Operator)
	case INT32:
		value, _ := column.GetInt32(rowIndex)
		threshold := filter.Value.(int32)
		return op.compareInt32(value, threshold, filter.Operator)
	case STRING:
		value, _ := column.GetString(rowIndex)
		threshold := filter.Value.(string)
		return op.compareString(value, threshold, filter.Operator)
	default:
		return false
	}
}

// Comparison functions for different data types
func (op *SIMDVectorizedFilterOperator) compareFloat64(a, b float64, operator FilterOperator) bool {
	switch operator {
	case EQ: return a == b
	case NE: return a != b
	case LT: return a < b
	case LE: return a <= b
	case GT: return a > b
	case GE: return a >= b
	default: return false
	}
}

func (op *SIMDVectorizedFilterOperator) compareInt64(a, b int64, operator FilterOperator) bool {
	switch operator {
	case EQ: return a == b
	case NE: return a != b
	case LT: return a < b
	case LE: return a <= b
	case GT: return a > b
	case GE: return a >= b
	default: return false
	}
}

func (op *SIMDVectorizedFilterOperator) compareInt32(a, b int32, operator FilterOperator) bool {
	switch operator {
	case EQ: return a == b
	case NE: return a != b
	case LT: return a < b
	case LE: return a <= b
	case GT: return a > b
	case GE: return a >= b
	default: return false
	}
}

func (op *SIMDVectorizedFilterOperator) compareString(a, b string, operator FilterOperator) bool {
	switch operator {
	case EQ: return a == b
	case NE: return a != b
	case LT: return a < b
	case LE: return a <= b
	case GT: return a > b
	case GE: return a >= b
	default: return false
	}
}

// copyFilteredColumn copies filtered data to output column
func (op *SIMDVectorizedFilterOperator) copyFilteredColumn(input, output *Vector, selection *SelectionVector) error {
	output.DataType = input.DataType
	output.Length = selection.Length
	
	switch input.DataType {
	case FLOAT64:
		inputData := input.Data.([]float64)
		outputData := make([]float64, selection.Length)
		for i := 0; i < selection.Length; i++ {
			outputData[i] = inputData[selection.Indices[i]]
		}
		output.Data = outputData
		
	case INT64:
		inputData := input.Data.([]int64)
		outputData := make([]int64, selection.Length)
		for i := 0; i < selection.Length; i++ {
			outputData[i] = inputData[selection.Indices[i]]
		}
		output.Data = outputData
		
	case INT32:
		inputData := input.Data.([]int32)
		outputData := make([]int32, selection.Length)
		for i := 0; i < selection.Length; i++ {
			outputData[i] = inputData[selection.Indices[i]]
		}
		output.Data = outputData
		
	case STRING:
		inputData := input.Data.([]string)
		outputData := make([]string, selection.Length)
		for i := 0; i < selection.Length; i++ {
			outputData[i] = inputData[selection.Indices[i]]
		}
		output.Data = outputData
		
	default:
		return fmt.Errorf("unsupported data type: %v", input.DataType)
	}
	
	// Copy null mask if present
	if input.Nulls != nil {
		output.Nulls = NewNullMask(selection.Length)
		for i := 0; i < selection.Length; i++ {
			if input.Nulls.IsNull(selection.Indices[i]) {
				output.Nulls.SetNull(i)
			}
		}
	}
	
	return nil
}

func (op *SIMDVectorizedFilterOperator) GetOutputSchema() *Schema {
	return op.OutputSchema
}

func (op *SIMDVectorizedFilterOperator) GetEstimatedRowCount() int {
	// Estimate 50% selectivity for filters
	return op.Input.GetEstimatedRowCount() / 2
}

// SIMD-optimized aggregation operator
type SIMDAggregateOperator struct {
	*VectorizedAggregateOperator
	simdOps *SIMDOperations
	config  *SIMDConfig
}

// NewSIMDAggregateOperator creates a SIMD-optimized aggregate operator
func NewSIMDAggregateOperator(input VectorizedOperator, groupByColumns []int, aggregates []*VectorizedAggregate, config *SIMDConfig) *SIMDAggregateOperator {
	baseOp := NewVectorizedAggregateOperator(input, groupByColumns, aggregates)
	
	if config == nil {
		config = DefaultSIMDConfig()
	}
	
	return &SIMDAggregateOperator{
		VectorizedAggregateOperator: baseOp,
		simdOps:                     NewSIMDOperations(),
		config:                      config,
	}
}

// PerformSIMDSum computes sum using SIMD when beneficial
func (op *SIMDAggregateOperator) PerformSIMDSum(column *Vector) (float64, error) {
	if column.DataType != FLOAT64 {
		return 0, fmt.Errorf("SIMD sum only supports FLOAT64")
	}
	
	if column.Length < op.config.MinVectorSize {
		// Use scalar sum for small vectors
		return op.scalarSum(column)
	}
	
	return op.simdOps.VectorizedSum(column)
}

// scalarSum fallback sum implementation
func (op *SIMDAggregateOperator) scalarSum(column *Vector) (float64, error) {
	if column.DataType != FLOAT64 {
		return 0, fmt.Errorf("scalar sum only supports FLOAT64")
	}
	
	data := column.Data.([]float64)
	sum := 0.0
	for i := 0; i < column.Length; i++ {
		if !column.IsNull(i) {
			sum += data[i]
		}
	}
	
	return sum, nil
}

// SIMDPerformanceTester provides benchmarking utilities
type SIMDPerformanceTester struct {
	simdOps *SIMDOperations
}

func NewSIMDPerformanceTester() *SIMDPerformanceTester {
	return &SIMDPerformanceTester{
		simdOps: NewSIMDOperations(),
	}
}

// BenchmarkFilterPerformance compares SIMD vs scalar filtering performance
func (tester *SIMDPerformanceTester) BenchmarkFilterPerformance(dataSize int) *SIMDPerfResult {
	// Create test data
	data := make([]float64, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = float64(i%1000) + 100.0
	}
	
	vector := NewVector(FLOAT64, dataSize)
	vector.Data = data
	
	threshold := 500.0
	selection := make([]int, dataSize)
	
	// Benchmark SIMD filtering
	start := time.Now()
	simdCount := SIMDFilterGreaterThan(data, threshold, selection)
	simdTime := time.Since(start)
	
	// Benchmark scalar filtering
	selection2 := make([]int, dataSize)
	start = time.Now()
	scalarCount := 0
	for i, val := range data {
		if val > threshold {
			selection2[scalarCount] = i
			scalarCount++
		}
	}
	scalarTime := time.Since(start)
	
	return &SIMDPerfResult{
		DataSize:     dataSize,
		SIMDTime:     simdTime,
		ScalarTime:   scalarTime,
		SIMDCount:    simdCount,
		ScalarCount:  scalarCount,
		Speedup:      float64(scalarTime) / float64(simdTime),
		SIMDThroughput: float64(dataSize) / simdTime.Seconds() / 1000000, // Million elements/sec
		ScalarThroughput: float64(dataSize) / scalarTime.Seconds() / 1000000,
	}
}

// SIMDPerfResult holds benchmark results
type SIMDPerfResult struct {
	DataSize         int
	SIMDTime         time.Duration
	ScalarTime       time.Duration
	SIMDCount        int
	ScalarCount      int
	Speedup          float64
	SIMDThroughput   float64
	ScalarThroughput float64
}

func (result *SIMDPerfResult) String() string {
	return fmt.Sprintf(
		"Data Size: %d\n"+
		"SIMD Time: %v (%.2f M elements/sec)\n"+
		"Scalar Time: %v (%.2f M elements/sec)\n"+
		"Results: SIMD=%d, Scalar=%d\n"+
		"Speedup: %.2fx\n",
		result.DataSize,
		result.SIMDTime, result.SIMDThroughput,
		result.ScalarTime, result.ScalarThroughput,
		result.SIMDCount, result.ScalarCount,
		result.Speedup,
	)
}

// GetSIMDCapabilities returns information about SIMD support
func GetSIMDCapabilities() map[string]bool {
	capabilities := make(map[string]bool)
	
	// For a real implementation, you'd use CPU feature detection
	capabilities["AVX2"] = hasAVX2()
	capabilities["AVX512"] = false // Not implemented
	capabilities["SSE4.2"] = true   // Assume SSE4.2 is available
	capabilities["CGO"] = true      // CGO SIMD implementation available
	
	return capabilities
}

// PrintSIMDInfo displays SIMD capabilities and recommendations
func PrintSIMDInfo() {
	fmt.Println("SIMD Capabilities:")
	capabilities := GetSIMDCapabilities()
	
	for feature, available := range capabilities {
		status := "❌ Not Available"
		if available {
			status = "✅ Available"
		}
		fmt.Printf("  %s: %s\n", feature, status)
	}
	
	fmt.Printf("\nRuntime Info:\n")
	fmt.Printf("  Go Version: %s\n", runtime.Version())
	fmt.Printf("  Architecture: %s\n", runtime.GOARCH)
	fmt.Printf("  Operating System: %s\n", runtime.GOOS)
	fmt.Printf("  CPU Count: %d\n", runtime.NumCPU())
	
	fmt.Printf("\nRecommendations:\n")
	if capabilities["AVX2"] {
		fmt.Println("  ✅ Use Go assembly SIMD implementation for best performance")
	} else {
		fmt.Println("  ⚠️  Use CGO SIMD implementation (requires C compiler)")
	}
	
	fmt.Println("  📊 SIMD works best with:")
	fmt.Println("    • Large datasets (>10K elements)")
	fmt.Println("    • Contiguous data (aligned memory)")
	fmt.Println("    • Simple operations (filters, math)")
	fmt.Println("    • FLOAT64 and INT64 data types")
}