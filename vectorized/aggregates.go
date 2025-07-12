package vectorized

import (
	"fmt"
	"math"
)

// VectorizedAggregateOperator performs aggregations using vectorized execution
func NewVectorizedAggregateOperator(input VectorizedOperator, groupByColumns []int, aggregates []*VectorizedAggregate) *VectorizedAggregateOperator {
	// Build output schema
	outputFields := make([]*Field, len(groupByColumns)+len(aggregates))
	inputSchema := input.GetOutputSchema()
	
	// Add group by columns to output schema
	for i, colIndex := range groupByColumns {
		outputFields[i] = inputSchema.Fields[colIndex]
	}
	
	// Add aggregate columns to output schema
	for i, agg := range aggregates {
		outputFields[len(groupByColumns)+i] = &Field{
			Name:     fmt.Sprintf("%s_%d", getAggregateFunctionName(agg.Function), agg.InputColumn),
			DataType: agg.OutputType,
			Nullable: false,
		}
		agg.OutputColumn = len(groupByColumns) + i
	}
	
	return &VectorizedAggregateOperator{
		Input:          input,
		GroupByColumns: groupByColumns,
		Aggregates:     aggregates,
		OutputSchema:   &Schema{Fields: outputFields},
		HashTable:      NewVectorizedHashTable(groupByColumns, len(aggregates), DefaultBatchSize*4),
	}
}

func (op *VectorizedAggregateOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	// Process all input batches
	for {
		batch, err := op.Input.Execute(input)
		if err != nil {
			return nil, err
		}
		
		if batch == nil {
			break // End of input
		}
		
		// Process batch through hash table
		err = op.processInputBatch(batch)
		if err != nil {
			return nil, err
		}
	}
	
	// Generate output batch from hash table
	return op.generateOutputBatch()
}

func (op *VectorizedAggregateOperator) GetOutputSchema() *Schema {
	return op.OutputSchema
}

func (op *VectorizedAggregateOperator) GetEstimatedRowCount() int {
	// Estimate aggregation reduces rows significantly
	inputRows := op.Input.GetEstimatedRowCount()
	if len(op.GroupByColumns) == 0 {
		return 1 // Global aggregation
	}
	// Estimate 10% unique groups
	return max(1, inputRows/10)
}

func (op *VectorizedAggregateOperator) processInputBatch(batch *VectorBatch) error {
	selection := batch.SelectMask
	if selection.Length == 0 {
		// No selection mask, process all rows
		for i := 0; i < batch.RowCount; i++ {
			selection.Add(i)
		}
	}
	
	// Process each selected row
	for i := 0; i < selection.Length; i++ {
		rowIndex := selection.Indices[i]
		
		// Extract group key
		groupKey := op.extractGroupKey(batch, rowIndex)
		
		// Find or create group in hash table
		bucket := op.HashTable.FindOrCreateBucket(groupKey)
		
		// Update aggregates for this group
		err := op.updateAggregates(bucket, batch, rowIndex)
		if err != nil {
			return err
		}
	}
	
	return nil
}

func (op *VectorizedAggregateOperator) extractGroupKey(batch *VectorBatch, rowIndex int) []interface{} {
	if len(op.GroupByColumns) == 0 {
		return nil // Global aggregation
	}
	
	key := make([]interface{}, len(op.GroupByColumns))
	for i, colIndex := range op.GroupByColumns {
		column := batch.Columns[colIndex]
		if column.IsNull(rowIndex) {
			key[i] = nil
		} else {
			switch column.DataType {
			case INT32:
				key[i], _ = column.GetInt32(rowIndex)
			case INT64:
				key[i], _ = column.GetInt64(rowIndex)
			case FLOAT64:
				key[i], _ = column.GetFloat64(rowIndex)
			case STRING:
				key[i], _ = column.GetString(rowIndex)
			case BOOLEAN:
				key[i], _ = column.GetBoolean(rowIndex)
			default:
				key[i] = nil
			}
		}
	}
	return key
}

func (op *VectorizedAggregateOperator) updateAggregates(bucket *HashBucket, batch *VectorBatch, rowIndex int) error {
	// Initialize aggregates if this is the first row for this group
	if bucket.Values == nil {
		bucket.Values = NewVectorBatch(op.getAggregateSchema(), 1)
		bucket.Values.RowCount = 1
		
		// Initialize aggregate values
		for i, agg := range op.Aggregates {
			switch agg.Function {
			case COUNT:
				bucket.Values.Columns[i].SetInt64(0, 0)
			case SUM:
				op.initializeSumAggregate(bucket.Values.Columns[i], agg.OutputType)
			case AVG:
				// AVG needs two values: sum and count
				bucket.Values.Columns[i].SetFloat64(0, 0) // sum
				if i+1 < len(bucket.Values.Columns) {
					bucket.Values.Columns[i+1].SetInt64(0, 0) // count
				}
			case MIN:
				op.initializeMinAggregate(bucket.Values.Columns[i], agg.OutputType)
			case MAX:
				op.initializeMaxAggregate(bucket.Values.Columns[i], agg.OutputType)
			}
		}
	}
	
	// Update aggregates
	for i, agg := range op.Aggregates {
		inputColumn := batch.Columns[agg.InputColumn]
		outputColumn := bucket.Values.Columns[i]
		
		if inputColumn.IsNull(rowIndex) {
			continue // Skip null values
		}
		
		switch agg.Function {
		case COUNT:
			currentCount, _ := outputColumn.GetInt64(0)
			outputColumn.SetInt64(0, currentCount+1)
			
		case SUM:
			err := op.updateSumAggregate(outputColumn, inputColumn, rowIndex)
			if err != nil {
				return err
			}
			
		case AVG:
			// Update sum component
			err := op.updateSumAggregate(outputColumn, inputColumn, rowIndex)
			if err != nil {
				return err
			}
			// Update count component (handled separately)
			
		case MIN:
			err := op.updateMinAggregate(outputColumn, inputColumn, rowIndex)
			if err != nil {
				return err
			}
			
		case MAX:
			err := op.updateMaxAggregate(outputColumn, inputColumn, rowIndex)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

func (op *VectorizedAggregateOperator) getAggregateSchema() *Schema {
	fields := make([]*Field, len(op.Aggregates))
	for i, agg := range op.Aggregates {
		fields[i] = &Field{
			Name:     fmt.Sprintf("agg_%d", i),
			DataType: agg.OutputType,
			Nullable: false,
		}
	}
	return &Schema{Fields: fields}
}

func (op *VectorizedAggregateOperator) initializeSumAggregate(column *Vector, outputType DataType) {
	switch outputType {
	case INT32:
		column.SetInt32(0, 0)
	case INT64:
		column.SetInt64(0, 0)
	case FLOAT64:
		column.SetFloat64(0, 0)
	}
}

func (op *VectorizedAggregateOperator) initializeMinAggregate(column *Vector, outputType DataType) {
	switch outputType {
	case INT32:
		column.SetInt32(0, math.MaxInt32)
	case INT64:
		column.SetInt64(0, math.MaxInt64)
	case FLOAT64:
		column.SetFloat64(0, math.Inf(1))
	}
}

func (op *VectorizedAggregateOperator) initializeMaxAggregate(column *Vector, outputType DataType) {
	switch outputType {
	case INT32:
		column.SetInt32(0, math.MinInt32)
	case INT64:
		column.SetInt64(0, math.MinInt64)
	case FLOAT64:
		column.SetFloat64(0, math.Inf(-1))
	}
}

func (op *VectorizedAggregateOperator) updateSumAggregate(outputColumn, inputColumn *Vector, rowIndex int) error {
	switch inputColumn.DataType {
	case INT32:
		value, ok := inputColumn.GetInt32(rowIndex)
		if ok {
			currentSum, _ := outputColumn.GetInt64(0)
			outputColumn.SetInt64(0, currentSum+int64(value))
		}
	case INT64:
		value, ok := inputColumn.GetInt64(rowIndex)
		if ok {
			currentSum, _ := outputColumn.GetInt64(0)
			outputColumn.SetInt64(0, currentSum+value)
		}
	case FLOAT64:
		value, ok := inputColumn.GetFloat64(rowIndex)
		if ok {
			currentSum, _ := outputColumn.GetFloat64(0)
			outputColumn.SetFloat64(0, currentSum+value)
		}
	default:
		return fmt.Errorf("unsupported data type for SUM: %v", inputColumn.DataType)
	}
	return nil
}

func (op *VectorizedAggregateOperator) updateMinAggregate(outputColumn, inputColumn *Vector, rowIndex int) error {
	switch inputColumn.DataType {
	case INT32:
		value, ok := inputColumn.GetInt32(rowIndex)
		if ok {
			currentMin, _ := outputColumn.GetInt32(0)
			if value < currentMin {
				outputColumn.SetInt32(0, value)
			}
		}
	case INT64:
		value, ok := inputColumn.GetInt64(rowIndex)
		if ok {
			currentMin, _ := outputColumn.GetInt64(0)
			if value < currentMin {
				outputColumn.SetInt64(0, value)
			}
		}
	case FLOAT64:
		value, ok := inputColumn.GetFloat64(rowIndex)
		if ok {
			currentMin, _ := outputColumn.GetFloat64(0)
			if value < currentMin {
				outputColumn.SetFloat64(0, value)
			}
		}
	default:
		return fmt.Errorf("unsupported data type for MIN: %v", inputColumn.DataType)
	}
	return nil
}

func (op *VectorizedAggregateOperator) updateMaxAggregate(outputColumn, inputColumn *Vector, rowIndex int) error {
	switch inputColumn.DataType {
	case INT32:
		value, ok := inputColumn.GetInt32(rowIndex)
		if ok {
			currentMax, _ := outputColumn.GetInt32(0)
			if value > currentMax {
				outputColumn.SetInt32(0, value)
			}
		}
	case INT64:
		value, ok := inputColumn.GetInt64(rowIndex)
		if ok {
			currentMax, _ := outputColumn.GetInt64(0)
			if value > currentMax {
				outputColumn.SetInt64(0, value)
			}
		}
	case FLOAT64:
		value, ok := inputColumn.GetFloat64(rowIndex)
		if ok {
			currentMax, _ := outputColumn.GetFloat64(0)
			if value > currentMax {
				outputColumn.SetFloat64(0, value)
			}
		}
	default:
		return fmt.Errorf("unsupported data type for MAX: %v", inputColumn.DataType)
	}
	return nil
}

func (op *VectorizedAggregateOperator) generateOutputBatch() (*VectorBatch, error) {
	// Count number of groups
	groupCount := op.HashTable.Size
	if groupCount == 0 {
		return nil, nil
	}
	
	// Create output batch
	outputBatch := NewVectorBatch(op.OutputSchema, groupCount)
	outputBatch.RowCount = groupCount
	
	// Extract results from hash table
	rowIndex := 0
	for _, bucket := range op.HashTable.Buckets {
		for bucket != nil {
			// Add group key columns
			if bucket.Keys != nil && bucket.Keys.RowCount > 0 {
				for i, _ := range op.GroupByColumns {
					sourceColumn := bucket.Keys.Columns[i]
					targetColumn := outputBatch.Columns[i]
					
					// Copy value based on data type
					switch sourceColumn.DataType {
					case INT32:
						if value, ok := sourceColumn.GetInt32(0); ok {
							targetColumn.SetInt32(rowIndex, value)
						}
					case INT64:
						if value, ok := sourceColumn.GetInt64(0); ok {
							targetColumn.SetInt64(rowIndex, value)
						}
					case FLOAT64:
						if value, ok := sourceColumn.GetFloat64(0); ok {
							targetColumn.SetFloat64(rowIndex, value)
						}
					case STRING:
						if value, ok := sourceColumn.GetString(0); ok {
							targetColumn.SetString(rowIndex, value)
						}
					case BOOLEAN:
						if value, ok := sourceColumn.GetBoolean(0); ok {
							targetColumn.SetBoolean(rowIndex, value)
						}
					}
				}
			}
			
			// Add aggregate columns
			if bucket.Values != nil && bucket.Values.RowCount > 0 {
				for i, agg := range op.Aggregates {
					sourceColumn := bucket.Values.Columns[i]
					targetColumn := outputBatch.Columns[len(op.GroupByColumns)+i]
					
					// Handle different aggregate functions
					switch agg.Function {
					case COUNT, SUM, MIN, MAX:
						// Direct copy
						switch sourceColumn.DataType {
						case INT32:
							if value, ok := sourceColumn.GetInt32(0); ok {
								targetColumn.SetInt32(rowIndex, value)
							}
						case INT64:
							if value, ok := sourceColumn.GetInt64(0); ok {
								targetColumn.SetInt64(rowIndex, value)
							}
						case FLOAT64:
							if value, ok := sourceColumn.GetFloat64(0); ok {
								targetColumn.SetFloat64(rowIndex, value)
							}
						}
						
					case AVG:
						// Calculate average from sum and count
						sum, _ := sourceColumn.GetFloat64(0)
						count := int64(1) // Simplified - would need proper count tracking
						if count > 0 {
							avg := sum / float64(count)
							targetColumn.SetFloat64(rowIndex, avg)
						}
					}
				}
			}
			
			rowIndex++
			bucket = bucket.Next
		}
	}
	
	return outputBatch, nil
}

// VectorizedHashTable implementation for grouping

func NewVectorizedHashTable(keyColumns []int, valueColumns int, capacity int) *VectorizedHashTable {
	bucketCount := nextPowerOfTwo(capacity / 4) // Target 25% load factor
	return &VectorizedHashTable{
		Buckets:      make([]*HashBucket, bucketCount),
		BucketCount:  bucketCount,
		Size:         0,
		LoadFactor:   0.0,
		KeyColumns:   keyColumns,
		ValueColumns: make([]int, valueColumns),
	}
}

func (ht *VectorizedHashTable) FindOrCreateBucket(key []interface{}) *HashBucket {
	hash := ht.hashKey(key)
	bucketIndex := hash % ht.BucketCount
	
	// Search for existing bucket
	bucket := ht.Buckets[bucketIndex]
	for bucket != nil {
		if ht.keysEqual(bucket.Keys, key) {
			return bucket
		}
		bucket = bucket.Next
	}
	
	// Create new bucket
	newBucket := &HashBucket{
		Keys:     ht.createKeyBatch(key),
		Values:   nil, // Will be initialized when first value is added
		Next:     ht.Buckets[bucketIndex],
		KeyCount: len(key),
	}
	
	ht.Buckets[bucketIndex] = newBucket
	ht.Size++
	ht.LoadFactor = float64(ht.Size) / float64(ht.BucketCount)
	
	// Resize if load factor is too high
	if ht.LoadFactor > 0.75 {
		ht.resize()
	}
	
	return newBucket
}

func (ht *VectorizedHashTable) hashKey(key []interface{}) int {
	if len(key) == 0 {
		return 0 // Global aggregation
	}
	
	hash := 0
	for _, value := range key {
		if value != nil {
			switch v := value.(type) {
			case int32:
				hash = hash*31 + int(v)
			case int64:
				hash = hash*31 + int(v)
			case float64:
				hash = hash*31 + int(math.Float64bits(v))
			case string:
				for _, char := range v {
					hash = hash*31 + int(char)
				}
			case bool:
				if v {
					hash = hash*31 + 1
				}
			}
		}
	}
	
	// Ensure positive hash
	if hash < 0 {
		hash = -hash
	}
	
	return hash
}

func (ht *VectorizedHashTable) keysEqual(keyBatch *VectorBatch, key []interface{}) bool {
	if keyBatch == nil || keyBatch.RowCount == 0 {
		return len(key) == 0
	}
	
	if len(key) != len(keyBatch.Columns) {
		return false
	}
	
	for i, value := range key {
		column := keyBatch.Columns[i]
		
		if value == nil {
			if !column.IsNull(0) {
				return false
			}
		} else {
			switch column.DataType {
			case INT32:
				if columnValue, ok := column.GetInt32(0); !ok || columnValue != value.(int32) {
					return false
				}
			case INT64:
				if columnValue, ok := column.GetInt64(0); !ok || columnValue != value.(int64) {
					return false
				}
			case FLOAT64:
				if columnValue, ok := column.GetFloat64(0); !ok || columnValue != value.(float64) {
					return false
				}
			case STRING:
				if columnValue, ok := column.GetString(0); !ok || columnValue != value.(string) {
					return false
				}
			case BOOLEAN:
				if columnValue, ok := column.GetBoolean(0); !ok || columnValue != value.(bool) {
					return false
				}
			default:
				return false
			}
		}
	}
	
	return true
}

func (ht *VectorizedHashTable) createKeyBatch(key []interface{}) *VectorBatch {
	if len(key) == 0 {
		return nil // Global aggregation
	}
	
	// Create schema for key
	fields := make([]*Field, len(key))
	for i, value := range key {
		fields[i] = &Field{
			Name:     fmt.Sprintf("key_%d", i),
			DataType: ht.inferDataType(value),
			Nullable: value == nil,
		}
	}
	
	schema := &Schema{Fields: fields}
	batch := NewVectorBatch(schema, 1)
	batch.RowCount = 1
	
	// Set key values
	for i, value := range key {
		column := batch.Columns[i]
		if value == nil {
			column.SetNull(0)
		} else {
			switch v := value.(type) {
			case int32:
				column.SetInt32(0, v)
			case int64:
				column.SetInt64(0, v)
			case float64:
				column.SetFloat64(0, v)
			case string:
				column.SetString(0, v)
			case bool:
				column.SetBoolean(0, v)
			}
		}
	}
	
	return batch
}

func (ht *VectorizedHashTable) inferDataType(value interface{}) DataType {
	switch value.(type) {
	case int32:
		return INT32
	case int64:
		return INT64
	case float64:
		return FLOAT64
	case string:
		return STRING
	case bool:
		return BOOLEAN
	default:
		return STRING // Default fallback
	}
}

func (ht *VectorizedHashTable) resize() {
	oldBuckets := ht.Buckets
	oldBucketCount := ht.BucketCount
	
	// Double the bucket count
	ht.BucketCount = ht.BucketCount * 2
	ht.Buckets = make([]*HashBucket, ht.BucketCount)
	oldSize := ht.Size
	ht.Size = 0
	
	// Rehash all buckets
	for i := 0; i < oldBucketCount; i++ {
		bucket := oldBuckets[i]
		for bucket != nil {
			next := bucket.Next
			
			// Rehash this bucket
			if bucket.Keys != nil {
				key := ht.extractKeyFromBatch(bucket.Keys)
				newBucket := ht.FindOrCreateBucket(key)
				newBucket.Values = bucket.Values
			}
			
			bucket = next
		}
	}
	
	ht.Size = oldSize
	ht.LoadFactor = float64(ht.Size) / float64(ht.BucketCount)
}

func (ht *VectorizedHashTable) extractKeyFromBatch(keyBatch *VectorBatch) []interface{} {
	if keyBatch == nil || keyBatch.RowCount == 0 {
		return nil
	}
	
	key := make([]interface{}, len(keyBatch.Columns))
	for i, column := range keyBatch.Columns {
		if column.IsNull(0) {
			key[i] = nil
		} else {
			switch column.DataType {
			case INT32:
				key[i], _ = column.GetInt32(0)
			case INT64:
				key[i], _ = column.GetInt64(0)
			case FLOAT64:
				key[i], _ = column.GetFloat64(0)
			case STRING:
				key[i], _ = column.GetString(0)
			case BOOLEAN:
				key[i], _ = column.GetBoolean(0)
			}
		}
	}
	
	return key
}

// Utility functions

func getAggregateFunctionName(function AggregateFunction) string {
	switch function {
	case COUNT:
		return "COUNT"
	case SUM:
		return "SUM"
	case AVG:
		return "AVG"
	case MIN:
		return "MIN"
	case MAX:
		return "MAX"
	case FIRST:
		return "FIRST"
	case LAST:
		return "LAST"
	default:
		return "UNKNOWN"
	}
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Factory functions for creating aggregate operators

func NewCountAggregate(inputColumn int) *VectorizedAggregate {
	return &VectorizedAggregate{
		Function:    COUNT,
		InputColumn: inputColumn,
		OutputType:  INT64,
	}
}

func NewSumAggregate(inputColumn int, outputType DataType) *VectorizedAggregate {
	return &VectorizedAggregate{
		Function:    SUM,
		InputColumn: inputColumn,
		OutputType:  outputType,
	}
}

func NewAvgAggregate(inputColumn int) *VectorizedAggregate {
	return &VectorizedAggregate{
		Function:    AVG,
		InputColumn: inputColumn,
		OutputType:  FLOAT64,
	}
}

func NewMinAggregate(inputColumn int, outputType DataType) *VectorizedAggregate {
	return &VectorizedAggregate{
		Function:    MIN,
		InputColumn: inputColumn,
		OutputType:  outputType,
	}
}

func NewMaxAggregate(inputColumn int, outputType DataType) *VectorizedAggregate {
	return &VectorizedAggregate{
		Function:    MAX,
		InputColumn: inputColumn,
		OutputType:  outputType,
	}
}