package vectorized

import (
	"fmt"
	"unsafe"
)

// VectorBatch represents a batch of columnar data for vectorized processing
type VectorBatch struct {
	Schema     *Schema
	Columns    []*Vector
	RowCount   int
	Capacity   int
	SelectMask *SelectionVector // Tracks which rows are selected/valid
}

// Schema defines the structure of a vector batch
type Schema struct {
	Fields []*Field
}

// Field represents a column definition in the schema
type Field struct {
	Name     string
	DataType DataType
	Nullable bool
}

// DataType represents the supported data types for vectorized execution
type DataType int

const (
	INT32 DataType = iota
	INT64
	FLOAT32
	FLOAT64
	STRING
	BOOLEAN
	DATE
	TIMESTAMP
	DECIMAL
)

// Vector represents a columnar vector of data
type Vector struct {
	DataType   DataType
	Data       interface{} // Type-specific data array
	Nulls      *NullMask   // Bitmap for null values
	Length     int
	Capacity   int
	IsConstant bool        // Optimization for constant values
	ConstValue interface{} // Value when IsConstant is true
}

// NullMask efficiently tracks null values using bitmaps
type NullMask struct {
	Bits      []uint64
	Length    int
	NullCount int
}

// SelectionVector tracks which rows in a vector are selected
type SelectionVector struct {
	Indices  []int
	Length   int
	Capacity int
}

// Specialized vector types for better performance
type Int32Vector struct {
	Data     []int32
	Nulls    *NullMask
	Length   int
	Capacity int
}

type Int64Vector struct {
	Data     []int64
	Nulls    *NullMask
	Length   int
	Capacity int
}

type Float64Vector struct {
	Data     []float64
	Nulls    *NullMask
	Length   int
	Capacity int
}

type StringVector struct {
	Data     []string
	Nulls    *NullMask
	Length   int
	Capacity int
}

type BooleanVector struct {
	Data     []bool
	Nulls    *NullMask
	Length   int
	Capacity int
}

// Constants for vectorized execution
const (
	DefaultBatchSize    = 4096 // Optimal batch size for most operations
	MaxBatchSize        = 65536
	MinBatchSize        = 64
	CacheLineSize       = 64
	SIMDRegisterSize    = 256 // 256-bit AVX2 registers
	ElementsPerRegister = SIMDRegisterSize / 64 // 4 int64s per register
)

// NewVectorBatch creates a new vector batch with the given schema
func NewVectorBatch(schema *Schema, capacity int) *VectorBatch {
	if capacity <= 0 {
		capacity = DefaultBatchSize
	}
	
	columns := make([]*Vector, len(schema.Fields))
	for i, field := range schema.Fields {
		columns[i] = NewVector(field.DataType, capacity)
	}
	
	return &VectorBatch{
		Schema:     schema,
		Columns:    columns,
		RowCount:   0,
		Capacity:   capacity,
		SelectMask: NewSelectionVector(capacity),
	}
}

// NewVector creates a new vector of the specified type and capacity
func NewVector(dataType DataType, capacity int) *Vector {
	vector := &Vector{
		DataType: dataType,
		Length:   0,
		Capacity: capacity,
		Nulls:    NewNullMask(capacity),
	}
	
	// Allocate type-specific storage
	switch dataType {
	case INT32:
		vector.Data = make([]int32, capacity)
	case INT64:
		vector.Data = make([]int64, capacity)
	case FLOAT32:
		vector.Data = make([]float32, capacity)
	case FLOAT64:
		vector.Data = make([]float64, capacity)
	case STRING:
		vector.Data = make([]string, capacity)
	case BOOLEAN:
		vector.Data = make([]bool, capacity)
	default:
		vector.Data = make([]interface{}, capacity)
	}
	
	return vector
}

// NewNullMask creates a new null mask with the specified capacity
func NewNullMask(capacity int) *NullMask {
	// Calculate number of uint64s needed for the bitmask
	numWords := (capacity + 63) / 64
	return &NullMask{
		Bits:      make([]uint64, numWords),
		Length:    capacity,
		NullCount: 0,
	}
}

// NewSelectionVector creates a new selection vector
func NewSelectionVector(capacity int) *SelectionVector {
	return &SelectionVector{
		Indices:  make([]int, capacity),
		Length:   0,
		Capacity: capacity,
	}
}

// Vector access methods with type safety and performance optimization

// GetInt32 retrieves an int32 value from the vector
func (v *Vector) GetInt32(index int) (int32, bool) {
	if v.DataType != INT32 || index >= v.Length {
		return 0, false
	}
	if v.IsNull(index) {
		return 0, false
	}
	if v.IsConstant {
		return v.ConstValue.(int32), true
	}
	return v.Data.([]int32)[index], true
}

// GetInt64 retrieves an int64 value from the vector
func (v *Vector) GetInt64(index int) (int64, bool) {
	if v.DataType != INT64 || index >= v.Length {
		return 0, false
	}
	if v.IsNull(index) {
		return 0, false
	}
	if v.IsConstant {
		return v.ConstValue.(int64), true
	}
	return v.Data.([]int64)[index], true
}

// GetFloat64 retrieves a float64 value from the vector
func (v *Vector) GetFloat64(index int) (float64, bool) {
	if v.DataType != FLOAT64 || index >= v.Length {
		return 0, false
	}
	if v.IsNull(index) {
		return 0, false
	}
	if v.IsConstant {
		return v.ConstValue.(float64), true
	}
	return v.Data.([]float64)[index], true
}

// GetString retrieves a string value from the vector
func (v *Vector) GetString(index int) (string, bool) {
	if v.DataType != STRING || index >= v.Length {
		return "", false
	}
	if v.IsNull(index) {
		return "", false
	}
	if v.IsConstant {
		return v.ConstValue.(string), true
	}
	return v.Data.([]string)[index], true
}

// GetBoolean retrieves a boolean value from the vector
func (v *Vector) GetBoolean(index int) (bool, bool) {
	if v.DataType != BOOLEAN || index >= v.Length {
		return false, false
	}
	if v.IsNull(index) {
		return false, false
	}
	if v.IsConstant {
		return v.ConstValue.(bool), true
	}
	return v.Data.([]bool)[index], true
}

// SetInt32 sets an int32 value in the vector
func (v *Vector) SetInt32(index int, value int32) {
	if v.DataType != INT32 || index >= v.Capacity {
		return
	}
	v.Data.([]int32)[index] = value
	v.Nulls.SetNotNull(index)
	if index >= v.Length {
		v.Length = index + 1
	}
}

// SetInt64 sets an int64 value in the vector
func (v *Vector) SetInt64(index int, value int64) {
	if v.DataType != INT64 || index >= v.Capacity {
		return
	}
	v.Data.([]int64)[index] = value
	v.Nulls.SetNotNull(index)
	if index >= v.Length {
		v.Length = index + 1
	}
}

// SetFloat64 sets a float64 value in the vector
func (v *Vector) SetFloat64(index int, value float64) {
	if v.DataType != FLOAT64 || index >= v.Capacity {
		return
	}
	v.Data.([]float64)[index] = value
	v.Nulls.SetNotNull(index)
	if index >= v.Length {
		v.Length = index + 1
	}
}

// SetString sets a string value in the vector
func (v *Vector) SetString(index int, value string) {
	if v.DataType != STRING || index >= v.Capacity {
		return
	}
	v.Data.([]string)[index] = value
	v.Nulls.SetNotNull(index)
	if index >= v.Length {
		v.Length = index + 1
	}
}

// SetBoolean sets a boolean value in the vector
func (v *Vector) SetBoolean(index int, value bool) {
	if v.DataType != BOOLEAN || index >= v.Capacity {
		return
	}
	v.Data.([]bool)[index] = value
	v.Nulls.SetNotNull(index)
	if index >= v.Length {
		v.Length = index + 1
	}
}

// SetNull marks a position as null
func (v *Vector) SetNull(index int) {
	if index < v.Capacity {
		v.Nulls.SetNull(index)
		if index >= v.Length {
			v.Length = index + 1
		}
	}
}

// IsNull checks if a position is null
func (v *Vector) IsNull(index int) bool {
	return v.Nulls.IsNull(index)
}

// NullMask methods for efficient null handling

// IsNull checks if a bit is set (indicating null)
func (nm *NullMask) IsNull(index int) bool {
	if index >= nm.Length {
		return false
	}
	wordIndex := index / 64
	bitIndex := index % 64
	return (nm.Bits[wordIndex] & (1 << bitIndex)) != 0
}

// SetNull sets a bit to indicate null
func (nm *NullMask) SetNull(index int) {
	if index >= nm.Length {
		return
	}
	wordIndex := index / 64
	bitIndex := index % 64
	if (nm.Bits[wordIndex] & (1 << bitIndex)) == 0 {
		nm.Bits[wordIndex] |= (1 << bitIndex)
		nm.NullCount++
	}
}

// SetNotNull clears a bit to indicate not null
func (nm *NullMask) SetNotNull(index int) {
	if index >= nm.Length {
		return
	}
	wordIndex := index / 64
	bitIndex := index % 64
	if (nm.Bits[wordIndex] & (1 << bitIndex)) != 0 {
		nm.Bits[wordIndex] &^= (1 << bitIndex)
		nm.NullCount--
	}
}

// HasNulls returns true if there are any null values
func (nm *NullMask) HasNulls() bool {
	return nm.NullCount > 0
}

// SelectionVector methods for efficient filtering

// Add adds an index to the selection vector
func (sv *SelectionVector) Add(index int) {
	if sv.Length < sv.Capacity {
		sv.Indices[sv.Length] = index
		sv.Length++
	}
}

// Clear clears the selection vector
func (sv *SelectionVector) Clear() {
	sv.Length = 0
}

// Get retrieves an index from the selection vector
func (sv *SelectionVector) Get(index int) int {
	if index < sv.Length {
		return sv.Indices[index]
	}
	return -1
}

// VectorBatch methods

// Reset resets the batch for reuse
func (vb *VectorBatch) Reset() {
	vb.RowCount = 0
	vb.SelectMask.Clear()
	for _, col := range vb.Columns {
		col.Length = 0
		col.Nulls.NullCount = 0
		// Clear null mask bits
		for i := range col.Nulls.Bits {
			col.Nulls.Bits[i] = 0
		}
	}
}

// AddRow adds a row to the batch
func (vb *VectorBatch) AddRow(values []interface{}) error {
	if vb.RowCount >= vb.Capacity {
		return fmt.Errorf("batch is full")
	}
	
	if len(values) != len(vb.Columns) {
		return fmt.Errorf("value count mismatch: expected %d, got %d", len(vb.Columns), len(values))
	}
	
	for i, value := range values {
		col := vb.Columns[i]
		if value == nil {
			col.SetNull(vb.RowCount)
		} else {
			switch col.DataType {
			case INT32:
				if v, ok := value.(int32); ok {
					col.SetInt32(vb.RowCount, v)
				} else if v, ok := value.(int); ok {
					col.SetInt32(vb.RowCount, int32(v))
				}
			case INT64:
				if v, ok := value.(int64); ok {
					col.SetInt64(vb.RowCount, v)
				} else if v, ok := value.(int); ok {
					col.SetInt64(vb.RowCount, int64(v))
				}
			case FLOAT64:
				if v, ok := value.(float64); ok {
					col.SetFloat64(vb.RowCount, v)
				}
			case STRING:
				if v, ok := value.(string); ok {
					col.SetString(vb.RowCount, v)
				}
			case BOOLEAN:
				if v, ok := value.(bool); ok {
					col.SetBoolean(vb.RowCount, v)
				}
			}
		}
	}
	
	vb.RowCount++
	return nil
}

// GetColumn returns a column by index
func (vb *VectorBatch) GetColumn(index int) *Vector {
	if index < len(vb.Columns) {
		return vb.Columns[index]
	}
	return nil
}

// GetColumnByName returns a column by name
func (vb *VectorBatch) GetColumnByName(name string) *Vector {
	for i, field := range vb.Schema.Fields {
		if field.Name == name {
			return vb.Columns[i]
		}
	}
	return nil
}

// Memory management and SIMD optimization utilities

// AlignedAlloc allocates memory aligned to cache line boundaries for SIMD operations
func AlignedAlloc[T any](size int) []T {
	// Allocate extra space for alignment
	buffer := make([]T, size+CacheLineSize/int(unsafe.Sizeof(*new(T))))
	
	// Calculate aligned offset
	ptr := uintptr(unsafe.Pointer(&buffer[0]))
	aligned := (ptr + CacheLineSize - 1) &^ (CacheLineSize - 1)
	offset := int((aligned - ptr) / unsafe.Sizeof(*new(T)))
	
	return buffer[offset : offset+size]
}

// IsSIMDAligned checks if a slice is aligned for SIMD operations
func IsSIMDAligned[T any](slice []T) bool {
	if len(slice) == 0 {
		return true
	}
	ptr := uintptr(unsafe.Pointer(&slice[0]))
	return ptr%CacheLineSize == 0
}

// DataType utility methods

// String returns the string representation of a data type
func (dt DataType) String() string {
	switch dt {
	case INT32:
		return "INT32"
	case INT64:
		return "INT64"
	case FLOAT32:
		return "FLOAT32"
	case FLOAT64:
		return "FLOAT64"
	case STRING:
		return "STRING"
	case BOOLEAN:
		return "BOOLEAN"
	case DATE:
		return "DATE"
	case TIMESTAMP:
		return "TIMESTAMP"
	case DECIMAL:
		return "DECIMAL"
	default:
		return "UNKNOWN"
	}
}

// Size returns the size in bytes of the data type
func (dt DataType) Size() int {
	switch dt {
	case INT32, FLOAT32:
		return 4
	case INT64, FLOAT64:
		return 8
	case BOOLEAN:
		return 1
	case STRING:
		return 16 // Approximate for string header
	default:
		return 8 // Default size
	}
}

// IsNumeric returns true if the data type is numeric
func (dt DataType) IsNumeric() bool {
	switch dt {
	case INT32, INT64, FLOAT32, FLOAT64, DECIMAL:
		return true
	default:
		return false
	}
}

// IsFloatingPoint returns true if the data type is floating point
func (dt DataType) IsFloatingPoint() bool {
	switch dt {
	case FLOAT32, FLOAT64:
		return true
	default:
		return false
	}
}