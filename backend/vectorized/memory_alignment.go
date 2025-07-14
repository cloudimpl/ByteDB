package vectorized

import (
	"reflect"
	"unsafe"
)

// Memory alignment optimizations for SIMD operations
// Proper alignment is crucial for maximum SIMD performance

const (
	// Alignment constants for different SIMD instruction sets
	ALIGNMENT_SSE    = 16  // 128-bit alignment for SSE
	ALIGNMENT_AVX    = 32  // 256-bit alignment for AVX/AVX2
	ALIGNMENT_AVX512 = 64  // 512-bit alignment for AVX-512
	
	// Cache line alignment for optimal memory access
	CACHE_LINE_SIZE = 64   // Most modern CPUs have 64-byte cache lines
	
	// Default alignment for general purpose
	DEFAULT_ALIGNMENT = ALIGNMENT_AVX
)

// AlignedBuffer represents a memory buffer with guaranteed alignment
type AlignedBuffer struct {
	data      unsafe.Pointer // Aligned memory pointer
	raw       []byte         // Raw allocated memory (for cleanup)
	size      int            // Size in bytes
	alignment int            // Alignment in bytes
	capacity  int            // Capacity in elements
	dataType  DataType       // Type of elements stored
}

// NewAlignedBuffer creates a new aligned memory buffer
func NewAlignedBuffer(elementCount int, dataType DataType, alignment int) *AlignedBuffer {
	elementSize := getDataTypeSize(dataType)
	if elementSize == 0 {
		elementSize = 8 // Default to 8 bytes for unknown types
	}
	
	size := elementCount * elementSize
	
	// Allocate extra memory to ensure we can align properly
	rawSize := size + alignment - 1
	raw := make([]byte, rawSize)
	
	// Calculate aligned pointer
	rawPtr := uintptr(unsafe.Pointer(&raw[0]))
	alignedPtr := (rawPtr + uintptr(alignment-1)) &^ uintptr(alignment-1)
	
	return &AlignedBuffer{
		data:      unsafe.Pointer(alignedPtr),
		raw:       raw,
		size:      size,
		alignment: alignment,
		capacity:  elementCount,
		dataType:  dataType,
	}
}

// GetFloat64Slice returns a slice of float64 values backed by aligned memory
func (buf *AlignedBuffer) GetFloat64Slice() []float64 {
	if buf.dataType != FLOAT64 {
		panic("buffer is not of type FLOAT64")
	}
	
	// Create slice header manually to point to aligned memory
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&[]float64{}))
	slice.Data = uintptr(buf.data)
	slice.Len = buf.capacity
	slice.Cap = buf.capacity
	
	return *(*[]float64)(unsafe.Pointer(slice))
}

// GetInt64Slice returns a slice of int64 values backed by aligned memory
func (buf *AlignedBuffer) GetInt64Slice() []int64 {
	if buf.dataType != INT64 {
		panic("buffer is not of type INT64")
	}
	
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&[]int64{}))
	slice.Data = uintptr(buf.data)
	slice.Len = buf.capacity
	slice.Cap = buf.capacity
	
	return *(*[]int64)(unsafe.Pointer(slice))
}

// GetInt32Slice returns a slice of int32 values backed by aligned memory
func (buf *AlignedBuffer) GetInt32Slice() []int32 {
	if buf.dataType != INT32 {
		panic("buffer is not of type INT32")
	}
	
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&[]int32{}))
	slice.Data = uintptr(buf.data)
	slice.Len = buf.capacity
	slice.Cap = buf.capacity
	
	return *(*[]int32)(unsafe.Pointer(slice))
}

// GetBoolSlice returns a slice of bool values backed by aligned memory
func (buf *AlignedBuffer) GetBoolSlice() []bool {
	if buf.dataType != BOOLEAN {
		panic("buffer is not of type BOOLEAN")
	}
	
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&[]bool{}))
	slice.Data = uintptr(buf.data)
	slice.Len = buf.capacity
	slice.Cap = buf.capacity
	
	return *(*[]bool)(unsafe.Pointer(slice))
}

// IsAligned checks if a pointer is properly aligned
func (buf *AlignedBuffer) IsAligned() bool {
	return uintptr(buf.data)%uintptr(buf.alignment) == 0
}

// GetAlignment returns the alignment of the buffer
func (buf *AlignedBuffer) GetAlignment() int {
	return buf.alignment
}

// Size returns the size of the buffer in bytes
func (buf *AlignedBuffer) Size() int {
	return buf.size
}

// Capacity returns the number of elements the buffer can hold
func (buf *AlignedBuffer) Capacity() int {
	return buf.capacity
}

// AlignedVectorBatch extends VectorBatch with aligned memory for SIMD operations
type AlignedVectorBatch struct {
	*VectorBatch
	alignedBuffers []*AlignedBuffer
	alignment      int
}

// NewAlignedVectorBatch creates a new VectorBatch with aligned memory
func NewAlignedVectorBatch(schema *Schema, capacity int) *AlignedVectorBatch {
	caps := GetSIMDCapabilities()
	alignment := DEFAULT_ALIGNMENT
	
	// Choose optimal alignment based on available SIMD instructions
	if caps.HasAVX512F {
		alignment = ALIGNMENT_AVX512
	} else if caps.HasAVX2 {
		alignment = ALIGNMENT_AVX
	} else if caps.HasSSE2 {
		alignment = ALIGNMENT_SSE
	}
	
	// Create standard VectorBatch
	batch := NewVectorBatch(schema, capacity)
	
	// Create aligned buffers for each column
	alignedBuffers := make([]*AlignedBuffer, len(schema.Fields))
	
	for i, field := range schema.Fields {
		alignedBuffers[i] = NewAlignedBuffer(capacity, field.DataType, alignment)
		
		// Replace the column's data with aligned memory
		switch field.DataType {
		case FLOAT64:
			batch.Columns[i].Data = alignedBuffers[i].GetFloat64Slice()
		case INT64:
			batch.Columns[i].Data = alignedBuffers[i].GetInt64Slice()
		case INT32:
			batch.Columns[i].Data = alignedBuffers[i].GetInt32Slice()
		case BOOLEAN:
			batch.Columns[i].Data = alignedBuffers[i].GetBoolSlice()
		}
	}
	
	return &AlignedVectorBatch{
		VectorBatch:    batch,
		alignedBuffers: alignedBuffers,
		alignment:      alignment,
	}
}

// GetAlignedBuffer returns the aligned buffer for a specific column
func (batch *AlignedVectorBatch) GetAlignedBuffer(columnIndex int) *AlignedBuffer {
	if columnIndex < 0 || columnIndex >= len(batch.alignedBuffers) {
		return nil
	}
	return batch.alignedBuffers[columnIndex]
}

// IsColumnAligned checks if a specific column is properly aligned
func (batch *AlignedVectorBatch) IsColumnAligned(columnIndex int) bool {
	buffer := batch.GetAlignedBuffer(columnIndex)
	if buffer == nil {
		return false
	}
	return buffer.IsAligned()
}

// GetAlignment returns the alignment used by this batch
func (batch *AlignedVectorBatch) GetAlignment() int {
	return batch.alignment
}

// Utility functions for memory alignment

// IsPointerAligned checks if a pointer is aligned to the specified boundary
func IsPointerAligned(ptr unsafe.Pointer, alignment int) bool {
	return uintptr(ptr)%uintptr(alignment) == 0
}

// AlignPointer aligns a pointer to the specified boundary
func AlignPointer(ptr unsafe.Pointer, alignment int) unsafe.Pointer {
	aligned := (uintptr(ptr) + uintptr(alignment-1)) &^ uintptr(alignment-1)
	return unsafe.Pointer(aligned)
}

// GetOptimalAlignment returns the optimal alignment for the current system
func GetOptimalAlignment() int {
	caps := GetSIMDCapabilities()
	
	if caps.HasAVX512F {
		return ALIGNMENT_AVX512
	} else if caps.HasAVX2 {
		return ALIGNMENT_AVX
	} else if caps.HasSSE2 {
		return ALIGNMENT_SSE
	}
	
	return 8 // Fallback to 8-byte alignment
}

// AlignedSliceFloat64 creates an aligned slice of float64 values
func AlignedSliceFloat64(size int, alignment int) []float64 {
	buffer := NewAlignedBuffer(size, FLOAT64, alignment)
	return buffer.GetFloat64Slice()
}

// AlignedSliceInt64 creates an aligned slice of int64 values
func AlignedSliceInt64(size int, alignment int) []int64 {
	buffer := NewAlignedBuffer(size, INT64, alignment)
	return buffer.GetInt64Slice()
}

// AlignedSliceInt32 creates an aligned slice of int32 values
func AlignedSliceInt32(size int, alignment int) []int32 {
	buffer := NewAlignedBuffer(size, INT32, alignment)
	return buffer.GetInt32Slice()
}

// CheckSliceAlignment checks if a slice is properly aligned for SIMD operations
func CheckSliceAlignment(slice interface{}, alignment int) bool {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice {
		return false
	}
	
	if val.Len() == 0 {
		return true // Empty slice is considered aligned
	}
	
	// Get pointer to first element
	ptr := unsafe.Pointer(val.Pointer())
	return IsPointerAligned(ptr, alignment)
}

// AlignmentAwareVector extends Vector with alignment information
type AlignmentAwareVector struct {
	*Vector
	alignment int
	isAligned bool
}

// NewAlignmentAwareVector creates a new vector with specified alignment
func NewAlignmentAwareVector(dataType DataType, length int, alignment int) *AlignmentAwareVector {
	buffer := NewAlignedBuffer(length, dataType, alignment)
	
	vector := &Vector{
		DataType: dataType,
		Length:   length,
		Nulls:    NewNullMask(length),
	}
	
	// Set the aligned data
	switch dataType {
	case FLOAT64:
		vector.Data = buffer.GetFloat64Slice()
	case INT64:
		vector.Data = buffer.GetInt64Slice()
	case INT32:
		vector.Data = buffer.GetInt32Slice()
	case BOOLEAN:
		vector.Data = buffer.GetBoolSlice()
	default:
		// For unsupported types, use regular allocation
		vector.Data = make([]interface{}, length)
	}
	
	return &AlignmentAwareVector{
		Vector:    vector,
		alignment: alignment,
		isAligned: true,
	}
}

// IsAligned returns whether the vector data is properly aligned
func (v *AlignmentAwareVector) IsAligned() bool {
	return v.isAligned && CheckSliceAlignment(v.Data, v.alignment)
}

// GetAlignment returns the alignment of the vector
func (v *AlignmentAwareVector) GetAlignment() int {
	return v.alignment
}

// CanUseSIMD checks if this vector can benefit from SIMD operations
func (v *AlignmentAwareVector) CanUseSIMD() bool {
	caps := GetSIMDCapabilities()
	return v.IsAligned() && caps.CanUseSIMD(v.DataType, v.Length)
}

// Memory prefetching utilities for better cache performance

// PrefetchMemory provides hints to the CPU to prefetch memory into cache
func PrefetchMemory(ptr unsafe.Pointer, size int) {
	// Note: This would typically use architecture-specific prefetch instructions
	// For demonstration, we'll use a simple loop that encourages prefetching
	
	if size <= 0 {
		return
	}
	
	// Touch memory in cache line sized chunks to encourage prefetching
	for offset := 0; offset < size; offset += CACHE_LINE_SIZE {
		// Touch the memory location to encourage cache loading
		touchPtr := unsafe.Pointer(uintptr(ptr) + uintptr(offset))
		_ = *(*byte)(touchPtr)
	}
}

// PrefetchSlice prefetches a slice into CPU cache
func PrefetchSlice(slice interface{}) {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice || val.Len() == 0 {
		return
	}
	
	// Calculate size in bytes
	elemSize := int(val.Type().Elem().Size())
	totalSize := val.Len() * elemSize
	
	// Get pointer to data
	ptr := unsafe.Pointer(val.Pointer())
	
	PrefetchMemory(ptr, totalSize)
}

// SIMD-optimized memory operations

// AlignedMemcpy performs an aligned memory copy optimized for SIMD
func AlignedMemcpy(dst, src unsafe.Pointer, size int, alignment int) {
	// Verify both pointers are aligned
	if !IsPointerAligned(dst, alignment) || !IsPointerAligned(src, alignment) {
		// Fallback to regular copy
		copy(
			(*[1 << 30]byte)(dst)[:size:size],
			(*[1 << 30]byte)(src)[:size:size],
		)
		return
	}
	
	// For aligned pointers, we could use SIMD-optimized copy
	// For now, use regular copy (real implementation would use vectorized copy)
	copy(
		(*[1 << 30]byte)(dst)[:size:size],
		(*[1 << 30]byte)(src)[:size:size],
	)
}

// AlignedMemset sets memory to a specific value using SIMD when possible
func AlignedMemset(ptr unsafe.Pointer, value byte, size int, alignment int) {
	if !IsPointerAligned(ptr, alignment) {
		// Fallback to regular memset
		slice := (*[1 << 30]byte)(ptr)[:size:size]
		for i := range slice {
			slice[i] = value
		}
		return
	}
	
	// For aligned memory, use SIMD-optimized set
	// Real implementation would use vectorized set operations
	slice := (*[1 << 30]byte)(ptr)[:size:size]
	for i := range slice {
		slice[i] = value
	}
}

// Performance monitoring and diagnostics

// AlignmentStats provides statistics about memory alignment in the system
type AlignmentStats struct {
	OptimalAlignment     int
	VectorsCreated      int
	VectorsAligned      int
	BatchesCreated      int
	BatchesAligned      int
	SIMDOperationsUsed  int
	ScalarFallbacks     int
}

var globalAlignmentStats = &AlignmentStats{
	OptimalAlignment: GetOptimalAlignment(),
}

// GetAlignmentStats returns global alignment statistics
func GetAlignmentStats() *AlignmentStats {
	return globalAlignmentStats
}

// ResetAlignmentStats resets the global alignment statistics
func ResetAlignmentStats() {
	globalAlignmentStats = &AlignmentStats{
		OptimalAlignment: GetOptimalAlignment(),
	}
}

// RecordVectorCreation records the creation of a vector for statistics
func RecordVectorCreation(aligned bool) {
	globalAlignmentStats.VectorsCreated++
	if aligned {
		globalAlignmentStats.VectorsAligned++
	}
}

// RecordBatchCreation records the creation of a batch for statistics
func RecordBatchCreation(aligned bool) {
	globalAlignmentStats.BatchesCreated++
	if aligned {
		globalAlignmentStats.BatchesAligned++
	}
}

// RecordSIMDOperation records the use of a SIMD operation
func RecordSIMDOperation() {
	globalAlignmentStats.SIMDOperationsUsed++
}

// RecordScalarFallback records a fallback to scalar operations
func RecordScalarFallback() {
	globalAlignmentStats.ScalarFallbacks++
}