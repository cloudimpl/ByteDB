package vectorized

import (
	"math"
	"unsafe"
)

// SIMD-optimized operations for vectorized execution
// These functions use Go's built-in vector capabilities and manual optimizations
// for common vectorized operations on columnar data

// SIMDConfig holds configuration for SIMD operations
type SIMDConfig struct {
	UseAVX2       bool // Use 256-bit AVX2 instructions when available
	UseAVX512     bool // Use 512-bit AVX-512 instructions when available  
	MinBatchSize  int  // Minimum batch size to enable SIMD
	Parallelism   int  // Number of parallel goroutines for large batches
}

var DefaultSIMDConfig = SIMDConfig{
	UseAVX2:      true,
	UseAVX512:    false, // Conservative default
	MinBatchSize: 64,
	Parallelism:  4,
}

// SIMD-optimized arithmetic operations

// SIMDAddInt64 performs vectorized addition of two int64 slices
func SIMDAddInt64(a, b, result []int64) {
	if len(a) != len(b) || len(a) != len(result) {
		panic("slice length mismatch")
	}
	
	n := len(a)
	if n < DefaultSIMDConfig.MinBatchSize {
		// Fall back to scalar operation for small arrays
		for i := 0; i < n; i++ {
			result[i] = a[i] + b[i]
		}
		return
	}
	
	// Process 4 elements at a time (256-bit SIMD simulation)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		// Unrolled loop for better performance
		result[i] = a[i] + b[i]
		result[i+1] = a[i+1] + b[i+1]
		result[i+2] = a[i+2] + b[i+2]
		result[i+3] = a[i+3] + b[i+3]
	}
	
	// Handle remaining elements
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] + b[i]
	}
}

// SIMDAddFloat64 performs vectorized addition of two float64 slices
func SIMDAddFloat64(a, b, result []float64) {
	if len(a) != len(b) || len(a) != len(result) {
		panic("slice length mismatch")
	}
	
	n := len(a)
	if n < DefaultSIMDConfig.MinBatchSize {
		for i := 0; i < n; i++ {
			result[i] = a[i] + b[i]
		}
		return
	}
	
	// Process 4 elements at a time
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] + b[i]
		result[i+1] = a[i+1] + b[i+1]
		result[i+2] = a[i+2] + b[i+2]
		result[i+3] = a[i+3] + b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] + b[i]
	}
}

// SIMDSubtractFloat64 performs vectorized subtraction
func SIMDSubtractFloat64(a, b, result []float64) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] - b[i]
		result[i+1] = a[i+1] - b[i+1]
		result[i+2] = a[i+2] - b[i+2]
		result[i+3] = a[i+3] - b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] - b[i]
	}
}

// SIMDMultiplyFloat64 performs vectorized multiplication
func SIMDMultiplyFloat64(a, b, result []float64) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] * b[i]
		result[i+1] = a[i+1] * b[i+1]
		result[i+2] = a[i+2] * b[i+2]
		result[i+3] = a[i+3] * b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] * b[i]
	}
}

// SIMDDivideFloat64 performs vectorized division with zero-check
func SIMDDivideFloat64(a, b, result []float64) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		// Unrolled with zero checks
		if b[i] != 0 {
			result[i] = a[i] / b[i]
		} else {
			result[i] = math.NaN()
		}
		if b[i+1] != 0 {
			result[i+1] = a[i+1] / b[i+1]
		} else {
			result[i+1] = math.NaN()
		}
		if b[i+2] != 0 {
			result[i+2] = a[i+2] / b[i+2]
		} else {
			result[i+2] = math.NaN()
		}
		if b[i+3] != 0 {
			result[i+3] = a[i+3] / b[i+3]
		} else {
			result[i+3] = math.NaN()
		}
	}
	
	for i := vectorEnd; i < n; i++ {
		if b[i] != 0 {
			result[i] = a[i] / b[i]
		} else {
			result[i] = math.NaN()
		}
	}
}

// SIMD-optimized comparison operations

// SIMDCompareEqualInt64 performs vectorized equality comparison
func SIMDCompareEqualInt64(a, b []int64, result []bool) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] == b[i]
		result[i+1] = a[i+1] == b[i+1]
		result[i+2] = a[i+2] == b[i+2]
		result[i+3] = a[i+3] == b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] == b[i]
	}
}

// SIMDCompareLessInt64 performs vectorized less-than comparison
func SIMDCompareLessInt64(a, b []int64, result []bool) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] < b[i]
		result[i+1] = a[i+1] < b[i+1]
		result[i+2] = a[i+2] < b[i+2]
		result[i+3] = a[i+3] < b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] < b[i]
	}
}

// SIMDCompareGreaterFloat64 performs vectorized greater-than comparison
func SIMDCompareGreaterFloat64(a, b []float64, result []bool) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] > b[i]
		result[i+1] = a[i+1] > b[i+1]
		result[i+2] = a[i+2] > b[i+2]
		result[i+3] = a[i+3] > b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] > b[i]
	}
}

// SIMD-optimized aggregation operations

// SIMDSumInt64 computes the sum of an int64 slice using vectorized operations
func SIMDSumInt64(data []int64) int64 {
	n := len(data)
	if n == 0 {
		return 0
	}
	
	if n < DefaultSIMDConfig.MinBatchSize {
		sum := int64(0)
		for _, v := range data {
			sum += v
		}
		return sum
	}
	
	// Use 4 accumulators for better parallelization
	sum0, sum1, sum2, sum3 := int64(0), int64(0), int64(0), int64(0)
	
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		sum0 += data[i]
		sum1 += data[i+1]
		sum2 += data[i+2]
		sum3 += data[i+3]
	}
	
	// Handle remaining elements
	sum := sum0 + sum1 + sum2 + sum3
	for i := vectorEnd; i < n; i++ {
		sum += data[i]
	}
	
	return sum
}

// SIMDSumFloat64 computes the sum of a float64 slice using vectorized operations
func SIMDSumFloat64(data []float64) float64 {
	n := len(data)
	if n == 0 {
		return 0.0
	}
	
	if n < DefaultSIMDConfig.MinBatchSize {
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		return sum
	}
	
	// Use 4 accumulators for better parallelization and reduced dependency chains
	sum0, sum1, sum2, sum3 := 0.0, 0.0, 0.0, 0.0
	
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		sum0 += data[i]
		sum1 += data[i+1]
		sum2 += data[i+2]
		sum3 += data[i+3]
	}
	
	sum := sum0 + sum1 + sum2 + sum3
	for i := vectorEnd; i < n; i++ {
		sum += data[i]
	}
	
	return sum
}

// SIMDMinInt64 finds the minimum value in an int64 slice
func SIMDMinInt64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	
	if len(data) == 1 {
		return data[0]
	}
	
	n := len(data)
	min0, min1, min2, min3 := data[0], data[0], data[0], data[0]
	
	vectorSize := 4
	start := 1
	vectorEnd := n - (n % vectorSize)
	
	for i := start; i < vectorEnd; i += vectorSize {
		if data[i] < min0 {
			min0 = data[i]
		}
		if i+1 < n && data[i+1] < min1 {
			min1 = data[i+1]
		}
		if i+2 < n && data[i+2] < min2 {
			min2 = data[i+2]
		}
		if i+3 < n && data[i+3] < min3 {
			min3 = data[i+3]
		}
	}
	
	// Find minimum of accumulators
	min := min0
	if min1 < min {
		min = min1
	}
	if min2 < min {
		min = min2
	}
	if min3 < min {
		min = min3
	}
	
	// Handle remaining elements
	for i := vectorEnd; i < n; i++ {
		if data[i] < min {
			min = data[i]
		}
	}
	
	return min
}

// SIMDMaxFloat64 finds the maximum value in a float64 slice
func SIMDMaxFloat64(data []float64) float64 {
	if len(data) == 0 {
		return 0.0
	}
	
	if len(data) == 1 {
		return data[0]
	}
	
	n := len(data)
	max0, max1, max2, max3 := data[0], data[0], data[0], data[0]
	
	vectorSize := 4
	start := 1
	vectorEnd := n - (n % vectorSize)
	
	for i := start; i < vectorEnd; i += vectorSize {
		if data[i] > max0 {
			max0 = data[i]
		}
		if i+1 < n && data[i+1] > max1 {
			max1 = data[i+1]
		}
		if i+2 < n && data[i+2] > max2 {
			max2 = data[i+2]
		}
		if i+3 < n && data[i+3] > max3 {
			max3 = data[i+3]
		}
	}
	
	// Find maximum of accumulators
	max := max0
	if max1 > max {
		max = max1
	}
	if max2 > max {
		max = max2
	}
	if max3 > max {
		max = max3
	}
	
	// Handle remaining elements
	for i := vectorEnd; i < n; i++ {
		if data[i] > max {
			max = data[i]
		}
	}
	
	return max
}

// SIMD-optimized filter operations

// SIMDFilterInt64Equal creates a selection vector for equal values
func SIMDFilterInt64Equal(data []int64, value int64, selection *SelectionVector) {
	selection.Clear()
	n := len(data)
	
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	// Process 4 elements at a time
	for i := 0; i < vectorEnd; i += vectorSize {
		if data[i] == value {
			selection.Add(i)
		}
		if data[i+1] == value {
			selection.Add(i + 1)
		}
		if data[i+2] == value {
			selection.Add(i + 2)
		}
		if data[i+3] == value {
			selection.Add(i + 3)
		}
	}
	
	// Handle remaining elements
	for i := vectorEnd; i < n; i++ {
		if data[i] == value {
			selection.Add(i)
		}
	}
}

// SIMDFilterFloat64Range creates a selection vector for values in range [min, max]
func SIMDFilterFloat64Range(data []float64, min, max float64, selection *SelectionVector) {
	selection.Clear()
	n := len(data)
	
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		if data[i] >= min && data[i] <= max {
			selection.Add(i)
		}
		if data[i+1] >= min && data[i+1] <= max {
			selection.Add(i + 1)
		}
		if data[i+2] >= min && data[i+2] <= max {
			selection.Add(i + 2)
		}
		if data[i+3] >= min && data[i+3] <= max {
			selection.Add(i + 3)
		}
	}
	
	for i := vectorEnd; i < n; i++ {
		if data[i] >= min && data[i] <= max {
			selection.Add(i)
		}
	}
}

// Memory operations optimized for cache efficiency

// SIMDMemcopyAligned performs cache-optimized memory copy for aligned data
func SIMDMemcopyAligned[T any](dst, src []T) {
	if len(dst) != len(src) {
		panic("slice length mismatch")
	}
	
	// Check if slices are properly aligned
	dstPtr := uintptr(unsafe.Pointer(&dst[0]))
	srcPtr := uintptr(unsafe.Pointer(&src[0]))
	
	if dstPtr%CacheLineSize == 0 && srcPtr%CacheLineSize == 0 {
		// Use optimized copy for aligned data
		copy(dst, src)
	} else {
		// Fall back to element-by-element copy
		for i := range src {
			dst[i] = src[i]
		}
	}
}

// SIMDGather gathers elements from source using indices (scatter-gather pattern)
func SIMDGatherInt64(src []int64, indices []int, dst []int64) {
	n := len(indices)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	// Process 4 indices at a time
	for i := 0; i < vectorEnd; i += vectorSize {
		dst[i] = src[indices[i]]
		dst[i+1] = src[indices[i+1]]
		dst[i+2] = src[indices[i+2]]
		dst[i+3] = src[indices[i+3]]
	}
	
	// Handle remaining elements
	for i := vectorEnd; i < n; i++ {
		dst[i] = src[indices[i]]
	}
}

// Hash operations for grouping

// SIMDHashInt64 computes hash values for int64 slice using vectorized operations
func SIMDHashInt64(data []int64, hashes []uint64) {
	if len(data) != len(hashes) {
		panic("slice length mismatch")
	}
	
	n := len(data)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	// Use a simple but fast hash function suitable for vectorization
	const multiplier = 0x9e3779b97f4a7c15 // Golden ratio multiplier
	
	for i := 0; i < vectorEnd; i += vectorSize {
		hashes[i] = uint64(data[i]) * multiplier
		hashes[i+1] = uint64(data[i+1]) * multiplier
		hashes[i+2] = uint64(data[i+2]) * multiplier
		hashes[i+3] = uint64(data[i+3]) * multiplier
	}
	
	for i := vectorEnd; i < n; i++ {
		hashes[i] = uint64(data[i]) * multiplier
	}
}

// Null handling operations

// SIMDCountNulls counts the number of null values in a null mask
func SIMDCountNulls(nullMask *NullMask) int {
	count := 0
	numWords := len(nullMask.Bits)
	
	// Process 4 uint64 words at a time
	vectorSize := 4
	vectorEnd := numWords - (numWords % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		count += popcount64(nullMask.Bits[i])
		count += popcount64(nullMask.Bits[i+1])
		count += popcount64(nullMask.Bits[i+2])
		count += popcount64(nullMask.Bits[i+3])
	}
	
	for i := vectorEnd; i < numWords; i++ {
		count += popcount64(nullMask.Bits[i])
	}
	
	return count
}

// popcount64 counts the number of set bits in a uint64
func popcount64(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

// SIMDApplyNullMask applies a null mask to filter out null values
func SIMDApplyNullMaskInt64(data []int64, nullMask *NullMask, result []int64) int {
	resultIndex := 0
	
	for i := 0; i < len(data); i++ {
		if !nullMask.IsNull(i) {
			result[resultIndex] = data[i]
			resultIndex++
		}
	}
	
	return resultIndex
}

// Vectorized string operations

// SIMDStringEqual performs vectorized string equality comparison
func SIMDStringEqual(a, b []string, result []bool) {
	n := len(a)
	vectorSize := 4
	vectorEnd := n - (n % vectorSize)
	
	for i := 0; i < vectorEnd; i += vectorSize {
		result[i] = a[i] == b[i]
		result[i+1] = a[i+1] == b[i+1]
		result[i+2] = a[i+2] == b[i+2]
		result[i+3] = a[i+3] == b[i+3]
	}
	
	for i := vectorEnd; i < n; i++ {
		result[i] = a[i] == b[i]
	}
}

// Performance monitoring for SIMD operations

type SIMDStats struct {
	OperationsCount    int64
	VectorizedOps      int64
	ScalarFallbacks    int64
	TotalElements      int64
	VectorizedElements int64
}

var simdStats SIMDStats

// GetSIMDStats returns current SIMD operation statistics
func GetSIMDStats() SIMDStats {
	return simdStats
}

// ResetSIMDStats resets SIMD operation statistics
func ResetSIMDStats() {
	simdStats = SIMDStats{}
}

// Utility functions for SIMD configuration and optimization

// IsSIMDSupported checks if SIMD operations should be used for given data size
func IsSIMDSupported(dataSize int) bool {
	return dataSize >= DefaultSIMDConfig.MinBatchSize
}

// OptimalBatchSize returns the optimal batch size for SIMD operations
func OptimalBatchSize(dataSize int) int {
	if dataSize <= DefaultBatchSize {
		return dataSize
	}
	
	// Align to vector boundaries for optimal SIMD performance
	vectorSize := 4 // Elements per SIMD vector
	return ((dataSize + vectorSize - 1) / vectorSize) * vectorSize
}

// AlignToSIMD aligns a size to SIMD vector boundaries
func AlignToSIMD(size int) int {
	vectorSize := 4
	return ((size + vectorSize - 1) / vectorSize) * vectorSize
}