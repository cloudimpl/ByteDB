// +build cgo
// +build amd64

package vectorized

/*
#cgo CFLAGS: -mavx2 -O3 -march=native
#cgo LDFLAGS: -lm

#include <immintrin.h>
#include <stdint.h>
#include <string.h>

// CGO SIMD implementations using AVX2 intrinsics
// These provide high-performance fallbacks when Go assembly isn't available

// simd_add_float64_avx2 performs vectorized addition using AVX2 intrinsics
void simd_add_float64_avx2(double* a, double* b, double* result, int64_t length) {
    int64_t simd_length = length & ~3; // Round down to multiple of 4
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);      // Load 4 doubles from a
        __m256d vb = _mm256_loadu_pd(&b[i]);      // Load 4 doubles from b
        __m256d vresult = _mm256_add_pd(va, vb);  // Add in parallel
        _mm256_storeu_pd(&result[i], vresult);    // Store result
    }
    
    // Handle remaining elements with scalar code
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = a[i] + b[i];
    }
}

// simd_multiply_float64_avx2 performs vectorized multiplication
void simd_multiply_float64_avx2(double* a, double* b, double* result, int64_t length) {
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vresult = _mm256_mul_pd(va, vb);  // Multiply in parallel
        _mm256_storeu_pd(&result[i], vresult);
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = a[i] * b[i];
    }
}

// simd_subtract_float64_avx2 performs vectorized subtraction
void simd_subtract_float64_avx2(double* a, double* b, double* result, int64_t length) {
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vresult = _mm256_sub_pd(va, vb);  // Subtract in parallel
        _mm256_storeu_pd(&result[i], vresult);
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = a[i] - b[i];
    }
}

// simd_divide_float64_avx2 performs vectorized division
void simd_divide_float64_avx2(double* a, double* b, double* result, int64_t length) {
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vresult = _mm256_div_pd(va, vb);  // Divide in parallel
        _mm256_storeu_pd(&result[i], vresult);
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = a[i] / b[i];
    }
}

// simd_filter_gt_float64_avx2 performs vectorized greater-than filtering
int64_t simd_filter_gt_float64_avx2(double* data, double threshold, int64_t* selection, int64_t length) {
    __m256d vthreshold = _mm256_set1_pd(threshold);  // Broadcast threshold
    int64_t result_count = 0;
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        __m256d vcmp = _mm256_cmp_pd(vdata, vthreshold, _CMP_GT_OQ);
        int mask = _mm256_movemask_pd(vcmp);
        
        // Extract indices where condition is true
        if (mask & 1) selection[result_count++] = i;
        if (mask & 2) selection[result_count++] = i + 1;
        if (mask & 4) selection[result_count++] = i + 2;
        if (mask & 8) selection[result_count++] = i + 3;
    }
    
    // Handle remaining elements
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] > threshold) {
            selection[result_count++] = i;
        }
    }
    
    return result_count;
}

// simd_filter_lt_float64_avx2 performs vectorized less-than filtering
int64_t simd_filter_lt_float64_avx2(double* data, double threshold, int64_t* selection, int64_t length) {
    __m256d vthreshold = _mm256_set1_pd(threshold);
    int64_t result_count = 0;
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        __m256d vcmp = _mm256_cmp_pd(vdata, vthreshold, _CMP_LT_OQ);
        int mask = _mm256_movemask_pd(vcmp);
        
        if (mask & 1) selection[result_count++] = i;
        if (mask & 2) selection[result_count++] = i + 1;
        if (mask & 4) selection[result_count++] = i + 2;
        if (mask & 8) selection[result_count++] = i + 3;
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] < threshold) {
            selection[result_count++] = i;
        }
    }
    
    return result_count;
}

// simd_filter_eq_float64_avx2 performs vectorized equality filtering
int64_t simd_filter_eq_float64_avx2(double* data, double threshold, int64_t* selection, int64_t length) {
    __m256d vthreshold = _mm256_set1_pd(threshold);
    int64_t result_count = 0;
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        __m256d vcmp = _mm256_cmp_pd(vdata, vthreshold, _CMP_EQ_OQ);
        int mask = _mm256_movemask_pd(vcmp);
        
        if (mask & 1) selection[result_count++] = i;
        if (mask & 2) selection[result_count++] = i + 1;
        if (mask & 4) selection[result_count++] = i + 2;
        if (mask & 8) selection[result_count++] = i + 3;
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] == threshold) {
            selection[result_count++] = i;
        }
    }
    
    return result_count;
}

// simd_sum_float64_avx2 computes sum using vectorized operations
double simd_sum_float64_avx2(double* data, int64_t length) {
    __m256d vsum = _mm256_setzero_pd();  // Initialize accumulator
    int64_t simd_length = length & ~3;
    
    // SIMD accumulation
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        vsum = _mm256_add_pd(vsum, vdata);
    }
    
    // Horizontal reduction of the 4 partial sums
    __m128d sum_high = _mm256_extractf128_pd(vsum, 1);
    __m128d sum_low = _mm256_castpd256_pd128(vsum);
    sum_low = _mm_add_pd(sum_low, sum_high);
    __m128d sum_final = _mm_hadd_pd(sum_low, sum_low);
    
    double result = _mm_cvtsd_f64(sum_final);
    
    // Add remaining elements
    for (int64_t i = simd_length; i < length; i++) {
        result += data[i];
    }
    
    return result;
}

// simd_min_float64_avx2 finds minimum value using vectorized operations
double simd_min_float64_avx2(double* data, int64_t length) {
    if (length == 0) {
        return __builtin_inf(); // Return positive infinity for empty array
    }
    
    __m256d vmin = _mm256_broadcast_sd(&data[0]);  // Initialize with first element
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 4; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        vmin = _mm256_min_pd(vmin, vdata);
    }
    
    // Horizontal reduction to find minimum of 4 values
    __m128d min_high = _mm256_extractf128_pd(vmin, 1);
    __m128d min_low = _mm256_castpd256_pd128(vmin);
    min_low = _mm_min_pd(min_low, min_high);
    __m128d min_shuffle = _mm_permute_pd(min_low, 1);
    min_low = _mm_min_sd(min_low, min_shuffle);
    
    double result = _mm_cvtsd_f64(min_low);
    
    // Check remaining elements
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] < result) {
            result = data[i];
        }
    }
    
    return result;
}

// simd_max_float64_avx2 finds maximum value using vectorized operations
double simd_max_float64_avx2(double* data, int64_t length) {
    if (length == 0) {
        return -__builtin_inf(); // Return negative infinity for empty array
    }
    
    __m256d vmax = _mm256_broadcast_sd(&data[0]);
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 4; i < simd_length; i += 4) {
        __m256d vdata = _mm256_loadu_pd(&data[i]);
        vmax = _mm256_max_pd(vmax, vdata);
    }
    
    __m128d max_high = _mm256_extractf128_pd(vmax, 1);
    __m128d max_low = _mm256_castpd256_pd128(vmax);
    max_low = _mm_max_pd(max_low, max_high);
    __m128d max_shuffle = _mm_permute_pd(max_low, 1);
    max_low = _mm_max_sd(max_low, max_shuffle);
    
    double result = _mm_cvtsd_f64(max_low);
    
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] > result) {
            result = data[i];
        }
    }
    
    return result;
}

// simd_compare_eq_float64_avx2 performs vectorized equality comparison
void simd_compare_eq_float64_avx2(double* a, double* b, int8_t* result, int64_t length) {
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vcmp = _mm256_cmp_pd(va, vb, _CMP_EQ_OQ);
        int mask = _mm256_movemask_pd(vcmp);
        
        // Convert mask to boolean results
        result[i] = (mask & 1) ? 1 : 0;
        result[i + 1] = (mask & 2) ? 1 : 0;
        result[i + 2] = (mask & 4) ? 1 : 0;
        result[i + 3] = (mask & 8) ? 1 : 0;
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = (a[i] == b[i]) ? 1 : 0;
    }
}

// Integer operations for completeness

// simd_add_int64_avx2 performs vectorized addition for int64
void simd_add_int64_avx2(int64_t* a, int64_t* b, int64_t* result, int64_t length) {
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256i va = _mm256_loadu_si256((__m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((__m256i*)&b[i]);
        __m256i vresult = _mm256_add_epi64(va, vb);
        _mm256_storeu_si256((__m256i*)&result[i], vresult);
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        result[i] = a[i] + b[i];
    }
}

// simd_filter_gt_int64_avx2 performs vectorized greater-than filtering for int64
int64_t simd_filter_gt_int64_avx2(int64_t* data, int64_t threshold, int64_t* selection, int64_t length) {
    __m256i vthreshold = _mm256_set1_epi64x(threshold);
    int64_t result_count = 0;
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256i vdata = _mm256_loadu_si256((__m256i*)&data[i]);
        __m256i vcmp = _mm256_cmpgt_epi64(vdata, vthreshold);
        
        // Check each element individually (no movemask for 64-bit integers)
        if (_mm256_extract_epi64(vcmp, 0)) selection[result_count++] = i;
        if (_mm256_extract_epi64(vcmp, 1)) selection[result_count++] = i + 1;
        if (_mm256_extract_epi64(vcmp, 2)) selection[result_count++] = i + 2;
        if (_mm256_extract_epi64(vcmp, 3)) selection[result_count++] = i + 3;
    }
    
    for (int64_t i = simd_length; i < length; i++) {
        if (data[i] > threshold) {
            selection[result_count++] = i;
        }
    }
    
    return result_count;
}

// simd_sum_int64_avx2 computes sum for int64 arrays
int64_t simd_sum_int64_avx2(int64_t* data, int64_t length) {
    __m256i vsum = _mm256_setzero_si256();
    int64_t simd_length = length & ~3;
    
    for (int64_t i = 0; i < simd_length; i += 4) {
        __m256i vdata = _mm256_loadu_si256((__m256i*)&data[i]);
        vsum = _mm256_add_epi64(vsum, vdata);
    }
    
    // Extract and sum the 4 partial results
    int64_t result = _mm256_extract_epi64(vsum, 0) +
                     _mm256_extract_epi64(vsum, 1) +
                     _mm256_extract_epi64(vsum, 2) +
                     _mm256_extract_epi64(vsum, 3);
    
    for (int64_t i = simd_length; i < length; i++) {
        result += data[i];
    }
    
    return result;
}
*/
import "C"
import (
	"unsafe"
)

// CGO-based SIMD implementations
// These provide high-performance alternatives when Go assembly is not available
// or for operations that are complex to implement in assembly

// SIMDAddFloat64CGO performs vectorized addition using AVX2 intrinsics
func SIMDAddFloat64CGO(a, b, result []float64) {
	if len(a) == 0 {
		return
	}
	
	C.simd_add_float64_avx2(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&result[0])),
		C.int64_t(len(a)),
	)
}

// SIMDMultiplyFloat64CGO performs vectorized multiplication using AVX2
func SIMDMultiplyFloat64CGO(a, b, result []float64) {
	if len(a) == 0 {
		return
	}
	
	C.simd_multiply_float64_avx2(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&result[0])),
		C.int64_t(len(a)),
	)
}

// SIMDSubtractFloat64CGO performs vectorized subtraction using AVX2
func SIMDSubtractFloat64CGO(a, b, result []float64) {
	if len(a) == 0 {
		return
	}
	
	C.simd_subtract_float64_avx2(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&result[0])),
		C.int64_t(len(a)),
	)
}

// SIMDDivideFloat64CGO performs vectorized division using AVX2
func SIMDDivideFloat64CGO(a, b, result []float64) {
	if len(a) == 0 {
		return
	}
	
	C.simd_divide_float64_avx2(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&result[0])),
		C.int64_t(len(a)),
	)
}

// SIMDFilterGreaterThanFloat64CGO performs vectorized filtering using AVX2
func SIMDFilterGreaterThanFloat64CGO(data []float64, threshold float64, selection []int) int {
	if len(data) == 0 {
		return 0
	}
	
	// Convert selection slice for C compatibility
	cSelection := make([]int64, len(selection))
	count := int(C.simd_filter_gt_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.double(threshold),
		(*C.int64_t)(unsafe.Pointer(&cSelection[0])),
		C.int64_t(len(data)),
	))
	
	// Convert back to int slice
	for i := 0; i < count; i++ {
		selection[i] = int(cSelection[i])
	}
	
	return count
}

// SIMDFilterLessThanFloat64CGO performs vectorized less-than filtering
func SIMDFilterLessThanFloat64CGO(data []float64, threshold float64, selection []int) int {
	if len(data) == 0 {
		return 0
	}
	
	cSelection := make([]int64, len(selection))
	count := int(C.simd_filter_lt_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.double(threshold),
		(*C.int64_t)(unsafe.Pointer(&cSelection[0])),
		C.int64_t(len(data)),
	))
	
	for i := 0; i < count; i++ {
		selection[i] = int(cSelection[i])
	}
	
	return count
}

// SIMDFilterEqualFloat64CGO performs vectorized equality filtering
func SIMDFilterEqualFloat64CGO(data []float64, threshold float64, selection []int) int {
	if len(data) == 0 {
		return 0
	}
	
	cSelection := make([]int64, len(selection))
	count := int(C.simd_filter_eq_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.double(threshold),
		(*C.int64_t)(unsafe.Pointer(&cSelection[0])),
		C.int64_t(len(data)),
	))
	
	for i := 0; i < count; i++ {
		selection[i] = int(cSelection[i])
	}
	
	return count
}

// SIMDSumFloat64CGO computes sum using vectorized operations
func SIMDSumFloat64CGO(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	
	return float64(C.simd_sum_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.int64_t(len(data)),
	))
}

// SIMDMinFloat64CGO finds minimum using vectorized operations
func SIMDMinFloat64CGO(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	
	return float64(C.simd_min_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.int64_t(len(data)),
	))
}

// SIMDMaxFloat64CGO finds maximum using vectorized operations
func SIMDMaxFloat64CGO(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	
	return float64(C.simd_max_float64_avx2(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.int64_t(len(data)),
	))
}

// SIMDCompareEqualFloat64CGO performs vectorized equality comparison
func SIMDCompareEqualFloat64CGO(a, b []float64, result []bool) {
	if len(a) == 0 {
		return
	}
	
	// Convert bool slice to int8 for C compatibility
	cResult := make([]int8, len(result))
	C.simd_compare_eq_float64_avx2(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.int8_t)(unsafe.Pointer(&cResult[0])),
		C.int64_t(len(a)),
	)
	
	// Convert back to bool slice
	for i := 0; i < len(result); i++ {
		result[i] = cResult[i] != 0
	}
}

// Integer operations

// SIMDAddInt64CGO performs vectorized addition for int64
func SIMDAddInt64CGO(a, b, result []int64) {
	if len(a) == 0 {
		return
	}
	
	C.simd_add_int64_avx2(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&result[0])),
		C.int64_t(len(a)),
	)
}

// SIMDFilterGreaterThanInt64CGO performs vectorized filtering for int64
func SIMDFilterGreaterThanInt64CGO(data []int64, threshold int64, selection []int) int {
	if len(data) == 0 {
		return 0
	}
	
	cSelection := make([]int64, len(selection))
	count := int(C.simd_filter_gt_int64_avx2(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.int64_t(threshold),
		(*C.int64_t)(unsafe.Pointer(&cSelection[0])),
		C.int64_t(len(data)),
	))
	
	for i := 0; i < count; i++ {
		selection[i] = int(cSelection[i])
	}
	
	return count
}

// SIMDSumInt64CGO computes sum for int64 arrays
func SIMDSumInt64CGO(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	
	return int64(C.simd_sum_int64_avx2(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.int64_t(len(data)),
	))
}

// High-level interface functions that automatically choose the best implementation

// AutoSIMDAddFloat64 automatically selects the best SIMD implementation
func AutoSIMDAddFloat64(a, b, result []float64) {
	caps := GetSIMDCapabilities()
	
	if caps.CanUseSIMD(FLOAT64, len(a)) {
		// Prefer Go assembly if available, fallback to CGO
		if caps.HasAVX2 {
			SIMDAddFloat64CGO(a, b, result)
		} else {
			// Use manual vectorization fallback
			simdAddFloat64Manual(a, b, result)
		}
	} else {
		// Use scalar implementation for small arrays
		for i := range a {
			result[i] = a[i] + b[i]
		}
	}
}

// AutoSIMDFilterGreaterThan automatically selects the best filtering implementation
func AutoSIMDFilterGreaterThan(data []float64, threshold float64, selection []int) int {
	caps := GetSIMDCapabilities()
	
	if caps.CanUseSIMD(FLOAT64, len(data)) {
		return SIMDFilterGreaterThanFloat64CGO(data, threshold, selection)
	} else {
		// Scalar fallback
		count := 0
		for i, val := range data {
			if val > threshold {
				selection[count] = i
				count++
			}
		}
		return count
	}
}

// AutoSIMDSum automatically selects the best sum implementation
func AutoSIMDSum(data []float64) float64 {
	caps := GetSIMDCapabilities()
	
	if caps.CanUseSIMD(FLOAT64, len(data)) {
		return SIMDSumFloat64CGO(data)
	} else {
		// Scalar fallback
		sum := 0.0
		for _, val := range data {
			sum += val
		}
		return sum
	}
}