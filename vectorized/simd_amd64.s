// +build amd64
// SIMD implementations for x86_64 using AVX2 and AVX-512 instructions
// This file contains optimized assembly implementations for critical vectorized operations

#include "textflag.h"

// Constants for vector processing
#define FLOAT64_PER_AVX2 4  // 4 float64 values per 256-bit AVX2 register
#define FLOAT64_PER_AVX512 8 // 8 float64 values per 512-bit AVX-512 register

// simdAddFloat64AVX2 performs vectorized addition of two float64 slices using AVX2
// func simdAddFloat64AVX2(a, b, result []float64)
TEXT ·simdAddFloat64AVX2(SB), NOSPLIT, $0-72
    MOVQ a_base+0(FP), AX      // Load base address of slice a
    MOVQ b_base+24(FP), BX     // Load base address of slice b  
    MOVQ result_base+48(FP), CX // Load base address of result slice
    MOVQ a_len+8(FP), DX       // Load length of slices
    
    // Check if we have at least 4 elements for AVX2 processing
    CMPQ DX, $4
    JL scalar_add_fallback
    
    MOVQ $0, R9                // Initialize loop counter
    MOVQ DX, R8                
    ANDQ $-4, R8               // Round down to multiple of 4 (SIMD elements)
    
simd_add_loop:
    CMPQ R9, R8                // Compare counter with SIMD length
    JGE scalar_add_remainder   // Jump to scalar processing for remainder
    
    // Load 4 float64 values from each slice
    VMOVUPD (AX)(R9*8), Y0     // Load 4 values from a into YMM0
    VMOVUPD (BX)(R9*8), Y1     // Load 4 values from b into YMM1
    
    // Perform vectorized addition
    VADDPD Y0, Y1, Y2          // Y2 = Y0 + Y1 (4 additions in parallel)
    
    // Store result
    VMOVUPD Y2, (CX)(R9*8)     // Store result to memory
    
    ADDQ $4, R9                // Increment by 4 elements
    JMP simd_add_loop
    
scalar_add_remainder:
    // Handle remaining elements with scalar operations
    MOVQ DX, R8                // Total length
    
scalar_add_loop:
    CMPQ R9, R8                // Check if we've processed all elements
    JGE add_done
    
    // Load single float64 values
    MOVSD (AX)(R9*8), X0       // Load single value from a
    MOVSD (BX)(R9*8), X1       // Load single value from b
    ADDSD X0, X1               // Add them
    MOVSD X1, (CX)(R9*8)       // Store result
    
    INCQ R9                    // Increment counter
    JMP scalar_add_loop
    
scalar_add_fallback:
    // If less than 4 elements, use scalar processing for all
    MOVQ $0, R9
    JMP scalar_add_loop
    
add_done:
    VZEROUPPER                 // Clean up AVX state (important for performance)
    RET

// simdFilterGreaterThanFloat64AVX2 performs vectorized greater-than filtering
// Returns the count of elements that pass the filter
// func simdFilterGreaterThanFloat64AVX2(data []float64, threshold float64, selection []int) int
TEXT ·simdFilterGreaterThanFloat64AVX2(SB), NOSPLIT, $0-56
    MOVQ data_base+0(FP), AX   // Load base address of data slice
    MOVQ data_len+8(FP), BX    // Load length of data slice
    MOVSD threshold+24(FP), X0 // Load threshold value
    MOVQ selection_base+32(FP), CX // Load base address of selection slice
    
    // Broadcast threshold to all elements of YMM register
    VBROADCASTSD X0, Y1        // Y1 now contains 4 copies of threshold
    
    MOVQ $0, R9                // Loop counter (element index)
    MOVQ $0, R10               // Selection counter (number of matches)
    MOVQ BX, R8
    ANDQ $-4, R8               // Round down to multiple of 4
    
    // Check if we have enough elements for SIMD processing
    CMPQ BX, $4
    JL scalar_filter_fallback
    
simd_filter_loop:
    CMPQ R9, R8                // Check if SIMD portion is done
    JGE scalar_filter_remainder
    
    // Load 4 float64 values
    VMOVUPD (AX)(R9*8), Y0     // Load 4 data values
    
    // Compare with threshold (greater than)
    VCMPGTPD Y0, Y1, Y2        // Y2 = mask where data[i] > threshold
    
    // Extract comparison mask
    VMOVMSKPD Y2, R11          // R11 contains 4-bit mask (one bit per element)
    
    // Process mask to extract indices
    MOVQ R9, R12               // Current base index
    
    // Check each bit and add matching indices to selection array
    TESTQ $1, R11              // Test bit 0
    JZ skip_0
    MOVQ R12, (CX)(R10*8)      // Store index
    INCQ R10                   // Increment selection counter
skip_0:
    
    INCQ R12                   // Next index
    TESTQ $2, R11              // Test bit 1
    JZ skip_1
    MOVQ R12, (CX)(R10*8)
    INCQ R10
skip_1:
    
    INCQ R12
    TESTQ $4, R11              // Test bit 2
    JZ skip_2
    MOVQ R12, (CX)(R10*8)
    INCQ R10
skip_2:
    
    INCQ R12
    TESTQ $8, R11              // Test bit 3
    JZ skip_3
    MOVQ R12, (CX)(R10*8)
    INCQ R10
skip_3:
    
    ADDQ $4, R9                // Move to next group of 4 elements
    JMP simd_filter_loop
    
scalar_filter_remainder:
    // Process remaining elements with scalar operations
    MOVQ BX, R8                // Total length
    
scalar_filter_loop:
    CMPQ R9, R8
    JGE filter_done
    
    // Load single value and compare
    MOVSD (AX)(R9*8), X2
    UCOMISD X0, X2             // Compare data[i] with threshold
    JBE skip_scalar            // Jump if data[i] <= threshold
    
    // Element passes filter, add to selection
    MOVQ R9, (CX)(R10*8)
    INCQ R10
    
skip_scalar:
    INCQ R9
    JMP scalar_filter_loop
    
scalar_filter_fallback:
    // If less than 4 elements, use scalar for all
    MOVQ $0, R9
    JMP scalar_filter_loop
    
filter_done:
    MOVQ R10, ret+48(FP)       // Return selection count
    VZEROUPPER
    RET

// simdSumFloat64AVX2 computes the sum of a float64 slice using AVX2
// func simdSumFloat64AVX2(data []float64) float64
TEXT ·simdSumFloat64AVX2(SB), NOSPLIT, $0-32
    MOVQ data_base+0(FP), AX   // Load base address of data slice
    MOVQ data_len+8(FP), BX    // Load length of data slice
    
    // Initialize accumulator
    VXORPD Y0, Y0, Y0          // Clear YMM0 (will hold 4 partial sums)
    
    MOVQ $0, R9                // Loop counter
    MOVQ BX, R8
    ANDQ $-4, R8               // Round down to multiple of 4
    
    // Check if we have enough elements for SIMD
    CMPQ BX, $4
    JL scalar_sum_fallback
    
simd_sum_loop:
    CMPQ R9, R8
    JGE simd_sum_reduce
    
    // Load 4 values and add to accumulator
    VMOVUPD (AX)(R9*8), Y1     // Load 4 values
    VADDPD Y0, Y1, Y0          // Add to running sum
    
    ADDQ $4, R9
    JMP simd_sum_loop
    
simd_sum_reduce:
    // Reduce the 4 partial sums in Y0 to a single sum
    VEXTRACTF128 $1, Y0, X1    // Extract high 128 bits to X1
    VADDPD X0, X1, X0          // Add high and low parts
    VHADDPD X0, X0, X0         // Horizontal add within 128-bit register
    
    // Now X0 contains the sum, but we might have remainder elements
    MOVQ BX, R8                // Total length
    
scalar_sum_remainder:
    CMPQ R9, R8
    JGE sum_done
    
    // Add remaining elements
    MOVSD (AX)(R9*8), X1
    ADDSD X0, X1, X0
    
    INCQ R9
    JMP scalar_sum_remainder
    
scalar_sum_fallback:
    // Handle case with less than 4 elements
    VXORPD X0, X0, X0          // Clear scalar accumulator
    MOVQ $0, R9
    JMP scalar_sum_remainder
    
sum_done:
    MOVSD X0, ret+24(FP)       // Return the sum
    VZEROUPPER
    RET