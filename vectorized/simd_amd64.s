// +build amd64

#include "textflag.h"

// func simdAddFloat64AVX2(a, b, result []float64)
TEXT ·simdAddFloat64AVX2(SB), NOSPLIT, $0-72
	MOVQ a_base+0(FP), AX      // Load base address of slice a
	MOVQ b_base+24(FP), BX     // Load base address of slice b  
	MOVQ result_base+48(FP), CX // Load base address of result slice
	MOVQ a_len+8(FP), DX       // Load length of slice a
	
	// Check if we have AVX2 support (simplified check)
	MOVQ DX, R8                // Copy length to R8
	ANDQ $-4, R8               // Round down to multiple of 4 (AVX2 processes 4 float64 at once)
	JZ scalar_add              // If less than 4 elements, use scalar
	
	MOVQ $0, R9                // Initialize loop counter
	
simd_loop:
	CMPQ R9, R8                // Compare counter with SIMD length
	JGE scalar_add             // Jump to scalar if done with SIMD part
	
	// Load 4 float64 values using AVX2
	VMOVUPD (AX)(R9*8), Y0     // Load 4 values from a[i:i+4] into Y0
	VMOVUPD (BX)(R9*8), Y1     // Load 4 values from b[i:i+4] into Y1
	
	// Perform SIMD addition
	VADDPD Y0, Y1, Y2          // Y2 = Y0 + Y1 (4 additions in parallel)
	
	// Store result
	VMOVUPD Y2, (CX)(R9*8)     // Store result[i:i+4] = Y2
	
	ADDQ $4, R9                // Increment counter by 4
	JMP simd_loop              // Continue loop
	
scalar_add:
	// Handle remaining elements (< 4) with scalar operations
	CMPQ R9, DX                // Compare counter with total length
	JGE done                   // If done, exit
	
scalar_loop:
	MOVSD (AX)(R9*8), X0       // Load a[i] into X0
	MOVSD (BX)(R9*8), X1       // Load b[i] into X1
	ADDSD X0, X1               // X1 = X0 + X1
	MOVSD X1, (CX)(R9*8)       // Store result[i] = X1
	
	INCQ R9                    // Increment counter
	CMPQ R9, DX                // Compare with total length
	JL scalar_loop             // Continue if not done
	
done:
	VZEROUPPER                 // Clean up AVX state
	RET

// func simdFilterGreaterThanFloat64AVX2(data []float64, threshold float64, selection []int) int
TEXT ·simdFilterGreaterThanFloat64AVX2(SB), NOSPLIT, $0-80
	MOVQ data_base+0(FP), AX        // Load base address of data
	MOVSD threshold+24(FP), X0      // Load threshold into X0
	MOVQ selection_base+32(FP), BX  // Load base address of selection
	MOVQ data_len+8(FP), CX         // Load data length
	
	// Broadcast threshold to all 4 elements of Y1
	VBROADCASTSD X0, Y1             // Y1 = [threshold, threshold, threshold, threshold]
	
	MOVQ $0, R8                     // Initialize data index
	MOVQ $0, R9                     // Initialize result count
	MOVQ CX, R10                    // Copy length
	ANDQ $-4, R10                   // Round down to multiple of 4
	
simd_filter_loop:
	CMPQ R8, R10                    // Compare index with SIMD length
	JGE scalar_filter               // Jump to scalar if done with SIMD
	
	// Load 4 float64 values
	VMOVUPD (AX)(R8*8), Y0          // Load data[i:i+4] into Y0
	
	// Compare with threshold (greater than)
	VCMPGTPD Y1, Y0, Y2             // Y2 = Y0 > Y1 (comparison mask)
	
	// Extract comparison mask
	VMOVMSKPD Y2, R11               // Extract mask bits to R11
	
	// Check each bit and add indices to selection if true
	TESTL $1, R11                   // Test bit 0
	JZ check_bit1
	MOVL R8, (BX)(R9*4)             // selection[result_count] = i
	INCQ R9                         // Increment result count
	
check_bit1:
	TESTL $2, R11                   // Test bit 1
	JZ check_bit2
	MOVL R8, R12                    // Copy index
	INCL R12                        // i + 1
	MOVL R12, (BX)(R9*4)            // selection[result_count] = i + 1
	INCQ R9                         // Increment result count
	
check_bit2:
	TESTL $4, R11                   // Test bit 2
	JZ check_bit3
	MOVL R8, R12                    // Copy index
	ADDL $2, R12                    // i + 2
	MOVL R12, (BX)(R9*4)            // selection[result_count] = i + 2
	INCQ R9                         // Increment result count
	
check_bit3:
	TESTL $8, R11                   // Test bit 3
	JZ continue_simd_filter
	MOVL R8, R12                    // Copy index
	ADDL $3, R12                    // i + 3
	MOVL R12, (BX)(R9*4)            // selection[result_count] = i + 3
	INCQ R9                         // Increment result count
	
continue_simd_filter:
	ADDQ $4, R8                     // Increment index by 4
	JMP simd_filter_loop            // Continue loop
	
scalar_filter:
	// Handle remaining elements with scalar operations
	CMPQ R8, CX                     // Compare index with total length
	JGE filter_done                 // If done, exit
	
scalar_filter_loop:
	MOVSD (AX)(R8*8), X2            // Load data[i] into X2
	UCOMISD X0, X2                  // Compare data[i] with threshold
	JBE scalar_filter_continue      // Jump if data[i] <= threshold
	
	// data[i] > threshold, add to selection
	MOVL R8, (BX)(R9*4)             // selection[result_count] = i
	INCQ R9                         // Increment result count
	
scalar_filter_continue:
	INCQ R8                         // Increment index
	CMPQ R8, CX                     // Compare with total length
	JL scalar_filter_loop           // Continue if not done
	
filter_done:
	MOVQ R9, ret+72(FP)             // Return result count
	VZEROUPPER                      // Clean up AVX state
	RET

// func simdSumFloat64AVX2(data []float64) float64
TEXT ·simdSumFloat64AVX2(SB), NOSPLIT, $0-32
	MOVQ data_base+0(FP), AX        // Load base address of data
	MOVQ data_len+8(FP), BX         // Load data length
	
	// Initialize accumulator to zero
	VXORPD Y0, Y0, Y0               // Y0 = [0.0, 0.0, 0.0, 0.0]
	
	MOVQ $0, R8                     // Initialize index
	MOVQ BX, R9                     // Copy length
	ANDQ $-4, R9                    // Round down to multiple of 4
	
simd_sum_loop:
	CMPQ R8, R9                     // Compare index with SIMD length
	JGE scalar_sum                  // Jump to scalar if done with SIMD
	
	// Load 4 float64 values and add to accumulator
	VMOVUPD (AX)(R8*8), Y1          // Load data[i:i+4] into Y1
	VADDPD Y0, Y1, Y0               // Y0 = Y0 + Y1 (accumulate)
	
	ADDQ $4, R8                     // Increment index by 4
	JMP simd_sum_loop               // Continue loop
	
scalar_sum:
	// Handle remaining elements with scalar operations
	CMPQ R8, BX                     // Compare index with total length
	JGE horizontal_sum              // If done, compute horizontal sum
	
scalar_sum_loop:
	MOVSD (AX)(R8*8), X1            // Load data[i] into X1
	ADDSD X1, X0                    // Add to scalar accumulator (X0)
	
	INCQ R8                         // Increment index
	CMPQ R8, BX                     // Compare with total length
	JL scalar_sum_loop              // Continue if not done
	
horizontal_sum:
	// Compute horizontal sum of Y0 (4 elements -> 1 element)
	VEXTRACTF128 $1, Y0, X1         // Extract high 128 bits to X1
	VADDPD X0, X1, X0               // Add high and low parts
	VHADDPD X0, X0, X0              // Horizontal add within X0
	
	MOVSD X0, ret+24(FP)            // Return sum
	VZEROUPPER                      // Clean up AVX state
	RET