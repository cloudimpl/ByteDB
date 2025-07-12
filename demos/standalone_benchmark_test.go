package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Benchmark data sizes
var benchmarkSizes = []int{1000, 10000, 100000, 1000000}

// BenchmarkFilterOperations compares filtering performance
func BenchmarkFilterOperations(b *testing.B) {
	for _, size := range benchmarkSizes {
		// Generate test data
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 1000
		}
		threshold := 500.0
		selection := make([]int, size)

		b.Run(fmt.Sprintf("ScalarFilter_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				for j, val := range data {
					if val > threshold {
						selection[count] = j
						count++
					}
				}
				b.SetBytes(int64(size * 8)) // 8 bytes per float64
			}
		})

		b.Run(fmt.Sprintf("ManualVectorizedFilter_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := manualVectorizedFilter(data, threshold, selection)
				_ = count
				b.SetBytes(int64(size * 8))
			}
		})
	}
}

// BenchmarkArithmeticOperations compares arithmetic performance
func BenchmarkArithmeticOperations(b *testing.B) {
	for _, size := range benchmarkSizes {
		a := make([]float64, size)
		bSlice := make([]float64, size)
		result := make([]float64, size)

		for i := 0; i < size; i++ {
			a[i] = rand.Float64() * 100
			bSlice[i] = rand.Float64() * 100
		}

		b.Run(fmt.Sprintf("ScalarAdd_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range a {
					result[j] = a[j] + bSlice[j]
				}
				b.SetBytes(int64(size * 8 * 3)) // 3 arrays * 8 bytes
			}
		})

		b.Run(fmt.Sprintf("ManualVectorizedAdd_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				manualVectorizedAdd(a, bSlice, result)
				b.SetBytes(int64(size * 8 * 3))
			}
		})
	}
}

// BenchmarkAggregationOperations compares aggregation performance
func BenchmarkAggregationOperations(b *testing.B) {
	for _, size := range benchmarkSizes {
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 100
		}

		b.Run(fmt.Sprintf("ScalarSum_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum := 0.0
				for _, val := range data {
					sum += val
				}
				_ = sum
				b.SetBytes(int64(size * 8))
			}
		})

		b.Run(fmt.Sprintf("ManualVectorizedSum_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum := manualVectorizedSum(data)
				_ = sum
				b.SetBytes(int64(size * 8))
			}
		})
	}
}

// BenchmarkBatchProcessing compares different batch processing approaches
func BenchmarkBatchProcessing(b *testing.B) {
	totalSize := 1000000
	batchSizes := []int{1000, 4096, 16384, 65536}

	data := make([]float64, totalSize)
	for i := 0; i < totalSize; i++ {
		data[i] = rand.Float64() * 1000
	}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				totalSum := 0.0
				for offset := 0; offset < totalSize; offset += batchSize {
					end := offset + batchSize
					if end > totalSize {
						end = totalSize
					}
					batch := data[offset:end]
					sum := manualVectorizedSum(batch)
					totalSum += sum
				}
				_ = totalSum
				b.SetBytes(int64(totalSize * 8))
			}
		})
	}
}

// Manual vectorized implementations

func manualVectorizedFilter(data []float64, threshold float64, selection []int) int {
	count := 0
	i := 0
	
	// Process 8 elements at a time (simulates AVX-512)
	for ; i < len(data)-7; i += 8 {
		if data[i] > threshold {
			selection[count] = i
			count++
		}
		if data[i+1] > threshold {
			selection[count] = i + 1
			count++
		}
		if data[i+2] > threshold {
			selection[count] = i + 2
			count++
		}
		if data[i+3] > threshold {
			selection[count] = i + 3
			count++
		}
		if data[i+4] > threshold {
			selection[count] = i + 4
			count++
		}
		if data[i+5] > threshold {
			selection[count] = i + 5
			count++
		}
		if data[i+6] > threshold {
			selection[count] = i + 6
			count++
		}
		if data[i+7] > threshold {
			selection[count] = i + 7
			count++
		}
	}
	
	// Handle remaining elements
	for ; i < len(data); i++ {
		if data[i] > threshold {
			selection[count] = i
			count++
		}
	}
	
	return count
}

func manualVectorizedAdd(a, b, result []float64) {
	i := 0
	
	// Process 8 elements at a time
	for ; i < len(a)-7; i += 8 {
		result[i] = a[i] + b[i]
		result[i+1] = a[i+1] + b[i+1]
		result[i+2] = a[i+2] + b[i+2]
		result[i+3] = a[i+3] + b[i+3]
		result[i+4] = a[i+4] + b[i+4]
		result[i+5] = a[i+5] + b[i+5]
		result[i+6] = a[i+6] + b[i+6]
		result[i+7] = a[i+7] + b[i+7]
	}
	
	// Handle remaining elements
	for ; i < len(a); i++ {
		result[i] = a[i] + b[i]
	}
}

func manualVectorizedSum(data []float64) float64 {
	// Use multiple accumulators to enable vectorization
	sum1, sum2, sum3, sum4 := 0.0, 0.0, 0.0, 0.0
	i := 0
	
	// Process 8 elements at a time with 4 parallel accumulators
	for ; i < len(data)-7; i += 8 {
		sum1 += data[i] + data[i+4]
		sum2 += data[i+1] + data[i+5]
		sum3 += data[i+2] + data[i+6]
		sum4 += data[i+3] + data[i+7]
	}
	
	// Combine accumulators
	sum := sum1 + sum2 + sum3 + sum4
	
	// Handle remaining elements
	for ; i < len(data); i++ {
		sum += data[i]
	}
	
	return sum
}

// Manual test runner for quick results
func main() {
	if len(testing.Args) == 0 {
		// Run quick performance comparison
		fmt.Println("=== Quick Performance Comparison ===")
		runQuickComparison()
		return
	}
	
	// Normal test execution
	testing.Main(func(pat, str string) (bool, error) { return true, nil },
		[]testing.InternalTest{},
		[]testing.InternalBenchmark{
			{"BenchmarkFilterOperations", BenchmarkFilterOperations},
			{"BenchmarkArithmeticOperations", BenchmarkArithmeticOperations},
			{"BenchmarkAggregationOperations", BenchmarkAggregationOperations},
			{"BenchmarkBatchProcessing", BenchmarkBatchProcessing},
		},
		[]testing.InternalExample{})
}

func runQuickComparison() {
	sizes := []int{10000, 100000, 1000000}
	
	for _, size := range sizes {
		fmt.Printf("\n📊 Dataset Size: %d elements\n", size)
		fmt.Println("─────────────────────────────────")
		
		// Test data
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 1000
		}
		
		// Filter test
		threshold := 500.0
		selection := make([]int, size)
		
		start := time.Now()
		count1 := 0
		for j, val := range data {
			if val > threshold {
				selection[count1] = j
				count1++
			}
		}
		scalarTime := time.Since(start)
		
		start = time.Now()
		count2 := manualVectorizedFilter(data, threshold, selection)
		vectorTime := time.Since(start)
		
		speedup := float64(scalarTime) / float64(vectorTime)
		throughput := float64(size) / vectorTime.Seconds() / 1000000
		
		fmt.Printf("🔍 Filter (>%.0f): %.2fx speedup, %.1f M elem/sec\n", 
			threshold, speedup, throughput)
		
		// Sum test
		start = time.Now()
		sum1 := 0.0
		for _, val := range data {
			sum1 += val
		}
		scalarSumTime := time.Since(start)
		
		start = time.Now()
		sum2 := manualVectorizedSum(data)
		vectorSumTime := time.Since(start)
		
		sumSpeedup := float64(scalarSumTime) / float64(vectorSumTime)
		sumThroughput := float64(size) / vectorSumTime.Seconds() / 1000000
		
		fmt.Printf("📈 Sum: %.2fx speedup, %.1f M elem/sec\n", 
			sumSpeedup, sumThroughput)
	}
}