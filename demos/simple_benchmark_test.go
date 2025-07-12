package main

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkScalarSum benchmarks scalar summation
func BenchmarkScalarSum(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	
	for _, size := range sizes {
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 100
		}
		
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum := 0.0
				for _, val := range data {
					sum += val
				}
				_ = sum
			}
		})
	}
}

// BenchmarkVectorizedSum benchmarks vectorized summation
func BenchmarkVectorizedSum(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	
	for _, size := range sizes {
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 100
		}
		
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum := manualVectorizedSum(data)
				_ = sum
			}
		})
	}
}

// BenchmarkScalarFilter benchmarks scalar filtering
func BenchmarkScalarFilter(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	
	for _, size := range sizes {
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 1000
		}
		threshold := 500.0
		selection := make([]int, size)
		
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				for j, val := range data {
					if val > threshold {
						selection[count] = j
						count++
					}
				}
				_ = count
			}
		})
	}
}

// BenchmarkVectorizedFilter benchmarks vectorized filtering
func BenchmarkVectorizedFilter(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	
	for _, size := range sizes {
		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = rand.Float64() * 1000
		}
		threshold := 500.0
		selection := make([]int, size)
		
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := manualVectorizedFilter(data, threshold, selection)
				_ = count
			}
		})
	}
}

// Implementation functions

func manualVectorizedSum(data []float64) float64 {
	sum1, sum2, sum3, sum4 := 0.0, 0.0, 0.0, 0.0
	i := 0
	
	for ; i < len(data)-7; i += 8 {
		sum1 += data[i] + data[i+4]
		sum2 += data[i+1] + data[i+5]
		sum3 += data[i+2] + data[i+6]
		sum4 += data[i+3] + data[i+7]
	}
	
	sum := sum1 + sum2 + sum3 + sum4
	
	for ; i < len(data); i++ {
		sum += data[i]
	}
	
	return sum
}

func manualVectorizedFilter(data []float64, threshold float64, selection []int) int {
	count := 0
	i := 0
	
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
	
	for ; i < len(data); i++ {
		if data[i] > threshold {
			selection[count] = i
			count++
		}
	}
	
	return count
}