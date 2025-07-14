package vectorized

import (
	"fmt"
	"math/rand"
	"testing"
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
				count := simdFilterGreaterThanManual(data, threshold, selection)
				_ = count
				b.SetBytes(int64(size * 8))
			}
		})

		b.Run(fmt.Sprintf("SIMDFilter_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := SIMDFilterGreaterThan(data, threshold, selection)
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
				simdAddFloat64Manual(a, bSlice, result)
				b.SetBytes(int64(size * 8 * 3))
			}
		})

		b.Run(fmt.Sprintf("SIMDAdd_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SIMDAddFloat64(a, bSlice, result)
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
				sum := simdSumFloat64Manual(data)
				_ = sum
				b.SetBytes(int64(size * 8))
			}
		})

		b.Run(fmt.Sprintf("SIMDSum_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum := SIMDSumFloat64(data)
				_ = sum
				b.SetBytes(int64(size * 8))
			}
		})
	}
}

// BenchmarkVectorizedOperators benchmarks the high-level vectorized operators
func BenchmarkVectorizedOperators(b *testing.B) {
	schema := &Schema{
		Fields: []*Field{
			{Name: "id", DataType: INT64},
			{Name: "value", DataType: FLOAT64},
			{Name: "category", DataType: STRING},
		},
	}

	for _, size := range []int{1000, 10000, 100000} {
		batch := NewVectorBatch(schema, size)
		categories := []string{"A", "B", "C", "D", "E"}

		// Fill with test data
		for i := 0; i < size; i++ {
			batch.AddRow([]interface{}{
				int64(i),
				rand.Float64() * 1000,
				categories[i%len(categories)],
			})
		}

		b.Run(fmt.Sprintf("VectorizedFilter_%d", size), func(b *testing.B) {
			mockInput := &MockOperator{batch: batch}
			filters := []*VectorizedFilter{
				{ColumnIndex: 1, Operator: GT, Value: 500.0},
			}
			filterOp := NewVectorizedFilterOperator(mockInput, filters)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := filterOp.Execute(nil)
				if err != nil {
					b.Fatal(err)
				}
				_ = result
				b.SetBytes(int64(size * 24)) // Rough estimate of bytes processed
			}
		})

		b.Run(fmt.Sprintf("VectorizedAggregate_%d", size), func(b *testing.B) {
			mockInput := &MockOperator{batch: batch}
			aggregates := []*VectorizedAggregate{
				{Function: COUNT, InputColumn: 0, OutputColumn: 0, OutputType: INT64},
				{Function: SUM, InputColumn: 1, OutputColumn: 1, OutputType: FLOAT64},
				{Function: AVG, InputColumn: 1, OutputColumn: 2, OutputType: FLOAT64},
			}
			groupColumns := []int{2} // Group by category
			aggOp := NewVectorizedAggregateOperator(mockInput, groupColumns, aggregates)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := aggOp.Execute(nil)
				if err != nil {
					b.Fatal(err)
				}
				_ = result
				b.SetBytes(int64(size * 24))
			}
		})
	}
}

// BenchmarkMemoryAlignment compares aligned vs unaligned memory access
func BenchmarkMemoryAlignment(b *testing.B) {
	size := 100000

	// Regular slice
	regularData := make([]float64, size)
	for i := 0; i < size; i++ {
		regularData[i] = rand.Float64() * 100
	}

	// Aligned slice
	alignedData := AlignedSliceFloat64(size, GetOptimalAlignment())
	for i := 0; i < size; i++ {
		alignedData[i] = rand.Float64() * 100
	}

	b.Run("UnalignedSum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := 0.0
			for _, val := range regularData {
				sum += val
			}
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})

	b.Run("AlignedSum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := 0.0
			for _, val := range alignedData {
				sum += val
			}
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})

	b.Run("UnalignedSIMDSum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := SIMDSumFloat64(regularData)
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})

	b.Run("AlignedSIMDSum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := SIMDSumFloat64(alignedData)
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})
}

// BenchmarkBatchSizes compares different batch sizes for vectorized operations
func BenchmarkBatchSizes(b *testing.B) {
	batchSizes := []int{100, 1000, 4096, 10000, 65536}
	totalElements := 1000000

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			data := make([]float64, totalElements)
			for i := 0; i < totalElements; i++ {
				data[i] = rand.Float64() * 1000
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				totalSum := 0.0
				for offset := 0; offset < totalElements; offset += batchSize {
					end := offset + batchSize
					if end > totalElements {
						end = totalElements
					}
					batch := data[offset:end]
					sum := SIMDSumFloat64(batch)
					totalSum += sum
				}
				_ = totalSum
				b.SetBytes(int64(totalElements * 8))
			}
		})
	}
}

// BenchmarkDataTypes compares performance across different data types
func BenchmarkDataTypes(b *testing.B) {
	size := 100000

	// Float64 data
	float64Data := make([]float64, size)
	for i := 0; i < size; i++ {
		float64Data[i] = rand.Float64() * 100
	}

	// Int64 data
	int64Data := make([]int64, size)
	for i := 0; i < size; i++ {
		int64Data[i] = rand.Int63n(1000)
	}

	// Int32 data
	int32Data := make([]int32, size)
	for i := 0; i < size; i++ {
		int32Data[i] = rand.Int31n(1000)
	}

	b.Run("Float64Sum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := SIMDSumFloat64(float64Data)
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})

	b.Run("Int64Sum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := int64(0)
			for _, val := range int64Data {
				sum += val
			}
			_ = sum
			b.SetBytes(int64(size * 8))
		}
	})

	b.Run("Int32Sum", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sum := int32(0)
			for _, val := range int32Data {
				sum += val
			}
			_ = sum
			b.SetBytes(int64(size * 4))
		}
	})
}

// BenchmarkCPUFeatureDetection benchmarks the CPU detection overhead
func BenchmarkCPUFeatureDetection(b *testing.B) {
	b.Run("GetSIMDCapabilities", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			caps := GetSIMDCapabilities()
			_ = caps
		}
	})

	b.Run("CanUseSIMD", func(b *testing.B) {
		caps := GetSIMDCapabilities()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canUse := caps.CanUseSIMD(FLOAT64, 1000)
			_ = canUse
		}
	})
}

// Mock operator for testing
type MockOperator struct {
	batch *VectorBatch
}

func (m *MockOperator) Execute(input *VectorBatch) (*VectorBatch, error) {
	return m.batch, nil
}

func (m *MockOperator) GetOutputSchema() *Schema {
	return m.batch.Schema
}

func (m *MockOperator) GetEstimatedRowCount() int {
	return m.batch.RowCount
}