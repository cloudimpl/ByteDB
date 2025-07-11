package main

import (
	"testing"
	"time"
)

// Benchmark cache performance with repeated queries
func BenchmarkCachePerformance(b *testing.B) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	query := "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;"
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute(query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Benchmark performance without cache
func BenchmarkWithoutCache(b *testing.B) {
	generateSampleData()
	
	// Disable cache
	cacheConfig := CacheConfig{
		MaxMemoryMB: 100,
		DefaultTTL:  5 * time.Minute,
		Enabled:     false, // Disable cache
	}
	engine := NewQueryEngineWithCache("./data", cacheConfig)
	defer engine.Close()

	query := "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;"
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute(query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Test cache effectiveness with multiple repeated queries
func TestCacheEffectiveness(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	queries := []string{
		"SELECT COUNT(*) FROM employees;",
		"SELECT AVG(salary) FROM employees;",
		"SELECT * FROM employees WHERE department = 'Engineering';",
		"SELECT name, price FROM products ORDER BY price DESC LIMIT 5;",
	}

	// Execute each query multiple times to test cache effectiveness
	repetitions := 10
	
	start := time.Now()
	for rep := 0; rep < repetitions; rep++ {
		for _, query := range queries {
			result, err := engine.Execute(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Error != "" {
				t.Fatalf("Query returned error: %s", result.Error)
			}
		}
	}
	duration := time.Since(start)

	// Check cache statistics
	stats := engine.GetCacheStats()
	totalQueries := int64(len(queries) * repetitions)
	
	if stats.TotalQueries != totalQueries {
		t.Errorf("Expected %d total queries, got %d", totalQueries, stats.TotalQueries)
	}

	// We expect cache hits after the first execution of each query
	expectedMisses := int64(len(queries))
	expectedHits := totalQueries - expectedMisses
	
	if stats.Misses != expectedMisses {
		t.Errorf("Expected %d cache misses, got %d", expectedMisses, stats.Misses)
	}
	
	if stats.Hits != expectedHits {
		t.Errorf("Expected %d cache hits, got %d", expectedHits, stats.Hits)
	}

	// Calculate hit rate
	hitRate := stats.GetHitRate()
	expectedHitRate := float64(expectedHits) / float64(totalQueries) * 100.0
	
	if hitRate != expectedHitRate {
		t.Errorf("Expected hit rate %.1f%%, got %.1f%%", expectedHitRate, hitRate)
	}

	t.Logf("Cache Performance Results:")
	t.Logf("  Total execution time: %v", duration)
	t.Logf("  Total queries: %d", stats.TotalQueries)
	t.Logf("  Cache hits: %d", stats.Hits)
	t.Logf("  Cache misses: %d", stats.Misses)
	t.Logf("  Hit rate: %.1f%%", hitRate)
	t.Logf("  Memory usage: %.2f MB", stats.GetMemoryUsageMB())
	t.Logf("  Average time per query: %v", duration/time.Duration(totalQueries))
}

// Test cache with concurrent access (simple test)
func TestCacheConcurrency(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	query := "SELECT COUNT(*) FROM employees WHERE department = 'Engineering';"
	
	// Execute the same query multiple times concurrently
	done := make(chan bool, 5)
	
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				result, err := engine.Execute(query)
				if err != nil {
					t.Errorf("Concurrent query failed: %v", err)
					done <- false
					return
				}
				if result.Error != "" {
					t.Errorf("Concurrent query returned error: %s", result.Error)
					done <- false
					return
				}
			}
			done <- true
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		success := <-done
		if !success {
			t.Fatal("Concurrent test failed")
		}
	}
	
	// Check cache stats
	stats := engine.GetCacheStats()
	t.Logf("Concurrent test results:")
	t.Logf("  Total queries: %d", stats.TotalQueries)
	t.Logf("  Cache hits: %d", stats.Hits)
	t.Logf("  Cache misses: %d", stats.Misses)
	t.Logf("  Hit rate: %.1f%%", stats.GetHitRate())
	
	// Should have high hit rate due to same query being executed repeatedly
	if stats.GetHitRate() < 80.0 {
		t.Errorf("Expected high hit rate for repeated queries, got %.1f%%", stats.GetHitRate())
	}
}