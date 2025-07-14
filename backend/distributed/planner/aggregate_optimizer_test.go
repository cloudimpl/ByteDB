package planner

import (
	"bytedb/core"
	"fmt"
	"testing"
)

func TestAggregateOptimizer(t *testing.T) {
	optimizer := NewAggregateOptimizer(createTestCostEstimator())
	
	tests := []struct {
		name                 string
		query                *core.ParsedQuery
		expectedStages       int
		maxTransferBytes     int64 // Maximum expected bytes transferred to coordinator
		description          string
	}{
		{
			name: "Simple COUNT aggregation",
			query: &core.ParsedQuery{
				Type:        core.SELECT,
				TableName:   "orders",
				IsAggregate: true,
				Aggregates: []core.AggregateFunction{
					{Function: "COUNT", Column: "*", Alias: "total_count"},
				},
			},
			expectedStages:   3, // scan + partial agg + final agg
			maxTransferBytes: 1000, // Just a few numbers from each worker
			description:      "COUNT(*) should transfer minimal data",
		},
		{
			name: "GROUP BY with multiple aggregates",
			query: &core.ParsedQuery{
				Type:        core.SELECT,
				TableName:   "sales",
				IsAggregate: true,
				GroupBy:     []string{"category", "region"},
				Aggregates: []core.AggregateFunction{
					{Function: "COUNT", Column: "*", Alias: "count"},
					{Function: "SUM", Column: "amount", Alias: "total"},
					{Function: "AVG", Column: "price", Alias: "avg_price"},
				},
			},
			expectedStages:   3,
			maxTransferBytes: 100000, // Groups * columns * bytes
			description:      "Should transfer aggregated groups, not raw data",
		},
		{
			name: "High cardinality GROUP BY",
			query: &core.ParsedQuery{
				Type:        core.SELECT,
				TableName:   "events",
				IsAggregate: true,
				GroupBy:     []string{"user_id", "event_type"},
				Aggregates: []core.AggregateFunction{
					{Function: "COUNT", Column: "*", Alias: "event_count"},
				},
			},
			expectedStages:   4, // scan + local agg + shuffle + final agg
			maxTransferBytes: 1000000, // More groups but still aggregated
			description:      "High cardinality should use local pre-aggregation",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test plan
			plan := &DistributedPlan{
				ParsedQuery: tt.query,
				Partitioning: &PartitioningStrategy{
					Type:          PartitionHash,
					NumPartitions: 4,
					Colocation:    map[string]string{
						"hash_0": "worker1",
						"hash_1": "worker2", 
						"hash_2": "worker3",
						"hash_3": "worker4",
					},
				},
				Statistics: &PlanStatistics{
					EstimatedRows:  1000000,    // 1M rows
					EstimatedBytes: 1073741824, // 1GB of raw data
				},
			}
			
			context := createTestContext()
			
			// Optimize the query
			err := optimizer.OptimizeAggregateQuery(plan, tt.query, context)
			if err != nil {
				t.Fatalf("OptimizeAggregateQuery failed: %v", err)
			}
			
			// Verify stage count
			if len(plan.Stages) < tt.expectedStages {
				t.Errorf("Expected at least %d stages, got %d", tt.expectedStages, len(plan.Stages))
			}
			
			// Find the stage that transfers data to coordinator
			var coordinatorTransferBytes int64
			for _, stage := range plan.Stages {
				if stage.Type == StageAggregate && stage.Parallelism == 1 {
					// This is likely the final aggregation stage
					for _, dep := range stage.Dependencies {
						// Sum up data from dependent stages
						for _, depStage := range plan.Stages {
							if depStage.ID == dep {
								for _, fragment := range depStage.Fragments {
									coordinatorTransferBytes += fragment.EstimatedBytes
								}
							}
						}
					}
				}
			}
			
			// Verify data transfer is optimized
			if coordinatorTransferBytes > tt.maxTransferBytes {
				t.Errorf("%s: Transferred %d bytes to coordinator, expected <= %d bytes",
					tt.description, coordinatorTransferBytes, tt.maxTransferBytes)
			}
			
			// Log the improvement
			originalTransfer := plan.Statistics.EstimatedBytes
			reduction := float64(originalTransfer-coordinatorTransferBytes) / float64(originalTransfer) * 100
			t.Logf("%s: Reduced data transfer by %.2f%% (from %d to %d bytes)",
				tt.name, reduction, originalTransfer, coordinatorTransferBytes)
		})
	}
}

func TestAggregateOptimizer_AnalyzeAggregations(t *testing.T) {
	optimizer := NewAggregateOptimizer(nil)
	
	query := &core.ParsedQuery{
		IsAggregate: true,
		GroupBy:     []string{"category"},
		Aggregates: []core.AggregateFunction{
			{Function: "COUNT", Column: "*", Alias: "cnt"},
			{Function: "SUM", Column: "amount", Alias: "total"},
			{Function: "AVG", Column: "price", Alias: "avg_price"},
			{Function: "MIN", Column: "date", Alias: "first_date"},
			{Function: "MAX", Column: "date", Alias: "last_date"},
		},
	}
	
	info := optimizer.analyzeAggregations(query)
	
	// Verify function optimization
	if len(info.Functions) != 5 {
		t.Errorf("Expected 5 functions, got %d", len(info.Functions))
	}
	
	// Check AVG handling (should produce 2 intermediate columns)
	var avgFunc *AggregateFunction
	for _, f := range info.Functions {
		if f.Original.Function == "AVG" {
			avgFunc = &f
			break
		}
	}
	
	if avgFunc == nil {
		t.Fatal("AVG function not found")
	}
	
	if len(avgFunc.IntermediateColumns) != 2 {
		t.Errorf("AVG should produce 2 intermediate columns (sum and count), got %d", 
			len(avgFunc.IntermediateColumns))
	}
	
	// Verify all functions support incremental computation
	if !info.SupportsIncremental {
		t.Error("All standard aggregate functions should support incremental computation")
	}
}

func TestAggregateOptimizer_EstimateDataTransfer(t *testing.T) {
	optimizer := NewAggregateOptimizer(createTestCostEstimator())
	
	// Test case: 1 billion rows, GROUP BY with 1000 unique groups
	plan := &DistributedPlan{
		ParsedQuery: &core.ParsedQuery{
			Type:        core.SELECT,
			TableName:   "large_table",
			IsAggregate: true,
			GroupBy:     []string{"country"},
			Aggregates: []core.AggregateFunction{
				{Function: "COUNT", Column: "*", Alias: "cnt"},
				{Function: "SUM", Column: "revenue", Alias: "total_revenue"},
			},
		},
		Partitioning: &PartitioningStrategy{
			Type:          PartitionHash,
			NumPartitions: 10,
			Colocation:    createWorkerMapping(10),
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  1000000000,           // 1 billion rows
			EstimatedBytes: 100 * 1000000000,    // 100GB (100 bytes per row)
		},
	}
	
	context := createTestContext()
	
	// Analyze aggregations properly
	info := optimizer.analyzeAggregations(plan.ParsedQuery)
	// Override estimates for testing
	info.EstimatedGroups = 200    // 200 countries
	info.GroupByCardinality = 200
	
	// Create partial aggregation stage
	partialStage := optimizer.createPartialAggregationStage(plan, plan.ParsedQuery, info, context)
	
	// Calculate total data that would be transferred
	var totalPartialBytes int64
	for _, fragment := range partialStage.Fragments {
		totalPartialBytes += fragment.EstimatedBytes
	}
	
	// Verify massive reduction
	// Original: 100GB, After aggregation: ~16KB (200 groups * 80 bytes)
	expectedMaxBytes := int64(200 * 80) // groups * bytes per group
	
	if totalPartialBytes > expectedMaxBytes*2 { // Allow 2x margin
		t.Errorf("Partial aggregation produces too much data: %d bytes (expected ~%d bytes)",
			totalPartialBytes, expectedMaxBytes)
	}
	
	reduction := float64(plan.Statistics.EstimatedBytes-totalPartialBytes) / 
		float64(plan.Statistics.EstimatedBytes) * 100
	
	t.Logf("Data reduction: %.4f%% (from %d to %d bytes)", 
		reduction, plan.Statistics.EstimatedBytes, totalPartialBytes)
	
	if reduction < 99.9 {
		t.Errorf("Expected >99.9%% reduction for aggregation, got %.2f%%", reduction)
	}
}

// Helper function to create worker mapping
func createWorkerMapping(numPartitions int) map[string]string {
	mapping := make(map[string]string)
	for i := 0; i < numPartitions; i++ {
		mapping[fmt.Sprintf("hash_%d", i)] = fmt.Sprintf("worker%d", i%4)
	}
	return mapping
}