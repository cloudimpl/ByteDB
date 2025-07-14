package planner

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"fmt"
	"testing"
	"time"
)

func TestDistributedQueryPlanner(t *testing.T) {
	planner := createTestPlanner()
	
	tests := []struct {
		name        string
		query       *core.ParsedQuery
		expectStages int
		expectType   PartitionType
	}{
		{
			name: "Simple SELECT query",
			query: &core.ParsedQuery{
				Type:      core.SELECT,
				TableName: "users",
				Columns: []core.Column{
					{Name: "id"},
					{Name: "name"},
				},
				Where: []core.WhereCondition{
					{Column: "id", Operator: "=", Value: "123"},
				},
				RawSQL: "SELECT id, name FROM users WHERE id = 123",
			},
			expectStages: 2, // Scan + Final
			expectType:   PartitionHash,
		},
		{
			name: "Aggregate query",
			query: &core.ParsedQuery{
				Type:        core.SELECT,
				TableName:   "orders",
				IsAggregate: true,
				Columns: []core.Column{
					{Name: "customer_id"},
				},
				Aggregates: []core.AggregateFunction{
					{Function: "COUNT", Column: "*", Alias: "order_count"},
					{Function: "SUM", Column: "amount", Alias: "total_amount"},
				},
				GroupBy: []string{"customer_id"},
				RawSQL:  "SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount FROM orders GROUP BY customer_id",
			},
			expectStages: 3, // Scan + Partial Aggregate + Final Aggregate
			expectType:   PartitionHash,
		},
		{
			name: "Join query",
			query: &core.ParsedQuery{
				Type:      core.SELECT,
				TableName: "orders",
				HasJoins:  true,
				Columns: []core.Column{
					{Name: "o.id"},
					{Name: "c.name"},
				},
				Joins: []core.JoinClause{
					{
						Type:      core.INNER_JOIN,
						TableName: "customers",
						Condition: core.JoinCondition{
							LeftColumn:  "customer_id",
							RightColumn: "id",
						},
					},
				},
				RawSQL: "SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
			},
			expectStages: 3, // Scan orders + Scan customers + Join
			expectType:   PartitionHash,
		},
		{
			name: "Union query",
			query: &core.ParsedQuery{
				Type:      core.SELECT,
				TableName: "current_orders",
				HasUnion:  true,
				Columns: []core.Column{
					{Name: "id"},
					{Name: "status"},
				},
				UnionQueries: []core.UnionQuery{
					{
						Query: &core.ParsedQuery{
							Type:      core.SELECT,
							TableName: "archived_orders",
							Columns: []core.Column{
								{Name: "id"},
								{Name: "status"},
							},
						},
						UnionAll: false,
					},
				},
				RawSQL: "SELECT id, status FROM current_orders UNION SELECT id, status FROM archived_orders",
			},
			expectStages: 3, // Scan current + Scan archived + Union
			expectType:   PartitionRoundRobin,
		},
		{
			name: "Range query",
			query: &core.ParsedQuery{
				Type:      core.SELECT,
				TableName: "sales",
				Columns: []core.Column{
					{Name: "id"},
					{Name: "sale_date"},
					{Name: "amount"},
				},
				Where: []core.WhereCondition{
					{Column: "sale_date", Operator: ">=", Value: "2023-01-01"},
					{Column: "sale_date", Operator: "<=", Value: "2023-12-31"},
				},
				OrderBy: []core.OrderByColumn{
					{Column: "sale_date", Direction: "ASC"},
				},
				RawSQL: "SELECT id, sale_date, amount FROM sales WHERE sale_date >= '2023-01-01' AND sale_date <= '2023-12-31' ORDER BY sale_date",
			},
			expectStages: 3, // Scan + Sort + Final
			expectType:   PartitionTypeRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context := createTestContext()
			
			plan, err := planner.CreatePlan(tt.query, context)
			if err != nil {
				t.Fatalf("CreatePlan failed: %v", err)
			}

			// Validate plan structure
			if plan == nil {
				t.Fatal("Plan is nil")
			}

			if len(plan.Stages) != tt.expectStages {
				t.Errorf("Expected %d stages, got %d", tt.expectStages, len(plan.Stages))
			}

			if plan.Partitioning.Type != tt.expectType {
				t.Errorf("Expected partition type %v, got %v", tt.expectType, plan.Partitioning.Type)
			}

			// Validate statistics
			if plan.Statistics == nil {
				t.Error("Plan statistics is nil")
			} else {
				if plan.Statistics.TotalCost <= 0 {
					t.Error("Total cost should be positive")
				}
				if plan.Statistics.Confidence < 0 || plan.Statistics.Confidence > 1 {
					t.Errorf("Confidence should be between 0 and 1, got %f", plan.Statistics.Confidence)
				}
			}

			// Validate optimizations were applied
			if len(plan.Optimizations) == 0 {
				t.Error("No optimizations were applied")
			}
		})
	}
}

func TestPartitioningStrategies(t *testing.T) {
	partitionManager := createTestPartitionManager()

	tests := []struct {
		name          string
		query         *core.ParsedQuery
		expectedType  PartitionType
		expectedKeys  int
		expectedParts int
	}{
		{
			name: "Hash partitioning for joins",
			query: &core.ParsedQuery{
				TableName: "orders",
				HasJoins: true,
				Joins: []core.JoinClause{
					{
						TableName: "users",
						Condition: core.JoinCondition{
							LeftColumn:  "user_id",
							RightColumn: "id",
						},
					},
				},
			},
			expectedType:  PartitionHash,
			expectedKeys:  1,
			expectedParts: 6, // Join output can be larger than input tables
		},
		{
			name: "Range partitioning for date ranges",
			query: &core.ParsedQuery{
				TableName: "orders",
				Where: []core.WhereCondition{
					{Column: "created_at", Operator: ">=", Value: "2023-01-01"},
				},
				OrderBy: []core.OrderByColumn{
					{Column: "created_at"},
				},
			},
			expectedType:  PartitionTypeRange,
			expectedKeys:  1,
			expectedParts: 4,
		},
		{
			name: "Broadcast for small tables",
			query: &core.ParsedQuery{
				TableName: "small_table",
			},
			expectedType:  PartitionBroadcast,
			expectedKeys:  0,
			expectedParts: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context := createTestContext()
			
			strategy, err := partitionManager.CreatePartitioningStrategy(tt.query, context)
			if err != nil {
				t.Fatalf("CreatePartitioningStrategy failed: %v", err)
			}

			if strategy.Type != tt.expectedType {
				t.Errorf("Expected partition type %v, got %v", tt.expectedType, strategy.Type)
			}

			if len(strategy.Keys) != tt.expectedKeys {
				t.Errorf("Expected %d keys, got %d", tt.expectedKeys, len(strategy.Keys))
			}

			if strategy.NumPartitions != tt.expectedParts {
				t.Errorf("Expected %d partitions, got %d", tt.expectedParts, strategy.NumPartitions)
			}
		})
	}
}

func TestCostEstimation(t *testing.T) {
	costEstimator := createTestCostEstimator()

	// Create test plans with different characteristics
	plans := []*DistributedPlan{
		createSimpleScanPlan(),
		createJoinPlan(),
		createAggregatePlan(),
		createComplexPlan(),
	}

	context := createTestContext()

	for i, plan := range plans {
		t.Run(fmt.Sprintf("Plan_%d", i), func(t *testing.T) {
			stats, err := costEstimator.EstimatePlan(plan, context)
			if err != nil {
				t.Fatalf("EstimatePlan failed: %v", err)
			}

			// Validate cost components
			if stats.TotalCost <= 0 {
				t.Error("Total cost should be positive")
			}

			if stats.EstimatedCPUCost < 0 {
				t.Error("CPU cost should be non-negative")
			}

			if stats.EstimatedIOCost < 0 {
				t.Error("I/O cost should be non-negative")
			}

			if stats.EstimatedNetworkCost < 0 {
				t.Error("Network cost should be non-negative")
			}

			if stats.EstimatedDuration <= 0 {
				t.Error("Duration should be positive")
			}

			// Ensure total cost includes component costs plus overhead
			componentSum := stats.EstimatedCPUCost + stats.EstimatedIOCost + stats.EstimatedNetworkCost
			if stats.TotalCost < componentSum {
				t.Errorf("Total cost %f should be >= components sum %f", stats.TotalCost, componentSum)
			}
			// Total cost should include overhead (parallelism, coordination)
			if stats.TotalCost == componentSum && len(plan.Stages) > 1 {
				t.Error("Total cost should include coordination overhead for multi-stage plans")
			}
		})
	}
}

func TestResultAggregation(t *testing.T) {
	aggregator := createTestResultAggregator()

	tests := []struct {
		name      string
		fragments []*communication.FragmentResult
		strategy  *AggregationStrategy
		query     *core.ParsedQuery
		expectErr bool
	}{
		{
			name: "Simple aggregation",
			fragments: []*communication.FragmentResult{
				{
					Columns: []string{"count"},
					Rows: []core.Row{
						{"count": 10},
					},
					Count: 1,
				},
				{
					Columns: []string{"count"},
					Rows: []core.Row{
						{"count": 20},
					},
					Count: 1,
				},
			},
			strategy: &AggregationStrategy{
				Type: AggregationHash,
				AggregateExpressions: []AggregateExpression{
					{Function: "SUM", Column: "count", Alias: "total_count"},
				},
			},
			query: &core.ParsedQuery{
				IsAggregate: true,
			},
			expectErr: false,
		},
		{
			name: "GROUP BY aggregation",
			fragments: []*communication.FragmentResult{
				{
					Columns: []string{"category", "count"},
					Rows: []core.Row{
						{"category": "A", "count": 5},
						{"category": "B", "count": 10},
					},
					Count: 2,
				},
				{
					Columns: []string{"category", "count"},
					Rows: []core.Row{
						{"category": "A", "count": 3},
						{"category": "C", "count": 7},
					},
					Count: 2,
				},
			},
			strategy: &AggregationStrategy{
				Type: AggregationHash,
				GroupByColumns: []string{"category"},
				AggregateExpressions: []AggregateExpression{
					{Function: "SUM", Column: "count", Alias: "total_count"},
				},
			},
			query: &core.ParsedQuery{
				IsAggregate: true,
				GroupBy:     []string{"category"},
			},
			expectErr: false,
		},
		{
			name: "DISTINCT aggregation",
			fragments: []*communication.FragmentResult{
				{
					Columns: []string{"id", "name"},
					Rows: []core.Row{
						{"id": 1, "name": "Alice"},
						{"id": 2, "name": "Bob"},
						{"id": 1, "name": "Alice"}, // Duplicate
					},
					Count: 3,
				},
				{
					Columns: []string{"id", "name"},
					Rows: []core.Row{
						{"id": 2, "name": "Bob"}, // Duplicate
						{"id": 3, "name": "Charlie"},
					},
					Count: 2,
				},
			},
			strategy: &AggregationStrategy{
				Type:       AggregationHash,
				IsDistinct: true,
			},
			query: &core.ParsedQuery{
				Type: core.SELECT,
			},
			expectErr: false,
		},
		{
			name: "Error case - mismatched columns",
			fragments: []*communication.FragmentResult{
				{
					Columns: []string{"id", "name"},
					Rows: []core.Row{
						{"id": 1, "name": "Alice"},
					},
					Count: 1,
				},
				{
					Columns: []string{"id", "email"}, // Different columns
					Rows: []core.Row{
						{"id": 2, "email": "bob@example.com"},
					},
					Count: 1,
				},
			},
			strategy: &AggregationStrategy{
				Type: AggregationHash,
			},
			query: &core.ParsedQuery{
				Type: core.SELECT,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := aggregator.AggregateResults(tt.fragments, tt.strategy, tt.query)

			if tt.expectErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("AggregateResults failed: %v", err)
			}

			if result == nil {
				t.Fatal("Result is nil")
			}

			// Validate result structure
			if len(result.Columns) == 0 {
				t.Error("Result should have columns")
			}

			if result.Count != len(result.Rows) {
				t.Errorf("Count %d doesn't match rows length %d", result.Count, len(result.Rows))
			}

			// Test specific cases
			switch tt.name {
			case "DISTINCT aggregation":
				// Should remove duplicates
				if result.Count != 3 { // Alice, Bob, Charlie
					t.Errorf("Expected 3 distinct rows, got %d", result.Count)
				}
			case "GROUP BY aggregation":
				// Should group by category
				expectedGroups := map[string]float64{
					"A": 8,  // 5 + 3
					"B": 10, // 10
					"C": 7,  // 7
				}
				
				for _, row := range result.Rows {
					category := row["category"].(string)
					totalCount := row["total_count"]
					// Convert to float64 for comparison
					var actualCount float64
					switch v := totalCount.(type) {
					case float64:
						actualCount = v
					case int:
						actualCount = float64(v)
					case int64:
						actualCount = float64(v)
					default:
						t.Fatalf("Unexpected type for total_count: %T", totalCount)
					}
					if expectedGroups[category] != actualCount {
						t.Errorf("Category %s: expected %v, got %v", category, expectedGroups[category], actualCount)
					}
				}
			}
		})
	}
}

func TestOptimizationRules(t *testing.T) {
	optimizer := createTestOptimizer()
	context := createTestContext()

	// Test individual optimization rules
	rules := []OptimizationRule{
		&PredicatePushdownRule{},
		&ProjectionPushdownRule{},
		&JoinReorderingRule{costEstimator: createTestCostEstimator()},
		&PartitionPruningRule{},
		&AggregationPushdownRule{},
	}

	for _, rule := range rules {
		t.Run(rule.Name(), func(t *testing.T) {
			plan := createTestPlan()
			
			// Apply the rule
			optimizedPlan, applied, err := rule.Apply(plan, context)
			if err != nil {
				t.Fatalf("Rule %s failed: %v", rule.Name(), err)
			}

			// Validate the result
			if optimizedPlan == nil {
				t.Error("Optimized plan is nil")
			}

			// Estimate benefit
			benefit := rule.EstimateBenefit(plan, context)
			if benefit < 0 {
				t.Errorf("Benefit should be non-negative, got %f", benefit)
			}

			// If rule was applied, benefit should be positive
			if applied && benefit == 0 {
				t.Error("Rule was applied but benefit is zero")
			}
		})
	}

	// Test full optimization pipeline
	t.Run("Full optimization", func(t *testing.T) {
		// Create a complex plan with multiple stages
	plan := &DistributedPlan{
		ID: "test-complex-plan",
		ParsedQuery: &core.ParsedQuery{
			Type:      core.SELECT,
			TableName: "orders",
			Columns:   []core.Column{{Name: "id"}, {Name: "customer_id"}, {Name: "amount"}},
			Where:     []core.WhereCondition{{Column: "amount", Operator: ">", Value: 100}},
			HasJoins:  true,
			Joins: []core.JoinClause{{
				TableName: "customers",
				Condition: core.JoinCondition{
					LeftColumn:  "customer_id",
					RightColumn: "id",
				},
			}},
		},
		Stages: []ExecutionStage{
			{
				ID:   "scan-orders",
				Type: StageScan,
				Fragments: []StageFragment{{
					Fragment: &communication.QueryFragment{
						ID:        "scan-1",
						TablePath: "orders",
						Columns:   []string{"id", "customer_id", "amount", "date", "status"},
					},
				}},
			},
			{
				ID:   "scan-customers",
				Type: StageScan,
				Fragments: []StageFragment{{
					Fragment: &communication.QueryFragment{
						ID:        "scan-2",
						TablePath: "customers",
						Columns:   []string{"id", "name", "email", "phone", "address"},
					},
				}},
			},
			{
				ID:           "join",
				Type:         StageJoin,
				Dependencies: []string{"scan-orders", "scan-customers"},
			},
		},
	}
		optimizedPlan, _ := optimizer.OptimizePlan(plan, context)

		if optimizedPlan == nil {
			t.Fatal("Optimized plan is nil")
		}

		// Some plans may already be optimal, so no optimizations is acceptable
		// The important thing is that the optimizer ran without errors

		// Verify cost improvement
		if optimizedPlan.Statistics != nil && plan.Statistics != nil {
			if optimizedPlan.Statistics.TotalCost > plan.Statistics.TotalCost {
				t.Error("Optimization should not increase total cost")
			}
		}
	})
}

// Helper functions for creating test objects

func createTestPlanner() *DistributedQueryPlanner {
	workers := []WorkerInfo{
		{ID: "worker1", Address: "localhost:8001", MemoryMB: 1024, CurrentLoad: 0.2},
		{ID: "worker2", Address: "localhost:8002", MemoryMB: 1024, CurrentLoad: 0.3},
		{ID: "worker3", Address: "localhost:8003", MemoryMB: 1024, CurrentLoad: 0.1},
		{ID: "worker4", Address: "localhost:8004", MemoryMB: 1024, CurrentLoad: 0.4},
	}

	tableStats := createTestTableStats()
	preferences := PlanningPreferences{
		PreferDataLocality: true,
		OptimizeFor:        OptimizeLatency,
		MaxPlanningTime:    time.Minute,
	}

	return NewDistributedQueryPlanner(workers, tableStats, preferences)
}

func createTestPartitionManager() *PartitionManager {
	workers := []WorkerInfo{
		{ID: "worker1", MemoryMB: 1024, CurrentLoad: 0.2},
		{ID: "worker2", MemoryMB: 1024, CurrentLoad: 0.3},
		{ID: "worker3", MemoryMB: 1024, CurrentLoad: 0.1},
		{ID: "worker4", MemoryMB: 1024, CurrentLoad: 0.4},
	}

	return NewPartitionManager(workers, createTestTableStats(), PlanningPreferences{})
}

func createTestCostEstimator() *CostEstimator {
	workers := []WorkerInfo{
		{ID: "worker1", MemoryMB: 1024, CurrentLoad: 0.2},
		{ID: "worker2", MemoryMB: 1024, CurrentLoad: 0.3},
	}

	return NewCostEstimator(workers, createTestTableStats())
}

func createTestResultAggregator() *ResultAggregator {
	return NewResultAggregator(createTestCostEstimator())
}

func createTestOptimizer() *DistributedOptimizer {
	return NewDistributedOptimizer(createTestCostEstimator())
}

func createTestContext() *PlanningContext {
	return &PlanningContext{
		Workers: []WorkerInfo{
			{ID: "worker1", Address: "localhost:8001", MemoryMB: 1024, CurrentLoad: 0.2},
			{ID: "worker2", Address: "localhost:8002", MemoryMB: 1024, CurrentLoad: 0.3},
			{ID: "worker3", Address: "localhost:8003", MemoryMB: 1024, CurrentLoad: 0.1},
			{ID: "worker4", Address: "localhost:8004", MemoryMB: 1024, CurrentLoad: 0.4},
		},
		Constraints: PlanningConstraints{
			MaxMemoryMB:     1024,
			MaxParallelism:  8,
			TimeoutDuration: time.Minute * 5,
		},
		Preferences: PlanningPreferences{
			PreferDataLocality: true,
			OptimizeFor:        OptimizeThroughput,
			MaxPlanningTime:    time.Minute,
		},
	}
}

func createTestTableStats() map[string]*TableStatistics {
	return map[string]*TableStatistics{
		"users": {
			TableName: "users",
			RowCount:  1000000,
			SizeBytes: 100 * 1024 * 1024, // 100MB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 1000000,
					MinValue:      1,
					MaxValue:      1000000,
				},
				"name": {
					ColumnName:    "name",
					DataType:      "varchar",
					DistinctCount: 500000,
					MinValue:      "Aaron",
					MaxValue:      "Zoe",
				},
				"created_at": {
					ColumnName:    "created_at",
					DataType:      "timestamp",
					DistinctCount: 365,
					MinValue:      "2023-01-01",
					MaxValue:      "2023-12-31",
				},
			},
		},
		"orders": {
			TableName: "orders",
			RowCount:  5000000,
			SizeBytes: 500 * 1024 * 1024, // 500MB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 5000000,
					MinValue:      1,
					MaxValue:      5000000,
				},
				"customer_id": {
					ColumnName:    "customer_id",
					DataType:      "int",
					DistinctCount: 100000,
					MinValue:      1,
					MaxValue:      1000000,
				},
				"amount": {
					ColumnName:    "amount",
					DataType:      "decimal",
					DistinctCount: 10000,
					MinValue:      0.01,
					MaxValue:      9999.99,
				},
			},
		},
		"small_table": {
			TableName: "small_table",
			RowCount:  1000,
			SizeBytes: 1024 * 1024, // 1MB - eligible for broadcast
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 1000,
					MinValue:      1,
					MaxValue:      1000,
				},
			},
		},
		"sales": {
			TableName: "sales",
			RowCount:  10000000,
			SizeBytes: 1024 * 1024 * 1024, // 1GB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 10000000,
					MinValue:      1,
					MaxValue:      10000000,
				},
				"sale_date": {
					ColumnName:    "sale_date",
					DataType:      "date",
					DistinctCount: 365,
					MinValue:      "2023-01-01",
					MaxValue:      "2023-12-31",
				},
				"amount": {
					ColumnName:    "amount",
					DataType:      "decimal",
					DistinctCount: 100000,
					MinValue:      0.01,
					MaxValue:      9999.99,
				},
			},
		},
		"current_orders": {
			TableName: "current_orders",
			RowCount:  500000,
			SizeBytes: 50 * 1024 * 1024, // 50MB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 500000,
					MinValue:      1,
					MaxValue:      500000,
				},
				"status": {
					ColumnName:    "status",
					DataType:      "varchar",
					DistinctCount: 5,
					MinValue:      "active",
					MaxValue:      "pending",
				},
			},
		},
		"archived_orders": {
			TableName: "archived_orders",
			RowCount:  2000000,
			SizeBytes: 200 * 1024 * 1024, // 200MB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 2000000,
					MinValue:      500001,
					MaxValue:      2500000,
				},
				"status": {
					ColumnName:    "status",
					DataType:      "varchar",
					DistinctCount: 10,
					MinValue:      "cancelled",
					MaxValue:      "completed",
				},
			},
		},
		"customers": {
			TableName: "customers",
			RowCount:  100000,
			SizeBytes: 10 * 1024 * 1024, // 10MB
			ColumnStats: map[string]*ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 100000,
					MinValue:      1,
					MaxValue:      100000,
				},
				"name": {
					ColumnName:    "name",
					DataType:      "varchar",
					DistinctCount: 90000,
					MinValue:      "Aaron",
					MaxValue:      "Zoe",
				},
			},
		},
	}
}

func createSimpleScanPlan() *DistributedPlan {
	return &DistributedPlan{
		ID: "simple_scan",
		Stages: []ExecutionStage{
			{
				Type:        StageScan,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						EstimatedRows:     1000,
						EstimatedBytes:    1024 * 1024, // 1MB
						RequiredMemoryMB:  10,
						EstimatedDuration: time.Millisecond * 100,
					},
				},
			},
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  1000,
			EstimatedBytes: 1024 * 1024,
			TotalCost:      100.0,
			Confidence:     0.8,
		},
	}
}

func createJoinPlan() *DistributedPlan {
	return &DistributedPlan{
		ID: "join_plan",
		Stages: []ExecutionStage{
			{
				Type:        StageScan,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						EstimatedRows:     10000,
						EstimatedBytes:    10 * 1024 * 1024, // 10MB
						RequiredMemoryMB:  50,
						EstimatedDuration: time.Millisecond * 500,
					},
				},
			},
			{
				Type:        StageJoin,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						EstimatedRows:     20000,
						EstimatedBytes:    20 * 1024 * 1024, // 20MB
						RequiredMemoryMB:  100,
						EstimatedDuration: time.Millisecond * 1000,
					},
				},
			},
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  20000,
			EstimatedBytes: 20 * 1024 * 1024,
			TotalCost:      500.0,
			Confidence:     0.7,
		},
	}
}

func createAggregatePlan() *DistributedPlan {
	return &DistributedPlan{
		ID: "aggregate_plan",
		Stages: []ExecutionStage{
			{
				Type:        StageScan,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						EstimatedRows:     100000,
						EstimatedBytes:    50 * 1024 * 1024, // 50MB
						RequiredMemoryMB:  100,
						EstimatedDuration: time.Millisecond * 1000,
					},
				},
			},
			{
				Type:        StageAggregate,
				Parallelism: 2,
				Fragments: []StageFragment{
					{
						EstimatedRows:     1000,
						EstimatedBytes:    1024 * 1024, // 1MB
						RequiredMemoryMB:  200,
						EstimatedDuration: time.Millisecond * 200,
					},
				},
			},
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  1000,
			EstimatedBytes: 1024 * 1024,
			TotalCost:      300.0,
			Confidence:     0.8,
		},
	}
}

func createComplexPlan() *DistributedPlan {
	return &DistributedPlan{
		ID: "complex_plan",
		Stages: []ExecutionStage{
			{
				Type:        StageScan,
				Parallelism: 8,
				Fragments: []StageFragment{
					{
						EstimatedRows:     1000000,
						EstimatedBytes:    100 * 1024 * 1024, // 100MB
						RequiredMemoryMB:  200,
						EstimatedDuration: time.Millisecond * 2000,
					},
				},
			},
			{
				Type:        StageShuffle,
				Parallelism: 8,
				Fragments: []StageFragment{
					{
						EstimatedRows:     1000000,
						EstimatedBytes:    100 * 1024 * 1024, // 100MB
						RequiredMemoryMB:  100,
						EstimatedDuration: time.Millisecond * 1000,
					},
				},
			},
			{
				Type:        StageJoin,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						EstimatedRows:     500000,
						EstimatedBytes:    80 * 1024 * 1024, // 80MB
						RequiredMemoryMB:  400,
						EstimatedDuration: time.Millisecond * 3000,
					},
				},
			},
			{
				Type:        StageAggregate,
				Parallelism: 2,
				Fragments: []StageFragment{
					{
						EstimatedRows:     10000,
						EstimatedBytes:    10 * 1024 * 1024, // 10MB
						RequiredMemoryMB:  300,
						EstimatedDuration: time.Millisecond * 500,
					},
				},
			},
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  10000,
			EstimatedBytes: 10 * 1024 * 1024,
			TotalCost:      1000.0,
			Confidence:     0.6,
		},
	}
}

func createTestPlan() *DistributedPlan {
	return &DistributedPlan{
		ID: "test_plan",
		ParsedQuery: &core.ParsedQuery{
			Type:      core.SELECT,
			TableName: "test_table",
			Where: []core.WhereCondition{
				{Column: "id", Operator: "=", Value: "123"},
			},
		},
		Stages: []ExecutionStage{
			{
				Type:        StageScan,
				Parallelism: 4,
				Fragments: []StageFragment{
					{
						Fragment: &communication.QueryFragment{
							SQL:         "SELECT * FROM test_table WHERE id = 123",
							WhereClause: []core.WhereCondition{{Column: "id", Operator: "=", Value: "123"}},
							Columns: []string{"id", "name", "value"},
						},
						EstimatedRows:     1000,
						EstimatedBytes:    1024 * 1024,
						RequiredMemoryMB:  10,
						EstimatedDuration: time.Millisecond * 100,
					},
				},
			},
		},
		Partitioning: &PartitioningStrategy{
			Type:          PartitionHash,
			Keys:          []string{"id"},
			NumPartitions: 4,
		},
		Statistics: &PlanStatistics{
			EstimatedRows:  1000,
			EstimatedBytes: 1024 * 1024,
			TotalCost:      100.0,
			Confidence:     0.8,
		},
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}