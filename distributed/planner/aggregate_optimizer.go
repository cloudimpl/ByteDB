package planner

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"fmt"
	"strings"
	"time"
)

// AggregateOptimizer optimizes aggregate query execution to minimize data transfer
type AggregateOptimizer struct {
	costEstimator *CostEstimator
}

// NewAggregateOptimizer creates a new aggregate optimizer
func NewAggregateOptimizer(costEstimator *CostEstimator) *AggregateOptimizer {
	return &AggregateOptimizer{
		costEstimator: costEstimator,
	}
}

// OptimizeAggregateQuery optimizes an aggregate query plan to minimize data transfer
func (ao *AggregateOptimizer) OptimizeAggregateQuery(plan *DistributedPlan, query *core.ParsedQuery, context *PlanningContext) error {
	// Clear existing stages
	plan.Stages = []ExecutionStage{}
	
	// Analyze aggregation requirements
	aggInfo := ao.analyzeAggregations(query)
	
	// Stage 1: Scan with predicate pushdown and early aggregation
	scanStage := ao.createOptimizedScanStage(plan, query, aggInfo, context)
	plan.Stages = append(plan.Stages, scanStage)
	
	// Stage 2: Local pre-aggregation (if beneficial)
	if ao.shouldUseLocalPreAggregation(aggInfo, plan.Partitioning) {
		localAggStage := ao.createLocalAggregationStage(plan, query, aggInfo, context)
		plan.Stages = append(plan.Stages, localAggStage)
	}
	
	// Stage 3: Shuffle for GROUP BY (if needed)
	if len(query.GroupBy) > 0 && ao.requiresRepartitioning(query, plan.Partitioning) {
		shuffleStage := ao.createShuffleStage(plan, query, aggInfo, context)
		plan.Stages = append(plan.Stages, shuffleStage)
	}
	
	// Stage 4: Partial aggregation on workers
	partialAggStage := ao.createPartialAggregationStage(plan, query, aggInfo, context)
	plan.Stages = append(plan.Stages, partialAggStage)
	
	// Stage 5: Final aggregation on coordinator
	finalAggStage := ao.createFinalAggregationStage(plan, query, aggInfo, context)
	plan.Stages = append(plan.Stages, finalAggStage)
	
	return nil
}

// AggregateAnalysis contains analysis of aggregation requirements
type AggregateAnalysis struct {
	Functions           []AggregateFunction
	RequiresSort        bool
	RequiresDistinct    bool
	EstimatedGroups     int64
	GroupByCardinality  int64
	PartialResultSize   int64
	SupportsIncremental bool
}

// AggregateFunction represents an optimized aggregate function
type AggregateFunction struct {
	Original        core.AggregateFunction
	PartialFunction string   // Function to use for partial aggregation
	CombineFunction string   // Function to combine partial results
	RequiredColumns []string // Columns needed for computation
	IntermediateColumns []string // Columns produced by partial aggregation
}

// analyzeAggregations analyzes the aggregation requirements
func (ao *AggregateOptimizer) analyzeAggregations(query *core.ParsedQuery) *AggregateAnalysis {
	info := &AggregateAnalysis{
		Functions:           []AggregateFunction{},
		SupportsIncremental: true,
	}
	
	// Analyze each aggregate function
	for _, agg := range query.Aggregates {
		aggFunc := ao.optimizeAggregateFunction(agg)
		info.Functions = append(info.Functions, aggFunc)
		
		// Check if all functions support incremental computation
		if !ao.supportsIncremental(agg.Function) {
			info.SupportsIncremental = false
		}
	}
	
	// Estimate group cardinality
	if len(query.GroupBy) > 0 {
		info.GroupByCardinality = ao.estimateGroupCardinality(query)
		info.EstimatedGroups = info.GroupByCardinality
	} else {
		info.EstimatedGroups = 1 // Single group for global aggregation
	}
	
	// Calculate partial result size
	info.PartialResultSize = ao.calculatePartialResultSize(info)
	
	return info
}

// optimizeAggregateFunction creates an optimized version of an aggregate function
func (ao *AggregateOptimizer) optimizeAggregateFunction(agg core.AggregateFunction) AggregateFunction {
	switch strings.ToUpper(agg.Function) {
	case "COUNT":
		if agg.Column == "*" {
			return AggregateFunction{
				Original:        agg,
				PartialFunction: "COUNT(*)",
				CombineFunction: "SUM",
				RequiredColumns: []string{},
				IntermediateColumns: []string{agg.Alias + "_partial"},
			}
		}
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("COUNT(%s)", agg.Column),
			CombineFunction: "SUM",
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_partial"},
		}
		
	case "SUM":
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("SUM(%s)", agg.Column),
			CombineFunction: "SUM",
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_partial"},
		}
		
	case "AVG":
		// AVG requires both SUM and COUNT
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("SUM(%s), COUNT(%s)", agg.Column, agg.Column),
			CombineFunction: "SUM_DIV_SUM", // Custom combine function
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_sum", agg.Alias + "_count"},
		}
		
	case "MIN":
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("MIN(%s)", agg.Column),
			CombineFunction: "MIN",
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_partial"},
		}
		
	case "MAX":
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("MAX(%s)", agg.Column),
			CombineFunction: "MAX",
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_partial"},
		}
		
	case "COUNT_DISTINCT":
		// For COUNT(DISTINCT), we need to collect unique values
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("COLLECT_SET(%s)", agg.Column), // Hypothetical function
			CombineFunction: "COUNT_DISTINCT_MERGE",
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias + "_set"},
		}
		
	default:
		// For unknown functions, pass through without optimization
		return AggregateFunction{
			Original:        agg,
			PartialFunction: fmt.Sprintf("%s(%s)", agg.Function, agg.Column),
			CombineFunction: agg.Function,
			RequiredColumns: []string{agg.Column},
			IntermediateColumns: []string{agg.Alias},
		}
	}
}

// createOptimizedScanStage creates a scan stage with pushed-down predicates
func (ao *AggregateOptimizer) createOptimizedScanStage(plan *DistributedPlan, query *core.ParsedQuery, aggInfo *AggregateAnalysis, context *PlanningContext) ExecutionStage {
	stage := ExecutionStage{
		ID:          "optimized_scan",
		Type:        StageScan,
		Fragments:   []StageFragment{},
		Parallelism: plan.Partitioning.NumPartitions,
		Timeout:     30 * time.Second,
	}
	
	// Calculate required columns (GROUP BY + aggregate columns)
	requiredColumns := make(map[string]bool)
	for _, col := range query.GroupBy {
		requiredColumns[col] = true
	}
	for _, aggFunc := range aggInfo.Functions {
		for _, col := range aggFunc.RequiredColumns {
			requiredColumns[col] = true
		}
	}
	
	// Create scan fragments with column pruning
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		columns := []string{}
		for col := range requiredColumns {
			columns = append(columns, col)
		}
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("scan_fragment_%d", i),
			TablePath:    query.TableName,
			Columns:      columns, // Only required columns
			WhereClause:  query.Where,
			FragmentType: communication.FragmentTypeScan,
			IsPartial:    true,
		}
		
		// Add partition filter
		fragment.SQL = ao.buildOptimizedScanSQL(query, columns, i, plan.Partitioning)
		
		// Estimate size based on selectivity and column pruning
		originalSize := int64(100 * 1024 * 1024) // Default 100MB per partition
		if plan.Statistics != nil && plan.Statistics.EstimatedBytes > 0 {
			originalSize = plan.Statistics.EstimatedBytes / int64(plan.Partitioning.NumPartitions)
		}
		columnRatio := float64(len(columns)) / float64(len(query.Columns))
		if columnRatio == 0 || len(query.Columns) == 0 {
			columnRatio = 0.1 // Assume 10% if no specific columns
		}
		
		estimatedRows := int64(100000) // Default 100K rows per partition
		if plan.Statistics != nil && plan.Statistics.EstimatedRows > 0 {
			estimatedRows = plan.Statistics.EstimatedRows / int64(plan.Partitioning.NumPartitions)
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    estimatedRows,
			EstimatedBytes:   int64(float64(originalSize) * columnRatio),
			RequiredMemoryMB: 100,
		}
		
		stage.Fragments = append(stage.Fragments, stageFragment)
	}
	
	return stage
}

// createPartialAggregationStage creates the partial aggregation stage
func (ao *AggregateOptimizer) createPartialAggregationStage(plan *DistributedPlan, query *core.ParsedQuery, aggInfo *AggregateAnalysis, context *PlanningContext) ExecutionStage {
	stage := ExecutionStage{
		ID:           "partial_aggregation",
		Type:         StageAggregate,
		Dependencies: []string{"optimized_scan"},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      30 * time.Second,
	}
	
	// Create partial aggregation fragments
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("partial_agg_%d", i),
			SQL:          ao.buildPartialAggregationSQL(query, aggInfo),
			FragmentType: communication.FragmentTypeAggregate,
			IsPartial:    true,
		}
		
		// Calculate output size: groups * (group columns + aggregate results)
		groupsPerPartition := aggInfo.EstimatedGroups / int64(plan.Partitioning.NumPartitions)
		if groupsPerPartition < 1 {
			groupsPerPartition = 1
		}
		
		// Size per row: group columns + intermediate aggregate columns
		bytesPerRow := int64(8 * (len(query.GroupBy) + len(aggInfo.Functions)*2)) // Rough estimate
		estimatedBytes := groupsPerPartition * bytesPerRow
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    groupsPerPartition,
			EstimatedBytes:   estimatedBytes, // Much smaller than original data!
			RequiredMemoryMB: ao.estimateAggregationMemory(aggInfo, groupsPerPartition),
		}
		
		stage.Fragments = append(stage.Fragments, stageFragment)
	}
	
	return stage
}

// createFinalAggregationStage creates the final aggregation stage
func (ao *AggregateOptimizer) createFinalAggregationStage(plan *DistributedPlan, query *core.ParsedQuery, aggInfo *AggregateAnalysis, context *PlanningContext) ExecutionStage {
	stage := ExecutionStage{
		ID:           "final_aggregation",
		Type:         StageAggregate,
		Dependencies: []string{"partial_aggregation"},
		Fragments:    []StageFragment{},
		Parallelism:  1,
		Timeout:      10 * time.Second,
	}
	
	// Single fragment on coordinator
	fragment := &communication.QueryFragment{
		ID:           "final_agg",
		SQL:          ao.buildFinalAggregationSQL(query, aggInfo),
		FragmentType: communication.FragmentTypeAggregate,
		IsPartial:    false,
	}
	
	// Final result size
	bytesPerRow := int64(8 * (len(query.GroupBy) + len(query.Aggregates)))
	estimatedBytes := aggInfo.EstimatedGroups * bytesPerRow
	
	stageFragment := StageFragment{
		ID:               fragment.ID,
		WorkerID:         ao.selectCoordinatorWorker(context),
		Fragment:         fragment,
		EstimatedRows:    aggInfo.EstimatedGroups,
		EstimatedBytes:   estimatedBytes,
		RequiredMemoryMB: ao.estimateAggregationMemory(aggInfo, aggInfo.EstimatedGroups),
	}
	
	stage.Fragments = append(stage.Fragments, stageFragment)
	
	return stage
}

// Helper methods

func (ao *AggregateOptimizer) buildOptimizedScanSQL(query *core.ParsedQuery, columns []string, partitionIndex int, partitioning *PartitioningStrategy) string {
	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), query.TableName)
	
	// Add WHERE conditions (no partition filters - data is already physically partitioned)
	conditions := []string{}
	for _, where := range query.Where {
		// Properly format value based on type
		var valueStr string
		switch v := where.Value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", v)
		default:
			valueStr = fmt.Sprintf("%v", v)
		}
		conditions = append(conditions, fmt.Sprintf("%s %s %s", where.Column, where.Operator, valueStr))
	}
	
	if len(conditions) > 0 {
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}
	
	return sql
}

func (ao *AggregateOptimizer) buildPartialAggregationSQL(query *core.ParsedQuery, aggInfo *AggregateAnalysis) string {
	selectCols := []string{}
	
	// Add GROUP BY columns
	selectCols = append(selectCols, query.GroupBy...)
	
	// Add partial aggregate functions
	for _, aggFunc := range aggInfo.Functions {
		if aggFunc.CombineFunction == "SUM_DIV_SUM" {
			// Special case for AVG
			selectCols = append(selectCols, 
				fmt.Sprintf("SUM(%s) as %s_sum", aggFunc.Original.Column, aggFunc.Original.Alias),
				fmt.Sprintf("COUNT(%s) as %s_count", aggFunc.Original.Column, aggFunc.Original.Alias))
		} else {
			selectCols = append(selectCols, 
				fmt.Sprintf("%s as %s", aggFunc.PartialFunction, aggFunc.IntermediateColumns[0]))
		}
	}
	
	sql := fmt.Sprintf("SELECT %s FROM scan_results", strings.Join(selectCols, ", "))
	
	if len(query.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
	}
	
	return sql
}

func (ao *AggregateOptimizer) buildFinalAggregationSQL(query *core.ParsedQuery, aggInfo *AggregateAnalysis) string {
	selectCols := []string{}
	
	// Add GROUP BY columns
	selectCols = append(selectCols, query.GroupBy...)
	
	// Add final aggregate functions
	for _, aggFunc := range aggInfo.Functions {
		switch aggFunc.CombineFunction {
		case "SUM":
			selectCols = append(selectCols, 
				fmt.Sprintf("SUM(%s) as %s", aggFunc.IntermediateColumns[0], aggFunc.Original.Alias))
		case "MIN":
			selectCols = append(selectCols, 
				fmt.Sprintf("MIN(%s) as %s", aggFunc.IntermediateColumns[0], aggFunc.Original.Alias))
		case "MAX":
			selectCols = append(selectCols, 
				fmt.Sprintf("MAX(%s) as %s", aggFunc.IntermediateColumns[0], aggFunc.Original.Alias))
		case "SUM_DIV_SUM":
			// AVG = SUM(sums) / SUM(counts)
			selectCols = append(selectCols, 
				fmt.Sprintf("SUM(%s_sum) / SUM(%s_count) as %s", 
					aggFunc.Original.Alias, aggFunc.Original.Alias, aggFunc.Original.Alias))
		default:
			selectCols = append(selectCols, 
				fmt.Sprintf("%s(%s) as %s", 
					aggFunc.CombineFunction, aggFunc.IntermediateColumns[0], aggFunc.Original.Alias))
		}
	}
	
	sql := fmt.Sprintf("SELECT %s FROM partial_results", strings.Join(selectCols, ", "))
	
	if len(query.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
	}
	
	// Add ORDER BY if specified
	if len(query.OrderBy) > 0 {
		orderCols := []string{}
		for _, orderBy := range query.OrderBy {
			orderCols = append(orderCols, fmt.Sprintf("%s %s", orderBy.Column, orderBy.Direction))
		}
		sql += " ORDER BY " + strings.Join(orderCols, ", ")
	}
	
	// Add LIMIT if specified
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	
	return sql
}

func (ao *AggregateOptimizer) buildPartitionFilter(partitionIndex int, partitioning *PartitioningStrategy) string {
	switch partitioning.Type {
	case PartitionHash:
		if len(partitioning.Keys) > 0 {
			return fmt.Sprintf("MOD(HASH(%s), %d) = %d", 
				partitioning.Keys[0], partitioning.NumPartitions, partitionIndex)
		}
	case PartitionTypeRange:
		if partitionIndex < len(partitioning.Ranges) {
			r := partitioning.Ranges[partitionIndex]
			return fmt.Sprintf("%s >= %v AND %s < %v", r.Column, r.Start, r.Column, r.End)
		}
	}
	return ""
}

func (ao *AggregateOptimizer) shouldUseLocalPreAggregation(aggInfo *AggregateAnalysis, partitioning *PartitioningStrategy) bool {
	// Use local pre-aggregation if we have high cardinality data
	return aggInfo.GroupByCardinality > 10000 && aggInfo.SupportsIncremental
}

func (ao *AggregateOptimizer) requiresRepartitioning(query *core.ParsedQuery, partitioning *PartitioningStrategy) bool {
	// Check if current partitioning aligns with GROUP BY
	if len(partitioning.Keys) == 0 {
		return true
	}
	
	// If partitioned by GROUP BY key, no repartitioning needed
	for _, groupCol := range query.GroupBy {
		for _, partKey := range partitioning.Keys {
			if groupCol == partKey {
				return false
			}
		}
	}
	
	return true
}

func (ao *AggregateOptimizer) createLocalAggregationStage(plan *DistributedPlan, query *core.ParsedQuery, aggInfo *AggregateAnalysis, context *PlanningContext) ExecutionStage {
	// Create a local aggregation stage to reduce data before shuffle
	return ExecutionStage{
		ID:           "local_pre_aggregation",
		Type:         StageAggregate,
		Dependencies: []string{"optimized_scan"},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      20 * time.Second,
	}
}

func (ao *AggregateOptimizer) createShuffleStage(plan *DistributedPlan, query *core.ParsedQuery, aggInfo *AggregateAnalysis, context *PlanningContext) ExecutionStage {
	// Create shuffle stage to repartition by GROUP BY keys
	return ExecutionStage{
		ID:           "shuffle_for_groupby",
		Type:         StageShuffle,
		Dependencies: []string{"local_pre_aggregation"},
		ShuffleKey:   query.GroupBy,
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      30 * time.Second,
	}
}

func (ao *AggregateOptimizer) estimateGroupCardinality(query *core.ParsedQuery) int64 {
	// This would use table statistics in a real implementation
	// For now, use heuristics
	if len(query.GroupBy) == 1 {
		return 1000 // Assume moderate cardinality
	}
	return int64(100 * len(query.GroupBy)) // Higher for multiple columns
}

func (ao *AggregateOptimizer) calculatePartialResultSize(info *AggregateAnalysis) int64 {
	// Size = number of groups * size per group
	// Size per group = group columns + aggregate columns
	columnsPerGroup := len(info.Functions) * 2 // Worst case (e.g., AVG needs 2 columns)
	bytesPerGroup := int64(8 * columnsPerGroup) // 8 bytes per value (rough estimate)
	
	return info.EstimatedGroups * bytesPerGroup
}

func (ao *AggregateOptimizer) supportsIncremental(function string) bool {
	// Most common aggregates support incremental computation
	switch strings.ToUpper(function) {
	case "COUNT", "SUM", "MIN", "MAX", "AVG":
		return true
	default:
		return false
	}
}

func (ao *AggregateOptimizer) estimateAggregationMemory(aggInfo *AggregateAnalysis, numGroups int64) int {
	// Estimate memory needed for aggregation
	// Base: hash table for groups + aggregate state
	bytesPerGroup := int64(32) // Hash table overhead
	bytesPerGroup += int64(8 * len(aggInfo.Functions)) // Aggregate state
	
	totalBytes := numGroups * bytesPerGroup
	return int(totalBytes / (1024 * 1024)) + 10 // Convert to MB and add buffer
}

func (ao *AggregateOptimizer) selectCoordinatorWorker(context *PlanningContext) string {
	if len(context.Workers) > 0 {
		return context.Workers[0].ID
	}
	return "coordinator"
}