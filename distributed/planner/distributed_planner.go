package planner

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"crypto/md5"
	"fmt"
	"log"
	"strings"
	"time"
)

// DistributedQueryPlanner creates and optimizes distributed execution plans
type DistributedQueryPlanner struct {
	partitionManager *PartitionManager
	costEstimator    *CostEstimator
	optimizer        *DistributedOptimizer
	planCache        map[string]*DistributedPlan
}

// NewDistributedQueryPlanner creates a new distributed query planner
func NewDistributedQueryPlanner(workers []WorkerInfo, tableStats map[string]*TableStatistics, preferences PlanningPreferences) *DistributedQueryPlanner {
	partitionManager := NewPartitionManager(workers, tableStats, preferences)
	costEstimator := NewCostEstimator(workers, tableStats)
	optimizer := NewDistributedOptimizer(costEstimator)
	
	return &DistributedQueryPlanner{
		partitionManager: partitionManager,
		costEstimator:    costEstimator,
		optimizer:        optimizer,
		planCache:        make(map[string]*DistributedPlan),
	}
}

// CreatePlan creates a comprehensive distributed execution plan
func (dqp *DistributedQueryPlanner) CreatePlan(query *core.ParsedQuery, context *PlanningContext) (*DistributedPlan, error) {
	startTime := time.Now()
	
	// Check cache first
	cacheKey := dqp.generateCacheKey(query, context)
	if cachedPlan, exists := dqp.planCache[cacheKey]; exists {
		log.Printf("Using cached plan for query: %s", query.RawSQL)
		return cachedPlan, nil
	}
	
	log.Printf("Creating distributed plan for query: %s", query.RawSQL)
	
	// Create initial plan structure
	plan := &DistributedPlan{
		ID:          fmt.Sprintf("plan_%d", time.Now().UnixNano()),
		OriginalSQL: query.RawSQL,
		ParsedQuery: query,
		Stages:      []ExecutionStage{},
		CreatedAt:   time.Now(),
	}
	
	// Determine partitioning strategy
	partitioning, err := dqp.partitionManager.CreatePartitioningStrategy(query, context)
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioning strategy: %v", err)
	}
	plan.Partitioning = partitioning
	
	// Build execution stages based on query type
	switch {
	case query.HasJoins:
		err = dqp.planJoinQuery(plan, query, context)
	case query.HasUnion:
		err = dqp.planUnionQuery(plan, query, context)
	case query.IsAggregate:
		err = dqp.planAggregateQuery(plan, query, context)
	default:
		err = dqp.planSimpleQuery(plan, query, context)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %v", err)
	}
	
	// Create data flow graph
	plan.DataFlow = dqp.createDataFlowGraph(plan)
	
	// Estimate costs
	statistics, err := dqp.costEstimator.EstimatePlan(plan, context)
	if err != nil {
		log.Printf("Warning: cost estimation failed: %v", err)
		// Continue with default statistics
		statistics = &PlanStatistics{
			EstimatedRows:     1000,
			EstimatedBytes:    1024 * 1024, // 1MB default
			EstimatedDuration: time.Second,
			TotalCost:         100.0,
			Confidence:        0.5,
		}
	}
	plan.Statistics = statistics
	
	// Apply optimizations
	optimizedPlan, optimizations := dqp.optimizer.OptimizePlan(plan, context)
	optimizedPlan.Optimizations = optimizations
	
	// Validate the plan
	validation := optimizedPlan.Validate()
	if !validation.IsValid {
		return nil, fmt.Errorf("generated invalid plan: %v", validation.Errors)
	}
	
	// Cache the plan
	dqp.planCache[cacheKey] = optimizedPlan
	
	planningTime := time.Since(startTime)
	log.Printf("Created distributed plan in %v with %d stages, estimated cost: %.2f", 
		planningTime, len(optimizedPlan.Stages), optimizedPlan.Statistics.TotalCost)
	
	return optimizedPlan, nil
}

// planSimpleQuery creates a plan for simple SELECT queries
func (dqp *DistributedQueryPlanner) planSimpleQuery(plan *DistributedPlan, query *core.ParsedQuery, context *PlanningContext) error {
	// Stage 1: Parallel scan and filter
	scanStage := ExecutionStage{
		ID:           "scan_stage",
		Type:         StageScan,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      30 * time.Second,
	}
	
	// Create fragments for each partition
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("scan_fragment_%d", i),
			SQL:          dqp.buildScanSQL(query, i, plan.Partitioning),
			TablePath:    query.TableName,
			Columns:      dqp.extractColumnNames(query.Columns),
			WhereClause:  query.Where,
			Limit:        query.Limit,
			FragmentType: communication.FragmentTypeScan,
			IsPartial:    plan.Partitioning.NumPartitions > 1,
		}
		
		// Use default estimates if statistics not available yet
		estimatedRows := int64(1000)
		estimatedBytes := int64(1024 * 1024)
		if plan.Statistics != nil {
			estimatedRows = plan.Statistics.EstimatedRows / int64(plan.Partitioning.NumPartitions)
			estimatedBytes = plan.Statistics.EstimatedBytes / int64(plan.Partitioning.NumPartitions)
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    estimatedRows,
			EstimatedBytes:   estimatedBytes,
			RequiredMemoryMB: 100, // Base memory requirement
		}
		
		scanStage.Fragments = append(scanStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, scanStage)
	
	// Stage 2: Final result collection (if needed)
	if plan.Partitioning.NumPartitions > 1 {
		finalStage := ExecutionStage{
			ID:           "final_stage",
			Type:         StageFinal,
			Dependencies: []string{"scan_stage"},
			Fragments:    []StageFragment{},
			Parallelism:  1,
			Timeout:      10 * time.Second,
		}
		
		// Single fragment to collect and merge results
		// Use default estimates if statistics not available yet
		estimatedRows = int64(1000)
		estimatedBytes = int64(1024 * 1024)
		if plan.Statistics != nil {
			estimatedRows = plan.Statistics.EstimatedRows
			estimatedBytes = plan.Statistics.EstimatedBytes
		}
		
		finalFragment := StageFragment{
			ID:               "final_fragment",
			WorkerID:         dqp.selectCoordinatorWorker(context),
			Fragment:         dqp.createFinalFragment(query),
			EstimatedRows:    estimatedRows,
			EstimatedBytes:   estimatedBytes,
			RequiredMemoryMB: int(estimatedBytes / (1024 * 1024)), // 1MB per MB of data
		}
		
		finalStage.Fragments = append(finalStage.Fragments, finalFragment)
		plan.Stages = append(plan.Stages, finalStage)
	}
	
	return nil
}

// planAggregateQuery creates a plan for aggregate queries
func (dqp *DistributedQueryPlanner) planAggregateQuery(plan *DistributedPlan, query *core.ParsedQuery, context *PlanningContext) error {
	// Stage 1: Partial aggregation on each worker
	partialAggStage := ExecutionStage{
		ID:           "partial_agg_stage",
		Type:         StageAggregate,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      60 * time.Second,
	}
	
	// Create partial aggregation fragments
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("partial_agg_fragment_%d", i),
			SQL:          dqp.buildPartialAggregationSQL(query, i, plan.Partitioning),
			TablePath:    query.TableName,
			Columns:      dqp.extractColumnNames(query.Columns),
			WhereClause:  query.Where,
			GroupBy:      query.GroupBy,
			Aggregates:   dqp.convertAggregates(query.Aggregates),
			FragmentType: communication.FragmentTypeAggregate,
			IsPartial:    true,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    plan.Statistics.EstimatedRows / int64(plan.Partitioning.NumPartitions),
			EstimatedBytes:   plan.Statistics.EstimatedBytes / int64(plan.Partitioning.NumPartitions),
			RequiredMemoryMB: 200, // More memory for aggregation
		}
		
		partialAggStage.Fragments = append(partialAggStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, partialAggStage)
	
	// Stage 2: Final aggregation
	finalAggStage := ExecutionStage{
		ID:           "final_agg_stage",
		Type:         StageAggregate,
		Dependencies: []string{"partial_agg_stage"},
		Fragments:    []StageFragment{},
		Parallelism:  1,
		Timeout:      30 * time.Second,
	}
	
	// Single fragment for final aggregation
	finalAggFragment := StageFragment{
		ID:       "final_agg_fragment",
		WorkerID: dqp.selectCoordinatorWorker(context),
		Fragment: &communication.QueryFragment{
			ID:           "final_agg_fragment",
			SQL:          dqp.buildFinalAggregationSQL(query),
			FragmentType: communication.FragmentTypeAggregate,
			IsPartial:    false,
		},
		EstimatedRows:    int64(len(query.GroupBy)) * 100, // Estimated groups
		EstimatedBytes:   1024 * 1024,                     // 1MB for final result
		RequiredMemoryMB: 500,                             // Memory for final aggregation
	}
	
	finalAggStage.Fragments = append(finalAggStage.Fragments, finalAggFragment)
	plan.Stages = append(plan.Stages, finalAggStage)
	
	return nil
}

// planJoinQuery creates a plan for join queries
func (dqp *DistributedQueryPlanner) planJoinQuery(plan *DistributedPlan, query *core.ParsedQuery, context *PlanningContext) error {
	if len(query.Joins) == 0 {
		return fmt.Errorf("no joins found in join query")
	}
	
	// Analyze join to determine strategy
	joinInfo := dqp.analyzeJoin(query, context)
	
	switch joinInfo.Strategy {
	case JoinBroadcast:
		return dqp.planBroadcastJoin(plan, query, joinInfo, context)
	case JoinShuffle:
		return dqp.planShuffleJoin(plan, query, joinInfo, context)
	case JoinPartitioned:
		return dqp.planPartitionedJoin(plan, query, joinInfo, context)
	default:
		return dqp.planShuffleJoin(plan, query, joinInfo, context) // Default to shuffle
	}
}

// planUnionQuery creates a plan for UNION queries
func (dqp *DistributedQueryPlanner) planUnionQuery(plan *DistributedPlan, query *core.ParsedQuery, context *PlanningContext) error {
	// Stage 1: Execute each UNION branch in parallel
	unionStage := ExecutionStage{
		ID:           "union_stage",
		Type:         StageUnion,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  len(query.UnionQueries),
		Timeout:      60 * time.Second,
	}
	
	// Create fragment for each UNION branch
	for i, unionQuery := range query.UnionQueries {
		workerID := dqp.selectWorkerForUnionBranch(i, context)
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("union_fragment_%d", i),
			SQL:          unionQuery.Query.RawSQL,
			FragmentType: communication.FragmentTypeScan,
			IsPartial:    true,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    1000, // Estimated per branch
			EstimatedBytes:   1024 * 1024,
			RequiredMemoryMB: 100,
		}
		
		unionStage.Fragments = append(unionStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, unionStage)
	
	// Stage 2: Union and deduplicate (if not UNION ALL)
	finalUnionStage := ExecutionStage{
		ID:           "final_union_stage",
		Type:         StageFinal,
		Dependencies: []string{"union_stage"},
		Fragments:    []StageFragment{},
		Parallelism:  1,
		Timeout:      30 * time.Second,
	}
	
	finalFragment := StageFragment{
		ID:       "final_union_fragment",
		WorkerID: dqp.selectCoordinatorWorker(context),
		Fragment: &communication.QueryFragment{
			ID:           "final_union_fragment",
			SQL:          dqp.buildUnionFinalSQL(query),
			FragmentType: communication.FragmentTypeAggregate,
			IsPartial:    false,
		},
		EstimatedRows:    plan.Statistics.EstimatedRows,
		EstimatedBytes:   plan.Statistics.EstimatedBytes,
		RequiredMemoryMB: 200,
	}
	
	finalUnionStage.Fragments = append(finalUnionStage.Fragments, finalFragment)
	plan.Stages = append(plan.Stages, finalUnionStage)
	
	return nil
}

// planBroadcastJoin creates a broadcast join plan
func (dqp *DistributedQueryPlanner) planBroadcastJoin(plan *DistributedPlan, query *core.ParsedQuery, joinInfo *JoinInfo, context *PlanningContext) error {
	// Stage 1: Broadcast smaller table
	broadcastStage := ExecutionStage{
		ID:           "broadcast_stage",
		Type:         StageScan,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  len(context.Workers),
		Timeout:      30 * time.Second,
	}
	
	// Create broadcast fragments for smaller table
	for i, worker := range context.Workers {
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("broadcast_fragment_%d", i),
			SQL:          dqp.buildBroadcastSQL(joinInfo.BroadcastTable),
			TablePath:    joinInfo.BroadcastTable,
			FragmentType: communication.FragmentTypeScan,
			IsPartial:    false,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         worker.ID,
			Fragment:         fragment,
			EstimatedRows:    1000, // Smaller table
			EstimatedBytes:   1024 * 1024,
			RequiredMemoryMB: 50,
		}
		
		broadcastStage.Fragments = append(broadcastStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, broadcastStage)
	
	// Stage 2: Local join on each worker
	joinStage := ExecutionStage{
		ID:           "join_stage",
		Type:         StageJoin,
		Dependencies: []string{"broadcast_stage"},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      60 * time.Second,
	}
	
	// Create join fragments
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("join_fragment_%d", i),
			SQL:          dqp.buildJoinSQL(query, joinInfo, i),
			FragmentType: communication.FragmentTypeJoin,
			IsPartial:    true,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    joinInfo.EstimatedOutput / int64(plan.Partitioning.NumPartitions),
			EstimatedBytes:   plan.Statistics.EstimatedBytes,
			RequiredMemoryMB: 300, // More memory for joins
		}
		
		joinStage.Fragments = append(joinStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, joinStage)
	
	return nil
}

// planShuffleJoin creates a shuffle join plan
func (dqp *DistributedQueryPlanner) planShuffleJoin(plan *DistributedPlan, query *core.ParsedQuery, joinInfo *JoinInfo, context *PlanningContext) error {
	// Stage 1: Scan and partition both tables
	scanStage := ExecutionStage{
		ID:           "scan_stage",
		Type:         StageScan,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions * 2, // Both tables
		Timeout:      60 * time.Second,
	}
	
	// Create scan fragments for both tables
	tables := []string{query.TableName, query.Joins[0].TableName}
	for _, tableName := range tables {
		for i := 0; i < plan.Partitioning.NumPartitions; i++ {
			partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
			workerID := plan.Partitioning.Colocation[partitionID]
			
			fragment := &communication.QueryFragment{
				ID:           fmt.Sprintf("scan_%s_fragment_%d", tableName, i),
				SQL:          dqp.buildPartitionedScanSQL(tableName, query, i, plan.Partitioning),
				TablePath:    tableName,
				FragmentType: communication.FragmentTypeScan,
				IsPartial:    true,
			}
			
			stageFragment := StageFragment{
				ID:               fragment.ID,
				WorkerID:         workerID,
				Fragment:         fragment,
				EstimatedRows:    plan.Statistics.EstimatedRows / int64(plan.Partitioning.NumPartitions),
				EstimatedBytes:   plan.Statistics.EstimatedBytes / int64(plan.Partitioning.NumPartitions),
				RequiredMemoryMB: 150,
			}
			
			scanStage.Fragments = append(scanStage.Fragments, stageFragment)
		}
	}
	
	plan.Stages = append(plan.Stages, scanStage)
	
	// Stage 2: Shuffle and join
	joinStage := ExecutionStage{
		ID:           "shuffle_join_stage",
		Type:         StageJoin,
		Dependencies: []string{"scan_stage"},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		ShuffleKey:   joinInfo.JoinConditions,
		Timeout:      90 * time.Second,
	}
	
	// Create join fragments
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("shuffle_join_fragment_%d", i),
			SQL:          dqp.buildShuffleJoinSQL(query, joinInfo, i),
			FragmentType: communication.FragmentTypeJoin,
			IsPartial:    true,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    joinInfo.EstimatedOutput / int64(plan.Partitioning.NumPartitions),
			EstimatedBytes:   plan.Statistics.EstimatedBytes * 2, // Join expansion
			RequiredMemoryMB: 500, // High memory for shuffle join
		}
		
		joinStage.Fragments = append(joinStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, joinStage)
	
	return nil
}

// planPartitionedJoin creates a partitioned join plan (when data is already co-located)
func (dqp *DistributedQueryPlanner) planPartitionedJoin(plan *DistributedPlan, query *core.ParsedQuery, joinInfo *JoinInfo, context *PlanningContext) error {
	// Single stage: local join on each partition
	joinStage := ExecutionStage{
		ID:           "partitioned_join_stage",
		Type:         StageJoin,
		Dependencies: []string{},
		Fragments:    []StageFragment{},
		Parallelism:  plan.Partitioning.NumPartitions,
		Timeout:      60 * time.Second,
	}
	
	// Create join fragments for each partition
	for i := 0; i < plan.Partitioning.NumPartitions; i++ {
		partitionID := fmt.Sprintf("%s_%d", plan.Partitioning.Type, i)
		workerID := plan.Partitioning.Colocation[partitionID]
		
		fragment := &communication.QueryFragment{
			ID:           fmt.Sprintf("partitioned_join_fragment_%d", i),
			SQL:          dqp.buildPartitionedJoinSQL(query, joinInfo, i),
			FragmentType: communication.FragmentTypeJoin,
			IsPartial:    true,
		}
		
		stageFragment := StageFragment{
			ID:               fragment.ID,
			WorkerID:         workerID,
			Fragment:         fragment,
			EstimatedRows:    joinInfo.EstimatedOutput / int64(plan.Partitioning.NumPartitions),
			EstimatedBytes:   plan.Statistics.EstimatedBytes,
			RequiredMemoryMB: 250,
		}
		
		joinStage.Fragments = append(joinStage.Fragments, stageFragment)
	}
	
	plan.Stages = append(plan.Stages, joinStage)
	
	return nil
}

// Helper functions for SQL generation

func (dqp *DistributedQueryPlanner) buildScanSQL(query *core.ParsedQuery, partitionIndex int, partitioning *PartitioningStrategy) string {
	// Build SQL with partition-specific WHERE clauses
	sql := fmt.Sprintf("SELECT %s FROM %s", 
		dqp.columnsToString(query.Columns), 
		query.TableName)
	
	// Add partition filter
	partitionFilter := dqp.buildPartitionFilter(partitionIndex, partitioning)
	
	// Combine with existing WHERE conditions
	whereConditions := []string{}
	if partitionFilter != "" {
		whereConditions = append(whereConditions, partitionFilter)
	}
	
	for _, condition := range query.Where {
		whereConditions = append(whereConditions, 
			fmt.Sprintf("%s %s %v", condition.Column, condition.Operator, condition.Value))
	}
	
	if len(whereConditions) > 0 {
		sql += " WHERE " + strings.Join(whereConditions, " AND ")
	}
	
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	
	return sql
}

func (dqp *DistributedQueryPlanner) buildPartialAggregationSQL(query *core.ParsedQuery, partitionIndex int, partitioning *PartitioningStrategy) string {
	// Build partial aggregation SQL
	selectCols := []string{}
	
	// Add GROUP BY columns
	for _, col := range query.GroupBy {
		selectCols = append(selectCols, col)
	}
	
	// Add aggregate functions (modified for partial aggregation)
	for _, agg := range query.Aggregates {
		switch strings.ToUpper(agg.Function) {
		case "COUNT":
			selectCols = append(selectCols, fmt.Sprintf("COUNT(%s) as %s", agg.Column, agg.Alias))
		case "SUM":
			selectCols = append(selectCols, fmt.Sprintf("SUM(%s) as %s", agg.Column, agg.Alias))
		case "AVG":
			// For AVG, we need both SUM and COUNT
			selectCols = append(selectCols, 
				fmt.Sprintf("SUM(%s) as %s_sum", agg.Column, agg.Alias),
				fmt.Sprintf("COUNT(%s) as %s_count", agg.Column, agg.Alias))
		case "MIN":
			selectCols = append(selectCols, fmt.Sprintf("MIN(%s) as %s", agg.Column, agg.Alias))
		case "MAX":
			selectCols = append(selectCols, fmt.Sprintf("MAX(%s) as %s", agg.Column, agg.Alias))
		default:
			selectCols = append(selectCols, fmt.Sprintf("%s(%s) as %s", agg.Function, agg.Column, agg.Alias))
		}
	}
	
	sql := fmt.Sprintf("SELECT %s FROM %s", 
		strings.Join(selectCols, ", "), 
		query.TableName)
	
	// Add partition filter and WHERE conditions
	whereConditions := []string{}
	partitionFilter := dqp.buildPartitionFilter(partitionIndex, partitioning)
	if partitionFilter != "" {
		whereConditions = append(whereConditions, partitionFilter)
	}
	
	for _, condition := range query.Where {
		whereConditions = append(whereConditions, 
			fmt.Sprintf("%s %s %v", condition.Column, condition.Operator, condition.Value))
	}
	
	if len(whereConditions) > 0 {
		sql += " WHERE " + strings.Join(whereConditions, " AND ")
	}
	
	if len(query.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
	}
	
	return sql
}

func (dqp *DistributedQueryPlanner) buildFinalAggregationSQL(query *core.ParsedQuery) string {
	// Build final aggregation SQL to combine partial results
	selectCols := []string{}
	
	// Add GROUP BY columns
	for _, col := range query.GroupBy {
		selectCols = append(selectCols, col)
	}
	
	// Add final aggregate functions
	for _, agg := range query.Aggregates {
		switch strings.ToUpper(agg.Function) {
		case "COUNT":
			selectCols = append(selectCols, fmt.Sprintf("SUM(%s) as %s", agg.Alias, agg.Alias))
		case "SUM":
			selectCols = append(selectCols, fmt.Sprintf("SUM(%s) as %s", agg.Alias, agg.Alias))
		case "AVG":
			selectCols = append(selectCols, 
				fmt.Sprintf("SUM(%s_sum) / SUM(%s_count) as %s", agg.Alias, agg.Alias, agg.Alias))
		case "MIN":
			selectCols = append(selectCols, fmt.Sprintf("MIN(%s) as %s", agg.Alias, agg.Alias))
		case "MAX":
			selectCols = append(selectCols, fmt.Sprintf("MAX(%s) as %s", agg.Alias, agg.Alias))
		default:
			selectCols = append(selectCols, fmt.Sprintf("%s(%s) as %s", agg.Function, agg.Alias, agg.Alias))
		}
	}
	
	sql := fmt.Sprintf("SELECT %s FROM partial_results", strings.Join(selectCols, ", "))
	
	if len(query.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
	}
	
	if len(query.OrderBy) > 0 {
		orderCols := []string{}
		for _, orderBy := range query.OrderBy {
			direction := orderBy.Direction
			if direction == "" {
				direction = "ASC"
			}
			orderCols = append(orderCols, fmt.Sprintf("%s %s", orderBy.Column, direction))
		}
		sql += " ORDER BY " + strings.Join(orderCols, ", ")
	}
	
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	
	return sql
}

// Additional helper functions...

func (dqp *DistributedQueryPlanner) createDataFlowGraph(plan *DistributedPlan) *DataFlowGraph {
	nodes := []DataFlowNode{}
	edges := []DataFlowEdge{}
	
	// Create nodes for each stage
	for _, stage := range plan.Stages {
		node := DataFlowNode{
			ID:       stage.ID,
			Type:     stage.Type,
			Operator: string(stage.Type),
			Cost:     stage.EstimatedCost,
		}
		nodes = append(nodes, node)
	}
	
	// Create edges between dependent stages
	for _, stage := range plan.Stages {
		for _, depID := range stage.Dependencies {
			edge := DataFlowEdge{
				From:         depID,
				To:           stage.ID,
				DataSize:     1024 * 1024, // Default 1MB
				Partitioning: plan.Partitioning.Type,
				Compression:  false,
			}
			edges = append(edges, edge)
		}
	}
	
	return &DataFlowGraph{
		Nodes: nodes,
		Edges: edges,
	}
}

func (dqp *DistributedQueryPlanner) analyzeJoin(query *core.ParsedQuery, context *PlanningContext) *JoinInfo {
	if len(query.Joins) == 0 {
		return nil
	}
	
	join := query.Joins[0] // Handle first join for now
	
	joinInfo := &JoinInfo{
		JoinType:        dqp.joinTypeToString(join.Type),
		LeftTable:       query.TableName,
		RightTable:      join.TableName,
		JoinConditions:  []string{join.Condition.LeftColumn, join.Condition.RightColumn},
		EstimatedOutput: 10000, // Default estimate
	}
	
	// Determine join strategy based on table sizes
	leftSize := dqp.getTableSize(query.TableName)
	rightSize := dqp.getTableSize(join.TableName)
	
	// Small table threshold for broadcast join
	broadcastThreshold := int64(10 * 1024 * 1024) // 10MB
	
	if rightSize < broadcastThreshold {
		joinInfo.Strategy = JoinBroadcast
		joinInfo.BroadcastTable = join.TableName
	} else if leftSize < broadcastThreshold {
		joinInfo.Strategy = JoinBroadcast
		joinInfo.BroadcastTable = query.TableName
	} else {
		// Use shuffle join for large tables
		joinInfo.Strategy = JoinShuffle
	}
	
	return joinInfo
}

func (dqp *DistributedQueryPlanner) getTableSize(tableName string) int64 {
	if stats, exists := dqp.partitionManager.tableStats[tableName]; exists {
		return stats.SizeBytes
	}
	return 1024 * 1024 * 1024 // Default 1GB
}

func (dqp *DistributedQueryPlanner) generateCacheKey(query *core.ParsedQuery, context *PlanningContext) string {
	// Generate a cache key based on query and context
	return fmt.Sprintf("%x", md5.Sum([]byte(query.RawSQL)))
}

func (dqp *DistributedQueryPlanner) extractColumnNames(columns []core.Column) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

func (dqp *DistributedQueryPlanner) convertAggregates(aggregates []core.AggregateFunction) []core.Column {
	columns := make([]core.Column, len(aggregates))
	for i, agg := range aggregates {
		columns[i] = core.Column{
			Name:  agg.Function,
			Alias: agg.Alias,
		}
	}
	return columns
}

func (dqp *DistributedQueryPlanner) columnsToString(columns []core.Column) string {
	if len(columns) == 0 {
		return "*"
	}
	
	columnStrs := make([]string, len(columns))
	for i, col := range columns {
		if col.Alias != "" {
			columnStrs[i] = fmt.Sprintf("%s AS %s", col.Name, col.Alias)
		} else {
			columnStrs[i] = col.Name
		}
	}
	return strings.Join(columnStrs, ", ")
}

func (dqp *DistributedQueryPlanner) buildPartitionFilter(partitionIndex int, partitioning *PartitioningStrategy) string {
	switch partitioning.Type {
	case PartitionHash:
		if len(partitioning.Keys) > 0 {
			return fmt.Sprintf("MOD(HASH(%s), %d) = %d", 
				partitioning.Keys[0], partitioning.NumPartitions, partitionIndex)
		}
	case PartitionTypeRange:
		if partitionIndex < len(partitioning.Ranges) {
			r := partitioning.Ranges[partitionIndex]
			return fmt.Sprintf("%s >= %v AND %s < %v", 
				r.Column, r.Start, r.Column, r.End)
		}
	case PartitionRoundRobin:
		return fmt.Sprintf("MOD(ROW_NUMBER(), %d) = %d", 
			partitioning.NumPartitions, partitionIndex)
	}
	return ""
}

func (dqp *DistributedQueryPlanner) selectCoordinatorWorker(context *PlanningContext) string {
	// Select worker with lowest load as coordinator
	if len(context.Workers) == 0 {
		return "coordinator"
	}
	
	minLoad := context.Workers[0].CurrentLoad
	selectedWorker := context.Workers[0].ID
	
	for _, worker := range context.Workers {
		if worker.CurrentLoad < minLoad {
			minLoad = worker.CurrentLoad
			selectedWorker = worker.ID
		}
	}
	
	return selectedWorker
}

func (dqp *DistributedQueryPlanner) selectWorkerForUnionBranch(branchIndex int, context *PlanningContext) string {
	if len(context.Workers) == 0 {
		return "worker-0"
	}
	return context.Workers[branchIndex%len(context.Workers)].ID
}

func (dqp *DistributedQueryPlanner) createFinalFragment(query *core.ParsedQuery) *communication.QueryFragment {
	return &communication.QueryFragment{
		ID:           "final_collect",
		SQL:          "SELECT * FROM intermediate_results",
		FragmentType: communication.FragmentTypeScan,
		IsPartial:    false,
	}
}

// Additional SQL building methods would be implemented here...
func (dqp *DistributedQueryPlanner) buildBroadcastSQL(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s", tableName)
}

func (dqp *DistributedQueryPlanner) buildJoinSQL(query *core.ParsedQuery, joinInfo *JoinInfo, partitionIndex int) string {
	return fmt.Sprintf("SELECT * FROM %s JOIN %s ON %s", 
		joinInfo.LeftTable, joinInfo.RightTable, strings.Join(joinInfo.JoinConditions, " = "))
}

func (dqp *DistributedQueryPlanner) buildPartitionedScanSQL(tableName string, query *core.ParsedQuery, partitionIndex int, partitioning *PartitioningStrategy) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", 
		tableName, dqp.buildPartitionFilter(partitionIndex, partitioning))
}

func (dqp *DistributedQueryPlanner) buildShuffleJoinSQL(query *core.ParsedQuery, joinInfo *JoinInfo, partitionIndex int) string {
	return fmt.Sprintf("SELECT * FROM %s JOIN %s ON %s WHERE partition_id = %d", 
		joinInfo.LeftTable, joinInfo.RightTable, strings.Join(joinInfo.JoinConditions, " = "), partitionIndex)
}

func (dqp *DistributedQueryPlanner) buildPartitionedJoinSQL(query *core.ParsedQuery, joinInfo *JoinInfo, partitionIndex int) string {
	return fmt.Sprintf("SELECT * FROM %s JOIN %s ON %s", 
		joinInfo.LeftTable, joinInfo.RightTable, strings.Join(joinInfo.JoinConditions, " = "))
}

func (dqp *DistributedQueryPlanner) buildUnionFinalSQL(query *core.ParsedQuery) string {
	if query.HasUnion {
		if len(query.UnionQueries) > 0 && query.UnionQueries[0].UnionAll {
			return "SELECT * FROM union_results" // UNION ALL - no dedup needed
		}
		return "SELECT DISTINCT * FROM union_results" // UNION - deduplicate
	}
	return "SELECT * FROM union_results"
}

func (dqp *DistributedQueryPlanner) joinTypeToString(joinType core.JoinType) string {
	switch joinType {
	case core.INNER_JOIN:
		return "INNER"
	case core.LEFT_JOIN:
		return "LEFT"
	case core.RIGHT_JOIN:
		return "RIGHT"
	case core.FULL_OUTER_JOIN:
		return "FULL"
	default:
		return "INNER"
	}
}