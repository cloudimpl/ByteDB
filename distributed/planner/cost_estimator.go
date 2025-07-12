package planner

import (
	"math"
	"time"
)

// CostEstimator estimates costs for distributed query execution plans
type CostEstimator struct {
	workers    []WorkerInfo
	tableStats map[string]*TableStatistics
	costModel  *CostModel
}

// CostModel defines cost parameters for different operations
type CostModel struct {
	// CPU costs per operation
	CPUCostPerRow        float64 // Cost per row processed
	CPUCostPerComparison float64 // Cost per comparison operation
	CPUCostPerHash       float64 // Cost per hash operation
	CPUCostPerSort       float64 // Cost per sort comparison
	
	// I/O costs
	IOCostPerByte     float64 // Cost per byte read from disk
	IOCostPerSeek     float64 // Cost per disk seek
	IOCostRandomFactor float64 // Random I/O cost multiplier
	
	// Network costs
	NetworkCostPerByte    float64 // Cost per byte transferred over network
	NetworkLatencyMillis  float64 // Base network latency
	NetworkBandwidthMBps  float64 // Network bandwidth in MB/s
	
	// Memory costs
	MemoryCostPerMB   float64 // Cost per MB of memory used
	MemoryPressureFactor float64 // Cost multiplier when memory is tight
	
	// Join costs
	JoinCostFactor      float64 // Multiplier for join operations
	HashJoinSetupCost   float64 // Setup cost for hash joins
	SortMergeSetupCost  float64 // Setup cost for sort-merge joins
	
	// Aggregation costs
	AggregationCostFactor float64 // Multiplier for aggregation operations
	GroupBySetupCost      float64 // Setup cost for GROUP BY operations
	
	// Parallelism factors
	ParallelismOverhead   float64 // Overhead cost for each parallel worker
	CoordinationCost      float64 // Cost for coordinating multiple workers
}

// NewCostEstimator creates a new cost estimator
func NewCostEstimator(workers []WorkerInfo, tableStats map[string]*TableStatistics) *CostEstimator {
	return &CostEstimator{
		workers:    workers,
		tableStats: tableStats,
		costModel:  NewDefaultCostModel(),
	}
}

// NewDefaultCostModel creates a cost model with default parameters
func NewDefaultCostModel() *CostModel {
	return &CostModel{
		// CPU costs (arbitrary units)
		CPUCostPerRow:        0.001,
		CPUCostPerComparison: 0.0001,
		CPUCostPerHash:       0.0005,
		CPUCostPerSort:       0.001,
		
		// I/O costs
		IOCostPerByte:     0.000001, // 1 unit per MB
		IOCostPerSeek:     10.0,     // Seek is expensive
		IOCostRandomFactor: 4.0,     // Random I/O is 4x more expensive
		
		// Network costs
		NetworkCostPerByte:    0.00001, // Network is 10x more expensive than local I/O
		NetworkLatencyMillis:  1.0,     // 1ms base latency
		NetworkBandwidthMBps:  1000.0,  // 1GB/s bandwidth
		
		// Memory costs
		MemoryCostPerMB:       0.1,
		MemoryPressureFactor:  2.0,
		
		// Join costs
		JoinCostFactor:     1.5,
		HashJoinSetupCost:  100.0,
		SortMergeSetupCost: 200.0,
		
		// Aggregation costs
		AggregationCostFactor: 1.2,
		GroupBySetupCost:      50.0,
		
		// Parallelism costs
		ParallelismOverhead: 10.0,
		CoordinationCost:    5.0,
	}
}

// EstimatePlan estimates the total cost of a distributed plan
func (ce *CostEstimator) EstimatePlan(plan *DistributedPlan, context *PlanningContext) (*PlanStatistics, error) {
	totalCost := 0.0
	totalCPUCost := 0.0
	totalIOCost := 0.0
	totalNetworkCost := 0.0
	totalDuration := time.Duration(0)
	totalRows := int64(0)
	totalBytes := int64(0)
	
	// Estimate cost for each stage
	for _, stage := range plan.Stages {
		stageCost := ce.EstimateStage(&stage, context)
		
		totalCost += stageCost.TotalCost
		totalCPUCost += stageCost.CPUCost
		totalIOCost += stageCost.IOCost
		totalNetworkCost += stageCost.NetworkCost
		
		// Stages run sequentially (for dependencies) or in parallel
		if len(stage.Dependencies) > 0 {
			totalDuration += stageCost.Duration
		} else {
			// Parallel stage - duration is max of all parallel stages at this level
			if stageCost.Duration > totalDuration {
				totalDuration = stageCost.Duration
			}
		}
		
		// Accumulate data size
		for _, fragment := range stage.Fragments {
			totalRows += fragment.EstimatedRows
			totalBytes += fragment.EstimatedBytes
		}
		
		// Update stage cost estimate
		stage.EstimatedCost = stageCost.TotalCost
	}
	
	// Add coordination overhead
	coordinationCost := ce.costModel.CoordinationCost * float64(len(plan.Stages))
	totalCost += coordinationCost
	
	// Calculate confidence based on statistics availability
	confidence := ce.calculateConfidence(plan, context)
	
	return &PlanStatistics{
		EstimatedRows:        totalRows,
		EstimatedBytes:       totalBytes,
		EstimatedDuration:    totalDuration,
		EstimatedCPUCost:     totalCPUCost,
		EstimatedIOCost:      totalIOCost,
		EstimatedNetworkCost: totalNetworkCost,
		TotalCost:            totalCost,
		Confidence:           confidence,
	}, nil
}

// StageCost represents the cost breakdown for a single stage
type StageCost struct {
	TotalCost   float64
	CPUCost     float64
	IOCost      float64
	NetworkCost float64
	MemoryCost  float64
	Duration    time.Duration
}

// EstimateStage estimates the cost of a single execution stage
func (ce *CostEstimator) EstimateStage(stage *ExecutionStage, context *PlanningContext) *StageCost {
	stageCost := &StageCost{}
	
	// Estimate cost based on stage type
	switch stage.Type {
	case StageScan:
		ce.estimateScanCost(stage, stageCost, context)
	case StageJoin:
		ce.estimateJoinCost(stage, stageCost, context)
	case StageAggregate:
		ce.estimateAggregateCost(stage, stageCost, context)
	case StageShuffle:
		ce.estimateShuffleCost(stage, stageCost, context)
	case StageSort:
		ce.estimateSortCost(stage, stageCost, context)
	case StageUnion:
		ce.estimateUnionCost(stage, stageCost, context)
	case StageFinal:
		ce.estimateFinalCost(stage, stageCost, context)
	default:
		// Default cost estimation
		ce.estimateDefaultCost(stage, stageCost, context)
	}
	
	// Add parallelism overhead
	parallelismCost := ce.costModel.ParallelismOverhead * float64(stage.Parallelism)
	stageCost.TotalCost += parallelismCost
	
	// Calculate duration (assuming parallel execution)
	if stage.Parallelism > 0 {
		sequentialDuration := time.Duration(stageCost.TotalCost * float64(time.Millisecond))
		stageCost.Duration = time.Duration(float64(sequentialDuration) / float64(stage.Parallelism))
	}
	
	return stageCost
}

// estimateScanCost estimates the cost of scan operations
func (ce *CostEstimator) estimateScanCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// I/O cost for reading data
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		// CPU cost for processing rows
		cpuCost := float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerRow
		
		// Memory cost
		memoryCost := float64(fragment.RequiredMemoryMB) * ce.costModel.MemoryCostPerMB
		
		cost.IOCost += ioCost
		cost.CPUCost += cpuCost
		cost.MemoryCost += memoryCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
}

// estimateJoinCost estimates the cost of join operations
func (ce *CostEstimator) estimateJoinCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Base scan cost
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		// Join-specific CPU cost
		joinRows := fragment.EstimatedRows
		cpuCost := float64(joinRows) * ce.costModel.CPUCostPerRow * ce.costModel.JoinCostFactor
		
		// Hash join setup cost
		cpuCost += ce.costModel.HashJoinSetupCost
		
		// Memory cost for hash tables
		memoryCost := float64(fragment.RequiredMemoryMB) * ce.costModel.MemoryCostPerMB
		
		// Network cost for data movement (if applicable)
		networkCost := 0.0
		if stage.Type == StageJoin && len(stage.ShuffleKey) > 0 {
			// Shuffle join requires network transfer
			networkCost = float64(fragment.EstimatedBytes) * ce.costModel.NetworkCostPerByte
		}
		
		cost.IOCost += ioCost
		cost.CPUCost += cpuCost
		cost.MemoryCost += memoryCost
		cost.NetworkCost += networkCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost + cost.NetworkCost
}

// estimateAggregateCost estimates the cost of aggregation operations
func (ce *CostEstimator) estimateAggregateCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Base processing cost
		cpuCost := float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerRow * ce.costModel.AggregationCostFactor
		
		// GROUP BY setup cost
		cpuCost += ce.costModel.GroupBySetupCost
		
		// Hash table operations for grouping
		if fragment.EstimatedRows > 0 {
			cpuCost += float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerHash
		}
		
		// I/O cost for reading input
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		// Memory cost for aggregation state
		memoryCost := float64(fragment.RequiredMemoryMB) * ce.costModel.MemoryCostPerMB
		
		cost.IOCost += ioCost
		cost.CPUCost += cpuCost
		cost.MemoryCost += memoryCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
}

// estimateShuffleCost estimates the cost of data shuffling
func (ce *CostEstimator) estimateShuffleCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Network cost for shuffling data
		networkCost := float64(fragment.EstimatedBytes) * ce.costModel.NetworkCostPerByte
		
		// CPU cost for partitioning/hashing
		cpuCost := float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerHash
		
		// I/O cost for reading source data
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		cost.IOCost += ioCost
		cost.CPUCost += cpuCost
		cost.NetworkCost += networkCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.NetworkCost
}

// estimateSortCost estimates the cost of sorting operations
func (ce *CostEstimator) estimateSortCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Sort cost: O(n log n)
		rows := float64(fragment.EstimatedRows)
		if rows > 0 {
			sortCost := rows * math.Log2(rows) * ce.costModel.CPUCostPerSort
			cost.CPUCost += sortCost
		}
		
		// I/O cost for reading data
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		// Memory cost for sorting buffer
		memoryCost := float64(fragment.RequiredMemoryMB) * ce.costModel.MemoryCostPerMB
		
		cost.IOCost += ioCost
		cost.MemoryCost += memoryCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
}

// estimateUnionCost estimates the cost of union operations
func (ce *CostEstimator) estimateUnionCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Basic processing cost
		cpuCost := float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerRow
		
		// I/O cost
		ioCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		
		// Network cost for collecting results
		networkCost := float64(fragment.EstimatedBytes) * ce.costModel.NetworkCostPerByte * 0.5 // Reduce factor
		
		cost.IOCost += ioCost
		cost.CPUCost += cpuCost
		cost.NetworkCost += networkCost
	}
	
	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.NetworkCost
}

// estimateFinalCost estimates the cost of final result collection
func (ce *CostEstimator) estimateFinalCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Network cost for collecting results from workers
		networkCost := float64(fragment.EstimatedBytes) * ce.costModel.NetworkCostPerByte
		
		// CPU cost for merging results
		cpuCost := float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerRow * 0.5 // Reduced factor
		
		cost.CPUCost += cpuCost
		cost.NetworkCost += networkCost
	}
	
	cost.TotalCost = cost.CPUCost + cost.NetworkCost
}

// estimateDefaultCost provides a default cost estimation
func (ce *CostEstimator) estimateDefaultCost(stage *ExecutionStage, cost *StageCost, context *PlanningContext) {
	for _, fragment := range stage.Fragments {
		// Simple linear cost based on data size
		baseCost := float64(fragment.EstimatedBytes) * ce.costModel.IOCostPerByte
		baseCost += float64(fragment.EstimatedRows) * ce.costModel.CPUCostPerRow
		
		cost.TotalCost += baseCost
		cost.CPUCost += baseCost * 0.7
		cost.IOCost += baseCost * 0.3
	}
}

// calculateConfidence estimates the confidence in cost estimates
func (ce *CostEstimator) calculateConfidence(plan *DistributedPlan, context *PlanningContext) float64 {
	confidence := 1.0
	
	// Reduce confidence if we lack table statistics
	for _, stage := range plan.Stages {
		for _, fragment := range stage.Fragments {
			if fragment.Fragment != nil && fragment.Fragment.TablePath != "" {
				if _, hasStats := ce.tableStats[fragment.Fragment.TablePath]; !hasStats {
					confidence *= 0.7 // Reduce confidence by 30%
				}
			}
		}
	}
	
	// Reduce confidence for complex queries
	if len(plan.Stages) > 3 {
		confidence *= 0.9
	}
	
	// Reduce confidence if using estimated values
	if plan.Statistics != nil && plan.Statistics.EstimatedRows == 0 {
		confidence *= 0.8
	}
	
	return math.Max(confidence, 0.1) // Minimum 10% confidence
}

// EstimateSelectivity estimates the selectivity of WHERE conditions
func (ce *CostEstimator) EstimateSelectivity(conditions []string, tableName string) float64 {
	if len(conditions) == 0 {
		return 1.0
	}
	
	selectivity := 1.0
	
	// Use column statistics if available
	if tableStats, exists := ce.tableStats[tableName]; exists {
		for _, condition := range conditions {
			// Simple heuristic: each condition reduces selectivity
			if colStats, exists := tableStats.ColumnStats[condition]; exists {
				// Use distinct count to estimate selectivity
				if colStats.DistinctCount > 0 && tableStats.RowCount > 0 {
					conditionSelectivity := 1.0 / float64(colStats.DistinctCount)
					selectivity *= conditionSelectivity
				} else {
					selectivity *= 0.1 // Default 10% selectivity
				}
			} else {
				selectivity *= 0.1 // Default 10% selectivity
			}
		}
	} else {
		// No statistics available, use simple heuristic
		for range conditions {
			selectivity *= 0.1 // Assume each condition filters 90% of data
		}
	}
	
	return math.Max(selectivity, 0.001) // Minimum 0.1% selectivity
}

// EstimateJoinCardinality estimates the cardinality of join results
func (ce *CostEstimator) EstimateJoinCardinality(leftTable, rightTable string, joinConditions []string) int64 {
	leftStats, hasLeftStats := ce.tableStats[leftTable]
	rightStats, hasRightStats := ce.tableStats[rightTable]
	
	if !hasLeftStats || !hasRightStats {
		return 10000 // Default estimate
	}
	
	leftRows := leftStats.RowCount
	rightRows := rightStats.RowCount
	
	if len(joinConditions) == 0 {
		// Cartesian product
		return leftRows * rightRows
	}
	
	// Estimate based on join key cardinality
	joinColumn := joinConditions[0] // Use first join condition
	
	leftDistinct := int64(1)
	rightDistinct := int64(1)
	
	if leftColStats, exists := leftStats.ColumnStats[joinColumn]; exists {
		leftDistinct = leftColStats.DistinctCount
	}
	
	if rightColStats, exists := rightStats.ColumnStats[joinColumn]; exists {
		rightDistinct = rightColStats.DistinctCount
	}
	
	// Join cardinality estimation
	maxDistinct := leftDistinct
	if rightDistinct > maxDistinct {
		maxDistinct = rightDistinct
	}
	
	if maxDistinct > 0 {
		return (leftRows * rightRows) / maxDistinct
	}
	
	return leftRows + rightRows // Conservative estimate
}

// ComparePlans compares two plans and returns the better one
func (ce *CostEstimator) ComparePlans(plan1, plan2 *DistributedPlan) *DistributedPlan {
	if plan1.Statistics == nil && plan2.Statistics == nil {
		return plan1 // Default to first plan
	}
	
	if plan1.Statistics == nil {
		return plan2
	}
	
	if plan2.Statistics == nil {
		return plan1
	}
	
	// Compare based on total cost, weighted by confidence
	cost1 := plan1.Statistics.TotalCost / plan1.Statistics.Confidence
	cost2 := plan2.Statistics.TotalCost / plan2.Statistics.Confidence
	
	if cost1 <= cost2 {
		return plan1
	}
	
	return plan2
}