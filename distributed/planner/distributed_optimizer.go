package planner

import (
	"bytedb/core"
	"fmt"
	"sort"
	"time"
)

// DistributedOptimizer applies optimization rules to distributed query plans
type DistributedOptimizer struct {
	costEstimator *CostEstimator
	rules         []OptimizationRule
}

// OptimizationRule defines an interface for optimization rules
type OptimizationRule interface {
	Name() string
	Description() string
	Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error)
	EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64
}

// NewDistributedOptimizer creates a new distributed optimizer
func NewDistributedOptimizer(costEstimator *CostEstimator) *DistributedOptimizer {
	optimizer := &DistributedOptimizer{
		costEstimator: costEstimator,
		rules:         []OptimizationRule{},
	}
	
	// Register optimization rules
	optimizer.RegisterRule(&PredicatePushdownRule{})
	optimizer.RegisterRule(&ProjectionPushdownRule{})
	optimizer.RegisterRule(&JoinReorderingRule{costEstimator: costEstimator})
	optimizer.RegisterRule(&PartitionPruningRule{})
	optimizer.RegisterRule(&AggregationPushdownRule{})
	optimizer.RegisterRule(&DataLocalityOptimizationRule{})
	optimizer.RegisterRule(&MemoryOptimizationRule{})
	optimizer.RegisterRule(&ParallelismOptimizationRule{})
	optimizer.RegisterRule(&CacheOptimizationRule{})
	optimizer.RegisterRule(&CompressionOptimizationRule{})
	
	return optimizer
}

// RegisterRule registers a new optimization rule
func (do *DistributedOptimizer) RegisterRule(rule OptimizationRule) {
	do.rules = append(do.rules, rule)
}

// OptimizePlan applies all optimization rules to improve the plan
func (do *DistributedOptimizer) OptimizePlan(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, []OptimizationApplied) {
	optimizedPlan := do.copyPlan(plan)
	appliedOptimizations := []OptimizationApplied{}
	
	// Calculate initial cost
	initialStats, err := do.costEstimator.EstimatePlan(optimizedPlan, context)
	if err != nil {
		// Return original plan if we can't estimate cost
		return plan, appliedOptimizations
	}
	optimizedPlan.Statistics = initialStats
	initialCost := initialStats.TotalCost
	
	// Apply rules in order of estimated benefit
	sortedRules := do.sortRulesByBenefit(optimizedPlan, context)
	
	for _, rule := range sortedRules {
		startTime := time.Now()
		
		// Apply the rule
		newPlan, applied, err := rule.Apply(optimizedPlan, context)
		if err != nil {
			continue // Skip rules that fail
		}
		
		if applied {
			// Estimate cost improvement
			newStats, err := do.costEstimator.EstimatePlan(newPlan, context)
			if err != nil {
				continue // Skip if cost estimation fails
			}
			
			costReduction := 0.0
			if optimizedPlan.Statistics != nil {
				costReduction = optimizedPlan.Statistics.TotalCost - newStats.TotalCost
			}
			
			// Accept the optimization if it improves the plan
			if costReduction > 0 || newStats.TotalCost < optimizedPlan.Statistics.TotalCost {
				optimizedPlan = newPlan
				optimizedPlan.Statistics = newStats
				
				appliedOptimizations = append(appliedOptimizations, OptimizationApplied{
					Name:        rule.Name(),
					Description: rule.Description(),
					Benefit:     costReduction,
					AppliedAt:   startTime,
				})
			}
		}
	}
	
	// Calculate final improvement
	finalCost := optimizedPlan.Statistics.TotalCost
	totalImprovement := initialCost - finalCost
	
	if totalImprovement > 0 {
		appliedOptimizations = append(appliedOptimizations, OptimizationApplied{
			Name:        "Total Optimization",
			Description: fmt.Sprintf("Total cost reduction: %.2f", totalImprovement),
			Benefit:     totalImprovement,
			AppliedAt:   time.Now(),
		})
	}
	
	return optimizedPlan, appliedOptimizations
}

// sortRulesByBenefit sorts optimization rules by their estimated benefit
func (do *DistributedOptimizer) sortRulesByBenefit(plan *DistributedPlan, context *PlanningContext) []OptimizationRule {
	type ruleWithBenefit struct {
		rule    OptimizationRule
		benefit float64
	}
	
	rulesWithBenefits := make([]ruleWithBenefit, len(do.rules))
	for i, rule := range do.rules {
		benefit := rule.EstimateBenefit(plan, context)
		rulesWithBenefits[i] = ruleWithBenefit{rule: rule, benefit: benefit}
	}
	
	// Sort by benefit (descending)
	sort.Slice(rulesWithBenefits, func(i, j int) bool {
		return rulesWithBenefits[i].benefit > rulesWithBenefits[j].benefit
	})
	
	// Extract sorted rules
	sortedRules := make([]OptimizationRule, len(rulesWithBenefits))
	for i, rwb := range rulesWithBenefits {
		sortedRules[i] = rwb.rule
	}
	
	return sortedRules
}

// copyPlan creates a deep copy of a distributed plan
func (do *DistributedOptimizer) copyPlan(plan *DistributedPlan) *DistributedPlan {
	// This is a simplified copy - in a real implementation, you'd need deep copying
	newPlan := *plan
	newPlan.Stages = make([]ExecutionStage, len(plan.Stages))
	copy(newPlan.Stages, plan.Stages)
	
	return &newPlan
}

// Optimization Rules Implementation

// PredicatePushdownRule pushes WHERE conditions down to scan stages
type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Name() string {
	return "Predicate Pushdown"
}

func (r *PredicatePushdownRule) Description() string {
	return "Push WHERE conditions down to reduce data movement"
}

func (r *PredicatePushdownRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan // Simplified - should deep copy
	
	// Look for scan stages that can have additional predicates pushed down
	for i := range newPlan.Stages {
		stage := &newPlan.Stages[i]
		if stage.Type == StageScan {
			// Check if we can push down additional predicates
			for j := range stage.Fragments {
				fragment := stage.Fragments[j]
				if fragment.Fragment != nil && len(fragment.Fragment.WhereClause) < len(plan.ParsedQuery.Where) {
					// Push down more predicates
					fragment.Fragment.WhereClause = plan.ParsedQuery.Where
					modified = true
				}
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *PredicatePushdownRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	// High benefit for plans with WHERE conditions and multiple stages
	if plan != nil && plan.ParsedQuery != nil && len(plan.ParsedQuery.Where) > 0 && len(plan.Stages) > 1 {
		return 100.0
	}
	return 0.0
}

// ProjectionPushdownRule pushes column selection down to scan stages
type ProjectionPushdownRule struct{}

func (r *ProjectionPushdownRule) Name() string {
	return "Projection Pushdown"
}

func (r *ProjectionPushdownRule) Description() string {
	return "Push column selection down to reduce data transfer"
}

func (r *ProjectionPushdownRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Analyze which columns are actually needed
	requiredColumns := r.analyzeRequiredColumns(plan.ParsedQuery)
	
	// Update scan stages to only select required columns
	for i := range newPlan.Stages {
		stage := &newPlan.Stages[i]
		if stage.Type == StageScan {
			for j := range stage.Fragments {
				fragment := stage.Fragments[j]
				if fragment.Fragment != nil && len(fragment.Fragment.Columns) > len(requiredColumns) {
					fragment.Fragment.Columns = requiredColumns
					modified = true
				}
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *ProjectionPushdownRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	if plan == nil {
		return 0.0
	}
	
	// Benefit based on potential data reduction
	// Check if any scan stages have more columns than necessary
	for _, stage := range plan.Stages {
		if stage.Type == StageScan {
			for _, fragment := range stage.Fragments {
				if fragment.Fragment != nil && len(fragment.Fragment.Columns) > 5 {
					// Assume 10% benefit per column reduced
					return float64(len(fragment.Fragment.Columns)-5) * 10.0
				}
			}
		}
	}
	// If query has specific columns selected, estimate benefit
	if plan.ParsedQuery != nil && len(plan.ParsedQuery.Columns) > 0 && len(plan.ParsedQuery.Columns) < 10 {
		return 50.0
	}
	return 10.0 // Default small benefit for projection pushdown
}

func (r *ProjectionPushdownRule) analyzeRequiredColumns(query *core.ParsedQuery) []string {
	requiredColumns := make(map[string]bool)
	
	// Columns in SELECT
	for _, col := range query.Columns {
		requiredColumns[col.Name] = true
	}
	
	// Columns in WHERE
	for _, condition := range query.Where {
		requiredColumns[condition.Column] = true
	}
	
	// Columns in GROUP BY
	for _, col := range query.GroupBy {
		requiredColumns[col] = true
	}
	
	// Columns in ORDER BY
	for _, orderBy := range query.OrderBy {
		requiredColumns[orderBy.Column] = true
	}
	
	// Convert to slice
	columns := make([]string, 0, len(requiredColumns))
	for col := range requiredColumns {
		columns = append(columns, col)
	}
	
	return columns
}

// JoinReorderingRule reorders joins for optimal performance
type JoinReorderingRule struct {
	costEstimator *CostEstimator
}

func (r *JoinReorderingRule) Name() string {
	return "Join Reordering"
}

func (r *JoinReorderingRule) Description() string {
	return "Reorder joins to minimize intermediate result sizes"
}

func (r *JoinReorderingRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	// This would implement join reordering algorithms
	// For now, return unchanged
	return plan, false, nil
}

func (r *JoinReorderingRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	// High benefit for multi-join queries
	if len(plan.ParsedQuery.Joins) > 1 {
		return 90.0
	}
	return 0.0
}

// PartitionPruningRule eliminates unnecessary partitions
type PartitionPruningRule struct{}

func (r *PartitionPruningRule) Name() string {
	return "Partition Pruning"
}

func (r *PartitionPruningRule) Description() string {
	return "Eliminate partitions that cannot contain relevant data"
}

func (r *PartitionPruningRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Analyze WHERE conditions for partition pruning opportunities
	if plan.Partitioning != nil && plan.Partitioning.Type == PartitionTypeRange {
		prunedFragments := r.pruneRangePartitions(plan, context)
		if len(prunedFragments) > 0 {
			// Update scan stages with pruned fragments
			for i := range newPlan.Stages {
				stage := &newPlan.Stages[i]
				if stage.Type == StageScan {
					stage.Fragments = prunedFragments
					modified = true
				}
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *PartitionPruningRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	// High benefit for range-partitioned tables with range queries
	if plan.Partitioning != nil && plan.Partitioning.Type == PartitionTypeRange {
		return 85.0
	}
	return 0.0
}

func (r *PartitionPruningRule) pruneRangePartitions(plan *DistributedPlan, context *PlanningContext) []StageFragment {
	// This would implement actual partition pruning logic
	// For now, return original fragments
	if len(plan.Stages) > 0 {
		return plan.Stages[0].Fragments
	}
	return []StageFragment{}
}

// AggregationPushdownRule pushes aggregations down to workers
type AggregationPushdownRule struct{}

func (r *AggregationPushdownRule) Name() string {
	return "Aggregation Pushdown"
}

func (r *AggregationPushdownRule) Description() string {
	return "Perform partial aggregations on workers to reduce data transfer"
}

func (r *AggregationPushdownRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	// This would split aggregations into partial and final stages
	// Already handled in the planner, so return unchanged
	return plan, false, nil
}

func (r *AggregationPushdownRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	if plan.ParsedQuery.IsAggregate {
		return 70.0
	}
	return 0.0
}

// DataLocalityOptimizationRule optimizes for data locality
type DataLocalityOptimizationRule struct{}

func (r *DataLocalityOptimizationRule) Name() string {
	return "Data Locality Optimization"
}

func (r *DataLocalityOptimizationRule) Description() string {
	return "Schedule computation close to data to minimize network transfer"
}

func (r *DataLocalityOptimizationRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Reassign fragments to workers based on data locality
	for i := range newPlan.Stages {
		stage := &newPlan.Stages[i]
		for j := range stage.Fragments {
			fragment := &stage.Fragments[j]
			// Find worker with best data locality
			bestWorker := r.findBestWorkerForData(fragment, context)
			if bestWorker != fragment.WorkerID {
				fragment.WorkerID = bestWorker
				modified = true
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *DataLocalityOptimizationRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	return 60.0 // Always beneficial to optimize data locality
}

func (r *DataLocalityOptimizationRule) findBestWorkerForData(fragment *StageFragment, context *PlanningContext) string {
	// This would implement actual data locality detection
	// For now, return current worker
	return fragment.WorkerID
}

// MemoryOptimizationRule optimizes memory usage
type MemoryOptimizationRule struct{}

func (r *MemoryOptimizationRule) Name() string {
	return "Memory Optimization"
}

func (r *MemoryOptimizationRule) Description() string {
	return "Optimize memory usage across workers"
}

func (r *MemoryOptimizationRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Rebalance memory-intensive operations
	for i := range newPlan.Stages {
		stage := &newPlan.Stages[i]
		if stage.Type == StageJoin || stage.Type == StageAggregate {
			// Check if any fragments require too much memory
			for j := range stage.Fragments {
				fragment := &stage.Fragments[j]
				if fragment.RequiredMemoryMB > 1000 { // High memory threshold
					// Split or reassign fragment
					r.optimizeFragmentMemory(fragment, context)
					modified = true
				}
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *MemoryOptimizationRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	// Check for memory-intensive operations
	for _, stage := range plan.Stages {
		if stage.Type == StageJoin || stage.Type == StageAggregate {
			return 50.0
		}
	}
	return 0.0
}

func (r *MemoryOptimizationRule) optimizeFragmentMemory(fragment *StageFragment, context *PlanningContext) {
	// This would implement memory optimization strategies
	// For now, just reduce the memory requirement
	fragment.RequiredMemoryMB = fragment.RequiredMemoryMB / 2
}

// ParallelismOptimizationRule optimizes parallelism levels
type ParallelismOptimizationRule struct{}

func (r *ParallelismOptimizationRule) Name() string {
	return "Parallelism Optimization"
}

func (r *ParallelismOptimizationRule) Description() string {
	return "Optimize parallelism levels for optimal performance"
}

func (r *ParallelismOptimizationRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Adjust parallelism based on data size and worker capacity
	for i := range newPlan.Stages {
		stage := &newPlan.Stages[i]
		optimalParallelism := r.calculateOptimalParallelism(stage, context)
		if optimalParallelism != stage.Parallelism {
			stage.Parallelism = optimalParallelism
			modified = true
		}
	}
	
	return newPlan, modified, nil
}

func (r *ParallelismOptimizationRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	return 40.0 // Moderate benefit
}

func (r *ParallelismOptimizationRule) calculateOptimalParallelism(stage *ExecutionStage, context *PlanningContext) int {
	// Calculate based on data size and worker capacity
	totalDataMB := int64(0)
	for _, fragment := range stage.Fragments {
		totalDataMB += fragment.EstimatedBytes / (1024 * 1024)
	}
	
	// Target 100MB per parallel task
	optimalParallelism := int(totalDataMB / 100)
	
	// Constrain by available workers
	maxWorkers := len(context.Workers)
	if optimalParallelism > maxWorkers {
		optimalParallelism = maxWorkers
	}
	
	if optimalParallelism < 1 {
		optimalParallelism = 1
	}
	
	return optimalParallelism
}

// CacheOptimizationRule optimizes caching strategies
type CacheOptimizationRule struct{}

func (r *CacheOptimizationRule) Name() string {
	return "Cache Optimization"
}

func (r *CacheOptimizationRule) Description() string {
	return "Optimize caching strategies for frequently accessed data"
}

func (r *CacheOptimizationRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	// This would implement caching optimizations
	return plan, false, nil
}

func (r *CacheOptimizationRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	return 30.0 // Low benefit without historical data
}

// CompressionOptimizationRule optimizes data compression
type CompressionOptimizationRule struct{}

func (r *CompressionOptimizationRule) Name() string {
	return "Compression Optimization"
}

func (r *CompressionOptimizationRule) Description() string {
	return "Enable compression for network transfers"
}

func (r *CompressionOptimizationRule) Apply(plan *DistributedPlan, context *PlanningContext) (*DistributedPlan, bool, error) {
	modified := false
	newPlan := plan
	
	// Enable compression for network-intensive operations
	if newPlan.DataFlow != nil {
		for i := range newPlan.DataFlow.Edges {
			edge := &newPlan.DataFlow.Edges[i]
			if edge.DataSize > 1024*1024 && !edge.Compression { // > 1MB
				edge.Compression = true
				modified = true
			}
		}
	}
	
	return newPlan, modified, nil
}

func (r *CompressionOptimizationRule) EstimateBenefit(plan *DistributedPlan, context *PlanningContext) float64 {
	// Benefit for network-intensive operations
	if plan.DataFlow != nil {
		for _, edge := range plan.DataFlow.Edges {
			if edge.DataSize > 1024*1024 { // > 1MB transfer
				return 25.0
			}
		}
	}
	return 0.0
}