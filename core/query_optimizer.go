package core

import (
	"fmt"
	"sort"
	"strings"
)

// QueryOptimizer handles query optimization strategies
type QueryOptimizer struct {
	planner *QueryPlanner
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(planner *QueryPlanner) *QueryOptimizer {
	return &QueryOptimizer{
		planner: planner,
	}
}

// OptimizationRule represents a query optimization rule
type OptimizationRule interface {
	Name() string
	Apply(plan *QueryPlan) (*QueryPlan, bool) // returns optimized plan and whether changes were made
	Cost() int                                // relative cost of applying this rule
}

// PredicatePushdownRule pushes WHERE conditions down to scan nodes
type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Name() string { return "PredicatePushdown" }
func (r *PredicatePushdownRule) Cost() int    { return 1 }

func (r *PredicatePushdownRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	changed := false
	r.applyToNode(plan.Root, &changed)
	return plan, changed
}

func (r *PredicatePushdownRule) applyToNode(node *PlanNode, changed *bool) {
	if node == nil {
		return
	}

	// Look for filter nodes that can be pushed down
	if node.Type == PlanNodeFilter {
		// Try to push this filter down to scan nodes
		for _, child := range node.Children {
			if child.Type == PlanNodeScan {
				// Push the filter conditions to the scan node
				child.Filter = append(child.Filter, node.Filter...)
				// Remove this filter node by replacing it with its child
				*node = *child
				*changed = true
				return
			}
		}
	}

	// Recursively apply to children
	for _, child := range node.Children {
		r.applyToNode(child, changed)
	}
}

// ColumnPruningRule removes unnecessary columns from scan operations
type ColumnPruningRule struct{}

func (r *ColumnPruningRule) Name() string { return "ColumnPruning" }
func (r *ColumnPruningRule) Cost() int    { return 1 }

func (r *ColumnPruningRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	changed := false
	requiredColumns := r.collectRequiredColumns(plan.Root)
	r.applyColumnPruning(plan.Root, requiredColumns, &changed)
	return plan, changed
}

func (r *ColumnPruningRule) collectRequiredColumns(node *PlanNode) map[string]bool {
	required := make(map[string]bool)
	r.collectFromNode(node, required)
	return required
}

func (r *ColumnPruningRule) collectFromNode(node *PlanNode, required map[string]bool) {
	if node == nil {
		return
	}

	// Collect columns from this node
	for _, col := range node.Columns {
		required[col] = true
	}

	// Collect from filter conditions
	for _, condition := range node.FilterConditions {
		if condition.Column != "" {
			required[condition.Column] = true
		}
		// Also check function columns
		if condition.Function != nil {
			for _, arg := range condition.Function.Args {
				// Try to extract column names from function arguments
				if colName, ok := arg.(string); ok {
					required[colName] = true
				}
				// Handle Column type if it exists
				if col, ok := arg.(Column); ok {
					required[col.Name] = true
				}
			}
		}
	}

	// Recursively collect from children
	for _, child := range node.Children {
		r.collectFromNode(child, required)
	}
}

func (r *ColumnPruningRule) applyColumnPruning(node *PlanNode, required map[string]bool, changed *bool) {
	if node == nil {
		return
	}

	// For scan nodes, prune unnecessary columns
	if node.Type == PlanNodeScan {
		// If we have "*" and we know which columns are required, replace with specific columns
		if len(node.Columns) == 1 && node.Columns[0] == "*" && len(required) > 0 {
			var cols []string
			for col := range required {
				cols = append(cols, col)
			}
			sort.Strings(cols) // For consistent ordering
			node.Columns = cols
			*changed = true
		} else if len(node.Columns) == 0 {
			// No columns specified, set required columns
			var cols []string
			for col := range required {
				cols = append(cols, col)
			}
			sort.Strings(cols) // For consistent ordering
			node.Columns = cols
			*changed = true
		} else {
			// Remove columns not in required set
			var prunedCols []string
			for _, col := range node.Columns {
				if required[col] || col == "*" {
					prunedCols = append(prunedCols, col)
				}
			}
			if len(prunedCols) != len(node.Columns) {
				node.Columns = prunedCols
				*changed = true
			}
		}
	}

	// Recursively apply to children
	for _, child := range node.Children {
		r.applyColumnPruning(child, required, changed)
	}
}

// JoinOrderOptimizationRule optimizes the order of JOIN operations
type JoinOrderOptimizationRule struct{}

func (r *JoinOrderOptimizationRule) Name() string { return "JoinOrderOptimization" }
func (r *JoinOrderOptimizationRule) Cost() int    { return 3 }

func (r *JoinOrderOptimizationRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	changed := false
	r.optimizeJoinOrder(plan.Root, &changed)
	return plan, changed
}

func (r *JoinOrderOptimizationRule) optimizeJoinOrder(node *PlanNode, changed *bool) {
	if node == nil {
		return
	}

	// Look for join nodes and optimize their order
	if node.Type == PlanNodeJoin && len(node.Children) >= 2 {
		// Simple heuristic: put smaller table on the right (build side)
		left, right := node.Children[0], node.Children[1]

		// If left table is smaller than right table, swap them
		// to ensure smaller table is on the right (build side)
		if r.estimateRows(left) < r.estimateRows(right) {
			// Swap join order
			node.Children[0], node.Children[1] = right, left
			*changed = true
		}
	}

	// Recursively optimize children
	for _, child := range node.Children {
		r.optimizeJoinOrder(child, changed)
	}
}

func (r *JoinOrderOptimizationRule) estimateRows(node *PlanNode) int64 {
	if node == nil {
		return 1000 // Default estimate
	}
	return node.Rows
}

// ConstantFoldingRule folds constant expressions
type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Name() string { return "ConstantFolding" }
func (r *ConstantFoldingRule) Cost() int    { return 1 }

func (r *ConstantFoldingRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	changed := false
	r.foldConstants(plan.Root, &changed)
	return plan, changed
}

func (r *ConstantFoldingRule) foldConstants(node *PlanNode, changed *bool) {
	if node == nil {
		return
	}

	// Look for filter conditions with constant expressions
	for i, condition := range node.FilterConditions {
		if r.isConstantExpression(condition) {
			// Evaluate the constant expression
			if result := r.evaluateConstant(condition); result != nil {
				// Replace with simplified condition
				node.FilterConditions[i] = *result
				*changed = true
			}
		}
	}

	// Recursively process children
	for _, child := range node.Children {
		r.foldConstants(child, changed)
	}
}

func (r *ConstantFoldingRule) isConstantExpression(condition WhereCondition) bool {
	// Check if both sides are constants
	return condition.Column == "" && condition.Function == nil &&
		strings.Contains(condition.Operator, "=") &&
		condition.Value != nil
}

func (r *ConstantFoldingRule) evaluateConstant(condition WhereCondition) *WhereCondition {
	// Simple constant folding - in practice this would be more sophisticated
	if condition.Operator == "=" && condition.Value != nil {
		// Example: WHERE 1 = 1 becomes WHERE TRUE
		if fmt.Sprintf("%v", condition.Value) == fmt.Sprintf("%v", condition.Value) {
			return &WhereCondition{
				Column:   "",
				Operator: "TRUE",
				Value:    true,
			}
		}
	}
	return nil
}

// Optimize applies all optimization rules to a query plan
func (opt *QueryOptimizer) Optimize(plan *QueryPlan) *QueryPlan {
	if plan == nil {
		return plan
	}

	// Define optimization rules in order of application
	rules := []OptimizationRule{
		&PredicatePushdownRule{},
		&ColumnPruningRule{},
		&ConstantFoldingRule{},
		&JoinOrderOptimizationRule{},
	}

	optimizedPlan := plan
	maxIterations := 5 // Prevent infinite loops

	for iteration := 0; iteration < maxIterations; iteration++ {
		overallChanged := false

		for _, rule := range rules {
			newPlan, changed := rule.Apply(optimizedPlan)
			optimizedPlan = newPlan
			overallChanged = overallChanged || changed
		}

		// If no rules made changes, we're done
		if !overallChanged {
			break
		}
	}

	return optimizedPlan
}

// GetOptimizationStats returns statistics about applied optimizations
func (opt *QueryOptimizer) GetOptimizationStats(originalPlan, optimizedPlan *QueryPlan) map[string]interface{} {
	stats := make(map[string]interface{})

	// Count node types in original vs optimized
	originalCounts := opt.countNodeTypes(originalPlan.Root)
	optimizedCounts := opt.countNodeTypes(optimizedPlan.Root)

	stats["original_nodes"] = originalCounts
	stats["optimized_nodes"] = optimizedCounts

	// Calculate estimated performance improvement
	originalCost := opt.estimatePlanCost(originalPlan.Root)
	optimizedCost := opt.estimatePlanCost(optimizedPlan.Root)

	stats["original_cost"] = originalCost
	stats["optimized_cost"] = optimizedCost

	if originalCost > 0 {
		improvement := float64(originalCost-optimizedCost) / float64(originalCost) * 100
		stats["improvement_percent"] = improvement
	}

	return stats
}

func (opt *QueryOptimizer) countNodeTypes(node *PlanNode) map[string]int {
	counts := make(map[string]int)
	opt.countNode(node, counts)
	return counts
}

func (opt *QueryOptimizer) countNode(node *PlanNode, counts map[string]int) {
	if node == nil {
		return
	}

	counts[string(node.Type)]++

	for _, child := range node.Children {
		opt.countNode(child, counts)
	}
}

func (opt *QueryOptimizer) estimatePlanCost(node *PlanNode) int64 {
	if node == nil {
		return 0
	}

	cost := node.Rows // Base cost is the number of rows processed

	// Add costs for different operations
	switch node.Type {
	case PlanNodeScan:
		cost *= 1 // Scan is relatively cheap
	case PlanNodeFilter:
		cost *= 2 // Filtering requires evaluation
	case PlanNodeJoin:
		cost *= 5 // Joins are expensive
	case PlanNodeSort:
		cost *= 3 // Sorting is moderately expensive
	case PlanNodeAggregate:
		cost *= 2 // Aggregation requires grouping
	}

	// Add costs from children
	for _, child := range node.Children {
		cost += opt.estimatePlanCost(child)
	}

	return cost
}
