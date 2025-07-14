package core

import (
	"fmt"
)

// QueryPlanner creates execution plans for queries
type QueryPlanner struct {
	statsCollector *StatsCollector
}

// NewQueryPlanner creates a new query planner
func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{
		statsCollector: NewStatsCollector(),
	}
}

// CreatePlan creates an execution plan for a parsed query
func (qp *QueryPlanner) CreatePlan(query *ParsedQuery, engine *QueryEngine) (*QueryPlan, error) {
	// Handle CTEs first
	var cteNodes []*PlanNode
	if len(query.CTEs) > 0 {
		for _, cte := range query.CTEs {
			ctePlan, err := qp.CreatePlan(cte.Query, engine)
			if err != nil {
				return nil, fmt.Errorf("failed to create plan for CTE %s: %w", cte.Name, err)
			}
			cteNode := &PlanNode{
				Type:     PlanNodeCTE,
				CTEName:  cte.Name,
				Children: []*PlanNode{ctePlan.Root},
			}
			cteNodes = append(cteNodes, cteNode)
		}
	}

	// Build the main query plan
	var root *PlanNode

	// Start with table scan or CTE reference
	if query.TableName != "" {
		root = qp.createScanNode(query, engine)
	}

	// Add filter node if there are WHERE conditions
	if len(query.Where) > 0 && root != nil {
		root = qp.createFilterNode(query.Where, root)
	}

	// Add join nodes if present
	if query.HasJoins && len(query.Joins) > 0 {
		for _, join := range query.Joins {
			rightScan := &PlanNode{
				Type:      PlanNodeScan,
				TableName: join.TableName,
			}
			// Get statistics for the right table
			if stats := qp.getTableStats(join.TableName, engine); stats != nil {
				rightScan.Statistics = stats
				rightScan.Rows = stats.TableRows
			}

			root = qp.createJoinNode(join, root, rightScan)
		}
	}

	// Add aggregate node if needed
	if query.IsAggregate {
		root = qp.createAggregateNode(query, root)
	}

	// Add window function node if present
	if query.HasWindowFuncs {
		root = qp.createWindowNode(query.WindowFuncs, root)
	}

	// Add sort node if ORDER BY is present
	if len(query.OrderBy) > 0 && root != nil {
		root = qp.createSortNode(query.OrderBy, root)
	}

	// Add project node to select specific columns
	if len(query.Columns) > 0 && root != nil {
		root = qp.createProjectNode(query.Columns, root)
	}

	// Add limit node if LIMIT is present
	if query.Limit > 0 && root != nil {
		root = qp.createLimitNode(query.Limit, root)
	}

	// Prepend CTE nodes if any
	if len(cteNodes) > 0 {
		// CTEs are materialized first, then the main query executes
		for i := len(cteNodes) - 1; i >= 0; i-- {
			cteNodes[i].Children = append(cteNodes[i].Children, root)
			root = cteNodes[i]
		}
	}

	// Calculate costs and row estimates
	if root != nil {
		qp.estimatePlanCosts(root)
	}

	return &QueryPlan{Root: root}, nil
}

// createScanNode creates a table scan node
func (qp *QueryPlanner) createScanNode(query *ParsedQuery, engine *QueryEngine) *PlanNode {
	node := &PlanNode{
		Type:      PlanNodeScan,
		TableName: query.TableName,
	}

	// Get table statistics
	if stats := qp.getTableStats(query.TableName, engine); stats != nil {
		node.Statistics = stats
		node.Rows = stats.TableRows
	} else {
		node.Rows = 1000 // Default estimate
	}

	// Determine which columns to scan
	requiredCols := query.GetRequiredColumns()
	if len(requiredCols) > 0 {
		node.Columns = requiredCols
	}

	return node
}

// createFilterNode creates a filter node
func (qp *QueryPlanner) createFilterNode(conditions []WhereCondition, child *PlanNode) *PlanNode {
	node := &PlanNode{
		Type:     PlanNodeFilter,
		Filter:   conditions,
		Children: []*PlanNode{child},
	}

	// Estimate selectivity
	selectivity := qp.estimateSelectivity(conditions, child.Statistics)
	if node.Statistics == nil {
		node.Statistics = &NodeStatistics{}
	}
	node.Statistics.Selectivity = selectivity

	return node
}

// createProjectNode creates a projection node
func (qp *QueryPlanner) createProjectNode(columns []Column, child *PlanNode) *PlanNode {
	// Extract column names
	var colNames []string
	for _, col := range columns {
		if col.Name != "" {
			colNames = append(colNames, col.Name)
		}
	}

	return &PlanNode{
		Type:     PlanNodeProject,
		Columns:  colNames,
		Children: []*PlanNode{child},
	}
}

// createAggregateNode creates an aggregate node
func (qp *QueryPlanner) createAggregateNode(query *ParsedQuery, child *PlanNode) *PlanNode {
	return &PlanNode{
		Type:     PlanNodeAggregate,
		GroupBy:  query.GroupBy,
		Children: []*PlanNode{child},
	}
}

// createSortNode creates a sort node
func (qp *QueryPlanner) createSortNode(orderBy []OrderByColumn, child *PlanNode) *PlanNode {
	return &PlanNode{
		Type:     PlanNodeSort,
		OrderBy:  orderBy,
		Children: []*PlanNode{child},
	}
}

// createLimitNode creates a limit node
func (qp *QueryPlanner) createLimitNode(limit int, child *PlanNode) *PlanNode {
	return &PlanNode{
		Type:       PlanNodeLimit,
		LimitCount: limit,
		Children:   []*PlanNode{child},
	}
}

// createJoinNode creates a join node
func (qp *QueryPlanner) createJoinNode(join JoinClause, left, right *PlanNode) *PlanNode {
	return &PlanNode{
		Type:     PlanNodeJoin,
		JoinType: join.Type,
		JoinCond: join.Condition,
		Children: []*PlanNode{left, right},
	}
}

// createWindowNode creates a window function node
func (qp *QueryPlanner) createWindowNode(windowFuncs []WindowFunction, child *PlanNode) *PlanNode {
	return &PlanNode{
		Type:        PlanNodeWindow,
		WindowFuncs: windowFuncs,
		Children:    []*PlanNode{child},
	}
}

// estimatePlanCosts recursively estimates costs for all nodes in the plan
func (qp *QueryPlanner) estimatePlanCosts(node *PlanNode) {
	if node == nil {
		return
	}

	// First estimate costs for children
	for _, child := range node.Children {
		qp.estimatePlanCosts(child)
	}

	// Estimate rows based on parent
	if len(node.Children) > 0 {
		node.Rows = node.EstimateRows(node.Children[0].Rows)
	} else {
		node.Rows = node.EstimateRows(0)
	}

	// Estimate cost
	node.EstimateCost()

	// Estimate row width (simplified)
	node.Width = qp.estimateRowWidth(node)
}

// estimateRowWidth estimates the average row width in bytes
func (qp *QueryPlanner) estimateRowWidth(node *PlanNode) int {
	switch node.Type {
	case PlanNodeScan:
		// Estimate based on number of columns
		return len(node.Columns) * 20 // Assume average 20 bytes per column
	case PlanNodeProject:
		return len(node.Columns) * 20
	case PlanNodeAggregate:
		// Aggregates typically produce fewer, wider columns
		return 100
	default:
		if len(node.Children) > 0 {
			return node.Children[0].Width
		}
		return 50 // Default
	}
}

// estimateSelectivity estimates the selectivity of filter conditions
func (qp *QueryPlanner) estimateSelectivity(conditions []WhereCondition, stats *NodeStatistics) float64 {
	if len(conditions) == 0 {
		return 1.0
	}

	selectivity := 1.0
	for _, cond := range conditions {
		condSelectivity := qp.estimateConditionSelectivity(cond, stats)
		if cond.LogicalOp == "OR" && cond.IsComplex {
			// OR increases selectivity
			selectivity = selectivity + condSelectivity - (selectivity * condSelectivity)
		} else {
			// AND decreases selectivity
			selectivity *= condSelectivity
		}
	}

	return selectivity
}

// estimateConditionSelectivity estimates selectivity for a single condition
func (qp *QueryPlanner) estimateConditionSelectivity(cond WhereCondition, stats *NodeStatistics) float64 {
	// Handle complex conditions recursively
	if cond.IsComplex {
		leftSel := 0.5
		rightSel := 0.5
		if cond.Left != nil {
			leftSel = qp.estimateConditionSelectivity(*cond.Left, stats)
		}
		if cond.Right != nil {
			rightSel = qp.estimateConditionSelectivity(*cond.Right, stats)
		}

		if cond.LogicalOp == "OR" {
			return leftSel + rightSel - (leftSel * rightSel)
		}
		return leftSel * rightSel
	}

	// Use statistics if available
	if stats != nil && stats.DistinctValues != nil {
		if distinct, ok := stats.DistinctValues[cond.Column]; ok && distinct > 0 {
			switch cond.Operator {
			case "=":
				return 1.0 / float64(distinct)
			case "!=", "<>":
				return 1.0 - (1.0 / float64(distinct))
			case ">", ">=", "<", "<=":
				return 0.3 // Default range selectivity
			case "BETWEEN":
				return 0.25
			case "IN":
				if len(cond.ValueList) > 0 {
					return float64(len(cond.ValueList)) / float64(distinct)
				}
			}
		}
	}

	// Default selectivities
	switch cond.Operator {
	case "=":
		return 0.1
	case "!=", "<>":
		return 0.9
	case ">", ">=", "<", "<=":
		return 0.3
	case "BETWEEN":
		return 0.25
	case "NOT BETWEEN":
		return 0.75
	case "IN":
		if len(cond.ValueList) > 0 {
			return float64(len(cond.ValueList)) * 0.1
		}
		return 0.1
	case "NOT IN":
		return 0.9
	case "LIKE":
		return 0.25
	case "IS NULL":
		return 0.05
	case "IS NOT NULL":
		return 0.95
	case "EXISTS":
		return 0.5
	case "NOT EXISTS":
		return 0.5
	default:
		return 0.5
	}
}

// getTableStats retrieves statistics for a table
func (qp *QueryPlanner) getTableStats(tableName string, engine *QueryEngine) *NodeStatistics {
	// For now, return basic statistics
	// In a real implementation, this would read from stored statistics
	stats := &NodeStatistics{
		TableRows:      1000, // Default estimate
		DistinctValues: make(map[string]int64),
		MinValues:      make(map[string]interface{}),
		MaxValues:      make(map[string]interface{}),
		NullCount:      make(map[string]int64),
	}

	// Try to get actual row count by opening the reader
	reader, err := engine.getReader(tableName)
	if err == nil && reader != nil {
		// This is a simplified approach - in production, we'd cache these stats
		stats.TableRows = int64(reader.GetRowCount())

		// Estimate distinct values for common columns
		columns := reader.GetColumnNames()
		for _, col := range columns {
			// Rough estimates based on column names
			switch col {
			case "id", "employee_id", "product_id", "department_id":
				stats.DistinctValues[col] = stats.TableRows // Assume unique
			case "department", "category", "type", "status":
				stats.DistinctValues[col] = stats.TableRows / 10 // ~10% distinct
			case "name", "email":
				stats.DistinctValues[col] = stats.TableRows * 9 / 10 // ~90% distinct
			default:
				stats.DistinctValues[col] = stats.TableRows / 2 // ~50% distinct
			}
		}
	}

	return stats
}
