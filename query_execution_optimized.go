package main

import (
	"fmt"
)

// executeOptimized attempts to execute a query using optimization
func (qe *QueryEngine) executeOptimized(query *ParsedQuery) *QueryResult {
	// Create an execution plan
	plan, err := qe.planner.CreatePlan(query, qe)
	if err != nil {
		// If planning fails, fall back to original execution
		return nil
	}

	// Apply optimizations
	originalPlan := plan
	optimizedPlan := qe.optimizer.Optimize(plan)

	// Execute the optimized plan
	result, err := qe.executePlan(optimizedPlan, query)
	if err != nil {
		// If optimized execution fails, fall back to original
		return nil
	}

	// Add optimization statistics to the result if in debug mode
	if qe.isDebugMode() {
		stats := qe.optimizer.GetOptimizationStats(originalPlan, optimizedPlan)
		result.OptimizationStats = stats
	}

	return result
}

// executePlan executes a query plan
func (qe *QueryEngine) executePlan(plan *QueryPlan, query *ParsedQuery) (*QueryResult, error) {
	if plan == nil || plan.Root == nil {
		return nil, fmt.Errorf("invalid query plan")
	}

	// Execute the plan starting from the root node
	rows, err := qe.executeNode(plan.Root, query)
	if err != nil {
		return nil, err
	}

	// Build the result
	columns := qe.getResultColumns(rows, query.Columns)
	
	return &QueryResult{
		Columns: columns,
		Rows:    rows,
		Count:   len(rows),
		Query:   query.RawSQL,
	}, nil
}

// executeNode executes a single plan node
func (qe *QueryEngine) executeNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	switch node.Type {
	case PlanNodeScan:
		return qe.executeScanNode(node, query)
	case PlanNodeFilter:
		return qe.executeFilterNode(node, query)
	case PlanNodeProject:
		return qe.executeProjectNode(node, query)
	case PlanNodeJoin:
		return qe.executeJoinNode(node, query)
	case PlanNodeSort:
		return qe.executeSortNode(node, query)
	case PlanNodeLimit:
		return qe.executeLimitNode(node, query)
	case PlanNodeAggregate:
		return qe.executeAggregateNode(node, query)
	default:
		return nil, fmt.Errorf("unsupported node type: %v", node.Type)
	}
}

// executeScanNode executes a table scan with optimizations
func (qe *QueryEngine) executeScanNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	reader, err := qe.getReader(node.TableName)
	if err != nil {
		return nil, err
	}

	var rows []Row

	// Apply column pruning if specified
	if len(node.Columns) > 0 && !qe.hasWildcard(node.Columns) {
		rows, err = reader.ReadAllWithColumns(node.Columns)
	} else {
		rows, err = reader.ReadAll()
	}

	if err != nil {
		return nil, err
	}

	// Apply pushed-down filter conditions at scan level (predicate pushdown)
	if len(node.FilterConditions) > 0 {
		rows = qe.applyOptimizedFilters(rows, node.FilterConditions, reader)
	}

	return rows, nil
}

// executeFilterNode executes filter operations
func (qe *QueryEngine) executeFilterNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("filter node must have exactly one child")
	}

	// Execute child node first
	rows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Apply filters
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return nil, err
	}

	filtered := reader.FilterRowsWithEngine(rows, node.Filter, qe)
	return filtered, nil
}

// executeProjectNode executes column projection
func (qe *QueryEngine) executeProjectNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("project node must have exactly one child")
	}

	// Execute child node first
	rows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Apply column selection
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return nil, err
	}

	// Convert node columns to query columns format
	var queryColumns []Column
	for _, colName := range node.Columns {
		queryColumns = append(queryColumns, Column{Name: colName})
	}

	projected := reader.SelectColumns(rows, queryColumns)
	return projected, nil
}

// executeJoinNode executes join operations with optimization
func (qe *QueryEngine) executeJoinNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) < 2 {
		return nil, fmt.Errorf("join node must have at least two children")
	}

	// Execute left side (probe side)
	leftRows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Execute right side (build side)
	rightRows, err := qe.executeNode(node.Children[1], query)
	if err != nil {
		return nil, err
	}

	// Perform the join operation
	// This is a simplified hash join implementation
	joined := qe.performHashJoin(leftRows, rightRows, node.JoinCond, node.JoinType)
	return joined, nil
}

// executeSortNode executes sorting operations
func (qe *QueryEngine) executeSortNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("sort node must have exactly one child")
	}

	// Execute child node first
	rows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Apply sorting
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return nil, err
	}

	sorted := qe.sortRows(rows, node.OrderBy, reader)
	return sorted, nil
}

// executeLimitNode executes limit operations
func (qe *QueryEngine) executeLimitNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("limit node must have exactly one child")
	}

	// Execute child node first
	rows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Apply limit
	if node.LimitCount > 0 && len(rows) > node.LimitCount {
		rows = rows[:node.LimitCount]
	}

	return rows, nil
}

// executeAggregateNode executes aggregation operations
func (qe *QueryEngine) executeAggregateNode(node *PlanNode, query *ParsedQuery) ([]Row, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("aggregate node must have exactly one child")
	}

	// Execute child node first
	rows, err := qe.executeNode(node.Children[0], query)
	if err != nil {
		return nil, err
	}

	// Apply aggregation
	reader, err := qe.getReader(query.TableName)
	if err != nil {
		return nil, err
	}

	// Use existing aggregate execution logic
	result, err := qe.executeAggregate(query, rows, reader)
	if err != nil {
		return nil, err
	}

	return result.Rows, nil
}

// Helper methods for optimization

// applyOptimizedFilters applies filter conditions with optimizations
func (qe *QueryEngine) applyOptimizedFilters(rows []Row, conditions []WhereCondition, reader *ParquetReader) []Row {
	// Use the existing filter logic but with potential early termination optimizations
	return reader.FilterRowsWithEngine(rows, conditions, qe)
}

// performHashJoin performs an optimized hash join
func (qe *QueryEngine) performHashJoin(leftRows, rightRows []Row, joinCond JoinCondition, joinType JoinType) []Row {
	// Create a hash table from the smaller relation (typically the right side)
	rightHash := make(map[interface{}][]Row)
	
	for _, rightRow := range rightRows {
		if rightVal, exists := rightRow[joinCond.RightColumn]; exists {
			rightHash[rightVal] = append(rightHash[rightVal], rightRow)
		}
	}

	// Probe the hash table with the left relation
	var result []Row
	for _, leftRow := range leftRows {
		if leftVal, exists := leftRow[joinCond.LeftColumn]; exists {
			if rightMatches, found := rightHash[leftVal]; found {
				// Join the rows
				for _, rightRow := range rightMatches {
					joinedRow := make(Row)
					// Copy left row
					for k, v := range leftRow {
						joinedRow[k] = v
					}
					// Copy right row (with potential aliasing)
					for k, v := range rightRow {
						joinedRow[k] = v
					}
					result = append(result, joinedRow)
				}
			}
		}
	}

	return result
}

// hasWildcard checks if the column list contains a wildcard
func (qe *QueryEngine) hasWildcard(columns []string) bool {
	for _, col := range columns {
		if col == "*" {
			return true
		}
	}
	return false
}

// isDebugMode checks if debug mode is enabled (can be extended to check environment variables)
func (qe *QueryEngine) isDebugMode() bool {
	// For now, always return false. In production, this could check an environment variable
	// or a configuration setting
	return false
}

// GetOptimizationStats returns optimization statistics for the last query
func (qe *QueryEngine) GetOptimizationStats(sql string) (map[string]interface{}, error) {
	// Parse the query
	parsedQuery, err := qe.parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Create original plan
	originalPlan, err := qe.planner.CreatePlan(parsedQuery, qe)
	if err != nil {
		return nil, err
	}

	// Create optimized plan
	optimizedPlan := qe.optimizer.Optimize(originalPlan)

	// Return comparison statistics
	return qe.optimizer.GetOptimizationStats(originalPlan, optimizedPlan), nil
}