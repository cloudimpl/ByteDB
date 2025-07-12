package main

import (
	"fmt"
	"strings"
)

// PlanNodeType represents the type of operation in the query plan
type PlanNodeType string

const (
	PlanNodeScan        PlanNodeType = "TableScan"
	PlanNodeFilter      PlanNodeType = "Filter"
	PlanNodeProject     PlanNodeType = "Project"
	PlanNodeAggregate   PlanNodeType = "Aggregate"
	PlanNodeSort        PlanNodeType = "Sort"
	PlanNodeLimit       PlanNodeType = "Limit"
	PlanNodeJoin        PlanNodeType = "Join"
	PlanNodeCTE         PlanNodeType = "CTE"
	PlanNodeWindow      PlanNodeType = "Window"
	PlanNodeSubquery    PlanNodeType = "Subquery"
	PlanNodeMaterialize PlanNodeType = "Materialize"
)

// QueryPlan represents the execution plan for a query
type QueryPlan struct {
	Root *PlanNode
}

// PlanNode represents a single operation in the query plan
type PlanNode struct {
	Type        PlanNodeType
	Children    []*PlanNode
	Cost        float64 // Estimated cost of this operation
	Rows        int64   // Estimated number of rows
	Width       int     // Estimated average row width in bytes
	ActualRows  int64   // Actual rows (for EXPLAIN ANALYZE)
	ActualTime  float64 // Actual execution time in ms (for EXPLAIN ANALYZE)
	
	// Operation-specific fields
	TableName   string            // For Scan nodes
	Columns     []string          // For Project nodes
	Filter      []WhereCondition  // For Filter nodes
	JoinType    JoinType          // For Join nodes
	JoinCond    JoinCondition     // For Join nodes
	GroupBy     []string          // For Aggregate nodes
	OrderBy     []OrderByColumn   // For Sort nodes
	LimitCount  int               // For Limit nodes
	WindowFuncs []WindowFunction  // For Window nodes
	CTEName     string            // For CTE nodes
	
	// Statistics and metadata
	Statistics  *NodeStatistics
}

// NodeStatistics contains statistical information about a plan node
type NodeStatistics struct {
	TableRows      int64              // Total rows in table
	DistinctValues map[string]int64   // Distinct values per column
	MinValues      map[string]interface{} // Min values per column
	MaxValues      map[string]interface{} // Max values per column
	NullCount      map[string]int64   // Null count per column
	Selectivity    float64            // Estimated selectivity of filters
}

// String returns a string representation of the query plan
func (qp *QueryPlan) String() string {
	if qp.Root == nil {
		return "Empty query plan"
	}
	return qp.Root.String(0)
}

// String returns a string representation of a plan node
func (pn *PlanNode) String(indent int) string {
	return pn.StringWithOptions(indent, true)
}

// StringWithOptions returns a string representation with options
func (pn *PlanNode) StringWithOptions(indent int, showCosts bool) string {
	prefix := strings.Repeat("  ", indent)
	var result strings.Builder
	
	// Node type and basic info
	result.WriteString(fmt.Sprintf("%s-> %s", prefix, pn.Type))
	
	// Add operation-specific details
	switch pn.Type {
	case PlanNodeScan:
		result.WriteString(fmt.Sprintf(" on %s", pn.TableName))
		if len(pn.Columns) > 0 {
			result.WriteString(fmt.Sprintf(" [%s]", strings.Join(pn.Columns, ", ")))
		}
	case PlanNodeFilter:
		result.WriteString(fmt.Sprintf(" [%d conditions]", len(pn.Filter)))
	case PlanNodeProject:
		result.WriteString(fmt.Sprintf(" [%s]", strings.Join(pn.Columns, ", ")))
	case PlanNodeAggregate:
		if len(pn.GroupBy) > 0 {
			result.WriteString(fmt.Sprintf(" GROUP BY %s", strings.Join(pn.GroupBy, ", ")))
		}
	case PlanNodeSort:
		var sortKeys []string
		for _, col := range pn.OrderBy {
			sortKeys = append(sortKeys, fmt.Sprintf("%s %s", col.Column, col.Direction))
		}
		result.WriteString(fmt.Sprintf(" [%s]", strings.Join(sortKeys, ", ")))
	case PlanNodeLimit:
		result.WriteString(fmt.Sprintf(" %d", pn.LimitCount))
	case PlanNodeJoin:
		result.WriteString(fmt.Sprintf(" (%s)", getJoinTypeName(pn.JoinType)))
	case PlanNodeCTE:
		result.WriteString(fmt.Sprintf(" %s", pn.CTEName))
	}
	
	// Add cost information if requested
	if showCosts {
		result.WriteString(fmt.Sprintf(" (cost=%.2f rows=%d width=%d)", pn.Cost, pn.Rows, pn.Width))
	}
	
	// Add actual execution stats if available
	if pn.ActualRows > 0 || pn.ActualTime > 0 {
		result.WriteString(fmt.Sprintf(" (actual rows=%d time=%.3fms)", pn.ActualRows, pn.ActualTime))
	}
	
	result.WriteString("\n")
	
	// Recursively print children
	for _, child := range pn.Children {
		result.WriteString(child.StringWithOptions(indent + 1, showCosts))
	}
	
	return result.String()
}

// ExplainFormat represents the output format for EXPLAIN
type ExplainFormat string

const (
	ExplainFormatText ExplainFormat = "TEXT"
	ExplainFormatJSON ExplainFormat = "JSON"
	ExplainFormatYAML ExplainFormat = "YAML"
)

// ExplainOptions contains options for EXPLAIN command
type ExplainOptions struct {
	Analyze bool          // Run the query and show actual statistics
	Verbose bool          // Show additional details
	Costs   bool          // Show cost estimates (default true)
	Buffers bool          // Show buffer usage (for ANALYZE)
	Format  ExplainFormat // Output format
}

// getJoinTypeName returns a string representation of the join type
func getJoinTypeName(jt JoinType) string {
	switch jt {
	case INNER_JOIN:
		return "Inner Join"
	case LEFT_JOIN:
		return "Left Join"
	case RIGHT_JOIN:
		return "Right Join"
	case FULL_OUTER_JOIN:
		return "Full Outer Join"
	default:
		return "Unknown Join"
	}
}

// EstimateCost estimates the cost of executing this node
func (pn *PlanNode) EstimateCost() float64 {
	baseCost := 0.0
	
	switch pn.Type {
	case PlanNodeScan:
		// Base cost for scanning a table
		baseCost = float64(pn.Rows) * 0.01
		if pn.Statistics != nil && pn.Statistics.TableRows > 0 {
			// Adjust based on actual table size
			baseCost = float64(pn.Statistics.TableRows) * 0.01
		}
		
	case PlanNodeFilter:
		// Cost of evaluating conditions
		if len(pn.Children) > 0 {
			baseCost = pn.Children[0].Cost + float64(pn.Children[0].Rows)*0.001*float64(len(pn.Filter))
		}
		
	case PlanNodeProject:
		// Cost of selecting specific columns
		if len(pn.Children) > 0 {
			baseCost = pn.Children[0].Cost + float64(pn.Children[0].Rows)*0.0001*float64(len(pn.Columns))
		}
		
	case PlanNodeAggregate:
		// Cost of aggregation
		if len(pn.Children) > 0 {
			// Higher cost for grouping
			groupCost := 0.0
			if len(pn.GroupBy) > 0 {
				groupCost = float64(pn.Children[0].Rows) * 0.01 * float64(len(pn.GroupBy))
			}
			baseCost = pn.Children[0].Cost + groupCost + float64(pn.Children[0].Rows)*0.005
		}
		
	case PlanNodeSort:
		// Cost of sorting (n log n)
		if len(pn.Children) > 0 {
			rows := float64(pn.Children[0].Rows)
			if rows > 0 {
				baseCost = pn.Children[0].Cost + rows*logBase2(rows)*0.01
			}
		}
		
	case PlanNodeJoin:
		// Cost of join operation
		if len(pn.Children) >= 2 {
			leftRows := float64(pn.Children[0].Rows)
			rightRows := float64(pn.Children[1].Rows)
			
			switch pn.JoinType {
			case INNER_JOIN:
				// Nested loop join cost approximation
				baseCost = pn.Children[0].Cost + pn.Children[1].Cost + leftRows*rightRows*0.001
			case LEFT_JOIN, RIGHT_JOIN:
				// Slightly higher cost for outer joins
				baseCost = pn.Children[0].Cost + pn.Children[1].Cost + leftRows*rightRows*0.0015
			case FULL_OUTER_JOIN:
				// Highest cost for full outer join
				baseCost = pn.Children[0].Cost + pn.Children[1].Cost + leftRows*rightRows*0.002
			}
		}
		
	case PlanNodeLimit:
		// Minimal cost for limit
		if len(pn.Children) > 0 {
			baseCost = pn.Children[0].Cost + 0.001
		}
		
	case PlanNodeWindow:
		// Cost of window functions
		if len(pn.Children) > 0 {
			rows := float64(pn.Children[0].Rows)
			// Window functions require sorting and computation
			baseCost = pn.Children[0].Cost + rows*logBase2(rows)*0.02*float64(len(pn.WindowFuncs))
		}
		
	case PlanNodeCTE:
		// Cost of materializing CTE
		if len(pn.Children) > 0 {
			baseCost = pn.Children[0].Cost * 1.1 // 10% overhead for materialization
		}
		
	case PlanNodeSubquery:
		// Cost of subquery execution
		if len(pn.Children) > 0 {
			baseCost = pn.Children[0].Cost * 1.2 // 20% overhead for subquery
		}
	}
	
	pn.Cost = baseCost
	return baseCost
}

// EstimateRows estimates the number of rows this node will produce
func (pn *PlanNode) EstimateRows(parentRows int64) int64 {
	switch pn.Type {
	case PlanNodeScan:
		if pn.Statistics != nil && pn.Statistics.TableRows > 0 {
			return pn.Statistics.TableRows
		}
		return 1000 // Default estimate
		
	case PlanNodeFilter:
		// Apply selectivity estimate
		if pn.Statistics != nil && pn.Statistics.Selectivity > 0 {
			return int64(float64(parentRows) * pn.Statistics.Selectivity)
		}
		// Default: each condition reduces rows by 50%
		reduction := 1.0
		for i := 0; i < len(pn.Filter); i++ {
			reduction *= 0.5
		}
		return int64(float64(parentRows) * reduction)
		
	case PlanNodeProject:
		// Project doesn't change row count
		return parentRows
		
	case PlanNodeAggregate:
		if len(pn.GroupBy) > 0 {
			// Estimate based on distinct values
			if pn.Statistics != nil && len(pn.Statistics.DistinctValues) > 0 {
				minDistinct := int64(parentRows)
				for _, col := range pn.GroupBy {
					if distinct, ok := pn.Statistics.DistinctValues[col]; ok && distinct < minDistinct {
						minDistinct = distinct
					}
				}
				return minDistinct
			}
			// Default: 10% of input rows
			return parentRows / 10
		}
		// No GROUP BY means single row result
		return 1
		
	case PlanNodeSort:
		// Sort doesn't change row count
		return parentRows
		
	case PlanNodeLimit:
		if int64(pn.LimitCount) < parentRows {
			return int64(pn.LimitCount)
		}
		return parentRows
		
	case PlanNodeJoin:
		if len(pn.Children) >= 2 {
			leftRows := pn.Children[0].Rows
			rightRows := pn.Children[1].Rows
			
			switch pn.JoinType {
			case INNER_JOIN:
				// Estimate based on selectivity
				return int64(float64(leftRows*rightRows) * 0.1) // 10% selectivity default
			case LEFT_JOIN:
				return leftRows
			case RIGHT_JOIN:
				return rightRows
			case FULL_OUTER_JOIN:
				return leftRows + rightRows
			}
		}
		return parentRows
		
	default:
		return parentRows
	}
}

// logBase2 calculates log base 2
func logBase2(n float64) float64 {
	if n <= 1 {
		return 1
	}
	return float64(int(n/2)) + 1 // Simplified log calculation
}