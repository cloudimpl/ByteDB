package developer

import (
	"bytedb/core"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// QueryExplainer provides detailed query execution plans and analysis
type QueryExplainer struct {
	engine          *core.QueryEngine
	enableTiming    bool
	enableCostAnalysis bool
	verboseMode     bool
}

func NewQueryExplainer(engine *core.QueryEngine) *QueryExplainer {
	return &QueryExplainer{
		engine:          engine,
		enableTiming:    true,
		enableCostAnalysis: true,
		verboseMode:     false,
	}
}

// ExplainPlan represents a comprehensive query execution plan
type ExplainPlan struct {
	Query           string              `json:"query"`
	ParsedQuery     *core.ParsedQuery   `json:"parsed_query"`
	ExecutionPlan   *ExecutionPlan      `json:"execution_plan"`
	CostAnalysis    *CostAnalysis       `json:"cost_analysis"`
	OptimizationInfo *OptimizationInfo  `json:"optimization_info"`
	Recommendations []string            `json:"recommendations"`
	Warnings        []string            `json:"warnings"`
	Statistics      *PlanStatistics     `json:"statistics"`
	GeneratedAt     time.Time           `json:"generated_at"`
}

type ExecutionPlan struct {
	RootNode    *PlanNode       `json:"root_node"`
	NodeCount   int             `json:"node_count"`
	MaxDepth    int             `json:"max_depth"`
	Operations  []string        `json:"operations"`
	Tables      []string        `json:"tables"`
	Indexes     []string        `json:"indexes_used"`
	JoinTypes   []string        `json:"join_types"`
}

type PlanNode struct {
	ID           string            `json:"id"`
	Operation    string            `json:"operation"`
	Description  string            `json:"description"`
	Table        string            `json:"table,omitempty"`
	Columns      []string          `json:"columns,omitempty"`
	Condition    string            `json:"condition,omitempty"`
	EstimatedRows int64            `json:"estimated_rows"`
	EstimatedCost float64          `json:"estimated_cost"`
	EstimatedTime time.Duration    `json:"estimated_time"`
	Children     []*PlanNode       `json:"children,omitempty"`
	Properties   map[string]interface{} `json:"properties,omitempty"`
	Optimizations []string         `json:"optimizations,omitempty"`
}

type CostAnalysis struct {
	TotalCost        float64            `json:"total_cost"`
	IOCost           float64            `json:"io_cost"`
	CPUCost          float64            `json:"cpu_cost"`
	NetworkCost      float64            `json:"network_cost"`
	MemoryCost       float64            `json:"memory_cost"`
	CostBreakdown    map[string]float64 `json:"cost_breakdown"`
	BottleneckNodes  []string           `json:"bottleneck_nodes"`
	CostDistribution []NodeCost         `json:"cost_distribution"`
}

type NodeCost struct {
	NodeID      string  `json:"node_id"`
	Operation   string  `json:"operation"`
	Cost        float64 `json:"cost"`
	Percentage  float64 `json:"percentage"`
}

type OptimizationInfo struct {
	RulesApplied     []OptimizationRule `json:"rules_applied"`
	RulesSkipped     []OptimizationRule `json:"rules_skipped"`
	OriginalCost     float64            `json:"original_cost"`
	OptimizedCost    float64            `json:"optimized_cost"`
	CostReduction    float64            `json:"cost_reduction_percent"`
	OptimizationTime time.Duration      `json:"optimization_time"`
}

type OptimizationRule struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Applied     bool    `json:"applied"`
	Reason      string  `json:"reason,omitempty"`
	Impact      float64 `json:"impact,omitempty"`
}

type PlanStatistics struct {
	EstimatedDuration   time.Duration `json:"estimated_duration"`
	EstimatedRows       int64         `json:"estimated_rows"`
	EstimatedDataSize   int64         `json:"estimated_data_size_bytes"`
	TablesAccessed      int           `json:"tables_accessed"`
	JoinsPerformed      int           `json:"joins_performed"`
	FiltersApplied      int           `json:"filters_applied"`
	AggregationsUsed    int           `json:"aggregations_used"`
	SubqueriesExecuted  int           `json:"subqueries_executed"`
	IndexesUsed         int           `json:"indexes_used"`
	SortOperations      int           `json:"sort_operations"`
}

// Explain generates a comprehensive execution plan for a query
func (qe *QueryExplainer) Explain(sql string) (*ExplainPlan, error) {
	startTime := time.Now()
	
	// Parse the query
	// TODO: Add GetParser() method to QueryEngine for better architecture
	parser := core.NewSQLParser()
	parsedQuery, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}
	
	// Generate execution plan
	executionPlan := qe.generateExecutionPlan(parsedQuery)
	
	// Perform cost analysis
	costAnalysis := qe.analyzeCosts(executionPlan)
	
	// Generate optimization information
	optimizationInfo := qe.analyzeOptimizations(parsedQuery, executionPlan)
	
	// Generate recommendations and warnings
	recommendations := qe.generateRecommendations(parsedQuery, executionPlan, costAnalysis)
	warnings := qe.generateWarnings(parsedQuery, executionPlan)
	
	// Calculate statistics
	statistics := qe.calculateStatistics(executionPlan)
	
	plan := &ExplainPlan{
		Query:           sql,
		ParsedQuery:     parsedQuery,
		ExecutionPlan:   executionPlan,
		CostAnalysis:    costAnalysis,
		OptimizationInfo: optimizationInfo,
		Recommendations: recommendations,
		Warnings:        warnings,
		Statistics:      statistics,
		GeneratedAt:     startTime,
	}
	
	return plan, nil
}

// generateExecutionPlan creates a detailed execution plan tree
func (qe *QueryExplainer) generateExecutionPlan(query *core.ParsedQuery) *ExecutionPlan {
	rootNode := qe.buildPlanTree(query)
	
	plan := &ExecutionPlan{
		RootNode:   rootNode,
		NodeCount:  qe.countNodes(rootNode),
		MaxDepth:   qe.calculateDepth(rootNode),
		Operations: qe.extractOperations(rootNode),
		Tables:     qe.extractTables(query),
		JoinTypes:  qe.extractJoinTypes(query),
	}
	
	return plan
}

// buildPlanTree constructs the execution plan tree
func (qe *QueryExplainer) buildPlanTree(query *core.ParsedQuery) *PlanNode {
	var rootNode *PlanNode
	
	// Determine root operation based on query type
	switch query.Type {
	case core.SELECT:
		rootNode = qe.buildSelectPlan(query)
	default:
		rootNode = &PlanNode{
			ID:          "root",
			Operation:   string(query.Type),
			Description: fmt.Sprintf("%s operation", query.Type),
		}
	}
	
	return rootNode
}

// buildSelectPlan builds execution plan for SELECT queries
func (qe *QueryExplainer) buildSelectPlan(query *core.ParsedQuery) *PlanNode {
	var children []*PlanNode
	
	// Build table scan nodes
	scanNode := &PlanNode{
		ID:            "scan_1",
		Operation:     "TABLE_SCAN",
		Description:   fmt.Sprintf("Sequential scan on %s", query.TableName),
		Table:         query.TableName,
		Columns:       qe.extractColumnNames(query.Columns),
		EstimatedRows: 1000, // Would be calculated from table statistics
		EstimatedCost: 100.0,
		EstimatedTime: 50 * time.Millisecond,
		Properties: map[string]interface{}{
			"scan_type":     "sequential",
			"predicate_pushdown": len(query.Where) > 0,
			"column_pruning":     len(query.Columns) > 0 && query.Columns[0].Name != "*",
		},
	}
	
	// Add filter conditions
	if len(query.Where) > 0 {
		scanNode.Condition = qe.formatConditions(query.Where)
		scanNode.Optimizations = append(scanNode.Optimizations, "predicate_pushdown")
		scanNode.EstimatedRows = int64(float64(scanNode.EstimatedRows) * 0.3) // Assume 30% selectivity
	}
	
	children = append(children, scanNode)
	
	// Add join nodes if present
	if len(query.Joins) > 0 {
		joinNode := qe.buildJoinPlan(query)
		children = append(children, joinNode)
	}
	
	// Build aggregation node if needed
	var currentNode *PlanNode = scanNode
	if query.IsAggregate || len(query.GroupBy) > 0 {
		aggNode := &PlanNode{
			ID:            "aggregate_1",
			Operation:     "AGGREGATE",
			Description:   fmt.Sprintf("GROUP BY %s", strings.Join(query.GroupBy, ", ")),
			EstimatedRows: int64(float64(scanNode.EstimatedRows) * 0.1), // Assume 10% cardinality
			EstimatedCost: scanNode.EstimatedCost * 1.5,
			EstimatedTime: scanNode.EstimatedTime + 30*time.Millisecond,
			Children:      []*PlanNode{currentNode},
			Properties: map[string]interface{}{
				"aggregates":  query.Aggregates,
				"group_by":    query.GroupBy,
				"hash_based":  true,
			},
		}
		currentNode = aggNode
	}
	
	// Add sort node if needed
	if len(query.OrderBy) > 0 {
		sortNode := &PlanNode{
			ID:            "sort_1",
			Operation:     "SORT",
			Description:   fmt.Sprintf("ORDER BY %s", qe.formatOrderBy(query.OrderBy)),
			EstimatedRows: currentNode.EstimatedRows,
			EstimatedCost: currentNode.EstimatedCost + float64(currentNode.EstimatedRows)*0.01,
			EstimatedTime: currentNode.EstimatedTime + 20*time.Millisecond,
			Children:      []*PlanNode{currentNode},
			Properties: map[string]interface{}{
				"sort_algorithm": "quicksort",
				"in_memory":      currentNode.EstimatedRows < 10000,
			},
		}
		currentNode = sortNode
	}
	
	// Add limit node if needed
	if query.Limit > 0 {
		limitNode := &PlanNode{
			ID:            "limit_1",
			Operation:     "LIMIT",
			Description:   fmt.Sprintf("LIMIT %d", query.Limit),
			EstimatedRows: min(int64(query.Limit), currentNode.EstimatedRows),
			EstimatedCost: currentNode.EstimatedCost,
			EstimatedTime: currentNode.EstimatedTime,
			Children:      []*PlanNode{currentNode},
			Properties: map[string]interface{}{
				"limit_count": query.Limit,
				"early_termination": true,
			},
		}
		currentNode = limitNode
	}
	
	return currentNode
}

// buildJoinPlan creates join execution plan nodes
func (qe *QueryExplainer) buildJoinPlan(query *core.ParsedQuery) *PlanNode {
	joinNode := &PlanNode{
		ID:            "join_1",
		Operation:     "HASH_JOIN",
		Description:   fmt.Sprintf("%s JOIN", query.Joins[0].Type),
		EstimatedRows: 1500, // Would be calculated based on join selectivity
		EstimatedCost: 200.0,
		EstimatedTime: 75 * time.Millisecond,
		Properties: map[string]interface{}{
			"join_type":      query.Joins[0].Type,
			"join_algorithm": "hash_join",
			"build_side":     "right",
		},
	}
	
	if len(query.Joins) > 0 && (query.Joins[0].Condition.LeftColumn != "" || query.Joins[0].Condition.RightColumn != "") {
		joinNode.Condition = qe.formatJoinCondition(query.Joins[0].Condition)
	}
	
	return joinNode
}

// analyzeCosts performs detailed cost analysis
func (qe *QueryExplainer) analyzeCosts(plan *ExecutionPlan) *CostAnalysis {
	totalCost := plan.RootNode.EstimatedCost
	
	costBreakdown := make(map[string]float64)
	costDistribution := []NodeCost{}
	
	qe.calculateNodeCosts(plan.RootNode, totalCost, &costBreakdown, &costDistribution)
	
	analysis := &CostAnalysis{
		TotalCost:        totalCost,
		IOCost:           totalCost * 0.4,  // 40% IO
		CPUCost:          totalCost * 0.3,  // 30% CPU
		NetworkCost:      totalCost * 0.2,  // 20% Network
		MemoryCost:       totalCost * 0.1,  // 10% Memory
		CostBreakdown:    costBreakdown,
		CostDistribution: costDistribution,
		BottleneckNodes:  qe.identifyBottlenecks(costDistribution),
	}
	
	return analysis
}

// calculateNodeCosts recursively calculates cost for each node
func (qe *QueryExplainer) calculateNodeCosts(node *PlanNode, totalCost float64, breakdown *map[string]float64, distribution *[]NodeCost) {
	if node == nil {
		return
	}
	
	percentage := (node.EstimatedCost / totalCost) * 100
	(*breakdown)[node.Operation] += node.EstimatedCost
	
	*distribution = append(*distribution, NodeCost{
		NodeID:     node.ID,
		Operation:  node.Operation,
		Cost:       node.EstimatedCost,
		Percentage: percentage,
	})
	
	for _, child := range node.Children {
		qe.calculateNodeCosts(child, totalCost, breakdown, distribution)
	}
}

// identifyBottlenecks finds the most expensive operations
func (qe *QueryExplainer) identifyBottlenecks(distribution []NodeCost) []string {
	var bottlenecks []string
	
	for _, nodeCost := range distribution {
		if nodeCost.Percentage > 25.0 { // More than 25% of total cost
			bottlenecks = append(bottlenecks, fmt.Sprintf("%s (%s: %.1f%%)", 
				nodeCost.NodeID, nodeCost.Operation, nodeCost.Percentage))
		}
	}
	
	return bottlenecks
}

// analyzeOptimizations provides optimization analysis
func (qe *QueryExplainer) analyzeOptimizations(query *core.ParsedQuery, plan *ExecutionPlan) *OptimizationInfo {
	rulesApplied := []OptimizationRule{}
	rulesSkipped := []OptimizationRule{}
	
	// Predicate pushdown analysis
	if len(query.Where) > 0 {
		rulesApplied = append(rulesApplied, OptimizationRule{
			Name:        "predicate_pushdown",
			Description: "Push WHERE conditions to table scan",
			Applied:     true,
			Impact:      30.0, // 30% cost reduction
		})
	}
	
	// Column pruning analysis
	if len(query.Columns) > 0 && query.Columns[0].Name != "*" {
		rulesApplied = append(rulesApplied, OptimizationRule{
			Name:        "column_pruning",
			Description: "Read only required columns",
			Applied:     true,
			Impact:      15.0, // 15% cost reduction
		})
	} else {
		rulesSkipped = append(rulesSkipped, OptimizationRule{
			Name:        "column_pruning",
			Description: "Read only required columns",
			Applied:     false,
			Reason:      "SELECT * used - consider specifying columns",
		})
	}
	
	// Join order optimization
	if len(query.Joins) > 1 {
		rulesSkipped = append(rulesSkipped, OptimizationRule{
			Name:        "join_reordering",
			Description: "Optimize join order based on table sizes",
			Applied:     false,
			Reason:      "Multiple joins detected - join order optimization available",
		})
	}
	
	originalCost := plan.RootNode.EstimatedCost * 1.5 // Assume 50% reduction from optimizations
	optimizedCost := plan.RootNode.EstimatedCost
	costReduction := ((originalCost - optimizedCost) / originalCost) * 100
	
	return &OptimizationInfo{
		RulesApplied:     rulesApplied,
		RulesSkipped:     rulesSkipped,
		OriginalCost:     originalCost,
		OptimizedCost:    optimizedCost,
		CostReduction:    costReduction,
		OptimizationTime: 5 * time.Millisecond,
	}
}

// generateRecommendations provides query optimization suggestions
func (qe *QueryExplainer) generateRecommendations(query *core.ParsedQuery, plan *ExecutionPlan, cost *CostAnalysis) []string {
	var recommendations []string
	
	// Check for SELECT *
	if len(query.Columns) > 0 && query.Columns[0].Name == "*" {
		recommendations = append(recommendations, 
			"Consider specifying only required columns instead of SELECT * to reduce I/O")
	}
	
	// Check for missing WHERE clauses on large tables
	if len(query.Where) == 0 && plan.RootNode.EstimatedRows > 1000 {
		recommendations = append(recommendations,
			"Consider adding WHERE conditions to filter data early and reduce processing")
	}
	
	// Check for expensive operations
	for _, bottleneck := range cost.BottleneckNodes {
		if strings.Contains(bottleneck, "SORT") {
			recommendations = append(recommendations,
				"Consider adding an index on ORDER BY columns to avoid sorting")
		}
		if strings.Contains(bottleneck, "JOIN") {
			recommendations = append(recommendations,
				"Consider adding indexes on join columns for better performance")
		}
	}
	
	// Check for high estimated costs
	if cost.TotalCost > 1000 {
		recommendations = append(recommendations,
			"Query has high estimated cost - consider optimizing or adding indexes")
	}
	
	// Check for multiple joins
	if len(query.Joins) > 2 {
		recommendations = append(recommendations,
			"Multiple joins detected - ensure join order is optimal and consider denormalization")
	}
	
	return recommendations
}

// generateWarnings identifies potential issues
func (qe *QueryExplainer) generateWarnings(query *core.ParsedQuery, plan *ExecutionPlan) []string {
	var warnings []string
	
	// Check for Cartesian products
	for _, join := range query.Joins {
		if join.Condition.LeftColumn == "" || join.Condition.RightColumn == "" {
			warnings = append(warnings, "Potential Cartesian product detected - missing JOIN condition")
		}
	}
	
	// Check for large result sets
	if plan.RootNode.EstimatedRows > 100000 {
		warnings = append(warnings, 
			fmt.Sprintf("Large result set estimated (%d rows) - consider adding LIMIT", plan.RootNode.EstimatedRows))
	}
	
	// Check for complex aggregations
	if len(query.Aggregates) > 5 {
		warnings = append(warnings, "Multiple aggregation functions may impact performance")
	}
	
	// Check for subqueries
	if query.IsSubquery {
		warnings = append(warnings, "Subqueries detected - consider JOIN alternatives where possible")
	}
	
	return warnings
}

// Utility functions
func (qe *QueryExplainer) countNodes(node *PlanNode) int {
	if node == nil {
		return 0
	}
	
	count := 1
	for _, child := range node.Children {
		count += qe.countNodes(child)
	}
	return count
}

func (qe *QueryExplainer) calculateDepth(node *PlanNode) int {
	if node == nil || len(node.Children) == 0 {
		return 1
	}
	
	maxDepth := 0
	for _, child := range node.Children {
		depth := qe.calculateDepth(child)
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth + 1
}

func (qe *QueryExplainer) extractOperations(node *PlanNode) []string {
	if node == nil {
		return []string{}
	}
	
	operations := []string{node.Operation}
	for _, child := range node.Children {
		operations = append(operations, qe.extractOperations(child)...)
	}
	return operations
}

func (qe *QueryExplainer) extractTables(query *core.ParsedQuery) []string {
	tables := []string{query.TableName}
	for _, join := range query.Joins {
		tables = append(tables, join.TableName)
	}
	return tables
}

func (qe *QueryExplainer) extractJoinTypes(query *core.ParsedQuery) []string {
	var joinTypes []string
	for _, join := range query.Joins {
		joinTypes = append(joinTypes, qe.joinTypeToString(join.Type))
	}
	return joinTypes
}

// joinTypeToString converts core.JoinType to string
func (qe *QueryExplainer) joinTypeToString(joinType core.JoinType) string {
	switch joinType {
	case core.INNER_JOIN:
		return "INNER"
	case core.LEFT_JOIN:
		return "LEFT"
	case core.RIGHT_JOIN:
		return "RIGHT"
	case core.FULL_OUTER_JOIN:
		return "FULL_OUTER"
	default:
		return "UNKNOWN"
	}
}

func (qe *QueryExplainer) formatConditions(conditions []core.WhereCondition) string {
	var parts []string
	for _, cond := range conditions {
		parts = append(parts, fmt.Sprintf("%s %s %v", cond.Column, cond.Operator, cond.Value))
	}
	return strings.Join(parts, " AND ")
}

func (qe *QueryExplainer) formatOrderBy(orderBy []core.OrderByColumn) string {
	var parts []string
	for _, ob := range orderBy {
		parts = append(parts, fmt.Sprintf("%s %s", ob.Column, ob.Direction))
	}
	return strings.Join(parts, ", ")
}

func (qe *QueryExplainer) calculateStatistics(plan *ExecutionPlan) *PlanStatistics {
	return &PlanStatistics{
		EstimatedDuration:  plan.RootNode.EstimatedTime,
		EstimatedRows:      plan.RootNode.EstimatedRows,
		EstimatedDataSize:  plan.RootNode.EstimatedRows * 100, // Assume 100 bytes per row
		TablesAccessed:     len(plan.Tables),
		JoinsPerformed:     len(plan.JoinTypes),
		FiltersApplied:     qe.countOperations(plan.RootNode, "FILTER"),
		AggregationsUsed:   qe.countOperations(plan.RootNode, "AGGREGATE"),
		SortOperations:     qe.countOperations(plan.RootNode, "SORT"),
		IndexesUsed:        len(plan.Indexes),
	}
}

func (qe *QueryExplainer) countOperations(node *PlanNode, operation string) int {
	if node == nil {
		return 0
	}
	
	count := 0
	if node.Operation == operation {
		count = 1
	}
	
	for _, child := range node.Children {
		count += qe.countOperations(child, operation)
	}
	
	return count
}

// Output formatting methods

// FormatText returns a human-readable text representation
func (plan *ExplainPlan) FormatText() string {
	var sb strings.Builder
	
	sb.WriteString("=== QUERY EXECUTION PLAN ===\n")
	sb.WriteString(fmt.Sprintf("Query: %s\n", plan.Query))
	sb.WriteString(fmt.Sprintf("Generated: %s\n\n", plan.GeneratedAt.Format(time.RFC3339)))
	
	// Execution Plan Tree
	sb.WriteString("EXECUTION PLAN:\n")
	plan.formatPlanTree(&sb, plan.ExecutionPlan.RootNode, 0)
	
	// Cost Analysis
	sb.WriteString("\nCOST ANALYSIS:\n")
	sb.WriteString(fmt.Sprintf("  Total Cost: %.2f\n", plan.CostAnalysis.TotalCost))
	sb.WriteString(fmt.Sprintf("  I/O Cost: %.2f (%.1f%%)\n", 
		plan.CostAnalysis.IOCost, 
		(plan.CostAnalysis.IOCost/plan.CostAnalysis.TotalCost)*100))
	sb.WriteString(fmt.Sprintf("  CPU Cost: %.2f (%.1f%%)\n", 
		plan.CostAnalysis.CPUCost,
		(plan.CostAnalysis.CPUCost/plan.CostAnalysis.TotalCost)*100))
	
	if len(plan.CostAnalysis.BottleneckNodes) > 0 {
		sb.WriteString("  Bottlenecks:\n")
		for _, bottleneck := range plan.CostAnalysis.BottleneckNodes {
			sb.WriteString(fmt.Sprintf("    - %s\n", bottleneck))
		}
	}
	
	// Optimization Info
	sb.WriteString("\nOPTIMIZATIONS:\n")
	sb.WriteString(fmt.Sprintf("  Cost Reduction: %.1f%%\n", plan.OptimizationInfo.CostReduction))
	sb.WriteString(fmt.Sprintf("  Rules Applied: %d\n", len(plan.OptimizationInfo.RulesApplied)))
	for _, rule := range plan.OptimizationInfo.RulesApplied {
		sb.WriteString(fmt.Sprintf("    ✓ %s (%.1f%% impact)\n", rule.Name, rule.Impact))
	}
	
	// Recommendations
	if len(plan.Recommendations) > 0 {
		sb.WriteString("\nRECOMMENDATIONS:\n")
		for _, rec := range plan.Recommendations {
			sb.WriteString(fmt.Sprintf("  • %s\n", rec))
		}
	}
	
	// Warnings
	if len(plan.Warnings) > 0 {
		sb.WriteString("\nWARNINGS:\n")
		for _, warning := range plan.Warnings {
			sb.WriteString(fmt.Sprintf("  ⚠ %s\n", warning))
		}
	}
	
	// Statistics
	sb.WriteString("\nSTATISTICS:\n")
	sb.WriteString(fmt.Sprintf("  Estimated Duration: %v\n", plan.Statistics.EstimatedDuration))
	sb.WriteString(fmt.Sprintf("  Estimated Rows: %d\n", plan.Statistics.EstimatedRows))
	sb.WriteString(fmt.Sprintf("  Tables Accessed: %d\n", plan.Statistics.TablesAccessed))
	sb.WriteString(fmt.Sprintf("  Joins Performed: %d\n", plan.Statistics.JoinsPerformed))
	
	return sb.String()
}

func (plan *ExplainPlan) formatPlanTree(sb *strings.Builder, node *PlanNode, depth int) {
	if node == nil {
		return
	}
	
	indent := strings.Repeat("  ", depth)
	sb.WriteString(fmt.Sprintf("%s├─ %s [%s]\n", indent, node.Operation, node.ID))
	sb.WriteString(fmt.Sprintf("%s   %s\n", indent, node.Description))
	sb.WriteString(fmt.Sprintf("%s   Cost: %.2f, Rows: %d, Time: %v\n", 
		indent, node.EstimatedCost, node.EstimatedRows, node.EstimatedTime))
	
	if node.Condition != "" {
		sb.WriteString(fmt.Sprintf("%s   Condition: %s\n", indent, node.Condition))
	}
	
	if len(node.Optimizations) > 0 {
		sb.WriteString(fmt.Sprintf("%s   Optimizations: %s\n", indent, strings.Join(node.Optimizations, ", ")))
	}
	
	for _, child := range node.Children {
		plan.formatPlanTree(sb, child, depth+1)
	}
}

// FormatJSON returns a JSON representation
func (plan *ExplainPlan) FormatJSON() (string, error) {
	jsonBytes, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// Helper functions
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// extractColumnNames converts []core.Column to []string
func (qe *QueryExplainer) extractColumnNames(columns []core.Column) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

// formatJoinCondition converts core.JoinCondition to string
func (qe *QueryExplainer) formatJoinCondition(condition core.JoinCondition) string {
	if condition.LeftColumn == "" || condition.RightColumn == "" {
		return ""
	}
	leftTable := ""
	if condition.LeftTable != "" {
		leftTable = condition.LeftTable + "."
	}
	rightTable := ""
	if condition.RightTable != "" {
		rightTable = condition.RightTable + "."
	}
	operator := condition.Operator
	if operator == "" {
		operator = "="
	}
	return fmt.Sprintf("%s%s %s %s%s", leftTable, condition.LeftColumn, operator, rightTable, condition.RightColumn)
}