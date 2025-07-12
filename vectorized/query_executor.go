package vectorized

import (
	"bytedb/core"
	"fmt"
	"math"
	"time"
)

// VectorizedQueryExecutor integrates the vectorized execution engine with ByteDB's query planner
type VectorizedQueryExecutor struct {
	memoryManager *AdaptiveMemoryManager
	planner       *VectorizedQueryPlanner
	optimizer     *VectorizedOptimizer
	dataSourceRegistry map[string]VectorizedDataSource
}

// VectorizedQueryPlanner creates vectorized execution plans
type VectorizedQueryPlanner struct {
	costModel *VectorizedCostModel
}

// VectorizedOptimizer optimizes vectorized query plans
type VectorizedOptimizer struct {
	rules []OptimizationRule
}

// OptimizationRule represents a query optimization rule
type OptimizationRule interface {
	Apply(plan VectorizedOperator) VectorizedOperator
	CanApply(plan VectorizedOperator) bool
	Name() string
}

// VectorizedCostModel estimates costs for vectorized operations
type VectorizedCostModel struct {
	cpuCostPerRow    float64
	ioCostPerBlock   float64
	memoryCostPerMB  float64
	simdSpeedup      float64
}

// ExecutionContext holds context for query execution
type ExecutionContext struct {
	QueryID       string
	StartTime     time.Time
	MemoryBudget  int64
	RowsProcessed int64
	Canceled      bool
}

// NewVectorizedQueryExecutor creates a new vectorized query executor
func NewVectorizedQueryExecutor(memoryConfig *MemoryConfig) *VectorizedQueryExecutor {
	memoryManager := NewAdaptiveMemoryManager(memoryConfig)
	
	costModel := &VectorizedCostModel{
		cpuCostPerRow:   0.01,  // Cost per row for CPU operations
		ioCostPerBlock:  10.0,  // Cost per I/O block
		memoryCostPerMB: 0.1,   // Cost per MB of memory
		simdSpeedup:     4.0,   // Expected SIMD speedup
	}
	
	planner := &VectorizedQueryPlanner{
		costModel: costModel,
	}
	
	optimizer := &VectorizedOptimizer{
		rules: []OptimizationRule{
			&FilterPushdownRule{},
			&ColumnPruningRule{},
			&PredicatePushdownRule{},
			&JoinReorderingRule{},
			&AggregationPushdownRule{},
		},
	}
	
	return &VectorizedQueryExecutor{
		memoryManager:      memoryManager,
		planner:           planner,
		optimizer:         optimizer,
		dataSourceRegistry: make(map[string]VectorizedDataSource),
	}
}

// RegisterDataSource registers a vectorized data source for a table
func (vqe *VectorizedQueryExecutor) RegisterDataSource(tableName string, dataSource VectorizedDataSource) {
	vqe.dataSourceRegistry[tableName] = dataSource
}

// CreateVectorizedPlan creates a vectorized execution plan from a core query plan
func (vqe *VectorizedQueryExecutor) CreateVectorizedPlan(coreQuery *core.ParsedQuery) (VectorizedOperator, error) {
	return vqe.planner.CreateVectorizedPlan(coreQuery, vqe.dataSourceRegistry)
}

// ExecuteQuery executes a vectorized query plan
func (vqe *VectorizedQueryExecutor) ExecuteQuery(plan VectorizedOperator, ctx *ExecutionContext) (*core.QueryResult, error) {
	// Optimize the plan
	optimizedPlan := vqe.optimizer.Optimize(plan)
	
	// Execute the plan
	resultBatch, err := optimizedPlan.Execute(nil)
	if err != nil {
		return nil, fmt.Errorf("vectorized execution failed: %w", err)
	}
	
	// Convert VectorBatch back to core.QueryResult
	return vqe.convertToQueryResult(resultBatch, ctx)
}

// convertToQueryResult converts a VectorBatch to core.QueryResult
func (vqe *VectorizedQueryExecutor) convertToQueryResult(batch *VectorBatch, ctx *ExecutionContext) (*core.QueryResult, error) {
	if batch == nil || batch.RowCount == 0 {
		return &core.QueryResult{
			Columns: []string{},
			Rows:    []core.Row{},
			Count:   0,
			Query:   ctx.QueryID,
		}, nil
	}
	
	// Extract column names from schema
	columns := make([]string, len(batch.Schema.Fields))
	for i, field := range batch.Schema.Fields {
		columns[i] = field.Name
	}
	
	// Convert vector data to rows
	rows := make([]core.Row, batch.RowCount)
	for i := 0; i < batch.RowCount; i++ {
		row := make(core.Row)
		
		for j, column := range batch.Columns {
			fieldName := batch.Schema.Fields[j].Name
			
			if column.IsNull(i) {
				row[fieldName] = nil
			} else {
				value, err := vqe.extractValue(column, i)
				if err != nil {
					return nil, fmt.Errorf("failed to extract value from column %s: %w", fieldName, err)
				}
				row[fieldName] = value
			}
		}
		
		rows[i] = row
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    rows,
		Count:   len(rows),
		Query:   ctx.QueryID,
	}, nil
}

// extractValue extracts a value from a vector at the given index
func (vqe *VectorizedQueryExecutor) extractValue(vector *Vector, index int) (interface{}, error) {
	switch vector.DataType {
	case INT32:
		if value, ok := vector.GetInt32(index); ok {
			return value, nil
		}
	case INT64:
		if value, ok := vector.GetInt64(index); ok {
			return value, nil
		}
	case FLOAT64:
		if value, ok := vector.GetFloat64(index); ok {
			return value, nil
		}
	case STRING:
		if value, ok := vector.GetString(index); ok {
			return value, nil
		}
	case BOOLEAN:
		if value, ok := vector.GetBoolean(index); ok {
			return value, nil
		}
	default:
		return nil, fmt.Errorf("unsupported data type: %v", vector.DataType)
	}
	
	return nil, fmt.Errorf("failed to extract value at index %d", index)
}

// CreateVectorizedPlan creates a vectorized execution plan from a parsed query
func (vqp *VectorizedQueryPlanner) CreateVectorizedPlan(query *core.ParsedQuery, dataSources map[string]VectorizedDataSource) (VectorizedOperator, error) {
	// Start with data source (table scan or CTE)
	var root VectorizedOperator
	
	if query.TableName != "" {
		dataSource, exists := dataSources[query.TableName]
		if !exists {
			return nil, fmt.Errorf("data source not found for table: %s", query.TableName)
		}
		
		// Create scan operator
		root = NewVectorizedScanOperator(dataSource, DefaultBatchSize)
	} else {
		return nil, fmt.Errorf("queries without table sources not yet supported in vectorized execution")
	}
	
	// Add filter operators for WHERE conditions
	if len(query.Where) > 0 {
		filters := vqp.convertWhereConditions(query.Where, root.GetOutputSchema())
		if len(filters) > 0 {
			root = NewVectorizedFilterOperator(root, filters)
		}
	}
	
	// Add join operators
	if query.HasJoins {
		for _, join := range query.Joins {
			rightDataSource, exists := dataSources[join.TableName]
			if !exists {
				return nil, fmt.Errorf("data source not found for join table: %s", join.TableName)
			}
			
			rightScan := NewVectorizedScanOperator(rightDataSource, DefaultBatchSize)
			
			// Convert join condition
			joinColumns := []*JoinColumn{
				{
					LeftColumn:  vqp.findColumnIndex(root.GetOutputSchema(), join.Condition.LeftColumn),
					RightColumn: vqp.findColumnIndex(rightScan.GetOutputSchema(), join.Condition.RightColumn),
					Operator:    EQ, // Assuming equality join for now
				},
			}
			
			joinType := vqp.convertJoinType(join.Type)
			root = NewVectorizedJoinOperator(root, rightScan, joinType, joinColumns)
		}
	}
	
	// Add aggregation operators
	if query.IsAggregate {
		groupByColumns := vqp.convertGroupByColumns(query.GroupBy, root.GetOutputSchema())
		aggregates := vqp.convertAggregates(query.Aggregates, root.GetOutputSchema())
		
		if len(aggregates) > 0 {
			root = NewVectorizedAggregateOperator(root, groupByColumns, aggregates)
		}
	}
	
	// Add projection operator for column selection
	if len(query.Columns) > 0 && !vqp.isSelectStar(query.Columns) {
		projectedColumns := vqp.convertProjection(query.Columns, root.GetOutputSchema())
		root = NewVectorizedProjectOperator(root, projectedColumns)
	}
	
	// Add sort operator for ORDER BY
	if len(query.OrderBy) > 0 {
		sortKeys := vqp.convertOrderBy(query.OrderBy, root.GetOutputSchema())
		root = NewVectorizedSortOperator(root, sortKeys)
	}
	
	// Add limit operator
	if query.Limit > 0 {
		root = NewVectorizedLimitOperator(root, query.Limit)
	}
	
	return root, nil
}

// convertWhereConditions converts core WHERE conditions to vectorized filters
func (vqp *VectorizedQueryPlanner) convertWhereConditions(conditions []core.WhereCondition, schema *Schema) []*VectorizedFilter {
	var filters []*VectorizedFilter
	
	for _, cond := range conditions {
		// For now, handle simple equality and comparison conditions
		if cond.Column != "" && cond.Value != nil {
			// Find the column index in the schema
			columnIndex := vqp.findColumnIndex(schema, cond.Column)
			if columnIndex < 0 {
				continue // Skip unknown columns
			}
			
			op := vqp.convertOperator(cond.Operator)
			filter := &VectorizedFilter{
				ColumnIndex: columnIndex,
				Operator:    op,
				Value:       cond.Value,
			}
			filters = append(filters, filter)
		}
	}
	
	return filters
}

// convertOperator converts core operators to vectorized operators
func (vqp *VectorizedQueryPlanner) convertOperator(operator string) FilterOperator {
	switch operator {
	case "=":
		return EQ
	case "!=", "<>":
		return NE
	case "<":
		return LT
	case "<=":
		return LE
	case ">":
		return GT
	case ">=":
		return GE
	default:
		return EQ // Default to equality
	}
}

// convertJoinType converts core join types to vectorized join types
func (vqp *VectorizedQueryPlanner) convertJoinType(joinType core.JoinType) JoinType {
	switch joinType {
	case core.INNER_JOIN:
		return INNER_JOIN_VEC
	case core.LEFT_JOIN:
		return LEFT_JOIN_VEC
	case core.RIGHT_JOIN:
		return RIGHT_JOIN_VEC
	case core.FULL_OUTER_JOIN:
		return FULL_OUTER_JOIN_VEC
	default:
		return INNER_JOIN_VEC
	}
}

// findColumnIndex finds the index of a column in a schema
func (vqp *VectorizedQueryPlanner) findColumnIndex(schema *Schema, columnName string) int {
	for i, field := range schema.Fields {
		if field.Name == columnName {
			return i
		}
	}
	return -1 // Column not found
}

// convertGroupByColumns converts GROUP BY columns to column indices
func (vqp *VectorizedQueryPlanner) convertGroupByColumns(groupBy []string, schema *Schema) []int {
	var indices []int
	for _, col := range groupBy {
		if idx := vqp.findColumnIndex(schema, col); idx >= 0 {
			indices = append(indices, idx)
		}
	}
	return indices
}

// convertAggregates converts core aggregates to vectorized aggregates
func (vqp *VectorizedQueryPlanner) convertAggregates(aggregates []core.AggregateFunction, schema *Schema) []*VectorizedAggregate {
	var vectorizedAggs []*VectorizedAggregate
	
	for _, agg := range aggregates {
		columnIndex := vqp.findColumnIndex(schema, agg.Column)
		
		// Handle COUNT(*) specially - it doesn't need a specific column
		if agg.Column == "*" && agg.Function == "COUNT" {
			columnIndex = 0 // Use any column for COUNT(*), it doesn't matter
		} else if columnIndex < 0 {
			continue // Skip unknown columns
		}
		
		var aggFunc AggregateFunction
		switch agg.Function {
		case "COUNT":
			aggFunc = COUNT
		case "SUM":
			aggFunc = SUM
		case "AVG":
			aggFunc = AVG
		case "MIN":
			aggFunc = MIN
		case "MAX":
			aggFunc = MAX
		default:
			continue // Skip unknown aggregates
		}
		
		// Determine output type based on function and input type
		var outputType DataType = INT64 // Default for COUNT
		if columnIndex >= 0 && columnIndex < len(schema.Fields) {
			inputType := schema.Fields[columnIndex].DataType
			switch aggFunc {
			case SUM:
				outputType = inputType // SUM preserves input type
			case AVG:
				outputType = FLOAT64 // AVG always returns float
			case MIN, MAX:
				outputType = inputType // MIN/MAX preserve input type
			}
		}
		
		vectorizedAgg := &VectorizedAggregate{
			Function:    aggFunc,
			InputColumn: columnIndex,
			OutputType:  outputType,
		}
		
		vectorizedAggs = append(vectorizedAggs, vectorizedAgg)
	}
	
	return vectorizedAggs
}

// isSelectStar checks if the query is selecting all columns
func (vqp *VectorizedQueryPlanner) isSelectStar(columns []core.Column) bool {
	return len(columns) == 1 && columns[0].Name == "*"
}

// convertProjection converts column projections to vectorized projections
func (vqp *VectorizedQueryPlanner) convertProjection(columns []core.Column, schema *Schema) []int {
	var indices []int
	for _, col := range columns {
		if col.Name != "*" {
			if idx := vqp.findColumnIndex(schema, col.Name); idx >= 0 {
				indices = append(indices, idx)
			}
		}
	}
	return indices
}

// convertOrderBy converts ORDER BY clauses to vectorized sort keys
func (vqp *VectorizedQueryPlanner) convertOrderBy(orderBy []core.OrderByColumn, schema *Schema) []*SortKey {
	var sortKeys []*SortKey
	for _, order := range orderBy {
		columnIndex := vqp.findColumnIndex(schema, order.Column)
		if columnIndex >= 0 {
			sortKey := &SortKey{
				ColumnIndex: columnIndex,
				Ascending:   order.Direction != "DESC",
			}
			sortKeys = append(sortKeys, sortKey)
		}
	}
	return sortKeys
}

// Optimize applies optimization rules to a vectorized plan
func (vo *VectorizedOptimizer) Optimize(plan VectorizedOperator) VectorizedOperator {
	optimizedPlan := plan
	
	// Apply optimization rules in order
	for _, rule := range vo.rules {
		if rule.CanApply(optimizedPlan) {
			optimizedPlan = rule.Apply(optimizedPlan)
		}
	}
	
	return optimizedPlan
}

// EstimateCost estimates the cost of executing a vectorized operation
func (vcm *VectorizedCostModel) EstimateCost(op VectorizedOperator) float64 {
	baseRows := float64(op.GetEstimatedRowCount())
	
	switch op.(type) {
	case *VectorizedScanOperator:
		// I/O dominated operation
		blocks := baseRows / 1000 // Assume 1000 rows per block
		return blocks * vcm.ioCostPerBlock
		
	case *VectorizedFilterOperator:
		// CPU dominated with SIMD speedup
		cpuCost := baseRows * vcm.cpuCostPerRow
		return cpuCost / vcm.simdSpeedup
		
	case *VectorizedJoinOperator:
		// Hash join cost estimation
		buildCost := baseRows * vcm.cpuCostPerRow * 2 // Build hash table
		probeCost := baseRows * vcm.cpuCostPerRow     // Probe hash table
		return buildCost + probeCost
		
	case *VectorizedAggregateOperator:
		// Aggregation with hash grouping
		hashCost := baseRows * vcm.cpuCostPerRow * 1.5
		return hashCost / vcm.simdSpeedup
		
	case *VectorizedSortOperator:
		// Sort cost: O(n log n)
		if baseRows > 0 {
			sortCost := baseRows * math.Log2(baseRows) * vcm.cpuCostPerRow
			return sortCost
		}
		return 0
		
	default:
		// Default CPU cost
		return baseRows * vcm.cpuCostPerRow
	}
}

// Optimization Rules Implementation

// FilterPushdownRule pushes filters down the operator tree
type FilterPushdownRule struct{}

func (fpr *FilterPushdownRule) Apply(plan VectorizedOperator) VectorizedOperator {
	// Implementation would analyze the plan tree and push filters down
	// For now, return the plan unchanged
	return plan
}

func (fpr *FilterPushdownRule) CanApply(plan VectorizedOperator) bool {
	// Check if there are filters that can be pushed down
	return true
}

func (fpr *FilterPushdownRule) Name() string {
	return "FilterPushdown"
}

// ColumnPruningRule removes unnecessary columns from projections
type ColumnPruningRule struct{}

func (cpr *ColumnPruningRule) Apply(plan VectorizedOperator) VectorizedOperator {
	// Implementation would analyze required columns and prune unnecessary ones
	return plan
}

func (cpr *ColumnPruningRule) CanApply(plan VectorizedOperator) bool {
	return true
}

func (cpr *ColumnPruningRule) Name() string {
	return "ColumnPruning"
}

// PredicatePushdownRule pushes predicates to data sources
type PredicatePushdownRule struct{}

func (ppr *PredicatePushdownRule) Apply(plan VectorizedOperator) VectorizedOperator {
	// Implementation would push predicates to data sources for early filtering
	return plan
}

func (ppr *PredicatePushdownRule) CanApply(plan VectorizedOperator) bool {
	return true
}

func (ppr *PredicatePushdownRule) Name() string {
	return "PredicatePushdown"
}

// JoinReorderingRule reorders joins for optimal execution
type JoinReorderingRule struct{}

func (jrr *JoinReorderingRule) Apply(plan VectorizedOperator) VectorizedOperator {
	// Implementation would reorder joins based on cost estimates
	return plan
}

func (jrr *JoinReorderingRule) CanApply(plan VectorizedOperator) bool {
	return true
}

func (jrr *JoinReorderingRule) Name() string {
	return "JoinReordering"
}

// AggregationPushdownRule pushes aggregations closer to data sources
type AggregationPushdownRule struct{}

func (apr *AggregationPushdownRule) Apply(plan VectorizedOperator) VectorizedOperator {
	// Implementation would push partial aggregations down the tree
	return plan
}

func (apr *AggregationPushdownRule) CanApply(plan VectorizedOperator) bool {
	return true
}

func (apr *AggregationPushdownRule) Name() string {
	return "AggregationPushdown"
}