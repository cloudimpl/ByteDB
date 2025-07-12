package planner

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ResultAggregator handles robust aggregation of distributed query results
type ResultAggregator struct {
	costEstimator *CostEstimator
}

// NewResultAggregator creates a new result aggregator
func NewResultAggregator(costEstimator *CostEstimator) *ResultAggregator {
	return &ResultAggregator{
		costEstimator: costEstimator,
	}
}

// AggregationStrategy defines how results should be aggregated
type AggregationStrategy struct {
	Type                AggregationType             `json:"type"`
	GroupByColumns      []string                    `json:"group_by_columns"`
	AggregateExpressions []AggregateExpression      `json:"aggregate_expressions"`
	OrderByColumns      []OrderByExpression         `json:"order_by_columns"`
	LimitCount          int                         `json:"limit_count"`
	DistinctColumns     []string                    `json:"distinct_columns"`
	IsDistinct          bool                        `json:"is_distinct"`
	RequiresSorting     bool                        `json:"requires_sorting"`
	RequiresDeduplication bool                      `json:"requires_deduplication"`
}

// AggregateExpression represents an aggregate function
type AggregateExpression struct {
	Function    string      `json:"function"`    // COUNT, SUM, AVG, MIN, MAX, etc.
	Column      string      `json:"column"`      // Column to aggregate
	Alias       string      `json:"alias"`       // Result column alias
	DataType    string      `json:"data_type"`   // Expected data type
	IsPartial   bool        `json:"is_partial"`  // Whether this is a partial aggregate
	CombineFunc string      `json:"combine_func"` // Function to combine partial results
}

// OrderByExpression represents an ORDER BY clause
type OrderByExpression struct {
	Column    string `json:"column"`
	Direction string `json:"direction"` // ASC or DESC
	DataType  string `json:"data_type"`
}

// AggregateResults efficiently with proper handling of different SQL operations
func (ra *ResultAggregator) AggregateResults(
	fragments []*communication.FragmentResult,
	strategy *AggregationStrategy,
	query *core.ParsedQuery,
) (*core.QueryResult, error) {
	
	if len(fragments) == 0 {
		return &core.QueryResult{
			Columns: []string{},
			Rows:    []core.Row{},
			Count:   0,
		}, nil
	}
	
	// Handle single fragment case
	if len(fragments) == 1 {
		return ra.handleSingleFragment(fragments[0], strategy, query)
	}
	
	// Validate fragments consistency
	if err := ra.validateFragments(fragments); err != nil {
		return nil, fmt.Errorf("fragment validation failed: %v", err)
	}
	
	// Merge fragments based on query type and strategy
	var result *core.QueryResult
	var err error
	
	switch {
	case strategy.Type == AggregationHash && len(strategy.GroupByColumns) > 0:
		result, err = ra.aggregateGroupBy(fragments, strategy)
	case strategy.Type == AggregationHash && len(strategy.AggregateExpressions) > 0:
		result, err = ra.aggregateGlobal(fragments, strategy)
	case strategy.IsDistinct:
		result, err = ra.aggregateDistinct(fragments, strategy)
	case query.HasUnion:
		result, err = ra.aggregateUnion(fragments, strategy, query)
	case strategy.RequiresSorting:
		result, err = ra.aggregateWithSorting(fragments, strategy)
	default:
		result, err = ra.aggregateSimple(fragments, strategy)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Apply final operations
	result, err = ra.applyFinalOperations(result, strategy, query)
	if err != nil {
		return nil, err
	}
	
	return result, nil
}

// CreateAggregationStrategy analyzes a query and creates an appropriate aggregation strategy
func (ra *ResultAggregator) CreateAggregationStrategy(query *core.ParsedQuery) *AggregationStrategy {
	strategy := &AggregationStrategy{
		Type: AggregationHash, // Default
	}
	
	// Analyze GROUP BY
	if len(query.GroupBy) > 0 {
		strategy.GroupByColumns = query.GroupBy
		strategy.Type = AggregationHash
	}
	
	// Analyze aggregates
	if len(query.Aggregates) > 0 {
		strategy.AggregateExpressions = make([]AggregateExpression, len(query.Aggregates))
		for i, agg := range query.Aggregates {
			strategy.AggregateExpressions[i] = AggregateExpression{
				Function:    agg.Function,
				Column:      agg.Column,
				Alias:       agg.Alias,
				DataType:    ra.inferDataType(agg.Function),
				IsPartial:   false,
				CombineFunc: ra.getCombineFunction(agg.Function),
			}
		}
	}
	
	// Analyze ORDER BY
	if len(query.OrderBy) > 0 {
		strategy.OrderByColumns = make([]OrderByExpression, len(query.OrderBy))
		strategy.RequiresSorting = true
		for i, orderBy := range query.OrderBy {
			direction := orderBy.Direction
			if direction == "" {
				direction = "ASC"
			}
			strategy.OrderByColumns[i] = OrderByExpression{
				Column:    orderBy.Column,
				Direction: direction,
				DataType:  "string", // Default, should be inferred
			}
		}
	}
	
	// Handle DISTINCT
	if query.Type == core.SELECT {
		// Check if we need DISTINCT processing
		for _, col := range query.Columns {
			if strings.ToUpper(col.Name) == "DISTINCT" {
				strategy.IsDistinct = true
				strategy.RequiresDeduplication = true
				break
			}
		}
	}
	
	// Handle UNION
	if query.HasUnion {
		strategy.RequiresDeduplication = !query.UnionQueries[0].UnionAll // UNION vs UNION ALL
	}
	
	// Handle LIMIT
	if query.Limit > 0 {
		strategy.LimitCount = query.Limit
	}
	
	return strategy
}

// handleSingleFragment handles the case where there's only one fragment
func (ra *ResultAggregator) handleSingleFragment(
	fragment *communication.FragmentResult,
	strategy *AggregationStrategy,
	query *core.ParsedQuery,
) (*core.QueryResult, error) {
	
	if fragment.Error != "" {
		return nil, fmt.Errorf("fragment error: %s", fragment.Error)
	}
	
	result := &core.QueryResult{
		Columns: fragment.Columns,
		Rows:    fragment.Rows,
		Count:   fragment.Count,
	}
	
	// Apply final operations even for single fragment
	return ra.applyFinalOperations(result, strategy, query)
}

// validateFragments ensures all fragments are consistent
func (ra *ResultAggregator) validateFragments(fragments []*communication.FragmentResult) error {
	if len(fragments) == 0 {
		return fmt.Errorf("no fragments to validate")
	}
	
	// Check for errors
	for i, fragment := range fragments {
		if fragment.Error != "" {
			return fmt.Errorf("fragment %d has error: %s", i, fragment.Error)
		}
	}
	
	// Validate column consistency
	baseColumns := fragments[0].Columns
	for i, fragment := range fragments[1:] {
		if !ra.columnsMatch(baseColumns, fragment.Columns) {
			return fmt.Errorf("fragment %d has mismatched columns: expected %v, got %v", 
				i+1, baseColumns, fragment.Columns)
		}
	}
	
	return nil
}

// aggregateGroupBy handles GROUP BY aggregation
func (ra *ResultAggregator) aggregateGroupBy(fragments []*communication.FragmentResult, strategy *AggregationStrategy) (*core.QueryResult, error) {
	groupMap := make(map[string]*GroupState)
	
	// Process each fragment
	for _, fragment := range fragments {
		for _, row := range fragment.Rows {
			// Build group key
			groupKey := ra.buildGroupKey(row, strategy.GroupByColumns)
			
			// Get or create group state
			groupState, exists := groupMap[groupKey]
			if !exists {
				groupState = &GroupState{
					GroupKey:   groupKey,
					GroupValues: make(map[string]interface{}),
					Aggregates: make(map[string]*AggregateState),
				}
				
				// Set group by values
				for _, col := range strategy.GroupByColumns {
					if val, exists := row[col]; exists {
						groupState.GroupValues[col] = val
					}
				}
				
				groupMap[groupKey] = groupState
			}
			
			// Update aggregates
			for _, aggExpr := range strategy.AggregateExpressions {
				ra.updateAggregateState(groupState, &aggExpr, row)
			}
		}
	}
	
	// Convert group map to result rows
	resultRows := make([]core.Row, 0, len(groupMap))
	var columns []string
	
	// Determine result columns
	columns = append(columns, strategy.GroupByColumns...)
	for _, aggExpr := range strategy.AggregateExpressions {
		columns = append(columns, aggExpr.Alias)
	}
	
	// Build result rows
	for _, groupState := range groupMap {
		row := make(core.Row)
		
		// Add group by columns
		for _, col := range strategy.GroupByColumns {
			row[col] = groupState.GroupValues[col]
		}
		
		// Add aggregate columns
		for _, aggExpr := range strategy.AggregateExpressions {
			if aggState, exists := groupState.Aggregates[aggExpr.Alias]; exists {
				row[aggExpr.Alias] = ra.finalizeAggregate(&aggExpr, aggState)
			}
		}
		
		resultRows = append(resultRows, row)
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}, nil
}

// aggregateGlobal handles global aggregation (no GROUP BY)
func (ra *ResultAggregator) aggregateGlobal(fragments []*communication.FragmentResult, strategy *AggregationStrategy) (*core.QueryResult, error) {
	globalAggregates := make(map[string]*AggregateState)
	
	// Initialize aggregate states
	for _, aggExpr := range strategy.AggregateExpressions {
		globalAggregates[aggExpr.Alias] = &AggregateState{
			Function: aggExpr.Function,
			Count:    0,
			Sum:      0.0,
			Values:   []interface{}{},
		}
	}
	
	// Process all fragments
	for _, fragment := range fragments {
		for _, row := range fragment.Rows {
			for _, aggExpr := range strategy.AggregateExpressions {
				if aggState, exists := globalAggregates[aggExpr.Alias]; exists {
					ra.updateGlobalAggregateState(aggState, &aggExpr, row)
				}
			}
		}
	}
	
	// Build result
	resultRow := make(core.Row)
	columns := make([]string, 0, len(strategy.AggregateExpressions))
	
	for _, aggExpr := range strategy.AggregateExpressions {
		columns = append(columns, aggExpr.Alias)
		if aggState, exists := globalAggregates[aggExpr.Alias]; exists {
			resultRow[aggExpr.Alias] = ra.finalizeAggregate(&aggExpr, aggState)
		}
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    []core.Row{resultRow},
		Count:   1,
	}, nil
}

// aggregateDistinct handles DISTINCT queries
func (ra *ResultAggregator) aggregateDistinct(fragments []*communication.FragmentResult, strategy *AggregationStrategy) (*core.QueryResult, error) {
	distinctRows := make(map[string]core.Row)
	columns := fragments[0].Columns
	
	// Collect unique rows
	for _, fragment := range fragments {
		for _, row := range fragment.Rows {
			rowKey := ra.buildRowKey(row, columns)
			if _, exists := distinctRows[rowKey]; !exists {
				distinctRows[rowKey] = row
			}
		}
	}
	
	// Convert to slice
	resultRows := make([]core.Row, 0, len(distinctRows))
	for _, row := range distinctRows {
		resultRows = append(resultRows, row)
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}, nil
}

// aggregateUnion handles UNION queries
func (ra *ResultAggregator) aggregateUnion(fragments []*communication.FragmentResult, strategy *AggregationStrategy, query *core.ParsedQuery) (*core.QueryResult, error) {
	allRows := []core.Row{}
	columns := fragments[0].Columns
	
	// Collect all rows
	for _, fragment := range fragments {
		allRows = append(allRows, fragment.Rows...)
	}
	
	// Handle UNION vs UNION ALL
	if strategy.RequiresDeduplication {
		// UNION - remove duplicates
		distinctRows := make(map[string]core.Row)
		for _, row := range allRows {
			rowKey := ra.buildRowKey(row, columns)
			distinctRows[rowKey] = row
		}
		
		// Convert back to slice
		allRows = make([]core.Row, 0, len(distinctRows))
		for _, row := range distinctRows {
			allRows = append(allRows, row)
		}
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    allRows,
		Count:   len(allRows),
	}, nil
}

// aggregateWithSorting handles queries that require sorting
func (ra *ResultAggregator) aggregateWithSorting(fragments []*communication.FragmentResult, strategy *AggregationStrategy) (*core.QueryResult, error) {
	// First, combine all rows
	allRows := []core.Row{}
	columns := fragments[0].Columns
	
	for _, fragment := range fragments {
		allRows = append(allRows, fragment.Rows...)
	}
	
	// Sort the combined results
	err := ra.sortRows(allRows, strategy.OrderByColumns)
	if err != nil {
		return nil, fmt.Errorf("sorting failed: %v", err)
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    allRows,
		Count:   len(allRows),
	}, nil
}

// aggregateSimple handles simple queries (just combine results)
func (ra *ResultAggregator) aggregateSimple(fragments []*communication.FragmentResult, strategy *AggregationStrategy) (*core.QueryResult, error) {
	allRows := []core.Row{}
	columns := fragments[0].Columns
	
	for _, fragment := range fragments {
		allRows = append(allRows, fragment.Rows...)
	}
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    allRows,
		Count:   len(allRows),
	}, nil
}

// applyFinalOperations applies ORDER BY, LIMIT, etc.
func (ra *ResultAggregator) applyFinalOperations(result *core.QueryResult, strategy *AggregationStrategy, query *core.ParsedQuery) (*core.QueryResult, error) {
	// Apply ORDER BY
	if strategy.RequiresSorting && len(strategy.OrderByColumns) > 0 {
		err := ra.sortRows(result.Rows, strategy.OrderByColumns)
		if err != nil {
			return nil, fmt.Errorf("final sorting failed: %v", err)
		}
	}
	
	// Apply LIMIT
	if strategy.LimitCount > 0 && len(result.Rows) > strategy.LimitCount {
		result.Rows = result.Rows[:strategy.LimitCount]
		result.Count = strategy.LimitCount
	}
	
	return result, nil
}

// Helper types and functions

// GroupState maintains state for GROUP BY aggregation
type GroupState struct {
	GroupKey    string
	GroupValues map[string]interface{}
	Aggregates  map[string]*AggregateState
}

// AggregateState maintains state for aggregate functions
type AggregateState struct {
	Function string
	Count    int64
	Sum      float64
	Min      interface{}
	Max      interface{}
	Values   []interface{} // For functions that need all values
}

// Helper functions

func (ra *ResultAggregator) buildGroupKey(row core.Row, groupByColumns []string) string {
	keyParts := make([]string, len(groupByColumns))
	for i, col := range groupByColumns {
		if val, exists := row[col]; exists {
			keyParts[i] = fmt.Sprintf("%v", val)
		} else {
			keyParts[i] = "NULL"
		}
	}
	return strings.Join(keyParts, "|")
}

func (ra *ResultAggregator) buildRowKey(row core.Row, columns []string) string {
	keyParts := make([]string, len(columns))
	for i, col := range columns {
		if val, exists := row[col]; exists {
			keyParts[i] = fmt.Sprintf("%v", val)
		} else {
			keyParts[i] = "NULL"
		}
	}
	return strings.Join(keyParts, "|")
}

func (ra *ResultAggregator) updateAggregateState(groupState *GroupState, aggExpr *AggregateExpression, row core.Row) {
	aggState, exists := groupState.Aggregates[aggExpr.Alias]
	if !exists {
		aggState = &AggregateState{
			Function: aggExpr.Function,
			Count:    0,
			Sum:      0.0,
			Values:   []interface{}{},
		}
		groupState.Aggregates[aggExpr.Alias] = aggState
	}
	
	ra.updateGlobalAggregateState(aggState, aggExpr, row)
}

func (ra *ResultAggregator) updateGlobalAggregateState(aggState *AggregateState, aggExpr *AggregateExpression, row core.Row) {
	val, exists := row[aggExpr.Column]
	if !exists {
		return
	}
	
	switch strings.ToUpper(aggExpr.Function) {
	case "COUNT":
		aggState.Count++
	case "SUM":
		if numVal, err := ra.toFloat64(val); err == nil {
			aggState.Sum += numVal
			aggState.Count++
		}
	case "AVG":
		if numVal, err := ra.toFloat64(val); err == nil {
			aggState.Sum += numVal
			aggState.Count++
		}
	case "MIN":
		if aggState.Count == 0 || ra.compareValues(val, aggState.Min) < 0 {
			aggState.Min = val
		}
		aggState.Count++
	case "MAX":
		if aggState.Count == 0 || ra.compareValues(val, aggState.Max) > 0 {
			aggState.Max = val
		}
		aggState.Count++
	default:
		// For custom functions, store all values
		aggState.Values = append(aggState.Values, val)
		aggState.Count++
	}
}

func (ra *ResultAggregator) finalizeAggregate(aggExpr *AggregateExpression, aggState *AggregateState) interface{} {
	switch strings.ToUpper(aggExpr.Function) {
	case "COUNT":
		return aggState.Count
	case "SUM":
		return aggState.Sum
	case "AVG":
		if aggState.Count > 0 {
			return aggState.Sum / float64(aggState.Count)
		}
		return 0.0
	case "MIN":
		return aggState.Min
	case "MAX":
		return aggState.Max
	default:
		// For custom functions, return count by default
		return aggState.Count
	}
}

func (ra *ResultAggregator) sortRows(rows []core.Row, orderByColumns []OrderByExpression) error {
	if len(orderByColumns) == 0 {
		return nil
	}
	
	sort.Slice(rows, func(i, j int) bool {
		for _, orderBy := range orderByColumns {
			valI, existsI := rows[i][orderBy.Column]
			valJ, existsJ := rows[j][orderBy.Column]
			
			if !existsI && !existsJ {
				continue
			}
			if !existsI {
				return orderBy.Direction == "ASC"
			}
			if !existsJ {
				return orderBy.Direction == "DESC"
			}
			
			cmp := ra.compareValues(valI, valJ)
			if cmp != 0 {
				if orderBy.Direction == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	
	return nil
}

func (ra *ResultAggregator) compareValues(a, b interface{}) int {
	// Convert to strings for comparison
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	
	// Try numeric comparison first
	if numA, errA := strconv.ParseFloat(strA, 64); errA == nil {
		if numB, errB := strconv.ParseFloat(strB, 64); errB == nil {
			if numA < numB {
				return -1
			} else if numA > numB {
				return 1
			}
			return 0
		}
	}
	
	// Fall back to string comparison
	if strA < strB {
		return -1
	} else if strA > strB {
		return 1
	}
	return 0
}

func (ra *ResultAggregator) toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func (ra *ResultAggregator) columnsMatch(cols1, cols2 []string) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	for i, col := range cols1 {
		if col != cols2[i] {
			return false
		}
	}
	return true
}

func (ra *ResultAggregator) inferDataType(function string) string {
	switch strings.ToUpper(function) {
	case "COUNT":
		return "bigint"
	case "SUM", "AVG":
		return "decimal"
	case "MIN", "MAX":
		return "varchar" // Depends on source column
	default:
		return "varchar"
	}
}

func (ra *ResultAggregator) getCombineFunction(function string) string {
	switch strings.ToUpper(function) {
	case "COUNT":
		return "SUM"
	case "SUM":
		return "SUM"
	case "AVG":
		return "WEIGHTED_AVG"
	case "MIN":
		return "MIN"
	case "MAX":
		return "MAX"
	default:
		return "FIRST"
	}
}