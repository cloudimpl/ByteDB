package coordinator

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"bytedb/distributed/planner"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Coordinator manages distributed query execution
type Coordinator struct {
	workers            map[string]*WorkerConnection
	parser             *core.SQLParser
	planner            *core.QueryPlanner
	distributedPlanner *planner.DistributedQueryPlanner
	optimizer          *core.QueryOptimizer
	transport          communication.Transport
	mutex              sync.RWMutex
	queryID            int64
	queryIDMux         sync.Mutex
}

// WorkerConnection represents a connection to a worker
type WorkerConnection struct {
	Info   *communication.WorkerInfo
	Client communication.WorkerClient
	Status *communication.WorkerStatus
	LastSeen time.Time
}

// NewCoordinator creates a new coordinator instance
func NewCoordinator(transport communication.Transport) *Coordinator {
	return &Coordinator{
		workers:   make(map[string]*WorkerConnection),
		parser:    core.NewSQLParser(),
		planner:   core.NewQueryPlanner(),
		optimizer: core.NewQueryOptimizer(core.NewQueryPlanner()),
		transport: transport,
		// Will initialize distributed planner when workers are registered
		distributedPlanner: nil,
	}
}

// ExecuteQuery implements the CoordinatorService interface
func (c *Coordinator) ExecuteQuery(ctx context.Context, req *communication.DistributedQueryRequest) (*communication.DistributedQueryResponse, error) {
	startTime := time.Now()
	
	log.Printf("Coordinator: Executing distributed query: %s", req.SQL)
	
	// Parse the query
	parsedQuery, err := c.parser.Parse(req.SQL)
	if err != nil {
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Parse error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	// Create distributed execution plan
	plan, err := c.createDistributedPlan(parsedQuery)
	if err != nil {
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Planning error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	// Execute the plan
	result, stats, err := c.executePlan(ctx, plan)
	if err != nil {
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Execution error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	duration := time.Since(startTime)
	stats.TotalTime = duration
	stats.CoordinatorTime = duration // Will be adjusted when we track worker time separately
	
	log.Printf("Coordinator: Query completed in %v, processed %d rows", duration, result.Count)
	
	return &communication.DistributedQueryResponse{
		RequestID: req.RequestID,
		Rows:      result.Rows,
		Columns:   result.Columns,
		Count:     result.Count,
		Stats:     *stats,
		Duration:  duration,
	}, nil
}

// RegisterWorker implements the CoordinatorService interface
func (c *Coordinator) RegisterWorker(ctx context.Context, info *communication.WorkerInfo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	log.Printf("Coordinator: Registering worker %s at %s", info.ID, info.Address)
	
	// Create client connection to worker
	client, err := c.transport.NewWorkerClient(info.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to worker %s: %v", info.ID, err)
	}
	
	// Test connection
	if err := client.Health(ctx); err != nil {
		client.Close()
		return fmt.Errorf("worker %s health check failed: %v", info.ID, err)
	}
	
	// Get initial status
	status, err := client.GetStatus(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to get worker %s status: %v", info.ID, err)
	}
	
	c.workers[info.ID] = &WorkerConnection{
		Info:     info,
		Client:   client,
		Status:   status,
		LastSeen: time.Now(),
	}
	
	log.Printf("Coordinator: Worker %s registered successfully", info.ID)
	
	// Initialize or update distributed planner with current workers
	c.updateDistributedPlanner()
	
	return nil
}

// UnregisterWorker implements the CoordinatorService interface
func (c *Coordinator) UnregisterWorker(ctx context.Context, workerID string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if worker, exists := c.workers[workerID]; exists {
		worker.Client.Close()
		delete(c.workers, workerID)
		log.Printf("Coordinator: Worker %s unregistered", workerID)
	}
	
	return nil
}

// GetClusterStatus implements the CoordinatorService interface
func (c *Coordinator) GetClusterStatus(ctx context.Context) (*communication.ClusterStatus, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	status := &communication.ClusterStatus{
		TotalWorkers:  len(c.workers),
		ActiveWorkers: 0,
		Workers:       make([]communication.WorkerStatus, 0, len(c.workers)),
	}
	
	for _, worker := range c.workers {
		if worker.Status.Status == "active" {
			status.ActiveWorkers++
		}
		status.Workers = append(status.Workers, *worker.Status)
	}
	
	return status, nil
}

// Health implements the CoordinatorService interface
func (c *Coordinator) Health(ctx context.Context) error {
	return nil
}

// Shutdown implements the CoordinatorService interface
func (c *Coordinator) Shutdown(ctx context.Context) error {
	c.mutex.Lock()
	
	log.Println("Coordinator: Shutting down...")
	
	workers := make(map[string]*WorkerConnection)
	for id, worker := range c.workers {
		workers[id] = worker
	}
	c.workers = make(map[string]*WorkerConnection)
	
	c.mutex.Unlock()
	
	// Close worker connections outside the lock
	for workerID, worker := range workers {
		worker.Client.Close()
		log.Printf("Coordinator: Disconnected from worker %s", workerID)
	}
	
	log.Println("Coordinator: Shutdown complete")
	return nil
}

// updateDistributedPlanner creates or updates the distributed query planner
func (c *Coordinator) updateDistributedPlanner() {
	// Gather worker info
	workers := make([]planner.WorkerInfo, 0, len(c.workers))
	for _, worker := range c.workers {
		if worker.Status.Status == "active" {
			workers = append(workers, planner.WorkerInfo{
				ID:          worker.Info.ID,
				Address:     worker.Info.Address,
				MemoryMB:    worker.Info.Resources.MemoryMB,
				CurrentLoad: float64(worker.Status.CPUUsage) / 100.0,
			})
		}
	}
	
	// Create table statistics (would be loaded from metadata in production)
	tableStats := c.createTableStatistics()
	
	// Create planning preferences
	preferences := planner.PlanningPreferences{
		PreferDataLocality: true,
		OptimizeFor:        planner.OptimizeThroughput,
		MaxPlanningTime:    time.Second,
	}
	
	// Create new distributed planner
	c.distributedPlanner = planner.NewDistributedQueryPlanner(workers, tableStats, preferences)
	log.Printf("Coordinator: Updated distributed planner with %d workers", len(workers))
}

// createTableStatistics creates table statistics for the planner
func (c *Coordinator) createTableStatistics() map[string]*planner.TableStatistics {
	// In production, this would be loaded from metadata store
	// For now, return basic statistics
	return map[string]*planner.TableStatistics{
		"employees": {
			TableName: "employees",
			RowCount:  10000,
			SizeBytes: 10 * 1024 * 1024, // 10MB
			ColumnStats: map[string]*planner.ColumnStatistics{
				"id": {
					ColumnName:    "id",
					DataType:      "int",
					DistinctCount: 10000,
					MinValue:      1,
					MaxValue:      10000,
				},
				"department": {
					ColumnName:    "department",
					DataType:      "varchar",
					DistinctCount: 5,
					MinValue:      "Engineering",
					MaxValue:      "Sales",
				},
				"salary": {
					ColumnName:    "salary",
					DataType:      "int",
					DistinctCount: 1000,
					MinValue:      30000,
					MaxValue:      200000,
				},
			},
		},
	}
}

// createDistributedPlan creates a distributed execution plan
func (c *Coordinator) createDistributedPlan(query *core.ParsedQuery) (*DistributedPlan, error) {
	c.mutex.RLock()
	distributedPlanner := c.distributedPlanner
	c.mutex.RUnlock()
	
	// Use distributed planner if available
	if distributedPlanner != nil {
		// Create planning context
		workers := make([]planner.WorkerInfo, 0)
		c.mutex.RLock()
		for _, worker := range c.workers {
			if worker.Status.Status == "active" {
				workers = append(workers, planner.WorkerInfo{
					ID:          worker.Info.ID,
					Address:     worker.Info.Address,
					MemoryMB:    worker.Info.Resources.MemoryMB,
					CurrentLoad: float64(worker.Status.CPUUsage) / 100.0,
				})
			}
		}
		c.mutex.RUnlock()
		
		context := &planner.PlanningContext{
			Workers: workers,
			Constraints: planner.PlanningConstraints{
				MaxMemoryMB:     1024,
				MaxParallelism:  8,
				TimeoutDuration: 5 * time.Minute,
			},
			Preferences: planner.PlanningPreferences{
				PreferDataLocality: true,
				OptimizeFor:        planner.OptimizeThroughput,
				MaxPlanningTime:    time.Second,
			},
		}
		
		// Create optimized distributed plan
		distributedPlan, err := distributedPlanner.CreatePlan(query, context)
		if err != nil {
			log.Printf("Coordinator: Failed to create distributed plan: %v", err)
			// Fall back to simple planning
			return c.createSimpleDistributedPlan(query)
		}
		
		// Convert to our internal format
		return c.convertDistributedPlan(distributedPlan, query)
	}
	
	// Fall back to simple planning
	return c.createSimpleDistributedPlan(query)
}

// createSimpleDistributedPlan creates a simple distributed plan (fallback)
func (c *Coordinator) createSimpleDistributedPlan(query *core.ParsedQuery) (*DistributedPlan, error) {
	c.mutex.RLock()
	availableWorkers := make([]*WorkerConnection, 0, len(c.workers))
	for _, worker := range c.workers {
		if worker.Status.Status == "active" {
			availableWorkers = append(availableWorkers, worker)
		}
	}
	c.mutex.RUnlock()
	
	if len(availableWorkers) == 0 {
		return nil, fmt.Errorf("no active workers available")
	}
	
	plan := &DistributedPlan{
		Query:     query,
		Fragments: make([]*communication.QueryFragment, 0),
		Workers:   availableWorkers,
	}
	
	// Simple scan fragments for each worker
	for _, worker := range availableWorkers {
		fragmentID := c.generateFragmentID()
		fragment := &communication.QueryFragment{
			ID:           fragmentID,
			SQL:          c.buildFragmentSQL(query, worker.Info.DataPath),
			TablePath:    worker.Info.DataPath,
			Columns:      c.extractColumns(query),
			WhereClause:  query.Where,
			Limit:        query.Limit,
			FragmentType: communication.FragmentTypeScan,
			IsPartial:    len(availableWorkers) > 1,
		}
		
		// Handle GROUP BY
		if len(query.GroupBy) > 0 {
			fragment.GroupBy = query.GroupBy
			fragment.Aggregates = convertAggregates(query.Aggregates)
			fragment.FragmentType = communication.FragmentTypeAggregate
		}
		
		plan.Fragments = append(plan.Fragments, fragment)
		log.Printf("Coordinator: Created simple fragment %s for worker %s", fragmentID, worker.Info.ID)
	}
	
	return plan, nil
}

// convertDistributedPlan converts from planner format to coordinator format
func (c *Coordinator) convertDistributedPlan(plannerPlan *planner.DistributedPlan, query *core.ParsedQuery) (*DistributedPlan, error) {
	c.mutex.RLock()
	availableWorkers := make([]*WorkerConnection, 0, len(c.workers))
	workerMap := make(map[string]*WorkerConnection)
	for _, worker := range c.workers {
		if worker.Status.Status == "active" {
			availableWorkers = append(availableWorkers, worker)
			workerMap[worker.Info.ID] = worker
		}
	}
	c.mutex.RUnlock()
	
	plan := &DistributedPlan{
		Query:     query,
		Fragments: make([]*communication.QueryFragment, 0),
		Workers:   availableWorkers,
	}
	
	// Convert each stage to fragments
	for _, stage := range plannerPlan.Stages {
		log.Printf("Coordinator: Converting stage %s (type: %s) with %d fragments", 
			stage.ID, stage.Type, len(stage.Fragments))
		
		// For aggregate queries, only execute the final stage fragments
		if plannerPlan.ParsedQuery.IsAggregate && stage.Type == planner.StageFinal {
			// This is the final aggregation stage - execute it
			for _, fragment := range stage.Fragments {
				if fragment.Fragment != nil {
					plan.Fragments = append(plan.Fragments, fragment.Fragment)
					log.Printf("Coordinator: Added final aggregation fragment %s", fragment.Fragment.ID)
				}
			}
		} else if !plannerPlan.ParsedQuery.IsAggregate {
			// For non-aggregate queries, execute scan fragments
			if stage.Type == planner.StageScan {
				for _, fragment := range stage.Fragments {
					if fragment.Fragment != nil {
						plan.Fragments = append(plan.Fragments, fragment.Fragment)
						log.Printf("Coordinator: Added scan fragment %s", fragment.Fragment.ID)
					}
				}
			}
		}
	}
	
	// If we're dealing with an aggregate query, we need to handle the partial aggregation
	if plannerPlan.ParsedQuery.IsAggregate && len(plannerPlan.Stages) >= 2 {
		// Execute scan and partial aggregation stages on workers
		scanFragments := make([]*communication.QueryFragment, 0)
		
		// Find scan stage
		for _, stage := range plannerPlan.Stages {
			if stage.Type == planner.StageScan || stage.ID == "optimized_scan" {
				for _, fragment := range stage.Fragments {
					if fragment.Fragment != nil {
						// Clone and modify the fragment to include partial aggregation
						aggFragment := &communication.QueryFragment{
							ID:           fragment.Fragment.ID + "_agg",
							SQL:          c.buildOptimizedAggregateSQL(query, fragment.WorkerID),
							TablePath:    fragment.Fragment.TablePath,
							Columns:      fragment.Fragment.Columns,
							WhereClause:  fragment.Fragment.WhereClause,
							GroupBy:      query.GroupBy,
							Aggregates:   convertAggregates(query.Aggregates),
							FragmentType: communication.FragmentTypeAggregate,
							IsPartial:    true,
						}
						scanFragments = append(scanFragments, aggFragment)
						log.Printf("Coordinator: Created optimized aggregate fragment %s for worker %s", 
							aggFragment.ID, fragment.WorkerID)
					}
				}
			}
		}
		
		// Replace fragments with optimized ones
		if len(scanFragments) > 0 {
			plan.Fragments = scanFragments
		}
	}
	
	log.Printf("Coordinator: Converted distributed plan with %d fragments using optimization", 
		len(plan.Fragments))
	
	// Log optimization info
	if plannerPlan.Statistics != nil {
		log.Printf("Coordinator: Estimated cost: %.2f, rows: %d, bytes: %d", 
			plannerPlan.Statistics.TotalCost,
			plannerPlan.Statistics.EstimatedRows,
			plannerPlan.Statistics.EstimatedBytes)
	}
	
	if len(plannerPlan.Optimizations) > 0 {
		log.Printf("Coordinator: Applied optimizations: %v", plannerPlan.Optimizations)
	}
	
	return plan, nil
}

// buildOptimizedAggregateSQL builds SQL for optimized aggregate queries
func (c *Coordinator) buildOptimizedAggregateSQL(query *core.ParsedQuery, workerID string) string {
	selectCols := []string{}
	
	// Add GROUP BY columns
	selectCols = append(selectCols, query.GroupBy...)
	
	// Add partial aggregate functions
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
	
	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ", "), query.TableName)
	
	// Add WHERE conditions
	if len(query.Where) > 0 {
		conditions := []string{}
		for _, where := range query.Where {
			conditions = append(conditions, fmt.Sprintf("%s %s %v", where.Column, where.Operator, where.Value))
		}
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}
	
	// Add GROUP BY
	if len(query.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
	}
	
	return sql
}

// executePlan executes a distributed plan
func (c *Coordinator) executePlan(ctx context.Context, plan *DistributedPlan) (*core.QueryResult, *communication.ClusterStats, error) {
	stats := &communication.ClusterStats{
		TotalFragments: len(plan.Fragments),
		WorkersUsed:    len(plan.Workers),
		WorkerStats:    make(map[string]communication.ExecutionStats),
	}
	
	// Execute fragments in parallel
	type fragmentResult struct {
		workerID string
		result   *communication.FragmentResult
		err      error
	}
	
	resultChan := make(chan fragmentResult, len(plan.Fragments))
	
	// Send fragments to workers
	for i, fragment := range plan.Fragments {
		go func(workerIdx int, frag *communication.QueryFragment) {
			worker := plan.Workers[workerIdx]
			result, err := worker.Client.ExecuteFragment(ctx, frag)
			resultChan <- fragmentResult{
				workerID: worker.Info.ID,
				result:   result,
				err:      err,
			}
		}(i, fragment)
	}
	
	// Collect results
	fragmentResults := make([]*communication.FragmentResult, 0, len(plan.Fragments))
	for i := 0; i < len(plan.Fragments); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				return nil, stats, fmt.Errorf("worker %s failed: %v", result.workerID, result.err)
			}
			fragmentResults = append(fragmentResults, result.result)
			stats.WorkerStats[result.workerID] = result.result.Stats
			log.Printf("Coordinator: Received result from worker %s: %d rows", result.workerID, result.result.Count)
		case <-ctx.Done():
			return nil, stats, ctx.Err()
		}
	}
	
	// Aggregate results
	finalResult, err := c.aggregateResults(plan.Query, fragmentResults)
	if err != nil {
		return nil, stats, err
	}
	
	return finalResult, stats, nil
}

// aggregateResults combines results from multiple workers
func (c *Coordinator) aggregateResults(query *core.ParsedQuery, results []*communication.FragmentResult) (*core.QueryResult, error) {
	if len(results) == 0 {
		return &core.QueryResult{
			Columns: []string{},
			Rows:    []core.Row{},
			Count:   0,
		}, nil
	}
	
	if len(results) == 1 {
		// Single worker result
		result := results[0]
		return &core.QueryResult{
			Columns: result.Columns,
			Rows:    result.Rows,
			Count:   result.Count,
		}, nil
	}
	
	// Multiple worker results - need to merge
	columns := results[0].Columns
	allRows := make([]core.Row, 0)
	totalCount := 0
	
	for _, result := range results {
		if result.Error != "" {
			return nil, fmt.Errorf("fragment error: %s", result.Error)
		}
		allRows = append(allRows, result.Rows...)
		totalCount += result.Count
	}
	
	// Handle aggregations if needed
	if len(query.GroupBy) > 0 || len(query.Aggregates) > 0 {
		aggregatedRows, err := c.performFinalAggregation(query, allRows, columns)
		if err != nil {
			return nil, err
		}
		allRows = aggregatedRows
		totalCount = len(aggregatedRows)
	}
	
	// Apply final LIMIT if specified
	if query.Limit > 0 && len(allRows) > query.Limit {
		allRows = allRows[:query.Limit]
		totalCount = query.Limit
	}
	
	log.Printf("Coordinator: Aggregated %d results into %d final rows", len(results), totalCount)
	
	return &core.QueryResult{
		Columns: columns,
		Rows:    allRows,
		Count:   totalCount,
	}, nil
}

// performFinalAggregation performs final aggregation on coordinator
func (c *Coordinator) performFinalAggregation(query *core.ParsedQuery, rows []core.Row, columns []string) ([]core.Row, error) {
	// Check if we're dealing with partial aggregation results
	hasPartialAggregates := false
	for _, col := range columns {
		if strings.HasSuffix(col, "_sum") || strings.HasSuffix(col, "_count") {
			hasPartialAggregates = true
			break
		}
	}
	
	if hasPartialAggregates {
		// Handle optimized aggregation - combine partial results
		return c.combinePartialAggregates(query, rows, columns)
	}
	
	// Fall back to regular aggregation
	aggregates := convertAggregates(query.Aggregates)
	
	if len(query.GroupBy) == 0 {
		// Global aggregation
		return c.aggregateGlobal(aggregates, rows, columns)
	}
	
	// Group by aggregation
	return c.aggregateByGroup(query.GroupBy, aggregates, rows, columns)
}

// combinePartialAggregates combines partial aggregate results from workers
func (c *Coordinator) combinePartialAggregates(query *core.ParsedQuery, rows []core.Row, columns []string) ([]core.Row, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	
	// Group results by GROUP BY columns
	groups := make(map[string]*aggregateGroup)
	
	for _, row := range rows {
		// Build group key
		groupKey := ""
		if len(query.GroupBy) > 0 {
			keyParts := make([]string, len(query.GroupBy))
			for i, col := range query.GroupBy {
				if val, exists := row[col]; exists {
					keyParts[i] = fmt.Sprintf("%v", val)
				}
			}
			groupKey = strings.Join(keyParts, "|")
		}
		
		// Get or create group
		group, exists := groups[groupKey]
		if !exists {
			group = &aggregateGroup{
				key:    groupKey,
				values: make(map[string]interface{}),
			}
			groups[groupKey] = group
			
			// Copy GROUP BY values
			for _, col := range query.GroupBy {
				if val, exists := row[col]; exists {
					group.values[col] = val
				}
			}
		}
		
		// Combine partial aggregates
		for _, agg := range query.Aggregates {
			switch strings.ToUpper(agg.Function) {
			case "COUNT":
				// Sum up partial counts
				if val, exists := row[agg.Alias]; exists {
					current := getFloat64(group.values[agg.Alias])
					group.values[agg.Alias] = current + getFloat64(val)
				}
				
			case "SUM":
				// Sum up partial sums
				if val, exists := row[agg.Alias]; exists {
					current := getFloat64(group.values[agg.Alias])
					group.values[agg.Alias] = current + getFloat64(val)
				}
				
			case "AVG":
				// Combine partial sums and counts
				sumKey := agg.Alias + "_sum"
				countKey := agg.Alias + "_count"
				
				if sumVal, exists := row[sumKey]; exists {
					currentSum := getFloat64(group.values[sumKey])
					group.values[sumKey] = currentSum + getFloat64(sumVal)
				}
				
				if countVal, exists := row[countKey]; exists {
					currentCount := getFloat64(group.values[countKey])
					group.values[countKey] = currentCount + getFloat64(countVal)
				}
				
			case "MIN":
				// Take minimum of minimums
				if val, exists := row[agg.Alias]; exists {
					if current, hasVal := group.values[agg.Alias]; !hasVal {
						group.values[agg.Alias] = val
					} else {
						if getFloat64(val) < getFloat64(current) {
							group.values[agg.Alias] = val
						}
					}
				}
				
			case "MAX":
				// Take maximum of maximums
				if val, exists := row[agg.Alias]; exists {
					if current, hasVal := group.values[agg.Alias]; !hasVal {
						group.values[agg.Alias] = val
					} else {
						if getFloat64(val) > getFloat64(current) {
							group.values[agg.Alias] = val
						}
					}
				}
			}
		}
	}
	
	// Convert groups back to rows
	finalRows := make([]core.Row, 0, len(groups))
	for _, group := range groups {
		row := make(core.Row)
		
		// Add GROUP BY columns
		for _, col := range query.GroupBy {
			if val, exists := group.values[col]; exists {
				row[col] = val
			}
		}
		
		// Add final aggregate values
		for _, agg := range query.Aggregates {
			switch strings.ToUpper(agg.Function) {
			case "AVG":
				// Calculate average from sum and count
				sumKey := agg.Alias + "_sum"
				countKey := agg.Alias + "_count"
				
				sum := getFloat64(group.values[sumKey])
				count := getFloat64(group.values[countKey])
				
				if count > 0 {
					row[agg.Alias] = sum / count
				} else {
					row[agg.Alias] = 0.0
				}
			default:
				// Direct copy for other aggregates
				if val, exists := group.values[agg.Alias]; exists {
					row[agg.Alias] = val
				}
			}
		}
		
		finalRows = append(finalRows, row)
	}
	
	log.Printf("Coordinator: Combined %d partial results into %d final groups", len(rows), len(finalRows))
	
	return finalRows, nil
}

// Helper functions
func (c *Coordinator) generateFragmentID() string {
	c.queryIDMux.Lock()
	defer c.queryIDMux.Unlock()
	c.queryID++
	return fmt.Sprintf("fragment_%d_%d", time.Now().Unix(), c.queryID)
}

func (c *Coordinator) buildFragmentSQL(query *core.ParsedQuery, dataPath string) string {
	// Build SQL for fragment execution
	// This is simplified - a full implementation would optimize the SQL for distributed execution
	var sql strings.Builder
	
	sql.WriteString("SELECT ")
	if len(query.Columns) > 0 {
		columnNames := make([]string, len(query.Columns))
		for i, col := range query.Columns {
			columnNames[i] = col.Name
		}
		sql.WriteString(strings.Join(columnNames, ", "))
	} else {
		sql.WriteString("*")
	}
	
	sql.WriteString(" FROM ")
	sql.WriteString(query.TableName)
	
	if len(query.Where) > 0 {
		sql.WriteString(" WHERE ")
		// Simplified WHERE clause building
		for i, cond := range query.Where {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(fmt.Sprintf("%s %s %v", cond.Column, cond.Operator, cond.Value))
		}
	}
	
	if len(query.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(query.GroupBy, ", "))
	}
	
	if query.Limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", query.Limit))
	}
	
	return sql.String()
}

func (c *Coordinator) extractColumns(query *core.ParsedQuery) []string {
	columns := make([]string, len(query.Columns))
	for i, col := range query.Columns {
		columns[i] = col.Name
	}
	return columns
}

func (c *Coordinator) aggregateGlobal(aggregates []core.Column, rows []core.Row, columns []string) ([]core.Row, error) {
	// Simplified global aggregation
	if len(rows) == 0 {
		return []core.Row{}, nil
	}
	
	result := make(core.Row)
	count := len(rows)
	
	// Handle COUNT
	for _, agg := range aggregates {
		if strings.ToUpper(agg.Name) == "COUNT" {
			result[agg.Alias] = count
		}
		// Add other aggregation functions as needed
	}
	
	return []core.Row{result}, nil
}

func (c *Coordinator) aggregateByGroup(groupBy []string, aggregates []core.Column, rows []core.Row, columns []string) ([]core.Row, error) {
	// Simplified group by aggregation
	groups := make(map[string][]core.Row)
	
	for _, row := range rows {
		key := c.buildGroupKey(groupBy, row)
		groups[key] = append(groups[key], row)
	}
	
	result := make([]core.Row, 0, len(groups))
	for _, groupRows := range groups {
		if len(groupRows) > 0 {
			aggregatedRow := make(core.Row)
			
			// Copy group by columns
			firstRow := groupRows[0]
			for _, col := range groupBy {
				aggregatedRow[col] = firstRow[col]
			}
			
			// Calculate aggregates
			for _, agg := range aggregates {
				if strings.ToUpper(agg.Name) == "COUNT" {
					aggregatedRow[agg.Alias] = len(groupRows)
				}
				// Add other aggregation functions as needed
			}
			
			result = append(result, aggregatedRow)
		}
	}
	
	return result, nil
}

func (c *Coordinator) buildGroupKey(groupBy []string, row core.Row) string {
	var key strings.Builder
	for i, col := range groupBy {
		if i > 0 {
			key.WriteString("|")
		}
		key.WriteString(fmt.Sprintf("%v", row[col]))
	}
	return key.String()
}

// convertAggregates converts core.AggregateFunction to core.Column
func convertAggregates(aggregates []core.AggregateFunction) []core.Column {
	columns := make([]core.Column, len(aggregates))
	for i, agg := range aggregates {
		columns[i] = core.Column{
			Name:  agg.Function,
			Alias: agg.Alias,
		}
	}
	return columns
}

// DistributedPlan represents a distributed query execution plan
type DistributedPlan struct {
	Query     *core.ParsedQuery
	Fragments []*communication.QueryFragment
	Workers   []*WorkerConnection
}

// aggregateGroup represents a group in aggregation
type aggregateGroup struct {
	key    string
	values map[string]interface{}
}

// getFloat64 safely converts interface{} to float64
func getFloat64(val interface{}) float64 {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	default:
		return 0
	}
}