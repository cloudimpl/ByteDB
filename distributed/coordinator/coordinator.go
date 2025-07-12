package coordinator

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Coordinator manages distributed query execution
type Coordinator struct {
	workers     map[string]*WorkerConnection
	parser      *core.SQLParser
	planner     *core.QueryPlanner
	optimizer   *core.QueryOptimizer
	transport   communication.Transport
	mutex       sync.RWMutex
	queryID     int64
	queryIDMux  sync.Mutex
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

// createDistributedPlan creates a distributed execution plan
func (c *Coordinator) createDistributedPlan(query *core.ParsedQuery) (*DistributedPlan, error) {
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
	
	// For now, create simple scan fragments for each worker
	// This is a basic implementation - more sophisticated planning will be added later
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
		log.Printf("Coordinator: Created fragment %s for worker %s", fragmentID, worker.Info.ID)
	}
	
	return plan, nil
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
	// This is a simplified aggregation implementation
	// In a full implementation, this would handle all SQL aggregation functions properly
	
	aggregates := convertAggregates(query.Aggregates)
	
	if len(query.GroupBy) == 0 {
		// Global aggregation
		return c.aggregateGlobal(aggregates, rows, columns)
	}
	
	// Group by aggregation
	return c.aggregateByGroup(query.GroupBy, aggregates, rows, columns)
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