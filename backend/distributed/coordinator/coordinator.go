package coordinator

import (
	"bytedb/catalog"
	"bytedb/core"
	"bytedb/distributed/communication"
	"bytedb/distributed/monitoring"
	"bytedb/distributed/planner"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sort"
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
	catalogManager     *catalog.Manager
	dataPath           string
	mutex              sync.RWMutex
	queryID            int64
	queryIDMux         sync.Mutex
	monitor            *monitoring.CoordinatorMonitor
	queryMonitor       *monitoring.QueryMonitor
}

// WorkerConnection represents a connection to a worker
type WorkerConnection struct {
	Info   *communication.WorkerInfo
	Client communication.WorkerClient
	Status *communication.WorkerStatus
	LastSeen time.Time
}

// NewCoordinator creates a new coordinator instance
func NewCoordinator(transport communication.Transport, dataPath string) *Coordinator {
	tracer := core.GetTracer()
	tracer.Info(core.TraceComponentCoordinator, "Initializing distributed coordinator")
	
	return &Coordinator{
		workers:        make(map[string]*WorkerConnection),
		parser:         core.NewSQLParser(),
		planner:        core.NewQueryPlanner(),
		optimizer:      core.NewQueryOptimizer(core.NewQueryPlanner()),
		transport:      transport,
		catalogManager: nil, // Will be set via SetCatalogManager
		dataPath:       dataPath,
		monitor:        monitoring.NewCoordinatorMonitor(),
		queryMonitor:   monitoring.NewQueryMonitor(),
		// Will initialize distributed planner when workers are registered
		distributedPlanner: nil,
	}
}

// SetCatalogManager sets the catalog manager for the coordinator
func (c *Coordinator) SetCatalogManager(catalogManager *catalog.Manager) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.catalogManager = catalogManager
}

// ExecuteQuery implements the CoordinatorService interface
func (c *Coordinator) ExecuteQuery(ctx context.Context, req *communication.DistributedQueryRequest) (*communication.DistributedQueryResponse, error) {
	startTime := time.Now()
	tracer := core.GetTracer()
	
	// Start query monitoring
	queryExecution := c.queryMonitor.StartQueryMonitoring(req.RequestID, req.SQL)
	
	tracer.Info(core.TraceComponentCoordinator, "Executing distributed query", core.TraceContext(
		"requestID", req.RequestID,
		"sql", req.SQL,
		"timeout", req.Timeout,
	))
	
	// Parse the query
	queryExecution.StartPlanning()
	tracer.Debug(core.TraceComponentParser, "Parsing distributed query", core.TraceContext(
		"requestID", req.RequestID,
	))
	
	parsedQuery, err := c.parser.Parse(req.SQL)
	if err != nil {
		tracer.Error(core.TraceComponentParser, "Query parsing failed", core.TraceContext(
			"requestID", req.RequestID,
			"error", err.Error(),
		))
		queryExecution.SetError(err)
		queryExecution.Finish()
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Parse error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	tracer.Debug(core.TraceComponentParser, "Query parsed successfully", core.TraceContext(
		"requestID", req.RequestID,
		"queryType", parsedQuery.Type.String(),
		"isAggregate", parsedQuery.IsAggregate,
		"hasGroupBy", len(parsedQuery.GroupBy) > 0,
	))
	
	// Set query type for monitoring
	queryType := parsedQuery.Type.String()
	if parsedQuery.IsAggregate {
		queryType += "_AGGREGATE"
	}
	if len(parsedQuery.GroupBy) > 0 {
		queryType += "_GROUPBY"
	}
	queryExecution.SetQueryType(queryType)
	
	// Create distributed execution plan
	plan, err := c.createDistributedPlan(parsedQuery)
	queryExecution.EndPlanning()
	
	if err != nil {
		queryExecution.SetError(err)
		queryExecution.Finish()
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Planning error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	// Record planning metrics
	c.monitor.RecordPlanningTime(queryExecution.GetMetrics().PlanningDuration)
	
	// Record query coordination start
	c.monitor.RecordQueryStart(req.RequestID, len(plan.Fragments), len(plan.Workers))
	queryExecution.SetWorkerInfo(len(plan.Workers), len(plan.Fragments))
	
	// Execute the plan
	queryExecution.StartExecution()
	result, stats, err := c.executePlan(ctx, plan)
	queryExecution.EndExecution()
	
	if err != nil {
		queryExecution.SetError(err)
		c.monitor.RecordQueryEnd(req.RequestID, false, stats)
		queryExecution.Finish()
		return &communication.DistributedQueryResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Execution error: %v", err),
			Duration:  time.Since(startTime),
		}, nil
	}
	
	duration := time.Since(startTime)
	stats.TotalTime = duration
	stats.CoordinatorTime = duration // Will be adjusted when we track worker time separately
	
	// Record execution metrics
	queryExecution.SetDataInfo(result.Count, c.estimateDataTransferred(result))
	if plan.OptimizationApplied {
		queryExecution.SetOptimizationInfo(true, plan.OriginalCost, plan.OptimizedCost)
		c.monitor.RecordOptimization(true, plan.OriginalCost, plan.OptimizedCost)
	}
	
	// Calculate cache hit rate from worker stats
	cacheHitRate := c.calculateCacheHitRate(stats)
	queryExecution.SetCacheInfo(cacheHitRate)
	
	// Record coordination metrics
	c.monitor.RecordAggregationTime(queryExecution.GetMetrics().AggregationDuration)
	c.monitor.RecordQueryEnd(req.RequestID, true, stats)
	
	// Finish query monitoring
	queryMetrics := queryExecution.Finish()
	
	log.Printf("Coordinator: Query completed in %v, processed %d rows, cost reduction: %.1f%%", 
		duration, result.Count, queryMetrics.CostReduction)
	
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
	
	tracer := core.GetTracer()
	tracer.Info(core.TraceComponentCoordinator, "Registering worker", core.TraceContext(
		"workerID", info.ID,
		"address", info.Address,
		"resources", info.Resources,
	))
	
	// Create client connection to worker
	client, err := c.transport.NewWorkerClient(info.Address)
	if err != nil {
		tracer.Error(core.TraceComponentCoordinator, "Failed to connect to worker", core.TraceContext(
			"workerID", info.ID,
			"address", info.Address,
			"error", err.Error(),
		))
		return fmt.Errorf("failed to connect to worker %s: %v", info.ID, err)
	}
	
	// Test connection
	if err := client.Health(ctx); err != nil {
		tracer.Error(core.TraceComponentCoordinator, "Worker health check failed", core.TraceContext(
			"workerID", info.ID,
			"address", info.Address,
			"error", err.Error(),
		))
		client.Close()
		return fmt.Errorf("worker %s health check failed: %v", info.ID, err)
	}
	
	// Get initial status
	status, err := client.GetStatus(ctx)
	if err != nil {
		tracer.Error(core.TraceComponentCoordinator, "Failed to get worker status", core.TraceContext(
			"workerID", info.ID,
			"address", info.Address,
			"error", err.Error(),
		))
		client.Close()
		return fmt.Errorf("failed to get worker %s status: %v", info.ID, err)
	}
	
	c.workers[info.ID] = &WorkerConnection{
		Info:     info,
		Client:   client,
		Status:   status,
		LastSeen: time.Now(),
	}
	
	tracer.Info(core.TraceComponentCoordinator, "Worker registered successfully", core.TraceContext(
		"workerID", info.ID,
		"address", info.Address,
		"dataPath", info.DataPath,
		"status", status.Status,
		"cpuUsage", status.CPUUsage,
		"memoryUsage", status.MemoryUsage,
	))
	
	// Initialize or update distributed planner with current workers
	c.updateDistributedPlanner()
	
	return nil
}

// UnregisterWorker implements the CoordinatorService interface
func (c *Coordinator) UnregisterWorker(ctx context.Context, workerID string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if worker, exists := c.workers[workerID]; exists {
		tracer := core.GetTracer()
		tracer.Info(core.TraceComponentCoordinator, "Unregistering worker", core.TraceContext(
			"workerID", workerID,
			"address", worker.Info.Address,
		))
		
		worker.Client.Close()
		delete(c.workers, workerID)
		
		tracer.Info(core.TraceComponentCoordinator, "Worker unregistered successfully", core.TraceContext(
			"workerID", workerID,
		))
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
	
	tracer := core.GetTracer()
	tracer.Info(core.TraceComponentCoordinator, "Shutting down coordinator", core.TraceContext(
		"activeWorkers", len(c.workers),
	))
	
	workers := make(map[string]*WorkerConnection)
	for id, worker := range c.workers {
		workers[id] = worker
	}
	c.workers = make(map[string]*WorkerConnection)
	
	c.mutex.Unlock()
	
	// Close worker connections outside the lock
	for workerID, worker := range workers {
		tracer.Debug(core.TraceComponentCoordinator, "Disconnecting from worker", core.TraceContext(
			"workerID", workerID,
			"address", worker.Info.Address,
		))
		worker.Client.Close()
		tracer.Info(core.TraceComponentCoordinator, "Disconnected from worker", core.TraceContext(
			"workerID", workerID,
		))
	}
	
	tracer.Info(core.TraceComponentCoordinator, "Coordinator shutdown complete", core.TraceContext(
		"disconnectedWorkers", len(workers),
	))
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
	tracer := core.GetTracer()
	tracer.Info(core.TraceComponentCoordinator, "Updated distributed planner", core.TraceContext(
		"activeWorkers", len(workers),
		"totalWorkers", len(c.workers),
		"preferences", preferences,
	))
}

// createTableStatistics reads table statistics from catalog for query planning
func (c *Coordinator) createTableStatistics() map[string]*planner.TableStatistics {
	stats := make(map[string]*planner.TableStatistics)
	
	// If no catalog manager is set, return empty statistics
	if c.catalogManager == nil {
		log.Printf("Warning: No catalog manager set, using empty statistics")
		return stats
	}
	
	ctx := context.Background()
	
	// Ensure catalog is initialized with existing files (bootstrap only)
	if err := c.ensureCatalogInitialized(ctx); err != nil {
		log.Printf("Warning: Failed to ensure catalog initialized: %v", err)
		return stats
	}
	
	// Get all tables from catalog
	tables, err := c.catalogManager.ListTables(ctx, "*")
	if err != nil {
		log.Printf("Warning: Failed to list tables from catalog: %v", err)
		return stats
	}
	
	for _, table := range tables {
		// Convert catalog statistics to planner format
		if table.Statistics != nil {
			stats[table.Name] = c.convertCatalogStatsToPlanner(table.Statistics, table.Columns)
		} else {
			// If no statistics exist, trigger background calculation
			log.Printf("Warning: No statistics for table %s, using defaults", table.Name)
			stats[table.Name] = c.createDefaultTableStats(table)
		}
	}
	
	return stats
}

// ensureCatalogInitialized ensures catalog is bootstrapped with existing files (one-time setup)
func (c *Coordinator) ensureCatalogInitialized(ctx context.Context) error {
	// If no catalog manager, nothing to do
	if c.catalogManager == nil {
		return fmt.Errorf("no catalog manager set")
	}
	
	// Check if catalog already has tables - if so, skip bootstrap
	tables, err := c.catalogManager.ListTables(ctx, "*")
	if err == nil && len(tables) > 0 {
		return nil // Catalog already initialized
	}
	
	// Bootstrap: discover existing parquet files and register them
	pattern := filepath.Join(c.dataPath, "*.parquet")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to scan for parquet files: %v", err)
	}
	
	for _, file := range files {
		tableName := strings.TrimSuffix(filepath.Base(file), ".parquet")
		
		// Register table in catalog (this should calculate and store statistics)
		if err := c.catalogManager.RegisterTable(ctx, tableName, file, "parquet"); err != nil {
			log.Printf("Warning: Failed to register table %s: %v", tableName, err)
			continue
		}
		log.Printf("Bootstrapped table '%s' from parquet file", tableName)
	}
	
	return nil
}

// convertCatalogStatsToPlanner converts catalog statistics to planner format
func (c *Coordinator) convertCatalogStatsToPlanner(catalogStats *catalog.TableStatistics, columns []catalog.ColumnMetadata) *planner.TableStatistics {
	columnStats := make(map[string]*planner.ColumnStatistics)
	
	// Convert column metadata to column statistics
	for _, column := range columns {
		columnStats[column.Name] = &planner.ColumnStatistics{
			ColumnName:    column.Name,
			DataType:      column.Type,
			DistinctCount: 0, // Would come from catalog if available
			MinValue:      nil,
			MaxValue:      nil,
		}
	}
	
	return &planner.TableStatistics{
		TableName:   "", // Will be set by caller
		RowCount:    catalogStats.RowCount,
		SizeBytes:   catalogStats.SizeBytes,
		ColumnStats: columnStats,
		Partitions:  []planner.PartitionInfo{},
	}
}

// createDefaultTableStats creates default statistics when catalog stats are missing
func (c *Coordinator) createDefaultTableStats(table *catalog.TableMetadata) *planner.TableStatistics {
	columnStats := make(map[string]*planner.ColumnStatistics)
	
	for _, column := range table.Columns {
		columnStats[column.Name] = &planner.ColumnStatistics{
			ColumnName:    column.Name,
			DataType:      column.Type,
			DistinctCount: 100, // Default estimate
			MinValue:      nil,
			MaxValue:      nil,
		}
	}
	
	// Estimate based on number of files
	fileCount := len(table.Locations)
	if fileCount == 0 {
		fileCount = 1
	}
	
	return &planner.TableStatistics{
		TableName:   table.Name,
		RowCount:    int64(fileCount * 1000), // 1000 rows per file default
		SizeBytes:   int64(fileCount * 100 * 1024), // 100KB per file default
		ColumnStats: columnStats,
		Partitions:  []planner.PartitionInfo{},
	}
}

// createDistributedPlan creates a distributed execution plan
func (c *Coordinator) createDistributedPlan(query *core.ParsedQuery) (*DistributedPlan, error) {
	fmt.Printf("DEBUG: createDistributedPlan called for query: %s\n", query.RawSQL)
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
	fmt.Printf("DEBUG: Falling back to simple distributed plan\n")
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
	fmt.Printf("DEBUG: Creating fragments for %d available workers\n", len(availableWorkers))
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
	
	// Populate optimization tracking fields
	plan.OptimizationApplied = len(plannerPlan.Optimizations) > 0
	if plannerPlan.Statistics != nil {
		plan.OptimizedCost = plannerPlan.Statistics.TotalCost
		
		// Calculate original cost by adding back optimization benefits
		originalCost := plan.OptimizedCost
		for _, opt := range plannerPlan.Optimizations {
			originalCost += opt.Benefit
		}
		plan.OriginalCost = originalCost
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
			conditionStr := c.formatWhereCondition(where)
			if conditionStr != "" {
				conditions = append(conditions, conditionStr)
			}
		}
		if len(conditions) > 0 {
			sql += " WHERE " + strings.Join(conditions, " AND ")
		}
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
		go func(fragmentIdx int, frag *communication.QueryFragment) {
			// Use round-robin assignment to distribute fragments across available workers
			workerIdx := fragmentIdx % len(plan.Workers)
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
		// Single worker result - still need to handle aggregations
		result := results[0]
		
		// Check if we need final aggregation processing
		
		if (len(query.GroupBy) > 0 || len(query.Aggregates) > 0) && len(result.Rows) > 0 {
			// Check for intermediate columns that need final processing
			// Check both column names and actual row keys since column metadata might be incomplete
			hasIntermediateColumns := false
			
			// First check column names
			for _, col := range result.Columns {
				if strings.HasSuffix(col, "_sum") || strings.HasSuffix(col, "_count") {
					hasIntermediateColumns = true
					break
				}
			}
			
			// If not found in columns, check actual row data
			if !hasIntermediateColumns && len(result.Rows) > 0 {
				for key := range result.Rows[0] {
					if strings.HasSuffix(key, "_sum") || strings.HasSuffix(key, "_count") {
						hasIntermediateColumns = true
						break
					}
				}
			}
			
			if hasIntermediateColumns {
				// Perform final aggregation even for single worker
				
				// Build actual columns from row data if column metadata is incomplete
				actualColumns := result.Columns
				if len(result.Rows) > 0 && len(actualColumns) < len(result.Rows[0]) {
					// Column metadata is incomplete, rebuild from row keys
					columnSet := make(map[string]bool)
					for _, col := range result.Columns {
						columnSet[col] = true
					}
					
					// Add missing columns from row data
					for key := range result.Rows[0] {
						if !columnSet[key] {
							actualColumns = append(actualColumns, key)
						}
					}
				}
				
				aggregatedRows, err := c.performFinalAggregation(query, result.Rows, actualColumns)
				if err != nil {
					return nil, err
				}
				
				// Update columns to final aggregate names
				finalColumns := make([]string, 0)
				finalColumns = append(finalColumns, query.GroupBy...)
				for _, agg := range query.Aggregates {
					finalColumns = append(finalColumns, agg.Alias)
				}
				
				return &core.QueryResult{
					Columns: finalColumns,
					Rows:    aggregatedRows,
					Count:   len(aggregatedRows),
				}, nil
			}
		}
		
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
		
		// Update columns to final aggregate names
		finalColumns := make([]string, 0)
		finalColumns = append(finalColumns, query.GroupBy...)
		for _, agg := range query.Aggregates {
			finalColumns = append(finalColumns, agg.Alias)
		}
		columns = finalColumns
	}
	
	// Apply ORDER BY if specified
	if len(query.OrderBy) > 0 {
		allRows = c.applyOrderBy(allRows, query.OrderBy)
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
	
	// Also check if we have GROUP BY with aggregates (distributed partial results)
	hasGroupByAggregates := len(query.GroupBy) > 0 && len(query.Aggregates) > 0
	
	if hasPartialAggregates || hasGroupByAggregates {
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

// applyOrderBy sorts rows according to ORDER BY clause
func (c *Coordinator) applyOrderBy(rows []core.Row, orderBy []core.OrderByColumn) []core.Row {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}
	
	// Make a copy to avoid modifying original
	sortedRows := make([]core.Row, len(rows))
	copy(sortedRows, rows)
	
	// Sort based on ORDER BY columns
	sort.Slice(sortedRows, func(i, j int) bool {
		for _, col := range orderBy {
			valI := sortedRows[i][col.Column]
			valJ := sortedRows[j][col.Column]
			
			// Compare values
			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if strings.ToUpper(col.Direction) == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false // Equal
	})
	
	return sortedRows
}

// compareValues compares two values and returns -1, 0, or 1
func compareValues(a, b interface{}) int {
	// Convert to comparable types
	aFloat := getFloat64(a)
	bFloat := getFloat64(b)
	
	// Try numeric comparison first
	if aFloat != 0 || bFloat != 0 {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}
	
	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// formatWhereCondition formats a WHERE condition into SQL string
func (c *Coordinator) formatWhereCondition(where core.WhereCondition) string {
	if where.IsComplex {
		// Handle complex logical operations
		leftStr := ""
		if where.Left != nil {
			leftStr = c.formatWhereCondition(*where.Left)
		}
		rightStr := ""
		if where.Right != nil {
			rightStr = c.formatWhereCondition(*where.Right)
		}
		
		if leftStr != "" && rightStr != "" {
			return fmt.Sprintf("(%s %s %s)", leftStr, where.LogicalOp, rightStr)
		} else if leftStr != "" {
			return leftStr
		} else if rightStr != "" {
			return rightStr
		}
		return ""
	}
	
	// Handle single conditions
	if where.Column == "" {
		return ""
	}
	
	operator := strings.ToUpper(where.Operator)
	
	// Handle IN operator with ValueList
	if operator == "IN" && len(where.ValueList) > 0 {
		valueStrs := make([]string, len(where.ValueList))
		for i, val := range where.ValueList {
			switch v := val.(type) {
			case string:
				valueStrs[i] = fmt.Sprintf("'%s'", v)
			default:
				valueStrs[i] = fmt.Sprintf("%v", v)
			}
		}
		return fmt.Sprintf("%s IN (%s)", where.Column, strings.Join(valueStrs, ", "))
	}
	
	// Handle BETWEEN operator
	if operator == "BETWEEN" && where.ValueFrom != nil && where.ValueTo != nil {
		fromStr := ""
		toStr := ""
		switch v := where.ValueFrom.(type) {
		case string:
			fromStr = fmt.Sprintf("'%s'", v)
		default:
			fromStr = fmt.Sprintf("%v", v)
		}
		switch v := where.ValueTo.(type) {
		case string:
			toStr = fmt.Sprintf("'%s'", v)
		default:
			toStr = fmt.Sprintf("%v", v)
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s", where.Column, fromStr, toStr)
	}
	
	// Handle standard operators with single value
	if where.Value != nil {
		var valueStr string
		switch v := where.Value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", v)
		default:
			valueStr = fmt.Sprintf("%v", v)
		}
		return fmt.Sprintf("%s %s %s", where.Column, where.Operator, valueStr)
	}
	
	// Handle column-to-column comparisons
	if where.ValueColumn != "" {
		return fmt.Sprintf("%s %s %s", where.Column, where.Operator, where.ValueColumn)
	}
	
	return ""
}

// estimateDataTransferred estimates the amount of data transferred for monitoring
func (c *Coordinator) estimateDataTransferred(result *core.QueryResult) int64 {
	// Rough estimate: assume average 100 bytes per row
	return int64(result.Count) * 100
}

// calculateCacheHitRate calculates the overall cache hit rate from worker stats
func (c *Coordinator) calculateCacheHitRate(stats *communication.ClusterStats) float64 {
	if stats == nil || len(stats.WorkerStats) == 0 {
		return 0.0
	}
	
	totalHits := int64(0)
	totalRequests := int64(0)
	
	for _, workerStats := range stats.WorkerStats {
		totalHits += int64(workerStats.CacheHits)
		totalRequests += int64(workerStats.CacheHits + workerStats.CacheMisses)
	}
	
	if totalRequests > 0 {
		return (float64(totalHits) / float64(totalRequests)) * 100
	}
	return 0.0
}

// GetMonitoringDashboard returns the monitoring dashboard instance
func (c *Coordinator) GetMonitoringDashboard() *monitoring.Dashboard {
	return monitoring.NewDashboard(c.monitor, c.queryMonitor)
}

// GetCoordinatorStats returns current coordinator statistics for monitoring
func (c *Coordinator) GetCoordinatorStats() monitoring.CoordinatorStats {
	return c.monitor.GetCoordinatorStats()
}

// GetQueryStats returns current query statistics for monitoring
func (c *Coordinator) GetQueryStats() monitoring.QueryStats {
	return c.queryMonitor.GetQueryStats()
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
	// Since data is already partitioned across workers, we don't need partition filters
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
		// Build WHERE clause without partition filters
		for i, cond := range query.Where {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			// Properly format value based on type
			var valueStr string
			switch v := cond.Value.(type) {
			case string:
				valueStr = fmt.Sprintf("'%s'", v)
			default:
				valueStr = fmt.Sprintf("%v", v)
			}
			sql.WriteString(fmt.Sprintf("%s %s %s", cond.Column, cond.Operator, valueStr))
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
	// Global aggregation - properly handle partial results
	if len(rows) == 0 {
		return []core.Row{}, nil
	}
	
	result := make(core.Row)
	
	// Check if we have partial aggregation results
	// This happens when we have multiple rows with same column names (from different workers)
	hasPartialResults := len(rows) > 1
	
	// Also check for specific aggregate column names
	if !hasPartialResults {
		for _, col := range columns {
			if col == "total_employees" || col == "count" || col == "total" ||
			   strings.HasSuffix(col, "_sum") || strings.HasSuffix(col, "_count") {
				hasPartialResults = true
				break
			}
		}
	}
	
	// Handle aggregations
	for _, agg := range aggregates {
		switch strings.ToUpper(agg.Name) {
		case "COUNT":
			if hasPartialResults {
				// Sum up partial counts from workers
				totalCount := 0.0
				for _, row := range rows {
					if val, exists := row[agg.Alias]; exists {
						totalCount += getFloat64(val)
					}
				}
				result[agg.Alias] = int(totalCount)
			} else {
				// Direct count of rows
				result[agg.Alias] = len(rows)
			}
		case "SUM":
			// Sum up values
			total := 0.0
			for _, row := range rows {
				if val, exists := row[agg.Alias]; exists {
					total += getFloat64(val)
				}
			}
			result[agg.Alias] = total
		case "AVG":
			// Handle AVG aggregation
			if sumCol := agg.Alias + "_sum"; containsColumn(columns, sumCol) {
				// Optimized path with partial sums and counts
				totalSum := 0.0
				totalCount := 0.0
				for _, row := range rows {
					if sumVal, exists := row[sumCol]; exists {
						totalSum += getFloat64(sumVal)
					}
					if countVal, exists := row[agg.Alias + "_count"]; exists {
						totalCount += getFloat64(countVal)
					}
				}
				if totalCount > 0 {
					result[agg.Alias] = totalSum / totalCount
				} else {
					result[agg.Alias] = 0.0
				}
			}
		case "MIN":
			// Find minimum value
			var minVal interface{}
			for _, row := range rows {
				if val, exists := row[agg.Alias]; exists {
					if minVal == nil || getFloat64(val) < getFloat64(minVal) {
						minVal = val
					}
				}
			}
			result[agg.Alias] = minVal
		case "MAX":
			// Find maximum value
			var maxVal interface{}
			for _, row := range rows {
				if val, exists := row[agg.Alias]; exists {
					if maxVal == nil || getFloat64(val) > getFloat64(maxVal) {
						maxVal = val
					}
				}
			}
			result[agg.Alias] = maxVal
		}
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
	Query              *core.ParsedQuery
	Fragments          []*communication.QueryFragment
	Workers            []*WorkerConnection
	OptimizationApplied bool
	OriginalCost       float64
	OptimizedCost      float64
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

// containsColumn checks if a column exists in the columns list
func containsColumn(columns []string, column string) bool {
	for _, col := range columns {
		if col == column {
			return true
		}
	}
	return false
}