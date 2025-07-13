package testing

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"bytedb/distributed/coordinator"
	"bytedb/distributed/worker"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

// DistributedTestCase extends TestCase for distributed queries
type DistributedTestCase struct {
	core.TestCase
	Distributed DistributedConfig `json:"distributed,omitempty"`
}

// DistributedConfig contains configuration for distributed test execution
type DistributedConfig struct {
	Workers            int                     `json:"workers,omitempty"`
	DataDistribution   string                  `json:"data_distribution,omitempty"` // "partitioned", "replicated", "random"
	TransportType      string                  `json:"transport_type,omitempty"`    // "memory", "grpc", "http"
	ExpectedFragments  int                     `json:"expected_fragments,omitempty"`
	ExpectedWorkers    int                     `json:"expected_workers,omitempty"`
	NetworkOptimization bool                   `json:"network_optimization,omitempty"`
	VerifyPartialAggs  bool                   `json:"verify_partial_aggregates,omitempty"`
	WorkerFailures     []WorkerFailureConfig   `json:"worker_failures,omitempty"`
}

// WorkerFailureConfig simulates worker failures for resilience testing
type WorkerFailureConfig struct {
	WorkerID    string        `json:"worker_id"`
	FailureType string        `json:"failure_type"` // "crash", "network", "slow"
	AtStage     string        `json:"at_stage"`     // "planning", "execution", "aggregation"
	Delay       time.Duration `json:"delay,omitempty"`
}

// DistributedTestRunner executes distributed SQL tests
type DistributedTestRunner struct {
	engine      *core.QueryEngine
	tracer      *core.Tracer
	coordinator *coordinator.Coordinator
	workers     []*worker.Worker
	transport   communication.Transport
	workerAddrs []string
	dataPath    string
	mutex       sync.Mutex
}

// NewDistributedTestRunner creates a new distributed test runner
func NewDistributedTestRunner(dataPath string) *DistributedTestRunner {
	engine := core.NewQueryEngine(dataPath)
	return &DistributedTestRunner{
		engine:   engine,
		tracer:   core.GetTracer(),
		dataPath: dataPath,
	}
}

// SetupCluster initializes the distributed cluster for testing
func (r *DistributedTestRunner) SetupCluster(numWorkers int, transportType string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create transport based on type
	switch transportType {
	case "memory":
		r.transport = communication.NewMemoryTransport()
	case "grpc":
		// TODO: Implement gRPC transport
		return fmt.Errorf("gRPC transport not yet implemented")
	case "http":
		// TODO: Implement HTTP transport
		return fmt.Errorf("HTTP transport not yet implemented")
	default:
		r.transport = communication.NewMemoryTransport()
	}

	// Create coordinator
	r.coordinator = coordinator.NewCoordinator(r.transport)
	coordAddr := "test-coordinator:8080"
	
	// Start coordinator server
	if err := r.transport.StartCoordinatorServer(coordAddr, r.coordinator); err != nil {
		return fmt.Errorf("failed to start coordinator server: %v", err)
	}

	// Create workers
	r.workers = make([]*worker.Worker, numWorkers)
	r.workerAddrs = make([]string, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("test-worker-%d", i+1)
		workerPath := fmt.Sprintf("%s/worker-%d", r.dataPath, i+1)
		
		// Ensure worker directory exists
		if err := os.MkdirAll(workerPath, 0755); err != nil {
			return fmt.Errorf("failed to create worker directory: %v", err)
		}

		r.workers[i] = worker.NewWorker(workerID, workerPath)
		r.workerAddrs[i] = fmt.Sprintf("%s:808%d", workerID, i+1)
		
		// Start worker server
		if err := r.transport.StartWorkerServer(r.workerAddrs[i], r.workers[i]); err != nil {
			return fmt.Errorf("failed to start worker server: %v", err)
		}

		// Register worker with coordinator
		info := &communication.WorkerInfo{
			ID:       workerID,
			Address:  r.workerAddrs[i],
			DataPath: workerPath,
			Status:   "active",
			Resources: communication.WorkerResources{
				CPUCores:    4,
				MemoryMB:    1024,
				DiskSpaceGB: 100,
			},
		}

		ctx := context.Background()
		if err := r.coordinator.RegisterWorker(ctx, info); err != nil {
			return fmt.Errorf("failed to register worker %s: %v", workerID, err)
		}
	}

	return nil
}

// RunDistributedTest executes a single distributed test case
func (r *DistributedTestRunner) RunDistributedTest(testCase *DistributedTestCase) *DistributedTestResult {
	startTime := time.Now()
	result := &DistributedTestResult{
		TestName:      testCase.Name,
		StartTime:     startTime,
		Metadata:      make(map[string]interface{}),
		TraceMessages: []string{},
	}

	// Apply trace configuration
	if testCase.Trace.Level != core.TraceLevelOff {
		r.applyTraceConfig(testCase.Trace)
	}

	// Run setup queries
	if err := r.runSetupQueries(testCase.Setup); err != nil {
		result.Error = fmt.Sprintf("Setup failed: %v", err)
		result.Success = false
		result.Duration = time.Since(startTime)
		return result
	}

	// Simulate worker failures if configured
	if len(testCase.Distributed.WorkerFailures) > 0 {
		if err := r.simulateWorkerFailures(testCase.Distributed.WorkerFailures); err != nil {
			result.Error = fmt.Sprintf("Failed to simulate worker failures: %v", err)
			result.Success = false
			result.Duration = time.Since(startTime)
			return result
		}
	}

	// Execute the distributed query
	ctx := context.Background()
	if testCase.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, testCase.Timeout)
		defer cancel()
	}

	queryStart := time.Now()
	
	req := &communication.DistributedQueryRequest{
		SQL:       testCase.SQL,
		RequestID: fmt.Sprintf("test-%s-%d", testCase.Name, time.Now().UnixNano()),
		Timeout:   30 * time.Second,
	}

	response, err := r.coordinator.ExecuteQuery(ctx, req)
	queryDuration := time.Since(queryStart)

	if err != nil {
		result.Error = fmt.Sprintf("Query execution failed: %v", err)
		result.Success = false
		result.Duration = time.Since(startTime)
		return result
	}

	if response.Error != "" {
		// Check if this is an expected error
		if testCase.Expected.Error != nil && response.Error == *testCase.Expected.Error {
			result.Success = true
		} else {
			result.Error = fmt.Sprintf("Query error: %s", response.Error)
			result.Success = false
		}
		result.Duration = time.Since(startTime)
		return result
	}

	// Store distributed execution stats
	result.Metadata["workers_used"] = response.Stats.WorkersUsed
	result.Metadata["total_fragments"] = response.Stats.TotalFragments
	result.Metadata["bytes_transferred"] = response.Stats.DataTransferBytes
	result.Metadata["query_duration_ms"] = queryDuration.Milliseconds()
	
	// Check for optimization based on data transfer size
	optimizationApplied := false
	partialAggregation := false
	if response.Stats.DataTransferBytes < 1000 && response.Stats.TotalFragments > 1 {
		// If data transfer is very small with multiple fragments, likely optimization was applied
		optimizationApplied = true
		partialAggregation = true
	}
	result.Metadata["optimization_applied"] = optimizationApplied
	result.Metadata["partial_aggregation"] = partialAggregation

	// Convert response to QueryResult for validation
	queryResult := &core.QueryResult{
		Columns: response.Columns,
		Rows:    response.Rows,
	}

	// Validate distributed-specific expectations
	if err := r.validateDistributedExpectations(testCase, response); err != nil {
		result.Error = err.Error()
		result.Success = false
		result.Duration = time.Since(startTime)
		return result
	}

	// Run standard validations
	validationErrors := r.validateResult(queryResult, testCase.Expected)
	if len(validationErrors) > 0 {
		result.Error = strings.Join(validationErrors, "; ")
		result.Success = false
	} else {
		result.Success = true
		result.RowCount = len(queryResult.Rows)
	}

	// Validate performance if specified
	if testCase.Expected.Performance != nil && testCase.Expected.Performance.MaxDuration != 0 {
		if queryDuration > testCase.Expected.Performance.MaxDuration {
			result.Error = fmt.Sprintf("Performance expectation failed: query took %v, expected max %v", 
				queryDuration, testCase.Expected.Performance.MaxDuration)
			result.Success = false
		}
	}

	// Run cleanup queries
	if err := r.runCleanupQueries(testCase.Cleanup); err != nil {
		log.Printf("Cleanup failed for test %s: %v", testCase.Name, err)
	}

	result.Duration = time.Since(startTime)
	return result
}

// DistributedTestResult represents the result of a distributed test
type DistributedTestResult struct {
	TestName      string        `json:"test_name"`
	Success       bool          `json:"success"`
	Error         string        `json:"error,omitempty"`
	Duration      time.Duration `json:"duration"`
	RowCount      int           `json:"row_count"`
	StartTime     time.Time     `json:"start_time"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	TraceMessages []string      `json:"trace_messages,omitempty"`
}

// validateDistributedExpectations validates distributed-specific test expectations
func (r *DistributedTestRunner) validateDistributedExpectations(testCase *DistributedTestCase, response *communication.DistributedQueryResponse) error {
	var errors []string

	// Validate expected number of workers used
	if testCase.Distributed.ExpectedWorkers > 0 && response.Stats.WorkersUsed != testCase.Distributed.ExpectedWorkers {
		errors = append(errors, fmt.Sprintf("Expected %d workers, but %d were used", 
			testCase.Distributed.ExpectedWorkers, response.Stats.WorkersUsed))
	}

	// Validate expected number of fragments
	if testCase.Distributed.ExpectedFragments > 0 && response.Stats.TotalFragments != testCase.Distributed.ExpectedFragments {
		errors = append(errors, fmt.Sprintf("Expected %d fragments, but %d were created", 
			testCase.Distributed.ExpectedFragments, response.Stats.TotalFragments))
	}

	// Validate network optimization based on data transfer
	if testCase.Distributed.NetworkOptimization {
		// Check if data transfer is minimal (indicating optimization)
		if response.Stats.DataTransferBytes > 10000 {
			errors = append(errors, fmt.Sprintf("Expected network optimization but transferred %d bytes", response.Stats.DataTransferBytes))
		}
	}

	// Validate partial aggregation
	if testCase.Distributed.VerifyPartialAggs {
		// For partial aggregation, data transfer should be minimal
		if response.Stats.DataTransferBytes > 10000 || response.Stats.TotalFragments < 2 {
			errors = append(errors, "Expected partial aggregation to be performed")
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "; "))
	}

	return nil
}

// simulateWorkerFailures simulates various worker failure scenarios
func (r *DistributedTestRunner) simulateWorkerFailures(failures []WorkerFailureConfig) error {
	// TODO: Implement worker failure simulation
	// This would involve:
	// 1. Finding the specified worker
	// 2. Simulating the failure type (crash, network issue, slowness)
	// 3. Timing the failure to occur at the specified stage
	return nil
}

// TeardownCluster cleans up the distributed cluster
func (r *DistributedTestRunner) TeardownCluster() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Clean up workers
	for _, w := range r.workers {
		// TODO: Add worker cleanup if needed
		_ = w
	}

	// Clean up transport
	// TODO: Add transport cleanup if needed

	r.coordinator = nil
	r.workers = nil
	r.transport = nil
	r.workerAddrs = nil

	return nil
}

// ParseDistributedTestFile parses a distributed SQL test file
func ParseDistributedTestFile(filePath string) (*DistributedTestSuite, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file: %v", err)
	}

	if strings.HasSuffix(filePath, ".json") {
		return parseDistributedJSONTestFile(content)
	}

	return parseDistributedSQLTestFile(string(content))
}

// DistributedTestSuite represents a collection of distributed test cases
type DistributedTestSuite struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Config      core.TestSuiteConfig   `json:"config,omitempty"`
	TestCases   []DistributedTestCase  `json:"test_cases"`
}

// parseDistributedJSONTestFile parses a JSON distributed test file
func parseDistributedJSONTestFile(content []byte) (*DistributedTestSuite, error) {
	var suite DistributedTestSuite
	if err := json.Unmarshal(content, &suite); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}
	return &suite, nil
}

// parseDistributedSQLTestFile parses a SQL distributed test file with annotations
func parseDistributedSQLTestFile(content string) (*DistributedTestSuite, error) {
	suite := &DistributedTestSuite{
		Name:      "distributed_sql_tests",
		TestCases: []DistributedTestCase{},
	}

	lines := strings.Split(content, "\n")
	var currentTest *DistributedTestCase
	var sqlBuilder strings.Builder

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		
		if strings.HasPrefix(trimmed, "-- @test") {
			// Save previous test if exists
			if currentTest != nil && sqlBuilder.Len() > 0 {
				currentTest.SQL = strings.TrimSpace(sqlBuilder.String())
				suite.TestCases = append(suite.TestCases, *currentTest)
			}
			
			// Start new test
			currentTest = &DistributedTestCase{
				TestCase: core.TestCase{
					Expected: core.TestExpectation{},
					Trace:    core.TestTraceConfig{},
				},
				Distributed: DistributedConfig{},
			}
			sqlBuilder.Reset()
			
			// Parse test name
			if parts := strings.SplitN(trimmed, "name=", 2); len(parts) == 2 {
				currentTest.Name = strings.TrimSpace(parts[1])
			}
		} else if strings.HasPrefix(trimmed, "-- @") && currentTest != nil {
			// Parse distributed-specific annotations
			if strings.HasPrefix(trimmed, "-- @workers") {
				if parts := strings.SplitN(trimmed, " ", 2); len(parts) == 2 {
					fmt.Sscanf(parts[1], "%d", &currentTest.Distributed.Workers)
				}
			} else if strings.HasPrefix(trimmed, "-- @data_distribution") {
				if parts := strings.SplitN(trimmed, " ", 2); len(parts) == 2 {
					currentTest.Distributed.DataDistribution = strings.TrimSpace(parts[1])
				}
			} else if strings.HasPrefix(trimmed, "-- @expected_fragments") {
				if parts := strings.SplitN(trimmed, " ", 2); len(parts) == 2 {
					fmt.Sscanf(parts[1], "%d", &currentTest.Distributed.ExpectedFragments)
				}
			} else if strings.HasPrefix(trimmed, "-- @expected_workers") {
				if parts := strings.SplitN(trimmed, " ", 2); len(parts) == 2 {
					fmt.Sscanf(parts[1], "%d", &currentTest.Distributed.ExpectedWorkers)
				}
			} else if strings.HasPrefix(trimmed, "-- @network_optimization") {
				currentTest.Distributed.NetworkOptimization = true
			} else if strings.HasPrefix(trimmed, "-- @verify_partial_aggs") {
				currentTest.Distributed.VerifyPartialAggs = true
			} else {
				// Parse standard test annotations (reuse existing logic)
				parseSQLAnnotation(trimmed, &currentTest.TestCase)
			}
		} else if !strings.HasPrefix(trimmed, "--") && currentTest != nil {
			// Collect SQL lines
			if sqlBuilder.Len() > 0 {
				sqlBuilder.WriteString("\n")
			}
			sqlBuilder.WriteString(line)
		}
	}

	// Save last test
	if currentTest != nil && sqlBuilder.Len() > 0 {
		currentTest.SQL = strings.TrimSpace(sqlBuilder.String())
		suite.TestCases = append(suite.TestCases, *currentTest)
	}

	return suite, nil
}

// Helper methods

// applyTraceConfig applies trace configuration for the test
func (r *DistributedTestRunner) applyTraceConfig(config core.TestTraceConfig) {
	r.tracer.SetLevel(config.Level)
	// TODO: Apply component filtering when available
}

// runSetupQueries runs setup queries before the test
func (r *DistributedTestRunner) runSetupQueries(queries []string) error {
	for _, query := range queries {
		if _, err := r.engine.Execute(query); err != nil {
			return fmt.Errorf("setup query failed: %v", err)
		}
	}
	return nil
}

// runCleanupQueries runs cleanup queries after the test
func (r *DistributedTestRunner) runCleanupQueries(queries []string) error {
	for _, query := range queries {
		if _, err := r.engine.Execute(query); err != nil {
			return fmt.Errorf("cleanup query failed: %v", err)
		}
	}
	return nil
}

// validateResult validates the query result against expectations
func (r *DistributedTestRunner) validateResult(result *core.QueryResult, expected core.TestExpectation) []string {
	var errors []string
	
	// Validate row count
	if expected.RowCount != nil && len(result.Rows) != *expected.RowCount {
		errors = append(errors, fmt.Sprintf("expected %d rows, got %d", *expected.RowCount, len(result.Rows)))
	}
	
	// Validate columns
	if len(expected.Columns) > 0 {
		if !reflect.DeepEqual(result.Columns, expected.Columns) {
			errors = append(errors, fmt.Sprintf("expected columns %v, got %v", expected.Columns, result.Columns))
		}
	}
	
	// TODO: Validate data content when specified
	
	return errors
}


// parseSQLAnnotation parses SQL annotations for test configuration
func parseSQLAnnotation(line string, testCase *core.TestCase) {
	line = strings.TrimPrefix(line, "-- @")
	
	if strings.HasPrefix(line, "description ") {
		testCase.Description = strings.TrimSpace(strings.TrimPrefix(line, "description "))
	} else if strings.HasPrefix(line, "expect_rows ") {
		var count int
		fmt.Sscanf(strings.TrimPrefix(line, "expect_rows "), "%d", &count)
		testCase.Expected.RowCount = &count
	} else if strings.HasPrefix(line, "expect_columns ") {
		colsStr := strings.TrimPrefix(line, "expect_columns ")
		testCase.Expected.Columns = strings.Split(colsStr, ",")
	} else if strings.HasPrefix(line, "expect_error ") {
		errorStr := strings.TrimPrefix(line, "expect_error ")
		testCase.Expected.Error = &errorStr
	} else if strings.HasPrefix(line, "trace_level ") {
		levelStr := strings.TrimPrefix(line, "trace_level ")
		testCase.Trace.Level = ParseTraceLevel(levelStr)
	} else if strings.HasPrefix(line, "trace_components ") {
		compsStr := strings.TrimPrefix(line, "trace_components ")
		compStrs := strings.Split(compsStr, ",")
		for _, compStr := range compStrs {
			if comp := ParseTraceComponent(strings.TrimSpace(compStr)); comp != "" {
				testCase.Trace.Components = append(testCase.Trace.Components, comp)
			}
		}
	} else if strings.HasPrefix(line, "timeout ") {
		timeoutStr := strings.TrimPrefix(line, "timeout ")
		if duration, err := time.ParseDuration(timeoutStr); err == nil {
			testCase.Timeout = duration
		}
	} else if strings.HasPrefix(line, "tags ") {
		tagsStr := strings.TrimPrefix(line, "tags ")
		testCase.Tags = strings.Split(tagsStr, ",")
		for i := range testCase.Tags {
			testCase.Tags[i] = strings.TrimSpace(testCase.Tags[i])
		}
	} else if strings.HasPrefix(line, "performance max_duration=") {
		perfStr := strings.TrimPrefix(line, "performance max_duration=")
		if duration, err := time.ParseDuration(perfStr); err == nil {
			if testCase.Expected.Performance == nil {
				testCase.Expected.Performance = &core.PerformanceExpectation{}
			}
			testCase.Expected.Performance.MaxDuration = duration
		}
	}
}

// ParseTraceLevel parses a string into a TraceLevel
func ParseTraceLevel(level string) core.TraceLevel {
	switch strings.ToUpper(level) {
	case "OFF":
		return core.TraceLevelOff
	case "ERROR":
		return core.TraceLevelError
	case "WARN":
		return core.TraceLevelWarn
	case "INFO":
		return core.TraceLevelInfo
	case "DEBUG":
		return core.TraceLevelDebug
	case "VERBOSE":
		return core.TraceLevelVerbose
	default:
		return core.TraceLevelOff
	}
}

// ParseTraceComponent parses a string into a TraceComponent
func ParseTraceComponent(comp string) core.TraceComponent {
	switch strings.ToUpper(comp) {
	case "QUERY":
		return core.TraceComponentQuery
	case "PARSER":
		return core.TraceComponentParser
	case "OPTIMIZER":
		return core.TraceComponentOptimizer
	case "EXECUTION":
		return core.TraceComponentExecution
	case "CASE":
		return core.TraceComponentCase
	case "SORT":
		return core.TraceComponentSort
	case "JOIN":
		return core.TraceComponentJoin
	case "FILTER":
		return core.TraceComponentFilter
	case "AGGREGATE":
		return core.TraceComponentAggregate
	case "CACHE":
		return core.TraceComponentCache
	default:
		return ""
	}
}