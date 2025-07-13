package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// TestCase represents a single SQL test case
type TestCase struct {
	Name        string                 `json:"name"`
	SQL         string                 `json:"sql"`
	Description string                 `json:"description,omitempty"`
	Expected    TestExpectation        `json:"expected"`
	Trace       TestTraceConfig        `json:"trace,omitempty"`
	Setup       []string               `json:"setup,omitempty"`
	Cleanup     []string               `json:"cleanup,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
	Timeout     time.Duration          `json:"timeout,omitempty"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// TestExpectation defines what to expect from the test
type TestExpectation struct {
	RowCount    *int                     `json:"row_count,omitempty"`
	Columns     []string                 `json:"columns,omitempty"`
	Data        []map[string]interface{} `json:"data,omitempty"`
	Error       *string                  `json:"error,omitempty"`
	Performance *PerformanceExpectation  `json:"performance,omitempty"`
	Traces      []TraceExpectation       `json:"traces,omitempty"`
}

// PerformanceExpectation defines performance constraints
type PerformanceExpectation struct {
	MaxDuration time.Duration `json:"max_duration,omitempty"`
	MinRows     int           `json:"min_rows,omitempty"`
	MaxRows     int           `json:"max_rows,omitempty"`
}

// TraceExpectation defines expected trace entries
type TraceExpectation struct {
	Component TraceComponent `json:"component"`
	Level     TraceLevel     `json:"level"`
	Message   string         `json:"message"`
	Contains  []string       `json:"contains,omitempty"`
}

// TestTraceConfig configures tracing for the test
type TestTraceConfig struct {
	Level      TraceLevel       `json:"level"`
	Components []TraceComponent `json:"components"`
	ClearAfter bool             `json:"clear_after"`
}

// TestSuite represents a collection of test cases
type TestSuite struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Setup       []string               `json:"setup,omitempty"`
	Cleanup     []string               `json:"cleanup,omitempty"`
	TestCases   []TestCase             `json:"test_cases"`
	Config      TestSuiteConfig        `json:"config,omitempty"`
	DataPath    string                 `json:"data_path,omitempty"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// TestSuiteConfig contains suite-level configuration
type TestSuiteConfig struct {
	Parallel        bool          `json:"parallel,omitempty"`
	Timeout         time.Duration `json:"timeout,omitempty"`
	StopOnFirstFail bool          `json:"stop_on_first_fail,omitempty"`
	Verbose         bool          `json:"verbose,omitempty"`
}

// TestResult represents the result of a single test
type TestResult struct {
	TestCase     TestCase      `json:"test_case"`
	Status       TestStatus    `json:"status"`
	Duration     time.Duration `json:"duration"`
	ActualRows   int           `json:"actual_rows"`
	ActualData   []Row         `json:"actual_data,omitempty"`
	Error        string        `json:"error,omitempty"`
	TraceEntries []TraceEntry  `json:"trace_entries,omitempty"`
	Message      string        `json:"message,omitempty"`
}

// TestStatus represents the status of a test
type TestStatus string

const (
	TestStatusPass    TestStatus = "PASS"
	TestStatusFail    TestStatus = "FAIL"
	TestStatusSkip    TestStatus = "SKIP"
	TestStatusTimeout TestStatus = "TIMEOUT"
	TestStatusError   TestStatus = "ERROR"
)

// TestRunner executes SQL test suites
type TestRunner struct {
	engine   *QueryEngine
	tracer   *Tracer
	verbose  bool
	parallel bool
}

// NewTestRunner creates a new test runner
func NewTestRunner(dataPath string) (*TestRunner, error) {
	engine := NewQueryEngine(dataPath)

	return &TestRunner{
		engine:  engine,
		tracer:  GetTracer(),
		verbose: false,
	}, nil
}

// LoadTestSuiteFromSQL loads a test suite from a SQL file with special comments
func (tr *TestRunner) LoadTestSuiteFromSQL(filePath string) (*TestSuite, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	return tr.parseTestSuiteFromSQL(string(content), filepath.Base(filePath))
}

// LoadTestSuiteFromJSON loads a test suite from a JSON file
func (tr *TestRunner) LoadTestSuiteFromJSON(filePath string) (*TestSuite, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
	}

	var suite TestSuite
	if err := json.Unmarshal(content, &suite); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &suite, nil
}

// parseTestSuiteFromSQL parses SQL file with special comment syntax
func (tr *TestRunner) parseTestSuiteFromSQL(content, fileName string) (*TestSuite, error) {
	suite := &TestSuite{
		Name:      strings.TrimSuffix(fileName, filepath.Ext(fileName)),
		TestCases: []TestCase{},
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	var currentTest *TestCase
	var sqlLines []string
	var inTest bool

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and regular comments
		if line == "" || (strings.HasPrefix(line, "--") && !strings.HasPrefix(line, "-- @")) {
			continue
		}

		// Test case start
		if strings.HasPrefix(line, "-- @test") {
			// Save previous test if exists
			if currentTest != nil {
				currentTest.SQL = strings.TrimSpace(strings.Join(sqlLines, "\n"))
				suite.TestCases = append(suite.TestCases, *currentTest)
			}

			// Start new test
			currentTest = &TestCase{
				Name:     parseTestAttribute(line, "name"),
				Expected: TestExpectation{},
				Trace:    TestTraceConfig{},
			}
			sqlLines = []string{}
			inTest = true
			continue
		}

		// Test metadata
		if currentTest != nil && strings.HasPrefix(line, "-- @") {
			tr.parseTestMetadata(line, currentTest)
			continue
		}

		// SQL content
		if inTest && currentTest != nil {
			sqlLines = append(sqlLines, line)
		}
	}

	// Save last test
	if currentTest != nil {
		currentTest.SQL = strings.TrimSpace(strings.Join(sqlLines, "\n"))
		suite.TestCases = append(suite.TestCases, *currentTest)
	}

	return suite, scanner.Err()
}

// parseTestMetadata parses test metadata from comments
func (tr *TestRunner) parseTestMetadata(line string, test *TestCase) {
	switch {
	case strings.HasPrefix(line, "-- @description"):
		test.Description = parseTestAttribute(line, "description")
	case strings.HasPrefix(line, "-- @expect_rows"):
		if count := parseTestAttributeInt(line, "expect_rows"); count != nil {
			test.Expected.RowCount = count
		}
	case strings.HasPrefix(line, "-- @expect_columns"):
		columns := parseTestAttributeList(line, "expect_columns")
		test.Expected.Columns = columns
	case strings.HasPrefix(line, "-- @expect_error"):
		error := parseTestAttribute(line, "expect_error")
		test.Expected.Error = &error
	case strings.HasPrefix(line, "-- @trace_level"):
		level := parseTraceLevel(parseTestAttribute(line, "trace_level"))
		test.Trace.Level = level
	case strings.HasPrefix(line, "-- @trace_components"):
		components := parseTraceComponents(parseTestAttributeList(line, "trace_components"))
		test.Trace.Components = components
	case strings.HasPrefix(line, "-- @tags"):
		test.Tags = parseTestAttributeList(line, "tags")
	case strings.HasPrefix(line, "-- @timeout"):
		if timeout := parseTestAttributeDuration(line, "timeout"); timeout != 0 {
			test.Timeout = timeout
		}
	}
}

// RunTestSuite executes all tests in a suite
func (tr *TestRunner) RunTestSuite(suite *TestSuite) ([]TestResult, error) {
	fmt.Printf("Running test suite: %s\n", suite.Name)
	if suite.Description != "" {
		fmt.Printf("Description: %s\n", suite.Description)
	}

	var results []TestResult

	// Run setup
	if err := tr.runSQLStatements(suite.Setup); err != nil {
		return nil, fmt.Errorf("suite setup failed: %w", err)
	}

	// Run tests
	for i, testCase := range suite.TestCases {
		fmt.Printf("\n[%d/%d] Running test: %s\n", i+1, len(suite.TestCases), testCase.Name)

		result := tr.runSingleTest(testCase)
		results = append(results, result)

		if tr.verbose {
			tr.printTestResult(result)
		}

		if suite.Config.StopOnFirstFail && result.Status == TestStatusFail {
			fmt.Printf("Stopping test suite due to failure in test: %s\n", testCase.Name)
			break
		}
	}

	// Run cleanup
	if err := tr.runSQLStatements(suite.Cleanup); err != nil {
		fmt.Printf("Warning: suite cleanup failed: %v\n", err)
	}

	tr.printSummary(results)
	return results, nil
}

// runSingleTest executes a single test case
func (tr *TestRunner) runSingleTest(test TestCase) TestResult {
	result := TestResult{
		TestCase: test,
		Status:   TestStatusPass,
	}

	startTime := time.Now()

	// Configure tracing
	if test.Trace.Level != TraceLevelOff {
		tr.tracer.SetLevel(test.Trace.Level)
		for _, component := range test.Trace.Components {
			tr.tracer.EnableComponent(component)
		}
		if test.Trace.ClearAfter {
			defer tr.tracer.Clear()
		}
	}

	// Run setup
	if err := tr.runSQLStatements(test.Setup); err != nil {
		result.Status = TestStatusError
		result.Error = fmt.Sprintf("Test setup failed: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	// Execute the main SQL query
	queryResult, err := tr.engine.Execute(test.SQL)
	result.Duration = time.Since(startTime)

	// Handle timeout
	if test.Timeout > 0 && result.Duration > test.Timeout {
		result.Status = TestStatusTimeout
		result.Error = fmt.Sprintf("Test exceeded timeout of %v", test.Timeout)
		return result
	}

	// Handle query error
	if err != nil {
		if test.Expected.Error != nil {
			if strings.Contains(err.Error(), *test.Expected.Error) {
				result.Status = TestStatusPass
				result.Message = "Expected error occurred"
			} else {
				result.Status = TestStatusFail
				result.Error = fmt.Sprintf("Expected error containing '%s', got: %v", *test.Expected.Error, err)
			}
		} else {
			result.Status = TestStatusFail
			result.Error = fmt.Sprintf("Unexpected error: %v", err)
		}
		return result
	}

	// If we expected an error but didn't get one
	if test.Expected.Error != nil {
		result.Status = TestStatusFail
		result.Error = fmt.Sprintf("Expected error containing '%s', but query succeeded", *test.Expected.Error)
		return result
	}

	// Store actual results
	result.ActualRows = len(queryResult.Rows)
	result.ActualData = queryResult.Rows

	// Validate results
	if err := tr.validateResults(queryResult, test.Expected, &result); err != nil {
		result.Status = TestStatusFail
		result.Error = err.Error()
		return result
	}

	// Validate traces
	if len(test.Expected.Traces) > 0 {
		result.TraceEntries = tr.tracer.GetEntries()
		if err := tr.validateTraces(result.TraceEntries, test.Expected.Traces); err != nil {
			result.Status = TestStatusFail
			result.Error = fmt.Sprintf("Trace validation failed: %v", err)
			return result
		}
	}

	// Run cleanup
	if err := tr.runSQLStatements(test.Cleanup); err != nil {
		fmt.Printf("Warning: test cleanup failed: %v\n", err)
	}

	result.Status = TestStatusPass
	return result
}

// validateResults validates query results against expectations
func (tr *TestRunner) validateResults(actual *QueryResult, expected TestExpectation, result *TestResult) error {
	// Check row count
	if expected.RowCount != nil && len(actual.Rows) != *expected.RowCount {
		return fmt.Errorf("expected %d rows, got %d", *expected.RowCount, len(actual.Rows))
	}

	// Check columns
	if len(expected.Columns) > 0 {
		if len(actual.Columns) != len(expected.Columns) {
			return fmt.Errorf("expected %d columns, got %d", len(expected.Columns), len(actual.Columns))
		}
		for i, expectedCol := range expected.Columns {
			if i >= len(actual.Columns) || actual.Columns[i] != expectedCol {
				return fmt.Errorf("expected column %d to be '%s', got '%s'", i, expectedCol, actual.Columns[i])
			}
		}
	}

	// Check specific data if provided
	if len(expected.Data) > 0 {
		if len(actual.Rows) != len(expected.Data) {
			return fmt.Errorf("expected %d data rows, got %d", len(expected.Data), len(actual.Rows))
		}
		for i, expectedRow := range expected.Data {
			if !tr.compareRows(actual.Rows[i], expectedRow) {
				return fmt.Errorf("row %d does not match expected data", i)
			}
		}
	}

	// Check performance expectations
	if expected.Performance != nil {
		perf := expected.Performance
		if perf.MaxDuration > 0 && result.Duration > perf.MaxDuration {
			return fmt.Errorf("query took %v, expected max %v", result.Duration, perf.MaxDuration)
		}
		if perf.MinRows > 0 && len(actual.Rows) < perf.MinRows {
			return fmt.Errorf("query returned %d rows, expected min %d", len(actual.Rows), perf.MinRows)
		}
		if perf.MaxRows > 0 && len(actual.Rows) > perf.MaxRows {
			return fmt.Errorf("query returned %d rows, expected max %d", len(actual.Rows), perf.MaxRows)
		}
	}

	return nil
}

// validateTraces validates trace entries against expectations
func (tr *TestRunner) validateTraces(actual []TraceEntry, expected []TraceExpectation) error {
	for _, expectedTrace := range expected {
		found := false
		for _, actualTrace := range actual {
			if actualTrace.Component == expectedTrace.Component &&
				actualTrace.Level == expectedTrace.Level {

				if expectedTrace.Message != "" && !strings.Contains(actualTrace.Message, expectedTrace.Message) {
					continue
				}

				allContainsMatch := true
				for _, contains := range expectedTrace.Contains {
					if !strings.Contains(actualTrace.Message, contains) {
						allContainsMatch = false
						break
					}
				}

				if allContainsMatch {
					found = true
					break
				}
			}
		}

		if !found {
			return fmt.Errorf("expected trace entry not found: %s/%s - %s",
				expectedTrace.Component, expectedTrace.Level, expectedTrace.Message)
		}
	}
	return nil
}

// compareRows compares two rows for equality
func (tr *TestRunner) compareRows(actual Row, expected map[string]interface{}) bool {
	for key, expectedValue := range expected {
		actualValue, exists := actual[key]
		if !exists {
			return false
		}
		if !reflect.DeepEqual(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

// runSQLStatements executes a list of SQL statements
func (tr *TestRunner) runSQLStatements(statements []string) error {
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tr.engine.Execute(stmt); err != nil {
			return fmt.Errorf("failed to execute statement '%s': %w", stmt, err)
		}
	}
	return nil
}

// printTestResult prints the result of a single test
func (tr *TestRunner) printTestResult(result TestResult) {
	status := result.Status
	switch status {
	case TestStatusPass:
		fmt.Printf("  ✓ PASS - %s (%v)\n", result.TestCase.Name, result.Duration)
	case TestStatusFail:
		fmt.Printf("  ✗ FAIL - %s (%v): %s\n", result.TestCase.Name, result.Duration, result.Error)
	case TestStatusSkip:
		fmt.Printf("  ⚬ SKIP - %s: %s\n", result.TestCase.Name, result.Error)
	case TestStatusTimeout:
		fmt.Printf("  ⏱ TIMEOUT - %s (%v): %s\n", result.TestCase.Name, result.Duration, result.Error)
	case TestStatusError:
		fmt.Printf("  ✗ ERROR - %s: %s\n", result.TestCase.Name, result.Error)
	}

	if tr.verbose && result.TestCase.Description != "" {
		fmt.Printf("    Description: %s\n", result.TestCase.Description)
	}

	if tr.verbose && len(result.TraceEntries) > 0 {
		fmt.Printf("    Trace entries: %d\n", len(result.TraceEntries))
	}
}

// printSummary prints a summary of all test results
func (tr *TestRunner) printSummary(results []TestResult) {
	passed := 0
	failed := 0
	skipped := 0
	errors := 0
	timeouts := 0

	var totalDuration time.Duration

	for _, result := range results {
		totalDuration += result.Duration
		switch result.Status {
		case TestStatusPass:
			passed++
		case TestStatusFail:
			failed++
		case TestStatusSkip:
			skipped++
		case TestStatusTimeout:
			timeouts++
		case TestStatusError:
			errors++
		}
	}

	fmt.Printf("\n=== Test Summary ===\n")
	fmt.Printf("Total: %d tests\n", len(results))
	fmt.Printf("Passed: %d\n", passed)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Printf("Skipped: %d\n", skipped)
	fmt.Printf("Timeouts: %d\n", timeouts)
	fmt.Printf("Errors: %d\n", errors)
	fmt.Printf("Duration: %v\n", totalDuration)

	if failed > 0 || errors > 0 || timeouts > 0 {
		fmt.Printf("\nFailed tests:\n")
		for _, result := range results {
			if result.Status != TestStatusPass && result.Status != TestStatusSkip {
				fmt.Printf("  - %s: %s\n", result.TestCase.Name, result.Error)
			}
		}
	}
}

// SetVerbose enables/disables verbose output
func (tr *TestRunner) SetVerbose(verbose bool) {
	tr.verbose = verbose
}

// Close cleans up the test runner
func (tr *TestRunner) Close() error {
	if tr.engine != nil {
		tr.engine.Close()
	}
	return nil
}

// Helper functions for parsing test attributes

func parseTestAttribute(line, attribute string) string {
	re := regexp.MustCompile(fmt.Sprintf(`-- @%s\s+(.+)`, attribute))
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

func parseTestAttributeInt(line, attribute string) *int {
	value := parseTestAttribute(line, attribute)
	if value == "" {
		return nil
	}
	if i, err := fmt.Sscanf(value, "%d", new(int)); err == nil && i == 1 {
		var result int
		fmt.Sscanf(value, "%d", &result)
		return &result
	}
	return nil
}

func parseTestAttributeList(line, attribute string) []string {
	value := parseTestAttribute(line, attribute)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	var result []string
	for _, part := range parts {
		result = append(result, strings.TrimSpace(part))
	}
	return result
}

func parseTestAttributeDuration(line, attribute string) time.Duration {
	value := parseTestAttribute(line, attribute)
	if value == "" {
		return 0
	}
	if duration, err := time.ParseDuration(value); err == nil {
		return duration
	}
	return 0
}

func parseTraceLevel(value string) TraceLevel {
	switch strings.ToUpper(value) {
	case "OFF":
		return TraceLevelOff
	case "ERROR":
		return TraceLevelError
	case "WARN":
		return TraceLevelWarn
	case "INFO":
		return TraceLevelInfo
	case "DEBUG":
		return TraceLevelDebug
	case "VERBOSE":
		return TraceLevelVerbose
	default:
		return TraceLevelOff
	}
}

func parseTraceComponents(values []string) []TraceComponent {
	var components []TraceComponent
	for _, value := range values {
		switch strings.ToUpper(value) {
		case "QUERY":
			components = append(components, TraceComponentQuery)
		case "PARSER":
			components = append(components, TraceComponentParser)
		case "OPTIMIZER":
			components = append(components, TraceComponentOptimizer)
		case "EXECUTION":
			components = append(components, TraceComponentExecution)
		case "CASE":
			components = append(components, TraceComponentCase)
		case "SORT":
			components = append(components, TraceComponentSort)
		case "JOIN":
			components = append(components, TraceComponentJoin)
		case "FILTER":
			components = append(components, TraceComponentFilter)
		case "AGGREGATE":
			components = append(components, TraceComponentAggregate)
		case "CACHE":
			components = append(components, TraceComponentCache)
		}
	}
	return components
}
