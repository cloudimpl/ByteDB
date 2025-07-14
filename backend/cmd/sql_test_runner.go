package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"bytedb/core"
)

func main() {
	var (
		dataPath    = flag.String("data", "./data", "Path to the data directory")
		testFile    = flag.String("file", "", "Path to the SQL test file")
		testDir     = flag.String("dir", "", "Path to directory containing SQL test files")
		verbose     = flag.Bool("verbose", false, "Enable verbose output")
		tags        = flag.String("tags", "", "Comma-separated list of tags to filter tests")
		traceLevel  = flag.String("trace-level", "", "Override trace level for all tests")
		traceComps  = flag.String("trace-components", "", "Override trace components for all tests")
		timeout     = flag.String("timeout", "", "Override timeout for all tests")
	)
	flag.Parse()

	if *testFile == "" && *testDir == "" {
		fmt.Println("Usage: sql_test_runner -file <test.sql> OR -dir <test_directory>")
		fmt.Println("Examples:")
		fmt.Println("  sql_test_runner -file tests/basic.sql")
		fmt.Println("  sql_test_runner -dir tests/ -verbose")
		fmt.Println("  sql_test_runner -file tests/case_expressions.sql -trace-level DEBUG")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Create test runner
	runner, err := core.NewTestRunner(*dataPath)
	if err != nil {
		log.Fatalf("Failed to create test runner: %v", err)
	}
	defer runner.Close()

	runner.SetVerbose(*verbose)

	var testFiles []string
	if *testFile != "" {
		testFiles = append(testFiles, *testFile)
	} else {
		testFiles, err = findTestFiles(*testDir)
		if err != nil {
			log.Fatalf("Failed to find test files: %v", err)
		}
	}

	fmt.Printf("Found %d test file(s)\n", len(testFiles))

	var allResults []core.TestResult
	totalPassed := 0
	totalFailed := 0

	for _, file := range testFiles {
		fmt.Printf("\n=== Processing: %s ===\n", file)

		var suite *core.TestSuite
		if strings.HasSuffix(file, ".json") {
			suite, err = runner.LoadTestSuiteFromJSON(file)
		} else {
			suite, err = runner.LoadTestSuiteFromSQL(file)
		}

		if err != nil {
			fmt.Printf("Error loading test suite: %v\n", err)
			continue
		}

		// Apply command line overrides
		applyOverrides(suite, *traceLevel, *traceComps, *timeout, *tags)

		results, err := runner.RunTestSuite(suite)
		if err != nil {
			fmt.Printf("Error running test suite: %v\n", err)
			continue
		}

		// Count results
		for _, result := range results {
			if result.Status == core.TestStatusPass {
				totalPassed++
			} else {
				totalFailed++
			}
		}

		allResults = append(allResults, results...)
	}

	// Print overall summary
	fmt.Printf("\n=== OVERALL SUMMARY ===\n")
	fmt.Printf("Total tests: %d\n", len(allResults))
	fmt.Printf("Passed: %d\n", totalPassed)
	fmt.Printf("Failed: %d\n", totalFailed)

	if totalFailed > 0 {
		os.Exit(1)
	}
}

func findTestFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (strings.HasSuffix(path, ".sql") || strings.HasSuffix(path, ".json")) {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func applyOverrides(suite *core.TestSuite, traceLevel, traceComps, timeout, tags string) {
	// Parse trace level
	var level core.TraceLevel
	if traceLevel != "" {
		switch strings.ToUpper(traceLevel) {
		case "OFF":
			level = core.TraceLevelOff
		case "ERROR":
			level = core.TraceLevelError
		case "WARN":
			level = core.TraceLevelWarn
		case "INFO":
			level = core.TraceLevelInfo
		case "DEBUG":
			level = core.TraceLevelDebug
		case "VERBOSE":
			level = core.TraceLevelVerbose
		}
	}

	// Parse trace components
	var components []core.TraceComponent
	if traceComps != "" {
		compNames := strings.Split(traceComps, ",")
		for _, name := range compNames {
			switch strings.ToUpper(strings.TrimSpace(name)) {
			case "QUERY":
				components = append(components, core.TraceComponentQuery)
			case "PARSER":
				components = append(components, core.TraceComponentParser)
			case "OPTIMIZER":
				components = append(components, core.TraceComponentOptimizer)
			case "EXECUTION":
				components = append(components, core.TraceComponentExecution)
			case "CASE":
				components = append(components, core.TraceComponentCase)
			case "SORT":
				components = append(components, core.TraceComponentSort)
			case "JOIN":
				components = append(components, core.TraceComponentJoin)
			case "FILTER":
				components = append(components, core.TraceComponentFilter)
			case "AGGREGATE":
				components = append(components, core.TraceComponentAggregate)
			case "CACHE":
				components = append(components, core.TraceComponentCache)
			}
		}
	}

	// Filter tests by tags
	var filterTags []string
	if tags != "" {
		filterTags = strings.Split(tags, ",")
		for i := range filterTags {
			filterTags[i] = strings.TrimSpace(filterTags[i])
		}
	}

	// Apply overrides to all test cases
	var filteredTests []core.TestCase
	for _, test := range suite.TestCases {
		// Check tag filter
		if len(filterTags) > 0 {
			hasMatchingTag := false
			for _, filterTag := range filterTags {
				for _, testTag := range test.Tags {
					if testTag == filterTag {
						hasMatchingTag = true
						break
					}
				}
				if hasMatchingTag {
					break
				}
			}
			if !hasMatchingTag {
				continue // Skip this test
			}
		}

		// Apply trace overrides
		if traceLevel != "" {
			test.Trace.Level = level
		}
		if len(components) > 0 {
			test.Trace.Components = components
		}

		filteredTests = append(filteredTests, test)
	}

	suite.TestCases = filteredTests
}