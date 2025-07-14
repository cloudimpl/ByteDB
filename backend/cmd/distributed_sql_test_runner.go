package main

import (
	"bytedb/core"
	"bytedb/distributed/testing"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	// Command-line flags
	var (
		dataPath       = flag.String("data", "./data", "Path to the data directory")
		testFile       = flag.String("file", "", "Path to the distributed SQL test file")
		testDir        = flag.String("dir", "", "Path to directory containing distributed SQL test files")
		verbose        = flag.Bool("verbose", false, "Enable verbose output")
		tags           = flag.String("tags", "", "Comma-separated list of tags to filter tests")
		workers        = flag.Int("workers", 3, "Number of worker nodes to use")
		transport      = flag.String("transport", "memory", "Transport type: memory, grpc, http")
		traceLevel     = flag.String("trace-level", "", "Override trace level for all tests")
		traceComps     = flag.String("trace-components", "", "Override trace components for all tests")
		timeout        = flag.String("timeout", "", "Override timeout for all tests")
		// reportFormat   = flag.String("format", "text", "Report format: text, json")
	)

	flag.Parse()

	if *testFile == "" && *testDir == "" {
		fmt.Println("Error: Either -file or -dir must be specified")
		flag.Usage()
		os.Exit(1)
	}

	// Create distributed test runner
	runner := testing.NewDistributedTestRunner(*dataPath)
	
	// Setup distributed cluster
	fmt.Printf("🚀 Setting up distributed cluster with %d workers...\n", *workers)
	if err := runner.SetupCluster(*workers, *transport); err != nil {
		log.Fatalf("Failed to setup cluster: %v", err)
	}
	defer func() {
		if err := runner.TeardownCluster(); err != nil {
			log.Printf("Failed to teardown cluster: %v", err)
		}
	}()

	// Collect test files
	var testFiles []string
	if *testFile != "" {
		testFiles = append(testFiles, *testFile)
	} else if *testDir != "" {
		entries, err := os.ReadDir(*testDir)
		if err != nil {
			log.Fatalf("Failed to read test directory: %v", err)
		}
		
		for _, entry := range entries {
			if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".sql") || 
				strings.HasSuffix(entry.Name(), ".json")) && 
				strings.Contains(entry.Name(), "distributed") {
				testFiles = append(testFiles, filepath.Join(*testDir, entry.Name()))
			}
		}
	}

	if len(testFiles) == 0 {
		fmt.Println("No distributed test files found")
		os.Exit(1)
	}

	// Process tags filter
	var tagFilter []string
	if *tags != "" {
		tagFilter = strings.Split(*tags, ",")
		for i := range tagFilter {
			tagFilter[i] = strings.TrimSpace(tagFilter[i])
		}
	}

	// Run tests
	totalTests := 0
	passedTests := 0
	failedTests := 0
	skippedTests := 0
	startTime := time.Now()

	fmt.Printf("\n📊 Running Distributed SQL Tests\n")
	fmt.Printf("================================\n")

	for _, file := range testFiles {
		fmt.Printf("\n📁 Test File: %s\n", filepath.Base(file))
		
		// Parse test file
		suite, err := testing.ParseDistributedTestFile(file)
		if err != nil {
			log.Printf("Failed to parse test file %s: %v", file, err)
			continue
		}

		if suite.Description != "" {
			fmt.Printf("   Description: %s\n", suite.Description)
		}

		// Run test cases
		for _, testCase := range suite.TestCases {
			// Apply tag filter
			if len(tagFilter) > 0 && !hasMatchingTag(testCase.Tags, tagFilter) {
				skippedTests++
				continue
			}

			totalTests++
			
			// Override trace settings if specified
			if *traceLevel != "" {
				testCase.Trace.Level = testing.ParseTraceLevel(*traceLevel)
			}
			if *traceComps != "" {
				compStrs := strings.Split(*traceComps, ",")
				testCase.Trace.Components = []core.TraceComponent{}
				for _, compStr := range compStrs {
					if comp := testing.ParseTraceComponent(strings.TrimSpace(compStr)); comp != "" {
						testCase.Trace.Components = append(testCase.Trace.Components, comp)
					}
				}
			}
			if *timeout != "" {
				duration, err := time.ParseDuration(*timeout)
				if err == nil {
					testCase.Timeout = duration
				}
			}

			// Override distributed settings if needed
			if testCase.Distributed.Workers == 0 {
				testCase.Distributed.Workers = *workers
			}
			if testCase.Distributed.TransportType == "" {
				testCase.Distributed.TransportType = *transport
			}

			fmt.Printf("\n   🧪 Test: %s", testCase.Name)
			if *verbose && testCase.Description != "" {
				fmt.Printf("\n      Description: %s", testCase.Description)
			}
			
			// Run the test
			result := runner.RunDistributedTest(&testCase)
			
			if result.Success {
				passedTests++
				fmt.Printf(" ✅ PASSED")
				if *verbose {
					fmt.Printf(" (%d rows in %v)", result.RowCount, result.Duration)
					
					// Show distributed execution stats
					if workersUsed, ok := result.Metadata["workers_used"].(int); ok {
						fmt.Printf("\n      Workers Used: %d", workersUsed)
					}
					if fragments, ok := result.Metadata["total_fragments"].(int); ok {
						fmt.Printf("\n      Fragments: %d", fragments)
					}
					if bytesTransferred, ok := result.Metadata["bytes_transferred"].(int64); ok && bytesTransferred > 0 {
						fmt.Printf("\n      Network Transfer: %s", formatBytes(bytesTransferred))
					}
					if optimized, ok := result.Metadata["optimization_applied"].(bool); ok && optimized {
						fmt.Printf("\n      ✨ Network Optimization Applied")
					}
				}
			} else {
				failedTests++
				fmt.Printf(" ❌ FAILED")
				if result.Error != "" {
					fmt.Printf("\n      Error: %s", result.Error)
				}
			}
			
			if *verbose {
				// Show trace messages if any
				if len(result.TraceMessages) > 0 {
					fmt.Printf("\n      📍 Trace Messages:")
					for _, msg := range result.TraceMessages {
						fmt.Printf("\n         %s", msg)
					}
				}
			}
		}
	}

	// Print summary
	duration := time.Since(startTime)
	fmt.Printf("\n\n📈 Test Summary\n")
	fmt.Printf("===============\n")
	fmt.Printf("Total Tests:    %d\n", totalTests)
	fmt.Printf("Passed:         %d (%.1f%%)\n", passedTests, float64(passedTests)/float64(totalTests)*100)
	fmt.Printf("Failed:         %d (%.1f%%)\n", failedTests, float64(failedTests)/float64(totalTests)*100)
	if skippedTests > 0 {
		fmt.Printf("Skipped:        %d\n", skippedTests)
	}
	fmt.Printf("Duration:       %v\n", duration)
	fmt.Printf("Workers:        %d\n", *workers)
	fmt.Printf("Transport:      %s\n", *transport)

	// Exit with appropriate code
	if failedTests > 0 {
		os.Exit(1)
	}
}

// hasMatchingTag checks if any of the test tags match the filter
func hasMatchingTag(testTags []string, filterTags []string) bool {
	for _, testTag := range testTags {
		for _, filterTag := range filterTags {
			if testTag == filterTag {
				return true
			}
		}
	}
	return false
}

// formatBytes formats byte count in human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}