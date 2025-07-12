package main

import (
	"bytedb/core"
	"bytedb/developer"
	"fmt"
	"log"
	"strings"
	"time"
)

func main() {
	fmt.Println("=== ByteDB Developer Experience Toolkit Demo ===")
	fmt.Println()

	// Create a query engine for demonstration
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Create developer tools
	explainer := developer.NewQueryExplainer(engine)
	inspector := developer.NewQueryInspector(engine)
	debugContext := developer.NewDebugContext(engine)

	// Demo SQL queries
	queries := []string{
		"SELECT * FROM employees",
		"SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
		"SELECT e.name, e.salary, d.budget FROM employees e JOIN departments d ON e.department = d.name WHERE e.salary > 70000",
		"SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
	}

	for i, sql := range queries {
		fmt.Printf("=== Demo Query %d ===\n", i+1)
		fmt.Printf("SQL: %s\n\n", sql)

		// 1. Generate Query Explanation Plan
		fmt.Println("📋 QUERY EXPLANATION PLAN:")
		fmt.Println("─────────────────────────────")
		explainPlan, err := explainer.Explain(sql)
		if err != nil {
			log.Printf("Explain error: %v\n", err)
		} else {
			fmt.Print(explainPlan.FormatText())
		}
		fmt.Println()

		// 2. Perform Query Inspection
		fmt.Println("🔍 QUERY INSPECTION ANALYSIS:")
		fmt.Println("─────────────────────────────")
		inspection, err := inspector.InspectQuery(sql)
		if err != nil {
			log.Printf("Inspection error: %v\n", err)
		} else {
			// Display static analysis
			fmt.Printf("Query Type: %s\n", inspection.StaticAnalysis.QueryType)
			fmt.Printf("Has Aggregates: %v\n", inspection.StaticAnalysis.HasAggregates)
			fmt.Printf("Has Joins: %v\n", inspection.StaticAnalysis.HasJoins)
			fmt.Printf("Has Subqueries: %v\n", inspection.StaticAnalysis.HasSubqueries)
			fmt.Printf("Uses SELECT *: %v\n", inspection.StaticAnalysis.UsesSelectStar)
			fmt.Printf("Readonly Operations: %v\n", inspection.StaticAnalysis.ReadonlyOperations)

			// Show potential issues
			if len(inspection.StaticAnalysis.PotentialIssues) > 0 {
				fmt.Println("\n⚠️  POTENTIAL ISSUES:")
				for _, issue := range inspection.StaticAnalysis.PotentialIssues {
					fmt.Printf("  - %s (%s): %s\n", issue.Type, issue.Severity, issue.Description)
					fmt.Printf("    Suggestion: %s\n", issue.Suggestion)
				}
			}

			// Show code smells
			if len(inspection.StaticAnalysis.CodeSmells) > 0 {
				fmt.Println("\n🔍 CODE SMELLS:")
				for _, smell := range inspection.StaticAnalysis.CodeSmells {
					fmt.Printf("  - %s: %s\n", smell.Type, smell.Description)
					fmt.Printf("    Impact: %s\n", smell.Impact)
					fmt.Printf("    Refactoring: %s\n", smell.Refactoring)
				}
			}

			// Show complexity analysis
			fmt.Printf("\n📊 COMPLEXITY ANALYSIS:\n")
			fmt.Printf("Overall Complexity: %s (Score: %.1f)\n", 
				inspection.ComplexityAnalysis.OverallComplexity, 
				inspection.ComplexityAnalysis.ComplexityScore)
			fmt.Printf("Join Complexity: %d\n", inspection.ComplexityAnalysis.JoinComplexity)
			fmt.Printf("Subquery Depth: %d\n", inspection.ComplexityAnalysis.SubqueryDepth)

			// Show performance predictions
			fmt.Printf("\n⚡ PERFORMANCE PREDICTIONS:\n")
			fmt.Printf("Estimated Duration: %v\n", inspection.Predictions.EstimatedDuration)
			fmt.Printf("Estimated Memory: %.2f MB\n", inspection.Predictions.EstimatedMemoryMB)
			fmt.Printf("Estimated I/O Ops: %d\n", inspection.Predictions.EstimatedIOOps)
			fmt.Printf("Performance Class: %s\n", inspection.Predictions.PerformanceClass)

			// Show risk factors
			if len(inspection.Predictions.RiskFactors) > 0 {
				fmt.Println("\n⚠️  RISK FACTORS:")
				for _, risk := range inspection.Predictions.RiskFactors {
					fmt.Printf("  - %s (%.0f%% probability): %s\n", 
						risk.Type, risk.Probability*100, risk.Description)
					fmt.Printf("    Mitigation: %s\n", risk.Mitigation)
				}
			}

			fmt.Printf("\nInspection completed in: %v\n", inspection.InspectionTime)
		}
		fmt.Println()

		// 3. Performance Profiling Demo
		fmt.Println("📈 PERFORMANCE PROFILING:")
		fmt.Println("─────────────────────────")
		queryID := fmt.Sprintf("demo-query-%d", i+1)
		
		// Start profiling
		debugContext.StartProfiling(queryID, sql)
		
		// Simulate query execution (replace with actual execution)
		time.Sleep(50 * time.Millisecond) // Simulate planning
		time.Sleep(150 * time.Millisecond) // Simulate execution
		
		// Stop profiling and get results
		profile := debugContext.StopProfiling(queryID)
		if profile != nil {
			fmt.Print(profile.FormatSummary())
		}
		
		fmt.Println("\n" + strings.Repeat("=", 60) + "\n")
	}

	fmt.Println("🎉 Developer Experience Toolkit Demo Completed!")
	fmt.Println()
	fmt.Println("Features demonstrated:")
	fmt.Println("✅ Query Explanation Plans with cost analysis")
	fmt.Println("✅ Comprehensive Query Inspection and static analysis") 
	fmt.Println("✅ Interactive Debugging and performance profiling")
	fmt.Println("✅ Performance predictions and risk analysis")
	fmt.Println("✅ Code smell detection and optimization recommendations")
	fmt.Println("✅ Complexity analysis and bottleneck identification")
	fmt.Println()
	fmt.Println("💡 These tools help developers:")
	fmt.Println("   • Understand query execution plans and costs")
	fmt.Println("   • Identify performance issues before they impact production")
	fmt.Println("   • Get actionable optimization recommendations")
	fmt.Println("   • Debug complex queries with detailed analysis")
	fmt.Println("   • Validate query patterns and detect code smells")
	fmt.Println()
	fmt.Println("🚀 Ready to enhance your ByteDB development experience!")
}