package developer

import (
	"bytedb/core"
	"fmt"
	"sort"
	"strings"
	"time"
)

// DebugContext provides comprehensive debugging information for query execution
type DebugContext struct {
	engine        *core.QueryEngine
	traceEnabled  bool
	profileEnabled bool
	verboseMode   bool
	traces        []TraceEvent
	profiles      map[string]*ExecutionProfile
}

func NewDebugContext(engine *core.QueryEngine) *DebugContext {
	return &DebugContext{
		engine:       engine,
		traceEnabled: true,
		profileEnabled: true,
		verboseMode:  true,
		traces:       make([]TraceEvent, 0),
		profiles:     make(map[string]*ExecutionProfile),
	}
}

// TraceEvent represents a single execution trace event
type TraceEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"event_type"`
	Operation   string                 `json:"operation"`
	Duration    time.Duration          `json:"duration,omitempty"`
	Details     map[string]interface{} `json:"details"`
	StackTrace  []string               `json:"stack_trace,omitempty"`
	MemoryUsage int64                  `json:"memory_usage_bytes"`
	ThreadID    string                 `json:"thread_id"`
}

// ExecutionProfile contains detailed performance analysis
type ExecutionProfile struct {
	QueryID         string                    `json:"query_id"`
	SQL             string                    `json:"sql"`
	StartTime       time.Time                 `json:"start_time"`
	EndTime         time.Time                 `json:"end_time"`
	TotalDuration   time.Duration             `json:"total_duration"`
	Phases          []ExecutionPhase          `json:"phases"`
	ResourceUsage   *ResourceUsage            `json:"resource_usage"`
	PerformanceMetrics *PerformanceMetrics    `json:"performance_metrics"`
	Bottlenecks     []PerformanceBottleneck   `json:"bottlenecks"`
	Optimizations   []OptimizationOpportunity `json:"optimizations"`
	CacheAnalysis   *CacheAnalysis            `json:"cache_analysis"`
}

type ExecutionPhase struct {
	Name        string        `json:"name"`
	StartTime   time.Time     `json:"start_time"`
	Duration    time.Duration `json:"duration"`
	Percentage  float64       `json:"percentage"`
	RowsIn      int64         `json:"rows_in"`
	RowsOut     int64         `json:"rows_out"`
	BytesRead   int64         `json:"bytes_read"`
	BytesWritten int64        `json:"bytes_written"`
	Operations  int           `json:"operations"`
	Details     map[string]interface{} `json:"details"`
}

type ResourceUsage struct {
	PeakMemoryMB     float64           `json:"peak_memory_mb"`
	AverageMemoryMB  float64           `json:"average_memory_mb"`
	CPUTime          time.Duration     `json:"cpu_time"`
	IOTime           time.Duration     `json:"io_time"`
	NetworkTime      time.Duration     `json:"network_time"`
	DiskReads        int64             `json:"disk_reads"`
	DiskWrites       int64             `json:"disk_writes"`
	NetworkBytes     int64             `json:"network_bytes"`
	CacheHits        int64             `json:"cache_hits"`
	CacheMisses      int64             `json:"cache_misses"`
	ResourceTimeline []ResourceSample  `json:"resource_timeline"`
}

type ResourceSample struct {
	Timestamp  time.Time `json:"timestamp"`
	MemoryMB   float64   `json:"memory_mb"`
	CPUPercent float64   `json:"cpu_percent"`
}

type PerformanceMetrics struct {
	RowsPerSecond       float64  `json:"rows_per_second"`
	BytesPerSecond      float64  `json:"bytes_per_second"`
	OperationsPerSecond float64  `json:"operations_per_second"`
	CacheHitRate        float64  `json:"cache_hit_rate"`
	EfficiencyScore     float64  `json:"efficiency_score"`
	Selectivity         float64  `json:"selectivity"`
	ParallelismFactor   float64  `json:"parallelism_factor"`
}

type PerformanceBottleneck struct {
	Type        string        `json:"type"`
	Component   string        `json:"component"`
	Description string        `json:"description"`
	Impact      float64       `json:"impact_percentage"`
	Duration    time.Duration `json:"duration"`
	Suggestion  string        `json:"suggestion"`
	Severity    string        `json:"severity"`
}

type OptimizationOpportunity struct {
	Type            string  `json:"type"`
	Description     string  `json:"description"`
	EstimatedGain   float64 `json:"estimated_gain_percentage"`
	Difficulty      string  `json:"difficulty"`
	Recommendation  string  `json:"recommendation"`
	Priority        string  `json:"priority"`
}

type CacheAnalysis struct {
	TotalAccesses     int64   `json:"total_accesses"`
	Hits              int64   `json:"hits"`
	Misses            int64   `json:"misses"`
	HitRate           float64 `json:"hit_rate"`
	MissLatencyAvg    time.Duration `json:"miss_latency_avg"`
	HitLatencyAvg     time.Duration `json:"hit_latency_avg"`
	CacheSize         int64   `json:"cache_size_bytes"`
	CacheUtilization  float64 `json:"cache_utilization"`
	EvictionCount     int64   `json:"eviction_count"`
	CacheEffectiveness float64 `json:"cache_effectiveness"`
}

// QueryInspector provides detailed query analysis and inspection
type QueryInspector struct {
	debugContext *DebugContext
	explainer    *QueryExplainer
}

func NewQueryInspector(engine *core.QueryEngine) *QueryInspector {
	return &QueryInspector{
		debugContext: NewDebugContext(engine),
		explainer:    NewQueryExplainer(engine),
	}
}

// InspectQuery provides comprehensive query analysis
func (qi *QueryInspector) InspectQuery(sql string) (*QueryInspection, error) {
	startTime := time.Now()
	
	// Parse and analyze the query
	explainPlan, err := qi.explainer.Explain(sql)
	if err != nil {
		return nil, fmt.Errorf("explain failed: %v", err)
	}
	
	// Perform static analysis
	staticAnalysis := qi.performStaticAnalysis(sql, explainPlan.ParsedQuery)
	
	// Schema analysis
	schemaAnalysis := qi.analyzeSchema(explainPlan.ParsedQuery)
	
	// Query complexity analysis
	complexityAnalysis := qi.analyzeComplexity(explainPlan.ParsedQuery)
	
	// Performance predictions
	predictions := qi.generatePerformancePredictions(explainPlan)
	
	inspection := &QueryInspection{
		SQL:                sql,
		ExplainPlan:        explainPlan,
		StaticAnalysis:     staticAnalysis,
		SchemaAnalysis:     schemaAnalysis,
		ComplexityAnalysis: complexityAnalysis,
		Predictions:        predictions,
		InspectionTime:     time.Since(startTime),
		GeneratedAt:        startTime,
	}
	
	return inspection, nil
}

type QueryInspection struct {
	SQL                string                    `json:"sql"`
	ExplainPlan        *ExplainPlan             `json:"explain_plan"`
	StaticAnalysis     *StaticAnalysis          `json:"static_analysis"`
	SchemaAnalysis     *SchemaAnalysis          `json:"schema_analysis"`
	ComplexityAnalysis *ComplexityAnalysis      `json:"complexity_analysis"`
	Predictions        *PerformancePredictions  `json:"predictions"`
	InspectionTime     time.Duration            `json:"inspection_time"`
	GeneratedAt        time.Time                `json:"generated_at"`
}

type StaticAnalysis struct {
	QueryType           string                    `json:"query_type"`
	HasSubqueries       bool                      `json:"has_subqueries"`
	HasAggregates       bool                      `json:"has_aggregates"`
	HasJoins            bool                      `json:"has_joins"`
	HasWindowFunctions  bool                      `json:"has_window_functions"`
	UsesSelectStar      bool                      `json:"uses_select_star"`
	HasOrderBy          bool                      `json:"has_order_by"`
	HasGroupBy          bool                      `json:"has_group_by"`
	HasLimit            bool                      `json:"has_limit"`
	PotentialIssues     []QueryIssue             `json:"potential_issues"`
	CodeSmells          []CodeSmell              `json:"code_smells"`
	SecurityIssues      []SecurityIssue          `json:"security_issues"`
	ReadonlyOperations  bool                      `json:"readonly_operations"`
}

type QueryIssue struct {
	Type        string `json:"type"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
	Location    string `json:"location"`
	Suggestion  string `json:"suggestion"`
}

type CodeSmell struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Impact      string `json:"impact"`
	Refactoring string `json:"refactoring_suggestion"`
}

type SecurityIssue struct {
	Type        string `json:"type"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
	Risk        string `json:"risk"`
	Mitigation  string `json:"mitigation"`
}

type SchemaAnalysis struct {
	TablesAccessed     []TableInfo      `json:"tables_accessed"`
	ColumnsAccessed    []ColumnInfo     `json:"columns_accessed"`
	MissingIndexes     []IndexSuggestion `json:"missing_indexes"`
	IndexUsage         []IndexUsage     `json:"index_usage"`
	ForeignKeyUsage    []ForeignKeyInfo `json:"foreign_key_usage"`
	DataTypeAnalysis   []DataTypeIssue  `json:"data_type_analysis"`
}

type TableInfo struct {
	Name            string  `json:"name"`
	EstimatedRows   int64   `json:"estimated_rows"`
	EstimatedSizeMB float64 `json:"estimated_size_mb"`
	AccessPattern   string  `json:"access_pattern"`
	Selectivity     float64 `json:"selectivity"`
}

type ColumnInfo struct {
	Table       string  `json:"table"`
	Column      string  `json:"column"`
	DataType    string  `json:"data_type"`
	Nullable    bool    `json:"nullable"`
	Cardinality int64   `json:"cardinality"`
	Usage       string  `json:"usage"`
}

type IndexSuggestion struct {
	Table       string   `json:"table"`
	Columns     []string `json:"columns"`
	Type        string   `json:"type"`
	Benefit     string   `json:"benefit"`
	EstimatedGain float64 `json:"estimated_gain"`
}

type IndexUsage struct {
	IndexName   string  `json:"index_name"`
	Table       string  `json:"table"`
	Used        bool    `json:"used"`
	Effectiveness float64 `json:"effectiveness"`
}

type ForeignKeyInfo struct {
	FromTable  string `json:"from_table"`
	FromColumn string `json:"from_column"`
	ToTable    string `json:"to_table"`
	ToColumn   string `json:"to_column"`
	Used       bool   `json:"used"`
}

type DataTypeIssue struct {
	Column      string `json:"column"`
	CurrentType string `json:"current_type"`
	Issue       string `json:"issue"`
	Suggestion  string `json:"suggestion"`
}

type ComplexityAnalysis struct {
	OverallComplexity   string                 `json:"overall_complexity"`
	ComplexityScore     float64                `json:"complexity_score"`
	ComplexityFactors   []ComplexityFactor     `json:"complexity_factors"`
	CyclomaticComplexity int                   `json:"cyclomatic_complexity"`
	JoinComplexity      int                    `json:"join_complexity"`
	SubqueryDepth       int                    `json:"subquery_depth"`
	AggregationComplexity int                  `json:"aggregation_complexity"`
}

type ComplexityFactor struct {
	Factor      string  `json:"factor"`
	Weight      float64 `json:"weight"`
	Description string  `json:"description"`
	Impact      string  `json:"impact"`
}

type PerformancePredictions struct {
	EstimatedDuration   time.Duration           `json:"estimated_duration"`
	EstimatedMemoryMB   float64                 `json:"estimated_memory_mb"`
	EstimatedIOOps      int64                   `json:"estimated_io_ops"`
	ScalabilityAnalysis *ScalabilityAnalysis    `json:"scalability_analysis"`
	RiskFactors         []RiskFactor            `json:"risk_factors"`
	PerformanceClass    string                  `json:"performance_class"`
}

type ScalabilityAnalysis struct {
	DataSizeImpact      string  `json:"data_size_impact"`
	ConcurrencyImpact   string  `json:"concurrency_impact"`
	ScalabilityScore    float64 `json:"scalability_score"`
	BottleneckFactors   []string `json:"bottleneck_factors"`
}

type RiskFactor struct {
	Type        string  `json:"type"`
	Probability float64 `json:"probability"`
	Impact      string  `json:"impact"`
	Description string  `json:"description"`
	Mitigation  string  `json:"mitigation"`
}

// Implementation of analysis methods

func (qi *QueryInspector) performStaticAnalysis(sql string, query *core.ParsedQuery) *StaticAnalysis {
	analysis := &StaticAnalysis{
		QueryType:          qi.getQueryTypeString(query.Type),
		HasSubqueries:      query.IsSubquery,
		HasAggregates:      query.IsAggregate || len(query.Aggregates) > 0,
		HasJoins:           len(query.Joins) > 0,
		HasOrderBy:         len(query.OrderBy) > 0,
		HasGroupBy:         len(query.GroupBy) > 0,
		HasLimit:           query.Limit > 0,
		UsesSelectStar:     len(query.Columns) > 0 && query.Columns[0].Name == "*",
		ReadonlyOperations: query.Type == core.SELECT,
	}
	
	// Detect potential issues
	var issues []QueryIssue
	var codeSmells []CodeSmell
	var securityIssues []SecurityIssue
	
	// Check for SELECT *
	if analysis.UsesSelectStar {
		codeSmells = append(codeSmells, CodeSmell{
			Type:        "select_star",
			Description: "Using SELECT * instead of specific columns",
			Impact:      "Unnecessary data transfer and reduced performance",
			Refactoring: "Specify only the columns you need",
		})
	}
	
	// Check for missing WHERE clauses
	if len(query.Where) == 0 && query.Type == core.SELECT {
		issues = append(issues, QueryIssue{
			Type:        "missing_filter",
			Severity:    "medium",
			Description: "Query lacks WHERE clause - may return large result set",
			Location:    "WHERE clause",
			Suggestion:  "Add appropriate filtering conditions",
		})
	}
	
	// Check for potential Cartesian products
	if len(query.Joins) > 0 {
		for i, join := range query.Joins {
			if join.Condition.LeftColumn == "" || join.Condition.RightColumn == "" {
				issues = append(issues, QueryIssue{
					Type:        "cartesian_product",
					Severity:    "high",
					Description: "JOIN without condition may create Cartesian product",
					Location:    fmt.Sprintf("JOIN %d", i+1),
					Suggestion:  "Add appropriate JOIN condition",
				})
			}
		}
	}
	
	// Check for potential SQL injection (basic checks)
	if strings.Contains(strings.ToLower(sql), "union") && strings.Contains(strings.ToLower(sql), "select") {
		securityIssues = append(securityIssues, SecurityIssue{
			Type:        "potential_injection",
			Severity:    "high",
			Description: "Query contains UNION with SELECT - potential SQL injection pattern",
			Risk:        "Data breach or unauthorized access",
			Mitigation:  "Use parameterized queries and input validation",
		})
	}
	
	analysis.PotentialIssues = issues
	analysis.CodeSmells = codeSmells
	analysis.SecurityIssues = securityIssues
	
	return analysis
}

func (qi *QueryInspector) analyzeSchema(query *core.ParsedQuery) *SchemaAnalysis {
	// This would integrate with actual schema metadata
	// For now, providing mock analysis
	
	tables := []TableInfo{
		{
			Name:            query.TableName,
			EstimatedRows:   10000,
			EstimatedSizeMB: 50.0,
			AccessPattern:   "sequential_scan",
			Selectivity:     0.1,
		},
	}
	
	columns := []ColumnInfo{}
	for _, col := range query.Columns {
		if col.Name != "*" {
			columns = append(columns, ColumnInfo{
				Table:       query.TableName,
				Column:      col.Name,
				DataType:    "unknown",
				Nullable:    true,
				Cardinality: 1000,
				Usage:       "projection",
			})
		}
	}
	
	// Suggest indexes based on WHERE and JOIN conditions
	var indexSuggestions []IndexSuggestion
	if len(query.Where) > 0 {
		var whereColumns []string
		for _, where := range query.Where {
			whereColumns = append(whereColumns, where.Column)
		}
		indexSuggestions = append(indexSuggestions, IndexSuggestion{
			Table:         query.TableName,
			Columns:       whereColumns,
			Type:          "btree",
			Benefit:       "Faster WHERE clause evaluation",
			EstimatedGain: 75.0,
		})
	}
	
	return &SchemaAnalysis{
		TablesAccessed:   tables,
		ColumnsAccessed:  columns,
		MissingIndexes:   indexSuggestions,
		IndexUsage:       []IndexUsage{},
		ForeignKeyUsage:  []ForeignKeyInfo{},
		DataTypeAnalysis: []DataTypeIssue{},
	}
}

func (qi *QueryInspector) analyzeComplexity(query *core.ParsedQuery) *ComplexityAnalysis {
	score := 0.0
	factors := []ComplexityFactor{}
	
	// Base complexity
	score += 1.0
	
	// Add complexity for joins
	if len(query.Joins) > 0 {
		joinScore := float64(len(query.Joins)) * 2.0
		score += joinScore
		factors = append(factors, ComplexityFactor{
			Factor:      "joins",
			Weight:      joinScore,
			Description: fmt.Sprintf("%d join operations", len(query.Joins)),
			Impact:      "Increases execution time and memory usage",
		})
	}
	
	// Add complexity for aggregates
	if len(query.Aggregates) > 0 {
		aggScore := float64(len(query.Aggregates)) * 1.5
		score += aggScore
		factors = append(factors, ComplexityFactor{
			Factor:      "aggregates",
			Weight:      aggScore,
			Description: fmt.Sprintf("%d aggregate functions", len(query.Aggregates)),
			Impact:      "Requires sorting and grouping operations",
		})
	}
	
	// Add complexity for subqueries
	if query.IsSubquery {
		subqueryScore := 3.0
		score += subqueryScore
		factors = append(factors, ComplexityFactor{
			Factor:      "subqueries",
			Weight:      subqueryScore,
			Description: "Contains subqueries",
			Impact:      "May require multiple passes through data",
		})
	}
	
	// Determine complexity class
	var complexity string
	switch {
	case score <= 3.0:
		complexity = "simple"
	case score <= 7.0:
		complexity = "moderate"
	case score <= 15.0:
		complexity = "complex"
	default:
		complexity = "very_complex"
	}
	
	return &ComplexityAnalysis{
		OverallComplexity:     complexity,
		ComplexityScore:       score,
		ComplexityFactors:     factors,
		CyclomaticComplexity:  len(query.Where) + len(query.Joins),
		JoinComplexity:        len(query.Joins),
		SubqueryDepth:         qi.calculateSubqueryDepth(query),
		AggregationComplexity: len(query.Aggregates),
	}
}

func (qi *QueryInspector) generatePerformancePredictions(plan *ExplainPlan) *PerformancePredictions {
	// Base predictions on explain plan
	estimatedDuration := plan.ExecutionPlan.RootNode.EstimatedTime
	estimatedMemory := float64(plan.ExecutionPlan.RootNode.EstimatedRows) * 0.1 // 0.1MB per 1000 rows
	estimatedIOOps := plan.ExecutionPlan.RootNode.EstimatedRows / 100 // 1 IO per 100 rows
	
	// Risk factors
	var riskFactors []RiskFactor
	
	if plan.Statistics.EstimatedRows > 100000 {
		riskFactors = append(riskFactors, RiskFactor{
			Type:        "large_result_set",
			Probability: 0.8,
			Impact:      "high",
			Description: "Query may return very large result set",
			Mitigation:  "Add LIMIT clause or more selective WHERE conditions",
		})
	}
	
	if len(plan.ExecutionPlan.JoinTypes) > 2 {
		riskFactors = append(riskFactors, RiskFactor{
			Type:        "complex_joins",
			Probability: 0.6,
			Impact:      "medium",
			Description: "Multiple joins may cause performance degradation",
			Mitigation:  "Ensure proper indexes on join columns",
		})
	}
	
	// Scalability analysis
	scalability := &ScalabilityAnalysis{
		DataSizeImpact:      "linear",
		ConcurrencyImpact:   "low",
		ScalabilityScore:    75.0,
		BottleneckFactors:   []string{},
	}
	
	if len(plan.ExecutionPlan.JoinTypes) > 0 {
		scalability.DataSizeImpact = "quadratic"
		scalability.ScalabilityScore = 50.0
		scalability.BottleneckFactors = append(scalability.BottleneckFactors, "join_operations")
	}
	
	// Performance class
	var perfClass string
	switch {
	case estimatedDuration < 100*time.Millisecond:
		perfClass = "fast"
	case estimatedDuration < 1*time.Second:
		perfClass = "moderate"
	case estimatedDuration < 10*time.Second:
		perfClass = "slow"
	default:
		perfClass = "very_slow"
	}
	
	return &PerformancePredictions{
		EstimatedDuration:   estimatedDuration,
		EstimatedMemoryMB:   estimatedMemory,
		EstimatedIOOps:      estimatedIOOps,
		ScalabilityAnalysis: scalability,
		RiskFactors:         riskFactors,
		PerformanceClass:    perfClass,
	}
}

func (qi *QueryInspector) calculateSubqueryDepth(query *core.ParsedQuery) int {
	// This would need actual subquery AST analysis
	// For now, return simple estimation
	if query.IsSubquery {
		return 1
	}
	return 0
}

// Helper method to convert QueryType to string
func (qi *QueryInspector) getQueryTypeString(queryType core.QueryType) string {
	switch queryType {
	case core.SELECT:
		return "SELECT"
	case core.INSERT:
		return "INSERT"
	case core.UPDATE:
		return "UPDATE"
	case core.DELETE:
		return "DELETE"
	case core.EXPLAIN:
		return "EXPLAIN"
	case core.UNION:
		return "UNION"
	default:
		return "UNSUPPORTED"
	}
}

// StartProfiling begins performance profiling for a query execution
func (dc *DebugContext) StartProfiling(queryID, sql string) {
	profile := &ExecutionProfile{
		QueryID:   queryID,
		SQL:       sql,
		StartTime: time.Now(),
		Phases:    make([]ExecutionPhase, 0),
		ResourceUsage: &ResourceUsage{
			ResourceTimeline: make([]ResourceSample, 0),
		},
	}
	
	dc.profiles[queryID] = profile
}

// StopProfiling ends profiling and analyzes results
func (dc *DebugContext) StopProfiling(queryID string) *ExecutionProfile {
	profile, exists := dc.profiles[queryID]
	if !exists {
		return nil
	}
	
	profile.EndTime = time.Now()
	profile.TotalDuration = profile.EndTime.Sub(profile.StartTime)
	
	// Analyze bottlenecks
	profile.Bottlenecks = dc.analyzeBottlenecks(profile)
	
	// Generate optimization opportunities
	profile.Optimizations = dc.generateOptimizations(profile)
	
	// Analyze cache performance
	profile.CacheAnalysis = dc.analyzeCachePerformance(profile)
	
	delete(dc.profiles, queryID)
	return profile
}

func (dc *DebugContext) analyzeBottlenecks(profile *ExecutionProfile) []PerformanceBottleneck {
	var bottlenecks []PerformanceBottleneck
	
	// Sort phases by duration to find slowest
	sort.Slice(profile.Phases, func(i, j int) bool {
		return profile.Phases[i].Duration > profile.Phases[j].Duration
	})
	
	// Identify phases taking > 25% of total time
	for _, phase := range profile.Phases {
		percentage := (float64(phase.Duration) / float64(profile.TotalDuration)) * 100
		if percentage > 25.0 {
			bottlenecks = append(bottlenecks, PerformanceBottleneck{
				Type:        "phase_bottleneck",
				Component:   phase.Name,
				Description: fmt.Sprintf("%s phase taking %.1f%% of execution time", phase.Name, percentage),
				Impact:      percentage,
				Duration:    phase.Duration,
				Suggestion:  dc.getOptimizationSuggestion(phase.Name),
				Severity:    dc.getSeverity(percentage),
			})
		}
	}
	
	return bottlenecks
}

func (dc *DebugContext) generateOptimizations(profile *ExecutionProfile) []OptimizationOpportunity {
	var opportunities []OptimizationOpportunity
	
	// Analyze resource usage patterns
	if profile.ResourceUsage.PeakMemoryMB > 1000 {
		opportunities = append(opportunities, OptimizationOpportunity{
			Type:           "memory_optimization",
			Description:    "High memory usage detected",
			EstimatedGain:  20.0,
			Difficulty:     "medium",
			Recommendation: "Consider using streaming or pagination",
			Priority:       "high",
		})
	}
	
	// Analyze cache performance
	if profile.CacheAnalysis != nil && profile.CacheAnalysis.HitRate < 0.5 {
		opportunities = append(opportunities, OptimizationOpportunity{
			Type:           "cache_optimization",
			Description:    "Low cache hit rate",
			EstimatedGain:  30.0,
			Difficulty:     "easy",
			Recommendation: "Increase cache size or improve cache strategy",
			Priority:       "medium",
		})
	}
	
	return opportunities
}

func (dc *DebugContext) analyzeCachePerformance(profile *ExecutionProfile) *CacheAnalysis {
	// Mock cache analysis - would integrate with actual cache metrics
	return &CacheAnalysis{
		TotalAccesses:      1000,
		Hits:               750,
		Misses:             250,
		HitRate:            0.75,
		MissLatencyAvg:     50 * time.Millisecond,
		HitLatencyAvg:      5 * time.Millisecond,
		CacheSize:          100 * 1024 * 1024, // 100MB
		CacheUtilization:   0.80,
		EvictionCount:      10,
		CacheEffectiveness: 0.85,
	}
}

// Utility functions for DebugContext
func (dc *DebugContext) getOptimizationSuggestion(phaseName string) string {
	suggestions := map[string]string{
		"scan":      "Consider adding indexes or using column pruning",
		"join":      "Ensure proper indexes on join columns",
		"aggregate": "Consider pre-aggregation or materialized views",
		"sort":      "Add indexes on ORDER BY columns",
		"filter":    "Move filters earlier in execution plan",
	}
	
	if suggestion, exists := suggestions[strings.ToLower(phaseName)]; exists {
		return suggestion
	}
	return "Review execution plan for optimization opportunities"
}

func (dc *DebugContext) getSeverity(percentage float64) string {
	switch {
	case percentage > 50:
		return "critical"
	case percentage > 35:
		return "high"
	case percentage > 25:
		return "medium"
	default:
		return "low"
	}
}

// Format output methods for debugging tools
func (profile *ExecutionProfile) FormatSummary() string {
	var sb strings.Builder
	
	sb.WriteString("=== EXECUTION PROFILE SUMMARY ===\n")
	sb.WriteString(fmt.Sprintf("Query ID: %s\n", profile.QueryID))
	sb.WriteString(fmt.Sprintf("Total Duration: %v\n", profile.TotalDuration))
	sb.WriteString(fmt.Sprintf("Peak Memory: %.2f MB\n", profile.ResourceUsage.PeakMemoryMB))
	
	if len(profile.Bottlenecks) > 0 {
		sb.WriteString("\nPERFORMANCE BOTTLENECKS:\n")
		for _, bottleneck := range profile.Bottlenecks {
			sb.WriteString(fmt.Sprintf("  %s: %s (%.1f%% impact)\n", 
				bottleneck.Severity, bottleneck.Description, bottleneck.Impact))
		}
	}
	
	if len(profile.Optimizations) > 0 {
		sb.WriteString("\nOPTIMIZATION OPPORTUNITIES:\n")
		for _, opt := range profile.Optimizations {
			sb.WriteString(fmt.Sprintf("  %s: %s (%.1f%% gain)\n", 
				opt.Priority, opt.Description, opt.EstimatedGain))
		}
	}
	
	return sb.String()
}