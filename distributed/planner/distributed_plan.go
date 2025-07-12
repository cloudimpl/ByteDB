package planner

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"fmt"
	"time"
)

// DistributedPlan represents a comprehensive distributed query execution plan
type DistributedPlan struct {
	ID           string                    `json:"id"`
	OriginalSQL  string                    `json:"original_sql"`
	ParsedQuery  *core.ParsedQuery         `json:"parsed_query"`
	Stages       []ExecutionStage          `json:"stages"`
	DataFlow     *DataFlowGraph            `json:"data_flow"`
	Partitioning *PartitioningStrategy     `json:"partitioning"`
	Statistics   *PlanStatistics           `json:"statistics"`
	Optimizations []OptimizationApplied    `json:"optimizations"`
	CreatedAt    time.Time                 `json:"created_at"`
}

// ExecutionStage represents a stage in distributed query execution
type ExecutionStage struct {
	ID            string            `json:"id"`
	Type          StageType         `json:"type"`
	Dependencies  []string          `json:"dependencies"`  // IDs of prerequisite stages
	Fragments     []StageFragment   `json:"fragments"`     // Fragments to execute in this stage
	Parallelism   int               `json:"parallelism"`   // Number of parallel executions
	ShuffleKey    []string          `json:"shuffle_key"`   // Keys for data redistribution
	EstimatedCost float64           `json:"estimated_cost"`
	Timeout       time.Duration     `json:"timeout"`
}

// StageType defines the type of execution stage
type StageType string

const (
	StageScan       StageType = "scan"         // Data scanning and filtering
	StageJoin       StageType = "join"         // Join operations
	StageAggregate  StageType = "aggregate"    // Aggregation operations
	StageShuffle    StageType = "shuffle"      // Data redistribution
	StageSort       StageType = "sort"         // Sorting operations
	StageUnion      StageType = "union"        // Union operations
	StageFinal      StageType = "final"        // Final result collection
)

// StageFragment represents a fragment within an execution stage
type StageFragment struct {
	ID                string                       `json:"id"`
	WorkerID          string                       `json:"worker_id"`
	Fragment          *communication.QueryFragment `json:"fragment"`
	InputPartitions   []PartitionInfo              `json:"input_partitions"`
	OutputPartitions  []PartitionInfo              `json:"output_partitions"`
	EstimatedRows     int64                        `json:"estimated_rows"`
	EstimatedBytes    int64                        `json:"estimated_bytes"`
	RequiredMemoryMB  int                          `json:"required_memory_mb"`
	EstimatedDuration time.Duration                `json:"estimated_duration"`
}

// PartitionInfo describes data partitioning
type PartitionInfo struct {
	ID            string            `json:"id"`
	Type          PartitionType     `json:"type"`
	Keys          []string          `json:"keys"`           // Partition key columns
	RangeInfo     *PartitionRange   `json:"range"`          // For range partitioning
	HashBuckets   int               `json:"hash_buckets"`   // For hash partitioning
	EstimatedSize int64             `json:"estimated_size"` // Estimated partition size
	Location      string            `json:"location"`       // Worker or file location
}

// PartitionType defines the partitioning strategy
type PartitionType string

const (
	PartitionHash       PartitionType = "hash"
	PartitionTypeRange  PartitionType = "range"
	PartitionRoundRobin PartitionType = "round_robin"
	PartitionBroadcast  PartitionType = "broadcast"
	PartitionReplicated PartitionType = "replicated"
)

// PartitionRange defines range partitioning bounds
type PartitionRange struct {
	Column string      `json:"column"`
	Start  interface{} `json:"start"`
	End    interface{} `json:"end"`
}

// DataFlowGraph represents the flow of data between stages
type DataFlowGraph struct {
	Nodes []DataFlowNode `json:"nodes"`
	Edges []DataFlowEdge `json:"edges"`
}

// DataFlowNode represents a processing node
type DataFlowNode struct {
	ID       string    `json:"id"`
	Type     StageType `json:"type"`
	Operator string    `json:"operator"`  // SQL operator (SELECT, JOIN, GROUP BY, etc.)
	Cost     float64   `json:"cost"`
}

// DataFlowEdge represents data movement between nodes
type DataFlowEdge struct {
	From         string        `json:"from"`
	To           string        `json:"to"`
	DataSize     int64         `json:"data_size"`      // Estimated data transfer size
	Partitioning PartitionType `json:"partitioning"`   // How data is partitioned
	Compression  bool          `json:"compression"`    // Whether data is compressed
}

// PartitioningStrategy defines how data should be partitioned
type PartitioningStrategy struct {
	Type           PartitionType     `json:"type"`
	Keys           []string          `json:"keys"`
	NumPartitions  int               `json:"num_partitions"`
	Ranges         []PartitionRange  `json:"ranges"`          // For range partitioning
	Distribution   map[string]int64  `json:"distribution"`    // Size distribution per partition
	Colocation     map[string]string `json:"colocation"`      // Worker affinity for partitions
}

// PlanStatistics contains cost and cardinality estimates
type PlanStatistics struct {
	EstimatedRows        int64         `json:"estimated_rows"`
	EstimatedBytes       int64         `json:"estimated_bytes"`
	EstimatedDuration    time.Duration `json:"estimated_duration"`
	EstimatedCPUCost     float64       `json:"estimated_cpu_cost"`
	EstimatedIOCost      float64       `json:"estimated_io_cost"`
	EstimatedNetworkCost float64       `json:"estimated_network_cost"`
	TotalCost            float64       `json:"total_cost"`
	Confidence           float64       `json:"confidence"`      // 0.0 to 1.0
}

// OptimizationApplied records which optimizations were applied
type OptimizationApplied struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Benefit     float64   `json:"benefit"`      // Cost reduction
	AppliedAt   time.Time `json:"applied_at"`
}

// JoinStrategy defines different join execution strategies
type JoinStrategy string

const (
	JoinBroadcast     JoinStrategy = "broadcast"      // Broadcast smaller table
	JoinShuffle       JoinStrategy = "shuffle"        // Shuffle both tables
	JoinPartitioned   JoinStrategy = "partitioned"    // Both tables already partitioned
	JoinCollocated    JoinStrategy = "collocated"     // Join on same worker
)

// JoinInfo contains join-specific planning information
type JoinInfo struct {
	Strategy        JoinStrategy `json:"strategy"`
	JoinType        string       `json:"join_type"`        // INNER, LEFT, RIGHT, FULL
	JoinConditions  []string     `json:"join_conditions"`
	LeftTable       string       `json:"left_table"`
	RightTable      string       `json:"right_table"`
	BroadcastTable  string       `json:"broadcast_table"`  // Table to broadcast (if any)
	EstimatedOutput int64        `json:"estimated_output"` // Estimated output rows
}

// AggregationInfo contains aggregation-specific planning information
type AggregationInfo struct {
	Type           AggregationType `json:"type"`
	GroupByKeys    []string        `json:"group_by_keys"`
	AggregateExprs []string        `json:"aggregate_exprs"`
	IsPartial      bool            `json:"is_partial"`      // Partial vs final aggregation
	CombineFunc    string          `json:"combine_func"`    // Function to combine partial results
}

// AggregationType defines aggregation strategies
type AggregationType string

const (
	AggregationHash   AggregationType = "hash"   // Hash-based aggregation
	AggregationSort   AggregationType = "sort"   // Sort-based aggregation
	AggregationStream AggregationType = "stream" // Streaming aggregation
)

// WorkerInfo contains information about available workers
type WorkerInfo struct {
	ID              string  `json:"id"`
	Address         string  `json:"address"`
	CPUCores        int     `json:"cpu_cores"`
	MemoryMB        int     `json:"memory_mb"`
	DiskSpaceGB     int64   `json:"disk_space_gb"`
	NetworkBandwidth int64  `json:"network_bandwidth_mbps"`
	CurrentLoad     float64 `json:"current_load"`        // 0.0 to 1.0
	DataLocality    map[string]float64 `json:"data_locality"` // Table -> locality score
}

// PlanningContext contains context information for planning
type PlanningContext struct {
	Workers       []WorkerInfo      `json:"workers"`
	TableStats    map[string]*TableStatistics `json:"table_stats"`
	IndexInfo     map[string][]IndexInfo      `json:"index_info"`
	Constraints   PlanningConstraints         `json:"constraints"`
	Preferences   PlanningPreferences         `json:"preferences"`
}

// TableStatistics contains table-level statistics
type TableStatistics struct {
	TableName     string            `json:"table_name"`
	RowCount      int64             `json:"row_count"`
	SizeBytes     int64             `json:"size_bytes"`
	ColumnStats   map[string]*ColumnStatistics `json:"column_stats"`
	Partitions    []PartitionInfo   `json:"partitions"`
	LastUpdated   time.Time         `json:"last_updated"`
}

// ColumnStatistics contains column-level statistics
type ColumnStatistics struct {
	ColumnName    string      `json:"column_name"`
	DataType      string      `json:"data_type"`
	NullCount     int64       `json:"null_count"`
	DistinctCount int64       `json:"distinct_count"`
	MinValue      interface{} `json:"min_value"`
	MaxValue      interface{} `json:"max_value"`
	AvgLength     float64     `json:"avg_length"`
	Histogram     []HistogramBucket `json:"histogram"`
}

// HistogramBucket represents a histogram bucket for value distribution
type HistogramBucket struct {
	RangeStart interface{} `json:"range_start"`
	RangeEnd   interface{} `json:"range_end"`
	Count      int64       `json:"count"`
	Frequency  float64     `json:"frequency"`
}

// IndexInfo contains information about available indexes
type IndexInfo struct {
	IndexName string   `json:"index_name"`
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
	IsUnique  bool     `json:"is_unique"`
	IsClustered bool   `json:"is_clustered"`
	SizeBytes int64    `json:"size_bytes"`
}

// PlanningConstraints defines limits and requirements for planning
type PlanningConstraints struct {
	MaxParallelism    int           `json:"max_parallelism"`
	MaxMemoryMB       int           `json:"max_memory_mb"`
	MaxNetworkMBps    int64         `json:"max_network_mbps"`
	TimeoutDuration   time.Duration `json:"timeout_duration"`
	PreferredWorkers  []string      `json:"preferred_workers"`
	ProhibitedWorkers []string      `json:"prohibited_workers"`
}

// PlanningPreferences defines optimization preferences
type PlanningPreferences struct {
	OptimizeFor        OptimizationGoal `json:"optimize_for"`
	AllowDataMovement  bool             `json:"allow_data_movement"`
	PreferDataLocality bool             `json:"prefer_data_locality"`
	MinConfidence      float64          `json:"min_confidence"`
	MaxPlanningTime    time.Duration    `json:"max_planning_time"`
}

// OptimizationGoal defines what to optimize for
type OptimizationGoal string

const (
	OptimizeLatency    OptimizationGoal = "latency"     // Minimize query execution time
	OptimizeThroughput OptimizationGoal = "throughput"  // Maximize queries per second
	OptimizeResource   OptimizationGoal = "resource"    // Minimize resource usage
	OptimizeCost       OptimizationGoal = "cost"        // Minimize monetary cost
)

// PlanExecution contains runtime execution information
type PlanExecution struct {
	PlanID          string                 `json:"plan_id"`
	StartTime       time.Time              `json:"start_time"`
	EndTime         *time.Time             `json:"end_time"`
	Status          ExecutionStatus        `json:"status"`
	CurrentStage    string                 `json:"current_stage"`
	CompletedStages []string               `json:"completed_stages"`
	FailedStages    []string               `json:"failed_stages"`
	StageResults    map[string]*StageResult `json:"stage_results"`
	TotalRows       int64                  `json:"total_rows"`
	TotalBytes      int64                  `json:"total_bytes"`
	ActualDuration  time.Duration          `json:"actual_duration"`
	ActualCost      float64                `json:"actual_cost"`
}

// ExecutionStatus represents the status of plan execution
type ExecutionStatus string

const (
	StatusPending   ExecutionStatus = "pending"
	StatusRunning   ExecutionStatus = "running"
	StatusCompleted ExecutionStatus = "completed"
	StatusFailed    ExecutionStatus = "failed"
	StatusCancelled ExecutionStatus = "cancelled"
)

// StageResult contains the result of executing a stage
type StageResult struct {
	StageID       string                               `json:"stage_id"`
	StartTime     time.Time                            `json:"start_time"`
	EndTime       time.Time                            `json:"end_time"`
	Status        ExecutionStatus                      `json:"status"`
	FragmentResults map[string]*communication.FragmentResult `json:"fragment_results"`
	OutputRows    int64                                `json:"output_rows"`
	OutputBytes   int64                                `json:"output_bytes"`
	ActualCost    float64                              `json:"actual_cost"`
	ErrorMessage  string                               `json:"error_message"`
}

// PlanValidation contains validation results for a plan
type PlanValidation struct {
	IsValid     bool     `json:"is_valid"`
	Errors      []string `json:"errors"`
	Warnings    []string `json:"warnings"`
	Suggestions []string `json:"suggestions"`
}

// Validate validates the distributed plan for correctness
func (dp *DistributedPlan) Validate() *PlanValidation {
	validation := &PlanValidation{
		IsValid:     true,
		Errors:      []string{},
		Warnings:    []string{},
		Suggestions: []string{},
	}

	// Check for circular dependencies
	if dp.hasCycles() {
		validation.IsValid = false
		validation.Errors = append(validation.Errors, "Circular dependency detected in execution stages")
	}

	// Validate stage dependencies
	stageMap := make(map[string]*ExecutionStage)
	for i := range dp.Stages {
		stageMap[dp.Stages[i].ID] = &dp.Stages[i]
	}

	for _, stage := range dp.Stages {
		for _, depID := range stage.Dependencies {
			if _, exists := stageMap[depID]; !exists {
				validation.IsValid = false
				validation.Errors = append(validation.Errors, 
					fmt.Sprintf("Stage %s depends on non-existent stage %s", stage.ID, depID))
			}
		}
	}

	// Check resource requirements
	for _, stage := range dp.Stages {
		totalMemory := 0
		for _, fragment := range stage.Fragments {
			totalMemory += fragment.RequiredMemoryMB
		}
		if totalMemory > 10240 { // 10GB warning threshold
			validation.Warnings = append(validation.Warnings,
				fmt.Sprintf("Stage %s requires %d MB memory, consider optimization", stage.ID, totalMemory))
		}
	}

	return validation
}

// hasCycles detects circular dependencies in the execution plan
func (dp *DistributedPlan) hasCycles() bool {
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	var dfs func(stageID string) bool
	dfs = func(stageID string) bool {
		visited[stageID] = true
		recursionStack[stageID] = true

		// Find the stage
		var currentStage *ExecutionStage
		for i := range dp.Stages {
			if dp.Stages[i].ID == stageID {
				currentStage = &dp.Stages[i]
				break
			}
		}

		if currentStage != nil {
			for _, depID := range currentStage.Dependencies {
				if !visited[depID] {
					if dfs(depID) {
						return true
					}
				} else if recursionStack[depID] {
					return true
				}
			}
		}

		recursionStack[stageID] = false
		return false
	}

	for _, stage := range dp.Stages {
		if !visited[stage.ID] {
			if dfs(stage.ID) {
				return true
			}
		}
	}

	return false
}

// GetExecutionOrder returns stages in topological order
func (dp *DistributedPlan) GetExecutionOrder() ([]string, error) {
	validation := dp.Validate()
	if !validation.IsValid {
		return nil, fmt.Errorf("invalid plan: %v", validation.Errors)
	}

	// Topological sort
	inDegree := make(map[string]int)
	graph := make(map[string][]string)

	// Initialize
	for _, stage := range dp.Stages {
		inDegree[stage.ID] = 0
		graph[stage.ID] = []string{}
	}

	// Build graph and calculate in-degrees
	for _, stage := range dp.Stages {
		for _, depID := range stage.Dependencies {
			graph[depID] = append(graph[depID], stage.ID)
			inDegree[stage.ID]++
		}
	}

	// Kahn's algorithm
	queue := []string{}
	for stageID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, stageID)
		}
	}

	result := []string{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		for _, neighbor := range graph[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(dp.Stages) {
		return nil, fmt.Errorf("circular dependency detected")
	}

	return result, nil
}

// EstimatedTotalCost returns the estimated total cost of the plan
func (dp *DistributedPlan) EstimatedTotalCost() float64 {
	if dp.Statistics != nil {
		return dp.Statistics.TotalCost
	}

	totalCost := 0.0
	for _, stage := range dp.Stages {
		totalCost += stage.EstimatedCost
	}
	return totalCost
}

// String returns a human-readable representation of the plan
func (dp *DistributedPlan) String() string {
	return fmt.Sprintf("DistributedPlan{ID: %s, Stages: %d, EstimatedCost: %.2f}", 
		dp.ID, len(dp.Stages), dp.EstimatedTotalCost())
}