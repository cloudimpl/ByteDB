package communication

import (
	"bytedb/core"
	"time"
)

// QueryFragment represents a portion of a query to be executed on a worker
type QueryFragment struct {
	ID           string                  `json:"id"`
	SQL          string                  `json:"sql"`
	TablePath    string                  `json:"table_path"`
	Columns      []string                `json:"columns"`
	WhereClause  []core.WhereCondition   `json:"where_clause"`
	GroupBy      []string                `json:"group_by"`
	Aggregates   []core.Column           `json:"aggregates"`
	Limit        int                     `json:"limit"`
	IsPartial    bool                    `json:"is_partial"`
	FragmentType FragmentType            `json:"fragment_type"`
}

// FragmentType defines the type of query fragment
type FragmentType string

const (
	FragmentTypeScan      FragmentType = "scan"
	FragmentTypeAggregate FragmentType = "aggregate"
	FragmentTypeJoin      FragmentType = "join"
	FragmentTypeFilter    FragmentType = "filter"
	FragmentTypeSort      FragmentType = "sort"
)

// FragmentResult represents the result of executing a query fragment
type FragmentResult struct {
	FragmentID string         `json:"fragment_id"`
	Rows       []core.Row     `json:"rows"`
	Columns    []string       `json:"columns"`
	Count      int           `json:"count"`
	Error      string        `json:"error"`
	Stats      ExecutionStats `json:"stats"`
}

// ExecutionStats provides execution statistics
type ExecutionStats struct {
	Duration      time.Duration `json:"duration"`
	RowsProcessed int          `json:"rows_processed"`
	BytesRead     int64        `json:"bytes_read"`
	CacheHits     int          `json:"cache_hits"`
	CacheMisses   int          `json:"cache_misses"`
}

// WorkerInfo contains information about a worker node
type WorkerInfo struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	DataPath  string `json:"data_path"`
	Status    string `json:"status"`
	Resources WorkerResources `json:"resources"`
}

// WorkerResources describes worker capabilities
type WorkerResources struct {
	CPUCores    int   `json:"cpu_cores"`
	MemoryMB    int   `json:"memory_mb"`
	DiskSpaceGB int64 `json:"disk_space_gb"`
}

// WorkerStatus represents current worker state
type WorkerStatus struct {
	ID            string        `json:"id"`
	Status        string        `json:"status"`
	ActiveQueries int           `json:"active_queries"`
	CPUUsage      float64       `json:"cpu_usage"`
	MemoryUsage   int           `json:"memory_usage"`
	LastHeartbeat time.Time     `json:"last_heartbeat"`
}

// ClusterStatus represents the overall cluster state
type ClusterStatus struct {
	TotalWorkers   int            `json:"total_workers"`
	ActiveWorkers  int            `json:"active_workers"`
	Workers        []WorkerStatus `json:"workers"`
	TotalQueries   int           `json:"total_queries"`
	QueriesPerSec  float64       `json:"queries_per_sec"`
}

// DistributedQueryRequest represents a query request to the coordinator
type DistributedQueryRequest struct {
	SQL       string            `json:"sql"`
	Options   map[string]string `json:"options"`
	Timeout   time.Duration     `json:"timeout"`
	RequestID string            `json:"request_id"`
}

// DistributedQueryResponse represents the response from the coordinator
type DistributedQueryResponse struct {
	RequestID   string         `json:"request_id"`
	Rows        []core.Row     `json:"rows"`
	Columns     []string       `json:"columns"`
	Count       int           `json:"count"`
	Error       string        `json:"error"`
	Stats       ClusterStats  `json:"stats"`
	Duration    time.Duration `json:"duration"`
}

// ClusterStats provides cluster-wide execution statistics
type ClusterStats struct {
	TotalFragments    int                      `json:"total_fragments"`
	WorkersUsed       int                      `json:"workers_used"`
	DataTransferBytes int64                    `json:"data_transfer_bytes"`
	WorkerStats       map[string]ExecutionStats `json:"worker_stats"`
	CoordinatorTime   time.Duration            `json:"coordinator_time"`
	TotalTime         time.Duration            `json:"total_time"`
}