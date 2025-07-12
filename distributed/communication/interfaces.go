package communication

import "context"

// CoordinatorService defines the interface for coordinator operations
type CoordinatorService interface {
	// ExecuteQuery executes a distributed query
	ExecuteQuery(ctx context.Context, req *DistributedQueryRequest) (*DistributedQueryResponse, error)
	
	// RegisterWorker registers a new worker with the coordinator
	RegisterWorker(ctx context.Context, info *WorkerInfo) error
	
	// UnregisterWorker removes a worker from the coordinator
	UnregisterWorker(ctx context.Context, workerID string) error
	
	// GetClusterStatus returns the current cluster status
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)
	
	// Health check for the coordinator
	Health(ctx context.Context) error
	
	// Shutdown gracefully shuts down the coordinator
	Shutdown(ctx context.Context) error
}

// WorkerService defines the interface for worker operations
type WorkerService interface {
	// ExecuteFragment executes a query fragment
	ExecuteFragment(ctx context.Context, fragment *QueryFragment) (*FragmentResult, error)
	
	// GetStatus returns the current worker status
	GetStatus(ctx context.Context) (*WorkerStatus, error)
	
	// GetCacheStats returns cache statistics
	GetCacheStats(ctx context.Context) (*CacheStats, error)
	
	// Health check for the worker
	Health(ctx context.Context) error
	
	// Shutdown gracefully shuts down the worker
	Shutdown(ctx context.Context) error
}

// CacheStats represents cache statistics
type CacheStats struct {
	Hits        int     `json:"hits"`
	Misses      int     `json:"misses"`
	HitRate     float64 `json:"hit_rate"`
	MemoryUsage int64   `json:"memory_usage"`
	EntryCount  int     `json:"entry_count"`
}

// CoordinatorClient defines the interface for clients to communicate with coordinator
type CoordinatorClient interface {
	// ExecuteQuery sends a query to the coordinator
	ExecuteQuery(ctx context.Context, req *DistributedQueryRequest) (*DistributedQueryResponse, error)
	
	// GetClusterStatus gets the cluster status
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)
	
	// Close closes the client connection
	Close() error
}

// WorkerClient defines the interface for coordinator to communicate with workers
type WorkerClient interface {
	// ExecuteFragment sends a fragment to a worker
	ExecuteFragment(ctx context.Context, fragment *QueryFragment) (*FragmentResult, error)
	
	// GetStatus gets worker status
	GetStatus(ctx context.Context) (*WorkerStatus, error)
	
	// GetCacheStats gets worker cache stats
	GetCacheStats(ctx context.Context) (*CacheStats, error)
	
	// Health checks worker health
	Health(ctx context.Context) error
	
	// Close closes the client connection
	Close() error
}

// Transport defines the interface for different communication mediums
type Transport interface {
	// NewCoordinatorClient creates a client to communicate with coordinator
	NewCoordinatorClient(address string) (CoordinatorClient, error)
	
	// NewWorkerClient creates a client to communicate with worker
	NewWorkerClient(address string) (WorkerClient, error)
	
	// StartCoordinatorServer starts a coordinator server
	StartCoordinatorServer(address string, service CoordinatorService) error
	
	// StartWorkerServer starts a worker server
	StartWorkerServer(address string, service WorkerService) error
	
	// Stop stops the transport
	Stop() error
}