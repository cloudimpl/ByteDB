package communication

import (
	"context"
	"fmt"
	"sync"
)

// MemoryTransport implements Transport interface for in-memory communication
// This is useful for testing and development
type MemoryTransport struct {
	coordinators map[string]CoordinatorService
	workers      map[string]WorkerService
	mutex        sync.RWMutex
}

// NewMemoryTransport creates a new in-memory transport
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		coordinators: make(map[string]CoordinatorService),
		workers:      make(map[string]WorkerService),
	}
}

// NewCoordinatorClient creates a client to communicate with coordinator
func (mt *MemoryTransport) NewCoordinatorClient(address string) (CoordinatorClient, error) {
	mt.mutex.RLock()
	service, exists := mt.coordinators[address]
	mt.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("coordinator not found at address: %s", address)
	}
	
	return &MemoryCoordinatorClient{
		service:   service,
		address:   address,
		transport: mt,
	}, nil
}

// NewWorkerClient creates a client to communicate with worker
func (mt *MemoryTransport) NewWorkerClient(address string) (WorkerClient, error) {
	mt.mutex.RLock()
	service, exists := mt.workers[address]
	mt.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("worker not found at address: %s", address)
	}
	
	return &MemoryWorkerClient{
		service:   service,
		address:   address,
		transport: mt,
	}, nil
}

// StartCoordinatorServer starts a coordinator server
func (mt *MemoryTransport) StartCoordinatorServer(address string, service CoordinatorService) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	
	if _, exists := mt.coordinators[address]; exists {
		return fmt.Errorf("coordinator already running at address: %s", address)
	}
	
	mt.coordinators[address] = service
	return nil
}

// StartWorkerServer starts a worker server
func (mt *MemoryTransport) StartWorkerServer(address string, service WorkerService) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	
	if _, exists := mt.workers[address]; exists {
		return fmt.Errorf("worker already running at address: %s", address)
	}
	
	mt.workers[address] = service
	return nil
}

// Stop stops the transport
func (mt *MemoryTransport) Stop() error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	
	// Just clear the registrations, don't call shutdown on services
	// Services should be shut down by their owners
	mt.coordinators = make(map[string]CoordinatorService)
	mt.workers = make(map[string]WorkerService)
	
	return nil
}

// MemoryCoordinatorClient implements CoordinatorClient for in-memory communication
type MemoryCoordinatorClient struct {
	service   CoordinatorService
	address   string
	transport *MemoryTransport
}

// ExecuteQuery sends a query to the coordinator
func (mcc *MemoryCoordinatorClient) ExecuteQuery(ctx context.Context, req *DistributedQueryRequest) (*DistributedQueryResponse, error) {
	return mcc.service.ExecuteQuery(ctx, req)
}

// GetClusterStatus gets the cluster status
func (mcc *MemoryCoordinatorClient) GetClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	return mcc.service.GetClusterStatus(ctx)
}

// Close closes the client connection
func (mcc *MemoryCoordinatorClient) Close() error {
	// Nothing to close for in-memory transport
	return nil
}

// MemoryWorkerClient implements WorkerClient for in-memory communication
type MemoryWorkerClient struct {
	service   WorkerService
	address   string
	transport *MemoryTransport
}

// ExecuteFragment sends a fragment to a worker
func (mwc *MemoryWorkerClient) ExecuteFragment(ctx context.Context, fragment *QueryFragment) (*FragmentResult, error) {
	return mwc.service.ExecuteFragment(ctx, fragment)
}

// GetStatus gets worker status
func (mwc *MemoryWorkerClient) GetStatus(ctx context.Context) (*WorkerStatus, error) {
	return mwc.service.GetStatus(ctx)
}

// GetCacheStats gets worker cache stats
func (mwc *MemoryWorkerClient) GetCacheStats(ctx context.Context) (*CacheStats, error) {
	return mwc.service.GetCacheStats(ctx)
}

// Health checks worker health
func (mwc *MemoryWorkerClient) Health(ctx context.Context) error {
	return mwc.service.Health(ctx)
}

// Close closes the client connection
func (mwc *MemoryWorkerClient) Close() error {
	// Nothing to close for in-memory transport
	return nil
}

// Helper methods for testing

// RegisterCoordinator directly registers a coordinator service (for testing)
func (mt *MemoryTransport) RegisterCoordinator(address string, service CoordinatorService) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	mt.coordinators[address] = service
}

// RegisterWorker directly registers a worker service (for testing)
func (mt *MemoryTransport) RegisterWorker(address string, service WorkerService) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	mt.workers[address] = service
}

// GetCoordinator gets a coordinator service (for testing)
func (mt *MemoryTransport) GetCoordinator(address string) CoordinatorService {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	return mt.coordinators[address]
}

// GetWorker gets a worker service (for testing)
func (mt *MemoryTransport) GetWorker(address string) WorkerService {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	return mt.workers[address]
}

// ListCoordinators returns all coordinator addresses
func (mt *MemoryTransport) ListCoordinators() []string {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	
	addresses := make([]string, 0, len(mt.coordinators))
	for addr := range mt.coordinators {
		addresses = append(addresses, addr)
	}
	return addresses
}

// ListWorkers returns all worker addresses
func (mt *MemoryTransport) ListWorkers() []string {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	
	addresses := make([]string, 0, len(mt.workers))
	for addr := range mt.workers {
		addresses = append(addresses, addr)
	}
	return addresses
}