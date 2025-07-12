# ByteDB Distributed Query Engine Design

## Overview

ByteDB Distributed extends the single-node query engine to support distributed query execution across multiple nodes, enabling horizontal scaling and processing of large datasets.

## Architecture

### Components

```
┌─────────────────┐
│   Client/CLI    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│   Coordinator   │ ← Query Planning & Result Aggregation
├─────────────────┤
│ - Query Parser  │
│ - Planner       │
│ - Optimizer     │
│ - Scheduler     │
└────────┬────────┘
         │
    ┌────┴────┬──────────┬──────────┐
    v         v          v          v
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│Worker 1│ │Worker 2│ │Worker 3│ │Worker N│
├────────┤ ├────────┤ ├────────┤ ├────────┤
│ Engine │ │ Engine │ │ Engine │ │ Engine │
│ Cache  │ │ Cache  │ │ Cache  │ │ Cache  │
└────────┘ └────────┘ └────────┘ └────────┘
    │          │          │          │
    v          v          v          v
[Parquet]  [Parquet]  [Parquet]  [Parquet]
```

### Key Components

1. **Coordinator Node**
   - Receives SQL queries from clients
   - Parses and creates distributed query plan
   - Distributes sub-queries to workers
   - Aggregates results from workers
   - Handles query optimization at cluster level

2. **Worker Nodes**
   - Execute query fragments on local data
   - Return partial results to coordinator
   - Handle local caching and optimization
   - Report statistics back to coordinator

3. **Communication Layer**
   - gRPC for inter-node communication
   - Protocol buffers for efficient serialization
   - HTTP API for client compatibility

## Query Execution Flow

### 1. Query Submission
```sql
SELECT department, COUNT(*), AVG(salary) 
FROM employees 
WHERE salary > 70000 
GROUP BY department
```

### 2. Distributed Planning
```
Coordinator creates execution plan:
├── Stage 1: Scan + Filter (Parallel on Workers)
│   ├── Worker 1: Scan employees_part1.parquet WHERE salary > 70000
│   ├── Worker 2: Scan employees_part2.parquet WHERE salary > 70000
│   └── Worker 3: Scan employees_part3.parquet WHERE salary > 70000
│
├── Stage 2: Partial Aggregation (On Workers)
│   ├── Worker 1: GROUP BY department, partial COUNT/SUM
│   ├── Worker 2: GROUP BY department, partial COUNT/SUM
│   └── Worker 3: GROUP BY department, partial COUNT/SUM
│
└── Stage 3: Final Aggregation (On Coordinator)
    └── Merge partial results, compute final COUNT/AVG
```

### 3. Execution Stages

#### Stage 1: Data Scan & Filter (Parallel)
- Each worker scans its local Parquet files
- Applies WHERE predicates locally
- Uses column pruning and predicate pushdown

#### Stage 2: Partial Aggregation
- Workers compute partial aggregates
- Reduces data transfer to coordinator
- Maintains intermediate results

#### Stage 3: Final Aggregation
- Coordinator receives partial results
- Merges and computes final aggregates
- Returns results to client

## Data Distribution Strategies

### 1. Hash Partitioning
```go
partition = hash(row[partition_key]) % num_workers
```
- Good for equality joins and GROUP BY
- Even data distribution

### 2. Range Partitioning
```go
if row[key] >= range_start && row[key] < range_end {
    // Assign to this partition
}
```
- Good for range queries
- Supports efficient pruning

### 3. Round-Robin
- Simple distribution
- Good for scanning operations

## Query Types and Distribution

### 1. Simple Aggregations
```sql
SELECT COUNT(*) FROM employees
```
- Each worker counts local rows
- Coordinator sums counts

### 2. GROUP BY Queries
```sql
SELECT department, AVG(salary) FROM employees GROUP BY department
```
- Workers compute partial aggregates
- Coordinator merges by group key

### 3. JOIN Operations
```sql
SELECT e.*, d.* FROM employees e JOIN departments d ON e.dept_id = d.id
```
- Broadcast small table (departments) to all workers
- Or use hash-partitioned join for large tables

### 4. Subqueries
- Coordinator plans subquery execution
- May require multiple rounds of communication

## Implementation Plan

### Phase 1: Basic Distribution
- [ ] Coordinator service with gRPC
- [ ] Worker service implementation
- [ ] Simple query distribution (scan, filter)
- [ ] Result collection and merging

### Phase 2: Advanced Features
- [ ] Distributed aggregations
- [ ] Distributed joins
- [ ] Query optimization for distribution
- [ ] Statistics collection

### Phase 3: Production Features
- [ ] Fault tolerance
- [ ] Worker discovery
- [ ] Load balancing
- [ ] Monitoring and metrics

## API Design

### Coordinator API
```protobuf
service Coordinator {
    rpc ExecuteQuery(QueryRequest) returns (QueryResponse);
    rpc RegisterWorker(WorkerInfo) returns (RegisterResponse);
    rpc GetClusterStatus(Empty) returns (ClusterStatus);
}
```

### Worker API
```protobuf
service Worker {
    rpc ExecuteFragment(QueryFragment) returns (FragmentResult);
    rpc GetStatus(Empty) returns (WorkerStatus);
    rpc CacheStats(Empty) returns (CacheStatistics);
}
```

## Configuration

### Coordinator Config
```yaml
coordinator:
  port: 50051
  http_port: 8080
  workers:
    - address: "worker1:50052"
      data_path: "/data/partition1"
    - address: "worker2:50052"
      data_path: "/data/partition2"
```

### Worker Config
```yaml
worker:
  port: 50052
  data_path: "/data/local"
  cache_size_mb: 1024
  coordinator: "coordinator:50051"
```

## Fault Tolerance

1. **Worker Failure**
   - Coordinator detects via heartbeat
   - Redistributes work to healthy workers
   - Optional: Replica workers

2. **Coordinator Failure**
   - Secondary coordinator (future)
   - Query state persistence

3. **Network Partitions**
   - Timeout handling
   - Retry with exponential backoff

## Performance Optimizations

1. **Data Locality**
   - Schedule computation near data
   - Minimize data movement

2. **Caching**
   - Distributed cache coordination
   - Cache popular datasets on multiple workers

3. **Query Optimization**
   - Cost-based optimization
   - Statistics-driven planning

4. **Compression**
   - Compress intermediate results
   - Use efficient serialization

## Monitoring & Observability

1. **Metrics**
   - Query latency
   - Data transfer volume
   - Worker utilization
   - Cache hit rates

2. **Logging**
   - Distributed tracing
   - Query execution logs
   - Error tracking

3. **Dashboard**
   - Cluster health
   - Query performance
   - Resource usage