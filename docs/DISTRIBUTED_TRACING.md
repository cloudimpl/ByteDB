# ByteDB Distributed Tracing Guide

## Overview

ByteDB's distributed tracing system provides comprehensive observability across coordinator and worker nodes, enabling deep insights into distributed query execution, performance optimization, and system health monitoring.

## Table of Contents

- [Quick Start](#quick-start)
- [Distributed Components](#distributed-components)
- [Configuration](#configuration)
- [Common Scenarios](#common-scenarios)
- [Performance Analysis](#performance-analysis)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Quick Start

### Basic Distributed Tracing

```bash
# Enable basic distributed tracing
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,WORKER,FRAGMENT

# Run distributed query test
./distributed_sql_test_runner -file tests/distributed_simple_test.json -verbose
```

### Expected Output

```
[18:24:37.043] INFO/COORDINATOR: Initializing distributed coordinator
[18:24:37.066] INFO/COORDINATOR: Executing distributed query | requestID=test-123 sql=SELECT COUNT(*) FROM employees timeout=30s
[18:24:37.067] INFO/WORKER: Initializing worker | workerID=worker-1 dataPath=/data/worker-1
[18:24:37.089] INFO/FRAGMENT: Executing query fragment | workerID=worker-1 fragmentID=scan_fragment_0_agg sql=SELECT COUNT(*) activeQueries=1
[18:24:37.092] INFO/FRAGMENT: Fragment execution completed | workerID=worker-1 fragmentID=scan_fragment_0_agg duration=3.422ms rowsReturned=1
```

## Distributed Components

### Core Distributed Components

| Component | Purpose | Key Traces |
|-----------|---------|-------------|
| `COORDINATOR` | Central query orchestration | Query distribution, result aggregation, worker management |
| `WORKER` | Fragment execution | Worker initialization, fragment processing, resource monitoring |
| `FRAGMENT` | Individual query fragments | Fragment lifecycle, execution metrics, performance data |
| `PLANNING` | Distributed query planning | Plan generation, cost estimation, optimization decisions |
| `NETWORK` | Network communications | Data transfer, optimization, bandwidth usage |
| `AGGREGATION` | Distributed aggregation | Partial aggregation, result combination, optimization effectiveness |
| `PARTITIONING` | Data distribution | Partition strategies, load balancing, data locality |
| `MONITORING` | System observability | Health checks, performance metrics, resource utilization |

### Component Relationships

```
┌─────────────────┐
│   COORDINATOR   │ ← Orchestrates entire distributed query
├─────────────────┤
│ • Query parsing │
│ • Plan creation │ ← PLANNING component
│ • Worker mgmt   │
│ • Result agg    │ ← AGGREGATION component
└─────────┬───────┘
          │
    ┌─────┴─────┬─────────┬─────────┐
    v           v         v         v
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│WORKER-1 │ │WORKER-2 │ │WORKER-3 │ │WORKER-N │
├─────────┤ ├─────────┤ ├─────────┤ ├─────────┤
│FRAGMENT │ │FRAGMENT │ │FRAGMENT │ │FRAGMENT │ ← Fragment execution
│MONITOR  │ │MONITOR  │ │MONITOR  │ │MONITOR  │ ← Health monitoring
└─────────┘ └─────────┘ └─────────┘ └─────────┘
```

## Configuration

### Environment Variable Combinations

#### Production Monitoring
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,MONITORING
```
- Tracks high-level operations and system health
- Minimal performance impact
- Essential for production observability

#### Development Debugging
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=ALL
```
- Maximum visibility into all operations
- Includes detailed execution flow
- Use for comprehensive debugging

#### Performance Analysis
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=FRAGMENT,AGGREGATION,NETWORK
```
- Focus on execution performance
- Network optimization tracking
- Aggregation efficiency monitoring

#### Query Planning Debug
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=PLANNING,OPTIMIZER,PARTITIONING
```
- Detailed planning decisions
- Cost estimation insights
- Optimization strategy tracking

### Test-Specific Configuration

```bash
# Enable tracing in test framework
./distributed_sql_test_runner \
  -file tests/distributed_basic_queries.sql \
  -trace-level DEBUG \
  -trace-components COORDINATOR,WORKER,FRAGMENT \
  -verbose
```

## Common Scenarios

### 1. Debugging Query Performance Issues

**Problem**: Distributed queries running slower than expected

**Tracing Setup**:
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,FRAGMENT,AGGREGATION,NETWORK
```

**What to Look For**:
- Fragment execution times in `FRAGMENT` traces
- Network optimization effectiveness in `NETWORK` traces
- Aggregation efficiency in `AGGREGATION` traces

**Sample Analysis**:
```bash
# Look for slow fragments
grep "Fragment execution completed" trace.log | grep -E "duration=[5-9][0-9]+ms|duration=[0-9]+s"

# Check network optimization
grep "Data transfer optimized" trace.log | grep -v "reduction=99"

# Verify aggregation effectiveness
grep "Partial aggregation applied" trace.log
```

### 2. Worker Health Monitoring

**Problem**: Need to monitor worker status and resource usage

**Tracing Setup**:
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=WORKER,MONITORING
```

**Key Traces**:
- Worker initialization and configuration loading
- Resource utilization metrics
- Health check results
- Active query counts

**Sample Output**:
```
[18:24:37.030] INFO/WORKER: Initializing worker | workerID=worker-1 dataPath=/data/worker-1
[18:24:37.031] INFO/WORKER: Loaded table mappings | workerID=worker-1 configPath=/data/worker-1/table_mappings.json
[18:24:37.032] DEBUG/MONITORING: Worker health check | cpuUsage=15% memoryUsage=45% status=healthy
```

### 3. Network Optimization Analysis

**Problem**: Understanding network transfer efficiency

**Tracing Setup**:
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=NETWORK,AGGREGATION,FRAGMENT
```

**Key Metrics**:
- Original vs transferred data sizes
- Optimization effectiveness percentages
- Partial aggregation impact

**Sample Analysis**:
```bash
# Check network savings
grep "Data transfer optimized" trace.log

# Verify partial aggregation
grep "Partial aggregation applied" trace.log

# Fragment data volumes
grep "Fragment execution completed" trace.log | grep "bytesRead"
```

### 4. Query Planning Deep Dive

**Problem**: Understanding why certain planning decisions were made

**Tracing Setup**:
```bash
export BYTEDB_TRACE_LEVEL=VERBOSE
export BYTEDB_TRACE_COMPONENTS=PLANNING,OPTIMIZER,PARTITIONING
```

**Analysis Points**:
- Cost estimation accuracy
- Optimization strategy selection
- Partition distribution decisions
- Fragment creation logic

## Performance Analysis

### Execution Timeline Analysis

Use tracing to build execution timelines:

```bash
# Extract timing information
grep -E "(COORDINATOR|FRAGMENT)" trace.log | \
  grep -E "(Executing|completed)" | \
  sort -k1,2
```

### Network Efficiency Metrics

```bash
# Calculate average network savings
grep "Data transfer optimized" trace.log | \
  awk -F'reduction=' '{print $2}' | \
  awk -F'%' '{sum+=$1; count++} END {print "Average reduction:", sum/count "%"}'
```

### Worker Load Distribution

```bash
# Analyze fragment distribution across workers
grep "Fragment execution completed" trace.log | \
  awk -F'workerID=' '{print $2}' | \
  awk -F' ' '{print $1}' | \
  sort | uniq -c
```

## Troubleshooting

### Common Issues and Solutions

#### Issue: No distributed traces appearing

**Check**:
1. Verify environment variables are set correctly
2. Ensure distributed test runner is being used
3. Check component names are spelled correctly

```bash
echo "Level: $BYTEDB_TRACE_LEVEL"
echo "Components: $BYTEDB_TRACE_COMPONENTS"
```

#### Issue: Too much trace output

**Solution**: Use more specific component filtering
```bash
# Instead of ALL, use specific components
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,FRAGMENT
```

#### Issue: Missing fragment execution details

**Solution**: Enable FRAGMENT component with DEBUG level
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=FRAGMENT,WORKER
```

#### Issue: Network optimization not showing

**Solution**: Enable NETWORK and AGGREGATION components
```bash
export BYTEDB_TRACE_COMPONENTS=NETWORK,AGGREGATION,FRAGMENT
```

### Debug Techniques

#### 1. Layered Debugging
Start with high-level components, then add detail:

```bash
# Step 1: High-level overview
export BYTEDB_TRACE_COMPONENTS=COORDINATOR

# Step 2: Add worker details
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,WORKER

# Step 3: Add fragment execution
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,WORKER,FRAGMENT

# Step 4: Full visibility
export BYTEDB_TRACE_COMPONENTS=ALL
```

#### 2. Selective Trace Analysis

```bash
# Filter coordinator operations only
grep "COORDINATOR" trace.log

# Focus on specific worker
grep "workerID=worker-1" trace.log

# Analyze fragment performance
grep "Fragment execution completed" trace.log | sort -k7 -nr
```

#### 3. Timing Analysis

```bash
# Extract execution timeline
grep -E "\[(.*)\].*/(COORDINATOR|FRAGMENT)" trace.log | \
  sed 's/\[//' | sed 's/\]//' | \
  sort -k1,1
```

## Best Practices

### 1. Production Environment

- Use `INFO` level for normal operations
- Enable `COORDINATOR` and `MONITORING` components
- Set up log rotation for trace files
- Monitor trace volume and performance impact

```bash
# Production-safe configuration
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,MONITORING
```

### 2. Development Environment

- Use `DEBUG` or `VERBOSE` levels for detailed debugging
- Enable all relevant components for comprehensive analysis
- Use component filtering to focus on specific issues

```bash
# Development configuration
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=ALL
```

### 3. Performance Testing

- Enable performance-related components
- Use `INFO` level to minimize tracing overhead
- Focus on timing and resource utilization

```bash
# Performance testing configuration
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=FRAGMENT,NETWORK,AGGREGATION,MONITORING
```

### 4. Integration with Monitoring Systems

#### Prometheus Integration
```bash
# Enable monitoring component for metrics export
export BYTEDB_TRACE_COMPONENTS=MONITORING
# Configure Prometheus scraper to collect metrics
```

#### Log Analysis Tools
```bash
# Format for ELK stack
grep "INFO/COORDINATOR" trace.log | jq -R 'split(" | ") | {timestamp: .[0], component: .[1], message: .[2]}'

# Format for Grafana
grep -E "(FRAGMENT|NETWORK)" trace.log | grep "duration="
```

### 5. Testing Best Practices

- Use tracing to validate distributed functionality
- Compare single-node vs distributed execution
- Verify optimization effectiveness
- Monitor resource utilization across workers

```bash
# Comprehensive test with tracing
./distributed_sql_test_runner \
  -dir tests \
  -trace-level DEBUG \
  -trace-components COORDINATOR,WORKER,FRAGMENT,AGGREGATION \
  -verbose \
  2>&1 | tee distributed_test_trace.log
```

## Advanced Usage

### Custom Trace Analysis Scripts

Create scripts to analyze specific patterns:

```bash
#!/bin/bash
# analyze_performance.sh

echo "=== Fragment Execution Analysis ==="
grep "Fragment execution completed" $1 | \
  awk -F'duration=' '{print $2}' | \
  awk -F'ms' '{sum+=$1; count++; if($1>max) max=$1; if(min=="" || $1<min) min=$1} END {
    printf "Average: %.2fms\n", sum/count
    printf "Max: %.2fms\n", max
    printf "Min: %.2fms\n", min
  }'

echo "=== Network Optimization Analysis ==="
grep "Data transfer optimized" $1 | \
  awk -F'reduction=' '{print $2}' | \
  awk -F'%' '{sum+=$1; count++} END {
    printf "Average reduction: %.2f%%\n", sum/count
  }'

echo "=== Worker Distribution ==="
grep "Fragment execution completed" $1 | \
  awk -F'workerID=' '{print $2}' | \
  awk -F' ' '{print $1}' | \
  sort | uniq -c | \
  awk '{printf "Worker %s: %d fragments\n", $2, $1}'
```

Usage:
```bash
chmod +x analyze_performance.sh
./analyze_performance.sh distributed_test_trace.log
```

### Real-time Monitoring

Monitor distributed system in real-time:

```bash
# Real-time coordinator monitoring
tail -f trace.log | grep "COORDINATOR"

# Real-time performance monitoring
tail -f trace.log | grep -E "(Fragment execution completed|Data transfer optimized)"

# Real-time worker health
tail -f trace.log | grep "MONITORING" | grep "health check"
```

This comprehensive distributed tracing system provides the visibility needed to effectively develop, debug, and optimize ByteDB's distributed query engine.