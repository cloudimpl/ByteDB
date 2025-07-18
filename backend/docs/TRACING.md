# ByteDB Tracing System

The ByteDB tracing system provides comprehensive visibility into query execution, optimization decisions, and internal operations. This is essential for debugging, performance analysis, and understanding query behavior.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Trace Levels](#trace-levels)
- [Components](#components)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

The ByteDB tracing system replaces ad-hoc debug statements with a structured, configurable logging system that can be dynamically controlled via environment variables. It provides:

- **Structured Logging**: Timestamped entries with component identification
- **Dynamic Control**: Enable/disable tracing without code changes
- **Performance Focused**: Minimal overhead when disabled
- **Component Granularity**: Trace specific system components
- **Context Rich**: Detailed contextual information for each trace entry

## Quick Start

### Enable Basic Tracing

```bash
# Enable INFO level tracing for all components
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=ALL

# Run your ByteDB application
go run main.go
```

### Enable Specific Component Tracing

```bash
# Debug CASE expressions and optimization decisions
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=CASE,OPTIMIZER

# Run your application
go run main.go
```

### Sample Output

```
[15:04:05.123] INFO/QUERY: Starting query execution | sql=SELECT * FROM employees
[15:04:05.124] DEBUG/PARSER: Successfully parsed SQL query | table=employees type=SELECT
[15:04:05.125] INFO/OPTIMIZER: Skipping optimization due to CASE expression ORDER BY | table=employees
[15:04:05.126] DEBUG/CASE: WHEN clause matched | clause_index=1 result=Medium alias=salary_grade
```

## Configuration

### Environment Variables

The tracing system is configured through environment variables:

| Variable | Description | Values | Default |
|----------|-------------|--------|---------|
| `BYTEDB_TRACE_LEVEL` | Sets the minimum trace level | `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `VERBOSE` | `OFF` |
| `BYTEDB_TRACE_COMPONENTS` | Specifies which components to trace | Component names (comma-separated) or `ALL` | None |

### Runtime Configuration

```go
// Get the global tracer
tracer := core.GetTracer()

// Set trace level programmatically
tracer.SetLevel(core.TraceLevelDebug)

// Enable specific components
tracer.EnableComponent(core.TraceComponentCase)
tracer.EnableComponent(core.TraceComponentOptimizer)

// Disable specific components
tracer.DisableComponent(core.TraceComponentParser)
```

## Trace Levels

Trace levels control the verbosity of logging output:

### TraceLevelOff (0)
- **Purpose**: Disables all tracing
- **Use Case**: Production environments
- **Performance**: Zero overhead

### TraceLevelError (1)
- **Purpose**: Critical errors only
- **Use Case**: Production error monitoring
- **Example**: Parse failures, execution errors

### TraceLevelWarn (2)
- **Purpose**: Warning conditions
- **Use Case**: Potential issues, deprecated features
- **Example**: Optimization fallbacks, compatibility warnings

### TraceLevelInfo (3)
- **Purpose**: General information
- **Use Case**: High-level operation tracking
- **Example**: Query start/end, cache hits/misses

### TraceLevelDebug (4)
- **Purpose**: Detailed execution information
- **Use Case**: Development debugging
- **Example**: Step-by-step execution, intermediate results

### TraceLevelVerbose (5)
- **Purpose**: Maximum detail
- **Use Case**: Deep debugging, performance analysis
- **Example**: Every condition evaluation, all internal state changes

## Components

The tracing system is organized by components, allowing granular control:

### TraceComponentQuery
- **Purpose**: Overall query lifecycle
- **Traces**: Query start, completion, errors
- **Example**: `Starting query execution | sql=SELECT ...`

### TraceComponentParser
- **Purpose**: SQL parsing operations
- **Traces**: Parse success/failure, AST construction
- **Example**: `Successfully parsed SQL query | table=employees type=SELECT`

### TraceComponentOptimizer
- **Purpose**: Query optimization decisions
- **Traces**: Optimization attempts, skips, plan changes
- **Example**: `Skipping optimization due to CASE expression ORDER BY`

### TraceComponentExecution
- **Purpose**: Query execution phases
- **Traces**: Execution plan steps, row processing
- **Example**: `Executing scan node | table=employees rows=100`

### TraceComponentCase
- **Purpose**: CASE expression evaluation
- **Traces**: WHEN clause evaluation, results
- **Example**: `WHEN clause matched | clause_index=1 result=Medium`

### TraceComponentSort
- **Purpose**: Sorting operations
- **Traces**: Sort algorithm, comparison operations
- **Example**: `Sorting rows | column=salary_grade order=DESC count=10`

### TraceComponentJoin
- **Purpose**: Join operations
- **Traces**: Join algorithm, hash table construction
- **Example**: `Hash join completed | left_rows=100 right_rows=50 result_rows=75`

### TraceComponentFilter
- **Purpose**: WHERE clause processing
- **Traces**: Filter conditions, row elimination
- **Example**: `Applied filter | condition=salary>50000 filtered_rows=25`

### TraceComponentAggregate
- **Purpose**: GROUP BY and aggregation functions
- **Traces**: Grouping operations, aggregate calculations
- **Example**: `Aggregate function applied | function=COUNT result=42`

### TraceComponentCache
- **Purpose**: Query result caching
- **Traces**: Cache hits, misses, evictions
- **Example**: `Cache miss for query | sql=SELECT ...`

## Distributed Components

### TraceComponentDistributed
- **Purpose**: General distributed operations
- **Traces**: Cluster operations, coordination activities
- **Example**: `Distributed cluster initialized | workers=3 transport=memory`

### TraceComponentCoordinator
- **Purpose**: Coordinator-specific operations
- **Traces**: Worker management, query distribution, result aggregation
- **Example**: `Executing distributed query | requestID=12345 sql=SELECT COUNT(*) workers=3`

### TraceComponentWorker
- **Purpose**: Worker-specific operations
- **Traces**: Worker initialization, status updates, resource monitoring
- **Example**: `Worker initialized | workerID=worker-1 dataPath=/data/worker-1`

### TraceComponentPlanning
- **Purpose**: Distributed query planning
- **Traces**: Plan generation, cost estimation, optimization decisions
- **Example**: `Creating distributed plan | fragments=3 estimatedCost=1024 optimization=partial_agg`

### TraceComponentFragment
- **Purpose**: Query fragment execution
- **Traces**: Fragment lifecycle, execution statistics, performance metrics
- **Example**: `Fragment execution completed | fragmentID=scan_0 duration=250ms rows=1000`

### TraceComponentNetwork
- **Purpose**: Network communications
- **Traces**: Data transfer, connection management, network optimization
- **Example**: `Data transfer optimized | originalBytes=1048576 transferredBytes=128 reduction=99.99%`

### TraceComponentPartitioning
- **Purpose**: Data partitioning strategies
- **Traces**: Partition assignment, data distribution, load balancing
- **Example**: `Data partitioned across workers | strategy=round_robin workers=3 fragments=6`

### TraceComponentAggregation
- **Purpose**: Distributed aggregation operations
- **Traces**: Partial aggregation, result combination, optimization effectiveness
- **Example**: `Partial aggregation applied | function=COUNT partialResults=3 finalResult=1000`

### TraceComponentMonitoring
- **Purpose**: System monitoring and metrics
- **Traces**: Performance metrics, health checks, resource utilization
- **Example**: `Worker health check completed | cpuUsage=45% memoryUsage=60% status=healthy`

## Usage Examples

### Debugging CASE Expression Issues

```bash
# Enable detailed CASE expression tracing
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=CASE,EXECUTION

# Run query with CASE expression
```

**Output:**
```
[15:04:05.126] DEBUG/CASE: Evaluating CASE expression | alias=salary_grade when_clauses=2
[15:04:05.126] DEBUG/CASE: WHEN clause matched | clause_index=1 result=Medium alias=salary_grade
[15:04:05.127] DEBUG/EXECUTION: Column evaluated | name=salary_grade value=Medium
```

### Performance Analysis

```bash
# Enable optimization and cache tracing
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=OPTIMIZER,CACHE,EXECUTION

# Run queries to analyze performance
```

**Output:**
```
[15:04:05.123] DEBUG/CACHE: Cache miss for query | sql=SELECT * FROM employees
[15:04:05.124] INFO/OPTIMIZER: Using optimized execution path | table=employees
[15:04:05.125] INFO/EXECUTION: Query completed | duration=25ms rows=100
```

### Parser Debugging

```bash
# Debug SQL parsing issues
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=PARSER

# Run query with complex SQL
```

**Output:**
```
[15:04:05.120] DEBUG/PARSER: Parsing SQL query | sql=SELECT CASE WHEN...
[15:04:05.122] DEBUG/PARSER: Parsed CASE expression | alias=salary_grade clauses=2
[15:04:05.123] DEBUG/PARSER: Successfully parsed SQL query | table=employees type=SELECT
```

## API Reference

### Tracer Interface

```go
type Tracer struct {
    // Global tracer access
    func GetTracer() *Tracer
    
    // Configuration
    func (t *Tracer) SetLevel(level TraceLevel)
    func (t *Tracer) EnableComponent(component TraceComponent)
    func (t *Tracer) DisableComponent(component TraceComponent)
    
    // Logging methods
    func (t *Tracer) Error(component TraceComponent, message string, context ...map[string]interface{})
    func (t *Tracer) Warn(component TraceComponent, message string, context ...map[string]interface{})
    func (t *Tracer) Info(component TraceComponent, message string, context ...map[string]interface{})
    func (t *Tracer) Debug(component TraceComponent, message string, context ...map[string]interface{})
    func (t *Tracer) Verbose(component TraceComponent, message string, context ...map[string]interface{})
    
    // Management
    func (t *Tracer) GetEntries() []TraceEntry
    func (t *Tracer) Clear()
    func (t *Tracer) GetStatus() map[string]interface{}
}
```

### Helper Functions

```go
// Create context for trace entries
func TraceContext(pairs ...interface{}) map[string]interface{}

// Example usage
tracer.Debug(TraceComponentCase, "CASE evaluation", 
    TraceContext("alias", "salary_grade", "result", "Medium"))
```

### Adding Tracing to New Code

```go
func (qe *QueryEngine) someNewFunction(param string) error {
    tracer := GetTracer()
    
    tracer.Debug(TraceComponentExecution, "Starting new function", 
        TraceContext("param", param))
    
    // ... function logic ...
    
    if err != nil {
        tracer.Error(TraceComponentExecution, "Function failed", 
            TraceContext("param", param, "error", err.Error()))
        return err
    }
    
    tracer.Info(TraceComponentExecution, "Function completed successfully", 
        TraceContext("param", param))
    return nil
}
```

## Best Practices

### Development Guidelines

1. **Use Appropriate Levels**
   - `Error`: Only for actual errors
   - `Warn`: For concerning but non-fatal conditions
   - `Info`: For important milestones
   - `Debug`: For detailed execution flow
   - `Verbose`: For maximum detail during deep debugging

2. **Provide Context**
   ```go
   // Good: Rich context
   tracer.Debug(TraceComponentCase, "WHEN clause matched", 
       TraceContext("clause_index", i, "result", result, "alias", alias))
   
   // Bad: Minimal context
   tracer.Debug(TraceComponentCase, "WHEN clause matched")
   ```

3. **Component Selection**
   - Choose the most specific component
   - Use `TraceComponentExecution` for general execution flow
   - Create new components for major subsystems

4. **Message Format**
   - Use present tense: "Evaluating CASE expression"
   - Be concise but descriptive
   - Include key identifiers in the message

### Performance Considerations

1. **Check Before Expensive Operations**
   ```go
   if tracer.IsEnabled(TraceLevelDebug, TraceComponentCase) {
       // Only compute expensive context if tracing is enabled
       context := computeExpensiveContext()
       tracer.Debug(TraceComponentCase, "Complex operation", context)
   }
   ```

2. **Batch Context Creation**
   ```go
   context := TraceContext(
       "param1", value1,
       "param2", value2,
       "param3", value3,
   )
   tracer.Debug(component, message, context)
   ```

### Production Usage

1. **Default to OFF**
   - Production systems should default to `TraceLevelOff`
   - Enable specific components only when debugging

2. **Monitor Resource Usage**
   - Tracing stores up to 1000 entries by default
   - Clear entries periodically in long-running processes
   - Monitor memory usage with high-volume tracing

3. **Security Considerations**
   - Avoid logging sensitive data in context
   - Be careful with SQL queries containing credentials
   - Consider log output destinations in secure environments

## Troubleshooting

### Common Issues

**Q: Tracing is not showing any output**
```bash
# Verify environment variables are set
echo $BYTEDB_TRACE_LEVEL
echo $BYTEDB_TRACE_COMPONENTS

# Check if components are properly specified
export BYTEDB_TRACE_COMPONENTS=ALL  # Enable all components
```

**Q: Too much output, system is slow**
```bash
# Reduce trace level
export BYTEDB_TRACE_LEVEL=INFO

# Enable only specific components
export BYTEDB_TRACE_COMPONENTS=QUERY,OPTIMIZER
```

**Q: Missing traces from specific operations**
```bash
# Enable VERBOSE level for maximum detail
export BYTEDB_TRACE_LEVEL=VERBOSE

# Check if the component is enabled
export BYTEDB_TRACE_COMPONENTS=ALL
```

### Debugging Specific Scenarios

**CASE Expression Issues:**
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=CASE,PARSER,EXECUTION
```

**Performance Problems:**
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=OPTIMIZER,CACHE,EXECUTION
```

**Query Parsing Errors:**
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=PARSER,QUERY
```

**Join Operation Issues:**
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=JOIN,EXECUTION
```

## Distributed Tracing Examples

### Debugging Distributed Query Performance

```bash
# Enable comprehensive distributed tracing
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,WORKER,FRAGMENT,AGGREGATION

# Run distributed query
./distributed_sql_test_runner -file tests/distributed_basic_queries.sql -verbose
```

**Output:**
```
[18:24:37.043] INFO/COORDINATOR: Initializing distributed coordinator
[18:24:37.066] INFO/COORDINATOR: Executing distributed query | requestID=test-123 sql=SELECT COUNT(*) FROM employees timeout=30s
[18:24:37.067] INFO/WORKER: Initializing worker | workerID=worker-1 dataPath=/data/worker-1
[18:24:37.089] INFO/FRAGMENT: Executing query fragment | workerID=worker-1 fragmentID=scan_fragment_0_agg sql=SELECT COUNT(*) as total_count FROM employees activeQueries=1
[18:24:37.092] INFO/FRAGMENT: Fragment execution completed | workerID=worker-1 fragmentID=scan_fragment_0_agg duration=3.422ms rowsReturned=1 bytesRead=4096 cacheHits=0 cacheMisses=1
[18:24:37.094] INFO/AGGREGATION: Partial aggregation applied | function=COUNT partialResults=3 finalResult=10
```

### Monitoring Worker Health and Performance

```bash
# Monitor worker performance and health
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=WORKER,MONITORING,NETWORK

# Run with worker monitoring
./distributed_sql_test_runner -workers 3 -transport memory -verbose
```

**Output:**
```
[18:24:37.030] INFO/WORKER: Initializing worker | workerID=test-worker-1 dataPath=./data/worker-1
[18:24:37.031] INFO/WORKER: Loaded table mappings | workerID=test-worker-1 configPath=./data/worker-1/table_mappings.json
[18:24:37.032] DEBUG/MONITORING: Worker health check completed | cpuUsage=15% memoryUsage=45% status=healthy
[18:24:37.094] DEBUG/NETWORK: Data transfer optimized | originalBytes=1048576 transferredBytes=256 reduction=99.98%
```

### Tracing Query Planning and Optimization

```bash
# Debug distributed query planning
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=PLANNING,OPTIMIZER,PARTITIONING

# Run complex distributed query
```

**Output:**
```
[18:24:37.055] DEBUG/PLANNING: Creating distributed plan | fragments=3 estimatedCost=610.46 optimization=partial_aggregation
[18:24:37.056] DEBUG/PARTITIONING: Data partitioned across workers | strategy=physical_partitioning workers=3 fragments=3
[18:24:37.057] DEBUG/OPTIMIZER: Applied query optimization | type=partial_aggregation costReduction=40% networkSavings=99.99%
```

### Comprehensive Distributed System Debugging

```bash
# Enable all distributed components for maximum visibility
export BYTEDB_TRACE_LEVEL=VERBOSE
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,WORKER,FRAGMENT,PLANNING,NETWORK,AGGREGATION,MONITORING

# Run distributed tests
./distributed_sql_test_runner -dir tests -verbose
```

This will provide complete visibility into:
- Coordinator query processing and worker management
- Worker fragment execution and resource utilization
- Network optimization and data transfer efficiency
- Query planning decisions and cost optimizations
- Aggregation strategies and result combination
- System health and performance monitoring

### Environment Variable Examples for Different Scenarios

**Production Monitoring:**
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=COORDINATOR,MONITORING
```

**Development Debugging:**
```bash
export BYTEDB_TRACE_LEVEL=DEBUG
export BYTEDB_TRACE_COMPONENTS=ALL
```

**Performance Analysis:**
```bash
export BYTEDB_TRACE_LEVEL=INFO
export BYTEDB_TRACE_COMPONENTS=FRAGMENT,AGGREGATION,NETWORK
```

**Worker Troubleshooting:**
```bash
export BYTEDB_TRACE_LEVEL=VERBOSE
export BYTEDB_TRACE_COMPONENTS=WORKER,FRAGMENT,MONITORING
```

### Getting Help

If you encounter issues with the tracing system:

1. Check environment variable configuration
2. Verify component names are correct (case-sensitive)
3. Try with `TraceLevelVerbose` and `ALL` components first
4. Check the tracer status: `tracer.GetStatus()`
5. Review recent entries: `tracer.GetEntries()`

For additional support, please refer to the main ByteDB documentation or file an issue in the project repository.