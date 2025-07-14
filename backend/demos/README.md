# ByteDB Demos

This directory contains various demonstration programs and examples for ByteDB features.

## Directory Structure

- `benchmark_demo.go` - Performance benchmarking demonstrations
- `demo_http_usage.go` - HTTP API usage examples
- `developer_demo/` - Developer tools demonstrations
- `distributed_demo.go` - Distributed query execution examples
- `distributed_monitoring_demo.go` - Distributed system monitoring
- `gen_data.go` - Sample data generation utility
- `integration_demo/` - Integration examples
- `metrics_demo/` - Metrics collection demonstrations
- `optimizer_demo.go` - Query optimizer demonstrations
- `real_performance_demo/` - Real performance comparison demos
- `realistic_benchmark.go` - Realistic benchmark scenarios
- `simd_demo/` - SIMD optimization demonstrations
- `simd_performance_demo.go` - SIMD performance comparisons
- `simd_simple_demo.go` - Simple SIMD examples
- `simple_optimizer_demo.go` - Simple optimizer examples
- `vectorized_demo/` - Vectorized execution demonstrations
- `visual_optimizer_demo.go` - Visual query optimizer demo
- `bandwidth_measurement_demo.go` - Bandwidth measurement examples

## Running Demos

Each demo can be run independently. For example:

```bash
# Run from the bytedb root directory
go run demos/demo_http_usage.go

# Or run a demo from a subdirectory
go run demos/vectorized_demo/vectorized_demo.go
```

## Test Files

Some files in this directory are test files (`*_test.go`) that demonstrate testing approaches and benchmarking techniques.

## Note

All executable Go programs (with `func main()`) except the main ByteDB binary have been moved to this directory to keep the project structure clean and organized.