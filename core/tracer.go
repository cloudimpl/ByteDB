package core

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// TraceLevel represents different levels of tracing
type TraceLevel int

const (
	TraceLevelOff TraceLevel = iota
	TraceLevelError
	TraceLevelWarn
	TraceLevelInfo
	TraceLevelDebug
	TraceLevelVerbose
)

// String returns the string representation of TraceLevel
func (tl TraceLevel) String() string {
	switch tl {
	case TraceLevelOff:
		return "OFF"
	case TraceLevelError:
		return "ERROR"
	case TraceLevelWarn:
		return "WARN"
	case TraceLevelInfo:
		return "INFO"
	case TraceLevelDebug:
		return "DEBUG"
	case TraceLevelVerbose:
		return "VERBOSE"
	default:
		return "UNKNOWN"
	}
}

// TraceComponent represents different components that can be traced
type TraceComponent string

const (
	TraceComponentQuery       TraceComponent = "QUERY"
	TraceComponentParser      TraceComponent = "PARSER"
	TraceComponentOptimizer   TraceComponent = "OPTIMIZER"
	TraceComponentExecution   TraceComponent = "EXECUTION"
	TraceComponentCase        TraceComponent = "CASE"
	TraceComponentSort        TraceComponent = "SORT"
	TraceComponentJoin        TraceComponent = "JOIN"
	TraceComponentFilter      TraceComponent = "FILTER"
	TraceComponentAggregate   TraceComponent = "AGGREGATE"
	TraceComponentCache       TraceComponent = "CACHE"
	// Distributed components
	TraceComponentDistributed    TraceComponent = "DISTRIBUTED"
	TraceComponentCoordinator    TraceComponent = "COORDINATOR"
	TraceComponentWorker         TraceComponent = "WORKER"
	TraceComponentPlanning       TraceComponent = "PLANNING"
	TraceComponentFragment       TraceComponent = "FRAGMENT"
	TraceComponentNetwork        TraceComponent = "NETWORK"
	TraceComponentPartitioning   TraceComponent = "PARTITIONING"
	TraceComponentAggregation    TraceComponent = "AGGREGATION"
	TraceComponentMonitoring     TraceComponent = "MONITORING"
)

// TraceEntry represents a single trace entry
type TraceEntry struct {
	Timestamp time.Time
	Level     TraceLevel
	Component TraceComponent
	Message   string
	Context   map[string]interface{}
}

// Tracer provides comprehensive tracing for ByteDB operations
type Tracer struct {
	level           TraceLevel
	enabledComponents map[TraceComponent]bool
	mutex           sync.RWMutex
	entries         []TraceEntry
	maxEntries      int
}

// Global tracer instance
var globalTracer *Tracer
var tracerOnce sync.Once

// GetTracer returns the global tracer instance
func GetTracer() *Tracer {
	tracerOnce.Do(func() {
		globalTracer = NewTracer()
	})
	return globalTracer
}

// NewTracer creates a new tracer with configuration from environment variables
func NewTracer() *Tracer {
	tracer := &Tracer{
		level:             TraceLevelOff,
		enabledComponents: make(map[TraceComponent]bool),
		entries:           make([]TraceEntry, 0),
		maxEntries:        1000, // Keep last 1000 entries
	}
	
	tracer.configureFromEnv()
	return tracer
}

// configureFromEnv configures the tracer from environment variables
func (t *Tracer) configureFromEnv() {
	// Set trace level from BYTEDB_TRACE_LEVEL
	if levelStr := os.Getenv("BYTEDB_TRACE_LEVEL"); levelStr != "" {
		switch strings.ToUpper(levelStr) {
		case "OFF":
			t.level = TraceLevelOff
		case "ERROR":
			t.level = TraceLevelError
		case "WARN":
			t.level = TraceLevelWarn
		case "INFO":
			t.level = TraceLevelInfo
		case "DEBUG":
			t.level = TraceLevelDebug
		case "VERBOSE":
			t.level = TraceLevelVerbose
		}
	}
	
	// Set enabled components from BYTEDB_TRACE_COMPONENTS (comma-separated)
	if componentsStr := os.Getenv("BYTEDB_TRACE_COMPONENTS"); componentsStr != "" {
		if strings.ToUpper(componentsStr) == "ALL" {
			// Enable all components
			for _, comp := range []TraceComponent{
				TraceComponentQuery, TraceComponentParser, TraceComponentOptimizer,
				TraceComponentExecution, TraceComponentCase, TraceComponentSort,
				TraceComponentJoin, TraceComponentFilter, TraceComponentAggregate,
				TraceComponentCache, TraceComponentDistributed, TraceComponentCoordinator,
				TraceComponentWorker, TraceComponentPlanning, TraceComponentFragment,
				TraceComponentNetwork, TraceComponentPartitioning, TraceComponentAggregation,
				TraceComponentMonitoring,
			} {
				t.enabledComponents[comp] = true
			}
		} else {
			// Enable specific components
			components := strings.Split(componentsStr, ",")
			for _, comp := range components {
				t.enabledComponents[TraceComponent(strings.TrimSpace(strings.ToUpper(comp)))] = true
			}
		}
	}
}

// SetLevel sets the trace level
func (t *Tracer) SetLevel(level TraceLevel) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.level = level
}

// EnableComponent enables tracing for a specific component
func (t *Tracer) EnableComponent(component TraceComponent) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.enabledComponents[component] = true
}

// DisableComponent disables tracing for a specific component
func (t *Tracer) DisableComponent(component TraceComponent) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.enabledComponents[component] = false
}

// IsEnabled checks if tracing is enabled for a given level and component
func (t *Tracer) IsEnabled(level TraceLevel, component TraceComponent) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	return t.level >= level && t.enabledComponents[component]
}

// trace adds a trace entry if the level and component are enabled
func (t *Tracer) trace(level TraceLevel, component TraceComponent, message string, context map[string]interface{}) {
	if !t.IsEnabled(level, component) {
		return
	}
	
	entry := TraceEntry{
		Timestamp: time.Now(),
		Level:     level,
		Component: component,
		Message:   message,
		Context:   context,
	}
	
	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	// Add entry
	t.entries = append(t.entries, entry)
	
	// Trim if exceeding max entries
	if len(t.entries) > t.maxEntries {
		t.entries = t.entries[len(t.entries)-t.maxEntries:]
	}
	
	// Print to stdout if enabled
	t.printEntry(entry)
}

// printEntry prints a trace entry to stdout
func (t *Tracer) printEntry(entry TraceEntry) {
	timestamp := entry.Timestamp.Format("15:04:05.000")
	fmt.Printf("[%s] %s/%s: %s", timestamp, entry.Level, entry.Component, entry.Message)
	
	if len(entry.Context) > 0 {
		fmt.Printf(" |")
		for k, v := range entry.Context {
			fmt.Printf(" %s=%v", k, v)
		}
	}
	fmt.Println()
}

// Error logs an error-level trace
func (t *Tracer) Error(component TraceComponent, message string, context ...map[string]interface{}) {
	ctx := make(map[string]interface{})
	if len(context) > 0 {
		ctx = context[0]
	}
	t.trace(TraceLevelError, component, message, ctx)
}

// Warn logs a warning-level trace
func (t *Tracer) Warn(component TraceComponent, message string, context ...map[string]interface{}) {
	ctx := make(map[string]interface{})
	if len(context) > 0 {
		ctx = context[0]
	}
	t.trace(TraceLevelWarn, component, message, ctx)
}

// Info logs an info-level trace
func (t *Tracer) Info(component TraceComponent, message string, context ...map[string]interface{}) {
	ctx := make(map[string]interface{})
	if len(context) > 0 {
		ctx = context[0]
	}
	t.trace(TraceLevelInfo, component, message, ctx)
}

// Debug logs a debug-level trace
func (t *Tracer) Debug(component TraceComponent, message string, context ...map[string]interface{}) {
	ctx := make(map[string]interface{})
	if len(context) > 0 {
		ctx = context[0]
	}
	t.trace(TraceLevelDebug, component, message, ctx)
}

// Verbose logs a verbose-level trace
func (t *Tracer) Verbose(component TraceComponent, message string, context ...map[string]interface{}) {
	ctx := make(map[string]interface{})
	if len(context) > 0 {
		ctx = context[0]
	}
	t.trace(TraceLevelVerbose, component, message, ctx)
}

// GetEntries returns all trace entries
func (t *Tracer) GetEntries() []TraceEntry {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	// Return a copy to avoid race conditions
	entries := make([]TraceEntry, len(t.entries))
	copy(entries, t.entries)
	return entries
}

// Clear clears all trace entries
func (t *Tracer) Clear() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.entries = make([]TraceEntry, 0)
}

// GetStatus returns the current tracer status
func (t *Tracer) GetStatus() map[string]interface{} {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	return map[string]interface{}{
		"level":      t.level.String(),
		"components": t.enabledComponents,
		"entries":    len(t.entries),
		"maxEntries": t.maxEntries,
	}
}

// TraceContext creates a context map for tracing
func TraceContext(pairs ...interface{}) map[string]interface{} {
	context := make(map[string]interface{})
	for i := 0; i < len(pairs)-1; i += 2 {
		if key, ok := pairs[i].(string); ok {
			context[key] = pairs[i+1]
		}
	}
	return context
}