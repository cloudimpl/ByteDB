package monitoring

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// MetricType represents different types of metrics
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeTimer     MetricType = "timer"
)

// Metric represents a single metric with its metadata
type Metric struct {
	Name        string                 `json:"name"`
	Type        MetricType             `json:"type"`
	Value       interface{}            `json:"value"`
	Labels      map[string]string      `json:"labels"`
	Description string                 `json:"description"`
	Timestamp   time.Time              `json:"timestamp"`
	Unit        string                 `json:"unit"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// CounterMetric tracks incremental values
type CounterMetric struct {
	mu    sync.RWMutex
	value int64
}

func (c *CounterMetric) Inc() {
	c.Add(1)
}

func (c *CounterMetric) Add(delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += delta
}

func (c *CounterMetric) Get() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

func (c *CounterMetric) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = 0
}

// GaugeMetric tracks point-in-time values
type GaugeMetric struct {
	mu    sync.RWMutex
	value float64
}

func (g *GaugeMetric) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

func (g *GaugeMetric) Add(delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value += delta
}

func (g *GaugeMetric) Get() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// HistogramMetric tracks distribution of values
type HistogramMetric struct {
	mu      sync.RWMutex
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

func NewHistogramMetric(buckets []float64) *HistogramMetric {
	sort.Float64s(buckets)
	return &HistogramMetric{
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1), // +1 for +Inf bucket
	}
}

func (h *HistogramMetric) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.sum += value
	h.count++
	
	// Find the appropriate bucket
	for i, bucket := range h.buckets {
		if value <= bucket {
			h.counts[i]++
			return
		}
	}
	// Value is greater than all buckets, goes to +Inf bucket
	h.counts[len(h.buckets)]++
}

func (h *HistogramMetric) GetStats() HistogramStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	stats := HistogramStats{
		Count:   h.count,
		Sum:     h.sum,
		Buckets: make(map[string]int64),
	}
	
	if h.count > 0 {
		stats.Mean = h.sum / float64(h.count)
	}
	
	for i, bucket := range h.buckets {
		stats.Buckets[fmt.Sprintf("%.1f", bucket)] = h.counts[i]
	}
	stats.Buckets["+Inf"] = h.counts[len(h.buckets)] // +Inf bucket
	
	return stats
}

type HistogramStats struct {
	Count   int64                    `json:"count"`
	Sum     float64                  `json:"sum"`
	Mean    float64                  `json:"mean"`
	Buckets map[string]int64         `json:"buckets"`
}

// TimerMetric tracks timing information
type TimerMetric struct {
	histogram *HistogramMetric
	startTime time.Time
	mu        sync.RWMutex
}

func NewTimerMetric() *TimerMetric {
	// Default buckets for timing (in milliseconds)
	buckets := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	return &TimerMetric{
		histogram: NewHistogramMetric(buckets),
	}
}

func (t *TimerMetric) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startTime = time.Now()
}

func (t *TimerMetric) Stop() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.startTime.IsZero() {
		return 0
	}
	
	duration := time.Since(t.startTime)
	t.histogram.Observe(float64(duration.Milliseconds()))
	t.startTime = time.Time{}
	
	return duration
}

func (t *TimerMetric) Time(fn func()) time.Duration {
	start := time.Now()
	fn()
	duration := time.Since(start)
	t.histogram.Observe(float64(duration.Milliseconds()))
	return duration
}

func (t *TimerMetric) GetStats() HistogramStats {
	return t.histogram.GetStats()
}

// MetricsRegistry manages all metrics in the system
type MetricsRegistry struct {
	mu       sync.RWMutex
	counters map[string]*CounterMetric
	gauges   map[string]*GaugeMetric
	histograms map[string]*HistogramMetric
	timers   map[string]*TimerMetric
	labels   map[string]map[string]string
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		counters:   make(map[string]*CounterMetric),
		gauges:     make(map[string]*GaugeMetric),
		histograms: make(map[string]*HistogramMetric),
		timers:     make(map[string]*TimerMetric),
		labels:     make(map[string]map[string]string),
	}
}

func (r *MetricsRegistry) Counter(name string) *CounterMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if counter, exists := r.counters[name]; exists {
		return counter
	}
	
	counter := &CounterMetric{}
	r.counters[name] = counter
	return counter
}

func (r *MetricsRegistry) Gauge(name string) *GaugeMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if gauge, exists := r.gauges[name]; exists {
		return gauge
	}
	
	gauge := &GaugeMetric{}
	r.gauges[name] = gauge
	return gauge
}

func (r *MetricsRegistry) Histogram(name string, buckets []float64) *HistogramMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if histogram, exists := r.histograms[name]; exists {
		return histogram
	}
	
	histogram := NewHistogramMetric(buckets)
	r.histograms[name] = histogram
	return histogram
}

func (r *MetricsRegistry) Timer(name string) *TimerMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if timer, exists := r.timers[name]; exists {
		return timer
	}
	
	timer := NewTimerMetric()
	r.timers[name] = timer
	return timer
}

func (r *MetricsRegistry) SetLabels(name string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.labels[name] = labels
}

func (r *MetricsRegistry) GetAllMetrics() []Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	var metrics []Metric
	now := time.Now()
	
	// Counters
	for name, counter := range r.counters {
		metrics = append(metrics, Metric{
			Name:      name,
			Type:      MetricTypeCounter,
			Value:     counter.Get(),
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "count",
		})
	}
	
	// Gauges
	for name, gauge := range r.gauges {
		metrics = append(metrics, Metric{
			Name:      name,
			Type:      MetricTypeGauge,
			Value:     gauge.Get(),
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "value",
		})
	}
	
	// Histograms
	for name, histogram := range r.histograms {
		stats := histogram.GetStats()
		metrics = append(metrics, Metric{
			Name:      name,
			Type:      MetricTypeHistogram,
			Value:     stats,
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "distribution",
		})
	}
	
	// Timers
	for name, timer := range r.timers {
		stats := timer.GetStats()
		metrics = append(metrics, Metric{
			Name:      name,
			Type:      MetricTypeTimer,
			Value:     stats,
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "milliseconds",
		})
	}
	
	return metrics
}

func (r *MetricsRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	for _, counter := range r.counters {
		counter.Reset()
	}
	
	// Note: Gauges, histograms, and timers are not reset as they represent current state/distributions
}

func (r *MetricsRegistry) GetMetric(name string) (Metric, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	now := time.Now()
	
	if counter, exists := r.counters[name]; exists {
		return Metric{
			Name:      name,
			Type:      MetricTypeCounter,
			Value:     counter.Get(),
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "count",
		}, true
	}
	
	if gauge, exists := r.gauges[name]; exists {
		return Metric{
			Name:      name,
			Type:      MetricTypeGauge,
			Value:     gauge.Get(),
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "value",
		}, true
	}
	
	if histogram, exists := r.histograms[name]; exists {
		stats := histogram.GetStats()
		return Metric{
			Name:      name,
			Type:      MetricTypeHistogram,
			Value:     stats,
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "distribution",
		}, true
	}
	
	if timer, exists := r.timers[name]; exists {
		stats := timer.GetStats()
		return Metric{
			Name:      name,
			Type:      MetricTypeTimer,
			Value:     stats,
			Labels:    r.labels[name],
			Timestamp: now,
			Unit:      "milliseconds",
		}, true
	}
	
	return Metric{}, false
}

// Global metrics registry
var GlobalRegistry = NewMetricsRegistry()

// Convenience functions for global registry
func Counter(name string) *CounterMetric {
	return GlobalRegistry.Counter(name)
}

func Gauge(name string) *GaugeMetric {
	return GlobalRegistry.Gauge(name)
}

func Histogram(name string, buckets []float64) *HistogramMetric {
	return GlobalRegistry.Histogram(name, buckets)
}

func Timer(name string) *TimerMetric {
	return GlobalRegistry.Timer(name)
}

func SetLabels(name string, labels map[string]string) {
	GlobalRegistry.SetLabels(name, labels)
}

func GetAllMetrics() []Metric {
	return GlobalRegistry.GetAllMetrics()
}

func GetMetric(name string) (Metric, bool) {
	return GlobalRegistry.GetMetric(name)
}

// Helper function to format metric for logging
func (m Metric) String() string {
	labels := ""
	if len(m.Labels) > 0 {
		labels = fmt.Sprintf(" %v", m.Labels)
	}
	return fmt.Sprintf("%s%s: %v %s", m.Name, labels, m.Value, m.Unit)
}