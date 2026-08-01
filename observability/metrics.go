package observability

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics 收敛 HTTP、Kafka、A2A、Run、Delegation 和 Loop 的低基数指标。
type Metrics struct {
	registry *prometheus.Registry

	httpRequests    *prometheus.CounterVec
	httpDuration    *prometheus.HistogramVec
	kafkaEvents     *prometheus.CounterVec
	runtimeEvents   *prometheus.CounterVec
	runtimeDuration *prometheus.HistogramVec
	runEvents       *prometheus.CounterVec
	runDuration     *prometheus.HistogramVec
	delegations     *prometheus.CounterVec
	a2aRequests     *prometheus.CounterVec
	loopEvents      *prometheus.CounterVec
	loopDuration    *prometheus.HistogramVec
}

// NewMetrics 创建隔离 Registry，避免测试之间污染全局 Prometheus 注册表。
func NewMetrics() (*Metrics, error) {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests handled by GoAI.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goai",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		kafkaEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "kafka",
			Name:      "events_total",
			Help:      "Kafka publish and consume events.",
		}, []string{"operation", "topic", "status"}),
		runtimeEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "runtime",
			Name:      "events_total",
			Help:      "Runtime coordination events.",
		}, []string{"operation", "status"}),
		runtimeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goai",
			Subsystem: "runtime",
			Name:      "duration_seconds",
			Help:      "Runtime coordination duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"operation", "status"}),
		runEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "run",
			Name:      "events_total",
			Help:      "Run execution events.",
		}, []string{"trigger_type", "status"}),
		runDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goai",
			Subsystem: "run",
			Name:      "duration_seconds",
			Help:      "Run execution duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"trigger_type", "status"}),
		delegations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "delegation",
			Name:      "events_total",
			Help:      "Delegation lifecycle events.",
		}, []string{"status"}),
		a2aRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "a2a",
			Name:      "requests_total",
			Help:      "A2A requests by operation and outcome.",
		}, []string{"operation", "status"}),
		loopEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goai",
			Subsystem: "loop",
			Name:      "events_total",
			Help:      "Loop lifecycle events.",
		}, []string{"loop_type", "status"}),
		loopDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goai",
			Subsystem: "loop",
			Name:      "duration_seconds",
			Help:      "Loop duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"loop_type", "status"}),
	}
	collectors := []prometheus.Collector{
		m.httpRequests, m.httpDuration, m.kafkaEvents, m.runtimeEvents, m.runtimeDuration, m.runEvents, m.runDuration,
		m.delegations, m.a2aRequests, m.loopEvents, m.loopDuration,
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("registering metric: %w", err)
		}
	}
	return m, nil
}

// Handler 返回 Prometheus 文本格式的 HTTP handler。
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotImplemented)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveHTTPRequest 记录一次 HTTP 请求及耗时。
func (m *Metrics) ObserveHTTPRequest(method, route string, status int, elapsed time.Duration) {
	if m == nil {
		return
	}
	if route == "" {
		route = "unknown"
	}
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(elapsed.Seconds())
}

// ObserveKafka 记录 Kafka 发布或消费结果。
func (m *Metrics) ObserveKafka(operation, topic, status string) {
	if m == nil {
		return
	}
	m.kafkaEvents.WithLabelValues(operation, topic, status).Inc()
}

// ObserveRuntime 记录 Runtime 协调操作及耗时。
func (m *Metrics) ObserveRuntime(operation, status string, elapsed time.Duration) {
	if m == nil {
		return
	}
	if operation == "" {
		operation = "unknown"
	}
	m.runtimeEvents.WithLabelValues(operation, status).Inc()
	m.runtimeDuration.WithLabelValues(operation, status).Observe(elapsed.Seconds())
}

// ObserveRun 记录 Run 终态和耗时。
func (m *Metrics) ObserveRun(triggerType, status string, elapsed time.Duration) {
	if m == nil {
		return
	}
	if triggerType == "" {
		triggerType = "unknown"
	}
	m.runEvents.WithLabelValues(triggerType, status).Inc()
	m.runDuration.WithLabelValues(triggerType, status).Observe(elapsed.Seconds())
}

// ObserveDelegation 记录 Delegation 状态变化。
func (m *Metrics) ObserveDelegation(status string) {
	if m == nil {
		return
	}
	m.delegations.WithLabelValues(status).Inc()
}

// ObserveA2A 记录 A2A 操作结果。
func (m *Metrics) ObserveA2A(operation, status string) {
	if m == nil {
		return
	}
	m.a2aRequests.WithLabelValues(operation, status).Inc()
}

// ObserveLoop 记录 Loop 终态和耗时。
func (m *Metrics) ObserveLoop(loopType, status string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.loopEvents.WithLabelValues(loopType, status).Inc()
	m.loopDuration.WithLabelValues(loopType, status).Observe(elapsed.Seconds())
}
