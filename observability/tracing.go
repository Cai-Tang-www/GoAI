package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Bundle 聚合 GoAI 的日志、指标和 Trace 能力，供应用装配层显式注入。
type Bundle struct {
	Logger  *slog.Logger
	Metrics *Metrics
	Tracer  *Tracer
}

// New 创建生产环境使用的可观测性组件。
func New(serviceName string, writer io.Writer) (*Bundle, error) {
	if writer == nil {
		writer = os.Stderr
	}
	logger := slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo}))
	metrics, err := NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("creating metrics: %w", err)
	}
	tracer, err := NewTracer(serviceName, logger)
	if err != nil {
		return nil, fmt.Errorf("creating tracer: %w", err)
	}
	return &Bundle{Logger: logger, Metrics: metrics, Tracer: tracer}, nil
}

// NewNoop 返回测试和局部路由装配可使用的隔离观测组件。
func NewNoop() *Bundle {
	metrics, err := NewMetrics()
	if err != nil {
		metrics = nil
	}
	return &Bundle{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: metrics,
		Tracer:  NewNoopTracer(),
	}
}

// Shutdown 刷新异步 Trace exporter。
func (b *Bundle) Shutdown(ctx context.Context) error {
	if b == nil || b.Tracer == nil {
		return nil
	}
	return b.Tracer.Shutdown(ctx)
}

// Tracer 使用 OTel SDK 创建 span，并把摘要以结构化日志输出，避免首版依赖外部 Collector。
type Tracer struct {
	tracer   oteltrace.Tracer
	provider shutdownProvider
}

type shutdownProvider interface {
	Shutdown(context.Context) error
}

// NewTracer 创建带日志 exporter 的 OTel tracer provider。
func NewTracer(serviceName string, logger *slog.Logger) (*Tracer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = "goai"
	}
	exporter := &logSpanExporter{logger: logger, serviceName: serviceName}
	provider := trace.NewTracerProvider(trace.WithBatcher(exporter, trace.WithBatchTimeout(100*time.Millisecond)))
	return &Tracer{
		tracer:   provider.Tracer(serviceName),
		provider: provider,
	}, nil
}

// NewNoopTracer 创建不输出 Trace 的 tracer，主要用于单元测试。
func NewNoopTracer() *Tracer {
	provider := oteltrace.NewNoopTracerProvider()
	return &Tracer{
		tracer:   provider.Tracer("goai-test"),
		provider: noopShutdownProvider{TracerProvider: provider},
	}
}

type noopShutdownProvider struct {
	oteltrace.TracerProvider
}

func (noopShutdownProvider) Shutdown(context.Context) error { return nil }

// Start 创建一个 span；调用方负责 End。
func (t *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil || t.tracer == nil {
		return ctx, oteltrace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name, oteltrace.WithAttributes(attrs...))
}

// Shutdown 等待 Trace exporter 刷新完成。
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

type logSpanExporter struct {
	logger      *slog.Logger
	serviceName string
	mu          sync.Mutex
	closed      bool
}

func (e *logSpanExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	for _, span := range spans {
		spanContext := span.SpanContext()
		attrs := []any{
			slog.String("service_name", e.serviceName),
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
			slog.String("parent_span_id", span.Parent().SpanID().String()),
			slog.String("span_name", span.Name()),
			slog.Int64("duration_ms", span.EndTime().Sub(span.StartTime()).Milliseconds()),
			slog.String("status", fmt.Sprint(span.Status().Code)),
		}
		for _, attr := range span.Attributes() {
			attrs = append(attrs, slog.Any(string(attr.Key), attr.Value.AsInterface()))
		}
		e.logger.Info("trace span", attrs...)
	}
	return nil
}

func (e *logSpanExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return nil
}

func (e *logSpanExporter) ForceFlush(context.Context) error { return nil }
