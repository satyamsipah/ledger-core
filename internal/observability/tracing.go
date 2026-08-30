package observability

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/satyamsipah/ledger-core/internal/config"
)

// ShutdownFunc flushes buffered telemetry. Callers must defer it: a process
// that exits without flushing loses exactly the spans covering the failure that
// made it exit.
type ShutdownFunc func(context.Context) error

// NewTracerProvider configures OTLP tracing and installs the W3C propagator.
//
// With no OTLP endpoint configured it returns a no-op shutdown and leaves the
// global provider alone, so `go run` against a bare machine does not spend
// every request retrying a collector that is not there.
func NewTracerProvider(ctx context.Context, cfg config.Observability, service, env string, logger *slog.Logger) (ShutdownFunc, error) {
	if cfg.OTLPEndpoint == "" {
		logger.Info("tracing disabled: no OTLP endpoint configured")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
		attribute.String("deployment.environment", env),
	))
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// ParentBased so a sampled trace stays sampled across service hops.
		// Sampling each service independently produces traces with holes in
		// them, which is worse than not sampling at all.
		sdktrace.WithSampler(sdktrace.ParentBased(
			sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio),
		)),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("tracing enabled",
		slog.String("endpoint", cfg.OTLPEndpoint),
		slog.Float64("sample_ratio", cfg.TraceSampleRatio))

	return provider.Shutdown, nil
}
