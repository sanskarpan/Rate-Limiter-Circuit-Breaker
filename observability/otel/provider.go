// Package otel provides a real, configured OpenTelemetry tracing path for the
// resilience library and its demo server. It replaces the historical no-op stub
// in server/telemetry with a genuine SDK TracerProvider: a service Resource, a
// parent-based ratio sampler, a batch span processor, and a selectable exporter
// (OTLP-HTTP, stdout, or none). The global provider and a W3C TraceContext
// propagator are installed on New so that context flows across process
// boundaries.
package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Exporter kinds understood by Config.Exporter.
const (
	ExporterOTLP   = "otlp"
	ExporterStdout = "stdout"
	ExporterNone   = "none"
)

// Config declares how the tracing pipeline should be built. The zero value is
// usable: it yields a "none" exporter, a default service name, and a sample
// ratio of 1.0 (sample everything, subject to parent decisions).
type Config struct {
	// ServiceName is reported as service.name on the Resource.
	ServiceName string
	// ServiceVersion is reported as service.version on the Resource.
	ServiceVersion string
	// Endpoint is the OTLP-HTTP collector endpoint (host:port or URL). Only
	// consulted when Exporter == "otlp". Empty uses the exporter default.
	Endpoint string
	// Exporter selects the span exporter: "otlp", "stdout", or "none".
	Exporter string
	// SampleRatio is the fraction of root traces to sample, in [0,1]. Values
	// <= 0 sample nothing (except when a parent already decided to sample);
	// values >= 1 sample everything. Non-root spans inherit the parent decision.
	SampleRatio float64
}

// Provider owns a configured SDK TracerProvider and exposes tracer access plus a
// clean shutdown. It is safe to call Shutdown more than once.
type Provider struct {
	tp         *sdktrace.TracerProvider
	propagator propagation.TextMapPropagator
}

// New builds and installs a tracing pipeline from cfg. It sets the resulting
// TracerProvider as the global provider (otel.SetTracerProvider) and installs a
// W3C TraceContext + Baggage propagator (otel.SetTextMapPropagator). The caller
// owns the returned Provider and must call Shutdown to flush spans.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "rate-limiter-circuit-breaker"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		// A schema-URL conflict from a merge is non-fatal; fall back to a
		// schemaless resource carrying the same core attributes.
		res = resource.NewSchemaless(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler(cfg.SampleRatio)),
	}

	exp, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if exp != nil {
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)

	return &Provider{tp: tp, propagator: propagator}, nil
}

// sampler builds a parent-based ratio sampler. Root spans are sampled at the
// given ratio; child spans respect the incoming parent decision so a sampled
// upstream trace stays intact end-to-end.
func sampler(ratio float64) sdktrace.Sampler {
	switch {
	case ratio <= 0:
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case ratio >= 1:
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
}

// newExporter constructs the exporter selected by cfg. It returns (nil, nil) for
// the "none" exporter so the provider records spans (observable via a custom
// processor in tests) without shipping them anywhere.
func newExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.Exporter {
	case ExporterNone, "":
		return nil, nil
	case ExporterStdout:
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	case ExporterOTLP:
		httpOpts := []otlptracehttp.Option{}
		if cfg.Endpoint != "" {
			httpOpts = append(httpOpts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		}
		return otlptracehttp.New(ctx, httpOpts...)
	default:
		return nil, fmt.Errorf("otel: unknown exporter %q", cfg.Exporter)
	}
}

// Tracer returns a named tracer from the underlying provider.
func (p *Provider) Tracer(name string) trace.Tracer {
	return p.tp.Tracer(name)
}

// Propagator returns the W3C TraceContext propagator installed by New.
func (p *Provider) Propagator() propagation.TextMapPropagator {
	return p.propagator
}

// Shutdown flushes buffered spans and shuts the provider down. It is idempotent:
// a second call is a no-op and returns nil.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	tp := p.tp
	p.tp = nil
	return tp.Shutdown(ctx)
}
