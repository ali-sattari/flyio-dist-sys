package main

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	serviceName = "txn"
)

func initTracer() func(context.Context) error {
	ctx := context.Background()

	exporter, _ := otlptrace.New(
		ctx,
		otlptracehttp.NewClient(),
	)

	otel.SetTracerProvider(
		sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(getResource(ctx)),
		),
	)

	return exporter.Shutdown
}

func initLogger() func(context.Context) error {
	ctx := context.Background()
	logExporter, _ := otlploghttp.New(ctx)

	lp := log.NewLoggerProvider(
		log.WithProcessor(
			log.NewSimpleProcessor(logExporter),
		),
		log.WithResource(getResource(ctx)),
	)

	global.SetLoggerProvider(lp)

	return lp.Shutdown
}

func initMetric() func(context.Context) error {
	ctx := context.Background()

	reader := metric.NewManualReader(
		metric.WithProducer(runtime.NewProducer()),
	)
	provider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(getResource(ctx)),
	)

	otel.SetMeterProvider(provider)

	return provider.Shutdown
}

func getResource(ctx context.Context) *resource.Resource {
	resources, _ := resource.New(
		ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("library.language", "go"),
		),
	)
	return resources
}
