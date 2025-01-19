package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	serviceName = "g-counter"
)

func initTracer() func(context.Context) error {
	ctx := context.Background()

	exporter, _ := otlptrace.New(
		ctx,
		otlptracehttp.NewClient(),
	)

	resources, _ := resource.New(
		ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("library.language", "go"),
		),
	)

	otel.SetTracerProvider(
		sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(resources),
		),
	)

	return exporter.Shutdown
}

func initLogger() func(context.Context) error {
	ctx := context.Background()
	logExporter, _ := otlploghttp.New(ctx)

	// logExporter, _ := stdoutlog.New()

	lp := log.NewLoggerProvider(
		log.WithProcessor(
			log.NewSimpleProcessor(logExporter),
			// log.NewBatchProcessor(logExporter),
		),
	)

	global.SetLoggerProvider(lp)

	return lp.Shutdown
}
