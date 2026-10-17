package toudaigatewayprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var componentType = component.MustNewType("toudaigateway")

// NewFactory returns the processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		componentType,
		func() component.Config { return &Config{} },
		processor.WithLogs(createLogs, component.StabilityLevelDevelopment),
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment),
	)
}

func createLogs(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	return processorhelper.NewLogs(ctx, set, cfg, next, newGateway().processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func createTraces(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return processorhelper.NewTraces(ctx, set, cfg, next, newGateway().processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
