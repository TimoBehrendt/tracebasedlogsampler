package tracebasedlogsampler

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var (
	typeStr = component.MustNewType("tracebasedlogsampler")
)

const (
	defaultTraceBufferDuration = 180 * time.Second
	defaultLogBufferDuration   = 90 * time.Second
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, component.StabilityLevelDevelopment),
		processor.WithLogs(createLogsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		BufferDurationTraces: defaultTraceBufferDuration.String(),
		BufferDurationLogs:   defaultLogBufferDuration.String(),
	}
}

func createTracesProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Traces) (processor.Traces, error) {
	logger := set.Logger
	tpCfg := cfg.(*Config)

	traceProc := NewLogSamplerProcessorSingleton(logger, nextConsumer, nil, tpCfg)

	return traceProc, nil
}

func createLogsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	logger := set.Logger
	lpCfg := cfg.(*Config)

	logProc := NewLogSamplerProcessorSingleton(logger, nil, nextConsumer, lpCfg)

	return logProc, nil
}
