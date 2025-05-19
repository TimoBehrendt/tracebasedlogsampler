package tracebasedlogsampler

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	traceIdTtlMap "gitea.t000-n.de/t.behrendt/tracebasedlogsampler/internals/traceIdTtlMap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	meter = otel.GetMeterProvider().Meter("tracebasedlogsampler")

	tracesProcessedCounter, _ = meter.Int64Counter(
		"otelcol_processor_tracebasedlogsampler_traces_processed",
		metric.WithDescription("Number of traces processed"),
		metric.WithUnit("1"),
	)

	emptyTraceIdsCounter, _ = meter.Int64Counter(
		"otelcol_processor_tracebasedlogsampler_traces_empty_trace_ids",
		metric.WithDescription("Number of spans with empty trace IDs"),
		metric.WithUnit("1"),
	)

	traceProcessingTimeHistogram, _ = meter.Int64Histogram(
		"otelcol_processor_tracebasedlogsampler_traces_processing_time",
		metric.WithDescription("Time spent processing traces"),
		metric.WithUnit("ms"),
	)

	logsProcessedCounter, _ = meter.Int64Counter(
		"otelcol_processor_tracebasedlogsampler_logs_processed",
		metric.WithDescription("Number of log batches processed"),
		metric.WithUnit("1"),
	)

	logsForwardedCounter, _ = meter.Int64Counter(
		"otelcol_processor_tracebasedlogsampler_logs_forwarded",
		metric.WithDescription("Number of log batches forwarded"),
		metric.WithUnit("1"),
	)

	logsDiscardedCounter, _ = meter.Int64Counter(
		"otelcol_processor_tracebasedlogsampler_logs_discarded",
		metric.WithDescription("Number of log batches discarded"),
		metric.WithUnit("1"),
	)

	logProcessingTimeHistogram, _ = meter.Int64Histogram(
		"otelcol_processor_tracebasedlogsampler_logs_processing_time",
		metric.WithDescription("Time spent processing logs"),
		metric.WithUnit("ms"),
	)

	traceAgeAtLogCheckHistogram, _ = meter.Int64Histogram(
		"otelcol_processor_tracebasedlogsampler_logs_trace_age_at_log_check",
		metric.WithDescription("Age of trace when checking logs after buffer expiration"),
		metric.WithUnit("ms"),
	)
)

type LogSamplerProcessor struct {
	host               component.Host
	cancel             context.CancelFunc
	logger             *zap.Logger
	nextTracesConsumer consumer.Traces
	nextLogsConsumer   consumer.Logs
	config             *Config

	traceIdTtlMap *traceIdTtlMap.TTLMap
}

var logSampleProcessorLock = sync.Mutex{}
var logSampleProcessor = make(map[component.ID]*LogSamplerProcessor)

/**
 * Creates a new LogSamplerProcessor as a singleton.
 */
func NewLogSamplerProcessorSingleton(id component.ID, logger *zap.Logger, nextTracesConsumer consumer.Traces, nextLogsConsumer consumer.Logs, cfg *Config) *LogSamplerProcessor {
	maxTraceTtl, _ := time.ParseDuration(cfg.BufferDurationTraces)

	logSampleProcessorLock.Lock()
	defer logSampleProcessorLock.Unlock()

	if existing, ok := logSampleProcessor[id]; ok {
		if nextTracesConsumer != nil {
			existing.nextTracesConsumer = nextTracesConsumer
		}

		if nextLogsConsumer != nil {
			existing.nextLogsConsumer = nextLogsConsumer
		}

		return existing
	}

	p := &LogSamplerProcessor{
		logger:             logger,
		nextTracesConsumer: nextTracesConsumer,
		nextLogsConsumer:   nextLogsConsumer,
		config:             cfg,
		traceIdTtlMap:      traceIdTtlMap.New(1000, int(maxTraceTtl.Seconds())),
	}
	logSampleProcessor[id] = p

	return p
}

func (tp *LogSamplerProcessor) Start(ctx context.Context, host component.Host) error {
	tp.host = host
	ctx = context.Background()
	_, tp.cancel = context.WithCancel(ctx)

	tp.logger.Debug("LogSamplerProcessor started")

	return nil
}

func (tp *LogSamplerProcessor) Shutdown(ctx context.Context) error {
	if tp.cancel != nil {
		tp.cancel()
	}

	tp.traceIdTtlMap.Stop()

	tp.logger.Debug("LogSamplerProcessor stopped")

	return nil
}

func (tp *LogSamplerProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

/**
 * Every trace's trace id that is processed by the processor is being remembered.
 */
func (tp *LogSamplerProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	startTime := time.Now()
	tracesProcessedCounter.Add(ctx, 1)

	for i := range td.ResourceSpans().Len() {
		resourceSpans := td.ResourceSpans().At(i)

		for j := range resourceSpans.ScopeSpans().Len() {
			scopeSpans := resourceSpans.ScopeSpans().At(j)

			for k := range scopeSpans.Spans().Len() {
				span := scopeSpans.Spans().At(k)
				traceId := span.TraceID()

				if !traceId.IsEmpty() {
					traceIdStr := hex.EncodeToString(traceId[:])
					tp.traceIdTtlMap.Add(traceIdStr)

					tp.logger.Debug("Trace added to buffer", zap.String("traceId", traceIdStr))
				} else {
					emptyTraceIdsCounter.Add(ctx, 1)
					tp.logger.Warn("Empty trace ID encountered")
				}
			}
		}
	}

	traceProcessingTimeHistogram.Record(ctx, time.Since(startTime).Milliseconds())

	return tp.nextTracesConsumer.ConsumeTraces(ctx, td)
}

/**
 * Upon receiving logs, check if each log's trace id matches any trace id in the buffer.
 * If it doesn't, put the log in the buffer for the configured amount of time.
 * If it does, forward the log.
 *
 * After the buffer expires, check again if the log's trace id matches any trace id in the buffer.
 * If it does, forward the log.
 * If not, discard the log.
 */
func (tp *LogSamplerProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	startTime := time.Now()
	logsProcessedCounter.Add(ctx, 1)

	// Rough estimation of the number of unique trace ids in the logs as the number of logs itself.
	// It may be an option to make this configurable or use runtime metrics to zero in on the right size.
	logsByTraceId := make(map[string]plog.Logs, logs.LogRecordCount())

	for i := range logs.ResourceLogs().Len() {
		resourceLogs := logs.ResourceLogs().At(i)
		for j := range resourceLogs.ScopeLogs().Len() {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := range scopeLogs.LogRecords().Len() {
				logRecord := scopeLogs.LogRecords().At(k)
				traceId := logRecord.TraceID()

				if !traceId.IsEmpty() {
					traceIdStr := hex.EncodeToString(traceId[:])

					batch, exists := logsByTraceId[traceIdStr]
					if !exists {
						batch = plog.NewLogs()
						logsByTraceId[traceIdStr] = batch
					}

					batchResourceLogs := batch.ResourceLogs().AppendEmpty()
					resourceLogs.Resource().CopyTo(batchResourceLogs.Resource())
					batchScopeLogs := batchResourceLogs.ScopeLogs().AppendEmpty()
					scopeLogs.Scope().CopyTo(batchScopeLogs.Scope())
					newRecord := batchScopeLogs.LogRecords().AppendEmpty()
					logRecord.CopyTo(newRecord)
				} else {
					emptyTraceIdsCounter.Add(ctx, 1)
					tp.logger.Warn("Log has no trace id", zap.Any("log", logs))
				}
			}
		}
	}

	for traceIdStr, batch := range logsByTraceId {
		ttl, ok := tp.traceIdTtlMap.Get(traceIdStr)
		if ok && ttl > 0 {
			// The tll value is the deletion time. So the traceId was initially stored at the ttl time - buffer duration.
			bufferDuration, _ := time.ParseDuration(tp.config.BufferDurationTraces)
			traceAge := time.Since(time.Unix(ttl, 0).Add(-bufferDuration))
			traceAgeAtLogCheckHistogram.Record(ctx, traceAge.Milliseconds())

			tp.logger.Debug("Logs forwarded directly", zap.String("traceId", traceIdStr))
			logsForwardedCounter.Add(ctx, 1)
			tp.nextLogsConsumer.ConsumeLogs(ctx, batch)
			continue
		}

		go func(ctx context.Context, traceIdStr string, batch plog.Logs) {
			tp.logger.Debug("Logs added to buffer", zap.String("traceId", traceIdStr))
			duration, _ := time.ParseDuration(tp.config.BufferDurationLogs)
			time.Sleep(duration)

			ttl, ok := tp.traceIdTtlMap.Get(traceIdStr)
			if ok && ttl > 0 {
				// The tll value is the deletion time. So the traceId was initially stored at the ttl time - buffer duration.
				bufferDuration, _ := time.ParseDuration(tp.config.BufferDurationTraces)
				traceAge := time.Since(time.Unix(ttl, 0).Add(-bufferDuration))

				tp.logger.Debug("Logs forwarded after buffer expiration", zap.String("traceId", traceIdStr))
				logsForwardedCounter.Add(ctx, 1)
				traceAgeAtLogCheckHistogram.Record(ctx, traceAge.Milliseconds())
				tp.nextLogsConsumer.ConsumeLogs(ctx, batch)
			} else {
				tp.logger.Debug("Logs discarded", zap.String("traceId", traceIdStr))
				logsDiscardedCounter.Add(ctx, 1)
			}
		}(ctx, traceIdStr, batch)
	}

	logProcessingTimeHistogram.Record(ctx, time.Since(startTime).Milliseconds())

	return nil
}
