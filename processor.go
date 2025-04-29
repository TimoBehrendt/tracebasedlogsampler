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
	"go.uber.org/zap"
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
var logSampleProcessor *LogSamplerProcessor

/**
 * Creates a new LogSamplerProcessor as a singleton.
 */
func NewLogSamplerProcessorSingleton(logger *zap.Logger, nextTracesConsumer consumer.Traces, nextLogsConsumer consumer.Logs, cfg *Config) *LogSamplerProcessor {
	maxTraceTtl, _ := time.ParseDuration(cfg.BufferDurationTraces)

	logSampleProcessorLock.Lock()
	defer logSampleProcessorLock.Unlock()

	if logSampleProcessor != nil {
		if nextTracesConsumer != nil {
			logSampleProcessor.nextTracesConsumer = nextTracesConsumer
		}

		if nextLogsConsumer != nil {
			logSampleProcessor.nextLogsConsumer = nextLogsConsumer
		}

	} else {
		logSampleProcessor = &LogSamplerProcessor{
			logger:             logger,
			nextTracesConsumer: nextTracesConsumer,
			nextLogsConsumer:   nextLogsConsumer,
			config:             cfg,
			// TODO: Pass size from config as well.
			traceIdTtlMap: traceIdTtlMap.New(1000, int(maxTraceTtl.Seconds())),
		}
	}

	return logSampleProcessor
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
 * For each trace, record the trace id in a buffer.
 */
func (tp *LogSamplerProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	traceId := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()

	if !traceId.IsEmpty() {
		tp.traceIdTtlMap.Add(hex.EncodeToString(traceId[:]))

		tp.logger.Debug("Trace added to buffer", zap.String("traceId", hex.EncodeToString(traceId[:])))
	}

	return tp.nextTracesConsumer.ConsumeTraces(ctx, td)
}

/**
 * Upon receiving a log, check if the log's trace id matches any trace id in the buffer.
 * If it doesn't, put the log in the buffer for the configured amount of time.
 * If it does, forward the log.
 *
 * After the buffer expires, check again if the log's trace id matches any trace id in the buffer.
 * If it does, forward the log.
 * If not, discard the log.
 */
func (tp *LogSamplerProcessor) ConsumeLogs(ctx context.Context, log plog.Logs) error {
	traceId := log.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).TraceID()

	if !traceId.IsEmpty() {
		exists := tp.traceIdTtlMap.Exists(hex.EncodeToString(traceId[:]))
		if exists {
			tp.logger.Debug("Log forwarded directly", zap.String("traceId", hex.EncodeToString(traceId[:])))
			return tp.nextLogsConsumer.ConsumeLogs(ctx, log)
		}

		go func(ctx context.Context, log plog.Logs) {
			tp.logger.Debug("Log added to buffer", zap.String("traceId", hex.EncodeToString(traceId[:])))

			// TODO: Find a better place for the parsed config, instead of using the non-parsed strings.
			duration, _ := time.ParseDuration(tp.config.BufferDurationLogs)
			time.Sleep(duration)

			exists := tp.traceIdTtlMap.Exists(hex.EncodeToString(traceId[:]))
			if exists {
				tp.logger.Debug("Log forwarded after buffer expiration", zap.String("traceId", hex.EncodeToString(traceId[:])))
				tp.nextLogsConsumer.ConsumeLogs(ctx, log)
			} else {
				tp.logger.Debug("Log discarded", zap.String("traceId", hex.EncodeToString(traceId[:])))
			}
		}(ctx, log)
	} else {
		tp.logger.Warn("Log has no trace id", zap.Any("log", log))
	}

	return nil
}
