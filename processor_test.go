package tracebasedlogsampler

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type mockTraceConsumer struct {
	mock.Mock
}

func (m *mockTraceConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (m *mockTraceConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	args := m.Called(td)
	return args.Error(0)
}

type mockLogConsumer struct {
	mock.Mock
	logs []plog.Logs
}

func (m *mockLogConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (m *mockLogConsumer) ConsumeLogs(_ context.Context, l plog.Logs) error {
	m.Called(l)
	m.logs = append(m.logs, l)
	return nil
}

func generateTestTrace(traceID [16]byte) ptrace.Traces {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().
		ScopeSpans().AppendEmpty().
		Spans().AppendEmpty()
	span.SetTraceID(traceID)
	return td
}

func generateTestLog(traceID [16]byte) plog.Logs {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().
		ScopeLogs().AppendEmpty().
		LogRecords().AppendEmpty()
	record.SetTraceID(traceID)
	return logs
}

var componentType = component.MustNewType("logbasedtracesampler")

func TestNewLogSamplerProcessorSingleton_CreatesDistinctForDifferentNames(t *testing.T) {
	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "1s",
	}
	logger := zap.NewNop()

	idA := component.NewIDWithName(componentType, "a")
	idB := component.NewIDWithName(componentType, "b")

	p1 := NewLogSamplerProcessorSingleton(idA, logger, new(mockTraceConsumer), new(mockLogConsumer), cfg)
	p2 := NewLogSamplerProcessorSingleton(idB, logger, new(mockTraceConsumer), new(mockLogConsumer), cfg)

	assert.NotSame(t, p1, p2, "different instance names should yield different processors")
}

func TestNewLogSamplerProcessorSingleton_SameNameReturnsSameWithUpdatedConsumers(t *testing.T) {
	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "1s",
	}
	logger := zap.NewNop()

	id := component.NewIDWithName(componentType, "foo")

	consumer := new(mockTraceConsumer)
	p1 := NewLogSamplerProcessorSingleton(id, logger, consumer, nil, cfg)

	logs := new(mockLogConsumer)
	p2 := NewLogSamplerProcessorSingleton(id, logger, nil, logs, cfg)

	assert.Same(t, p1, p2, "same instance name should return same processor")
	assert.Equal(t, logs, p2.nextLogsConsumer, "traces consumer should update on re-create")
	assert.Equal(t, consumer, p2.nextTracesConsumer, "logs consumer should remain as first set when nil passed")
}

func TestConsumeTraces_AddsToBuffer(t *testing.T) {
	traceID := [16]byte{0xde, 0xad, 0xbe, 0xef}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "300ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "addsToBuffer"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	td := generateTestTrace(traceID)
	err := processor.ConsumeTraces(context.Background(), td)
	assert.NoError(t, err)

	log := generateTestLog(traceID)
	err = processor.ConsumeLogs(context.Background(), log)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mockLog.AssertCalled(t, "ConsumeLogs", mock.Anything)
}

func TestConsumeTraces_AddsBatchedTracesToBuffer(t *testing.T) {
	traceID1 := [16]byte{0xde, 0xad, 0xbe, 0xef}
	traceID2 := [16]byte{0xba, 0xad, 0xf0, 0x0d}
	traceID3 := [16]byte{0xca, 0xfe, 0xba, 0xbe}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "300ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "addsBatchedTracesToBuffer"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	td := ptrace.NewTraces()

	resourceSpans1 := td.ResourceSpans().AppendEmpty()
	scopeSpans1 := resourceSpans1.ScopeSpans().AppendEmpty()
	span1 := scopeSpans1.Spans().AppendEmpty()
	span1.SetTraceID(traceID1)
	span2 := scopeSpans1.Spans().AppendEmpty()
	span2.SetTraceID(traceID2)

	resourceSpans2 := td.ResourceSpans().AppendEmpty()
	scopeSpans2 := resourceSpans2.ScopeSpans().AppendEmpty()
	span3 := scopeSpans2.Spans().AppendEmpty()
	span3.SetTraceID(traceID3)

	err := processor.ConsumeTraces(context.Background(), td)
	assert.NoError(t, err)

	assert.True(t, processor.traceIdTtlMap.Exists(hex.EncodeToString(traceID1[:])))
	assert.True(t, processor.traceIdTtlMap.Exists(hex.EncodeToString(traceID2[:])))
	assert.True(t, processor.traceIdTtlMap.Exists(hex.EncodeToString(traceID3[:])))
}

func TestConsumeLogs_BatchesLogsWithSameTraceId(t *testing.T) {
	traceID := [16]byte{0xde, 0xad, 0xbe, 0xef}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "300ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "batchesLogsWithSameTraceId"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	for i := 0; i < 3; i++ {
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.SetTraceID(traceID)
		logRecord.Body().SetStr(fmt.Sprintf("log %d", i))
	}

	td := generateTestTrace(traceID)
	err := processor.ConsumeTraces(context.Background(), td)
	assert.NoError(t, err)

	err = processor.ConsumeLogs(context.Background(), logs)
	assert.NoError(t, err)

	mockLog.AssertNumberOfCalls(t, "ConsumeLogs", 1)
	forwardedLogs := mockLog.logs[0]
	assert.Equal(t, 3, forwardedLogs.LogRecordCount())
}

func TestConsumeLogs_BatchesLogsWithDifferentTraceIds(t *testing.T) {
	traceID1 := [16]byte{0xde, 0xad, 0xbe, 0xef}
	traceID2 := [16]byte{0xba, 0xad, 0xf0, 0x0d}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "300ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "batchesLogsWithDifferentTraceIds"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	for i := range 2 {
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.SetTraceID(traceID1)
		logRecord.Body().SetStr(fmt.Sprintf("log1_%d", i))
	}

	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetTraceID(traceID2)
	logRecord.Body().SetStr("log2_0")

	td1 := generateTestTrace(traceID1)
	err := processor.ConsumeTraces(context.Background(), td1)
	assert.NoError(t, err)

	td2 := generateTestTrace(traceID2)
	err = processor.ConsumeTraces(context.Background(), td2)
	assert.NoError(t, err)

	err = processor.ConsumeLogs(context.Background(), logs)
	assert.NoError(t, err)

	mockLog.AssertNumberOfCalls(t, "ConsumeLogs", 2)
}

func TestConsumeLogs_PreservesIntegrity(t *testing.T) {
	/*
		Input structure:
		resource1 (service1)
			└── scope1
				├── log1 (traceId1)
				└── log2 (traceId2)
		resource2 (service2)
			└── scope1
				└── log3 (traceId1)

		Expected output structure (batched by traceId):
		traceId1 batch:
			resource1 (service1)
				└── scope1
					└── log1
			resource2 (service2)
				└── scope1
					└── log3
		traceId2 batch:
			resource1 (service1)
				└── scope1
					└── log2
	*/

	traceID1 := [16]byte{0xde, 0xad, 0xbe, 0xef}
	traceID2 := [16]byte{0xba, 0xad, 0xf0, 0x0d}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "1s",
		BufferDurationLogs:   "300ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "preservesIntegrity"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	logs := plog.NewLogs()

	resourceLogs1 := logs.ResourceLogs().AppendEmpty()
	resourceLogs1.Resource().Attributes().PutStr("service.name", "service1")
	scopeLogs1 := resourceLogs1.ScopeLogs().AppendEmpty()
	scopeLogs1.Scope().SetName("scope1")

	log1 := scopeLogs1.LogRecords().AppendEmpty()
	log1.SetTraceID(traceID1)
	log1.Body().SetStr("log1")
	log2 := scopeLogs1.LogRecords().AppendEmpty()
	log2.SetTraceID(traceID2)
	log2.Body().SetStr("log2")

	resourceLogs2 := logs.ResourceLogs().AppendEmpty()
	resourceLogs2.Resource().Attributes().PutStr("service.name", "service2")
	scopeLogs2 := resourceLogs2.ScopeLogs().AppendEmpty()
	scopeLogs2.Scope().SetName("scope1")
	log3 := scopeLogs2.LogRecords().AppendEmpty()
	log3.SetTraceID(traceID1)
	log3.Body().SetStr("log3")

	for _, traceID := range [][16]byte{traceID1, traceID2} {
		td := generateTestTrace(traceID)
		err := processor.ConsumeTraces(context.Background(), td)
		assert.NoError(t, err)
	}

	err := processor.ConsumeLogs(context.Background(), logs)
	assert.NoError(t, err)

	mockLog.AssertNumberOfCalls(t, "ConsumeLogs", 2)

	var traceID1Batch, traceID2Batch plog.Logs
	for _, batch := range mockLog.logs {
		if batch.LogRecordCount() > 0 {
			record := batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			traceID := record.TraceID()
			if hex.EncodeToString(traceID[:]) == hex.EncodeToString(traceID1[:]) {
				traceID1Batch = batch
			} else {
				traceID2Batch = batch
			}
		}
	}

	assert.Equal(t, 2, traceID1Batch.LogRecordCount(), "traceID1 batch should have 2 logs")
	assert.Equal(t, 2, traceID1Batch.ResourceLogs().Len(), "traceID1 batch should have 2 resources")

	resourceLogs1 = traceID1Batch.ResourceLogs().At(0)
	val1, exists := resourceLogs1.Resource().Attributes().Get("service.name")
	assert.True(t, exists, "service.name attribute should exist for first resource")
	assert.Equal(t, "service1", val1.Str(), "first resource should have service1")

	scopeLogs1 = resourceLogs1.ScopeLogs().At(0)
	assert.Equal(t, "scope1", scopeLogs1.Scope().Name(), "first scope name should be preserved")
	assert.Equal(t, "log1", scopeLogs1.LogRecords().At(0).Body().Str(), "first log body should be preserved")

	resourceLogs2 = traceID1Batch.ResourceLogs().At(1)
	val2, exists := resourceLogs2.Resource().Attributes().Get("service.name")
	assert.True(t, exists, "service.name attribute should exist for second resource")
	assert.Equal(t, "service2", val2.Str(), "second resource should have service2")

	scopeLogs2 = resourceLogs2.ScopeLogs().At(0)
	assert.Equal(t, "scope1", scopeLogs2.Scope().Name(), "second scope name should be preserved")
	assert.Equal(t, "log3", scopeLogs2.LogRecords().At(0).Body().Str(), "second log body should be preserved")

	assert.Equal(t, 1, traceID2Batch.LogRecordCount(), "traceID2 batch should have 1 log")
	assert.Equal(t, 1, traceID2Batch.ResourceLogs().Len(), "traceID2 batch should have 1 resource")

	resourceLogs := traceID2Batch.ResourceLogs().At(0)
	val, exists := resourceLogs.Resource().Attributes().Get("service.name")
	assert.True(t, exists, "service.name attribute should exist")
	assert.Equal(t, "service1", val.Str(), "should have service1")

	scopeLogs := resourceLogs.ScopeLogs().At(0)
	assert.Equal(t, "scope1", scopeLogs.Scope().Name(), "scope name should be preserved")
	assert.Equal(t, "log2", scopeLogs.LogRecords().At(0).Body().Str(), "log body should be preserved")
}

func TestConsumeLogs_LogIsDroppedIfNoTraceAppears(t *testing.T) {
	traceID := [16]byte{0xaa, 0xbb, 0xcc}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "200ms",
		BufferDurationLogs:   "200ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "logIsDroppedIfNoTraceAppears"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	log := generateTestLog(traceID)
	err := processor.ConsumeLogs(context.Background(), log)
	assert.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	mockLog.AssertNotCalled(t, "ConsumeLogs", mock.Anything)
}

func TestConsumeLogs_LogIsForwardedAfterSleep(t *testing.T) {
	traceID := [16]byte{0xba, 0xad, 0xf0, 0x0d}

	mockTrace := new(mockTraceConsumer)
	mockTrace.On("ConsumeTraces", mock.Anything).Return(nil)

	mockLog := new(mockLogConsumer)
	mockLog.On("ConsumeLogs", mock.Anything, mock.Anything).Return(nil)

	cfg := &Config{
		BufferDurationTraces: "30s",
		BufferDurationLogs:   "50ms",
	}

	processor := NewLogSamplerProcessorSingleton(component.NewIDWithName(componentType, "logIsForwardedAfterSleep"), zap.NewNop(), mockTrace, mockLog, cfg)
	defer processor.Shutdown(context.Background())

	logs := generateTestLog(traceID)
	err := processor.ConsumeLogs(context.Background(), logs)
	assert.NoError(t, err)

	td := generateTestTrace(traceID)
	err = processor.ConsumeTraces(context.Background(), td)
	assert.NoError(t, err)

	mockLog.AssertNotCalled(t, "ConsumeLogs", mock.Anything, mock.Anything)

	time.Sleep(100 * time.Millisecond)

	mockLog.AssertNumberOfCalls(t, "ConsumeLogs", 1)
}
