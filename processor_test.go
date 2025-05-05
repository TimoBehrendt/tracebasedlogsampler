package tracebasedlogsampler

import (
	"context"
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
