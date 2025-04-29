# Trace-based Log Sampling

This processor is used to sample logs based on the sampling decision of the trace they correlate to.

## How It Works

When a trace is sampled, the processor caches its `traceId`.
Logs are then filtered:

- If a log references a known sampled `traceId`, it is beimg forwarded.
- If a log references an unknown or unsampled `traceId`, it is buffered for a certain amount of time. After the buffer time expires, the `traceId` is checked again. If it exists, the log is forwarded. If not, it is discarded.

## Configuration

| Field                  | Type     | Default | Description                                                                                                                                                                                                                                            |
| ---------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| buffer_duration_traces | duration | 180s    | The duration for which traceIds are being remembered. The timer starts when the first trace or span of one traceId is received.                                                                                                                        |
| buffer_duration_logs   | duration | 90s     | The duration for which logs are being buffered for, before being re-evaluated. If your pipeline includes e.g. a tailbasedsampler processor, set this to above it's collection time. This ensures that logs "wait" until the traces have been processed |

### Example Configuration

The followinh config is an example configuration for the processor. It is configured to buffer traceIds for `180 seconds` and logs for `90 seconds`.

Note that both a traces and logs pipeline is required and both have to use the same instance of the processor.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  otlp:
    endpoint: 0.0.0.0:4317

processors:
  logtracesampler:
    buffer_duration_traces: 180s
    buffer_duration_logs: 90s

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [logtracesampler]
      exporters: [otlp]

    logs:
      receivers: [otlp]
      processors: [logtracesampler]
      exporters: [otlp]
```

## Building

When building a custom collector you can add this processor to the mainfest like the following (refer to [Building a custom collector](https://opentelemetry.io/docs/collector/custom-collector/) for more information):

```yaml
processors:
  - gomod: gitea.t000-n.de/t.behrendt/tracebasedlogsampler v0.0.0
```
