package tracebasedlogsampler

import (
	"fmt"
	"time"
)

type Config struct {
	// How long to buffer trace ids. Dictates how long to wait for logs to arrive.
	BufferDurationTraces string `mapstructure:"buffer_duration_traces"`

	// How long to buffer logs, before checking if a trace id exists in the trace id buffer.
	BufferDurationLogs string `mapstructure:"buffer_duration_logs"`
}

func (cfg *Config) Validate() error {
	bufferDurationTraces, _ := time.ParseDuration(cfg.BufferDurationTraces)
	if bufferDurationTraces.Minutes() <= 0 {
		return fmt.Errorf("buffer_duration_traces must be greater than 0")
	}

	bufferDurationLogs, _ := time.ParseDuration(cfg.BufferDurationLogs)
	if bufferDurationLogs <= 0 {
		return fmt.Errorf("buffer_duration_logs must be greater than 0")
	}

	return nil
}
