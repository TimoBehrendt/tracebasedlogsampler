package tracebasedlogsampler

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "sample_config.yaml"))
	assert.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(typeStr, "").String())
	assert.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	assert.Equal(t, &Config{
		BufferDurationTraces: "180s",
		BufferDurationLogs:   "90s",
	}, cfg)
}

func TestValidate(t *testing.T) {
	cfg := &Config{
		BufferDurationTraces: "0",
		BufferDurationLogs:   "10s",
	}
	assert.Error(t, cfg.Validate())

	cfg = &Config{
		BufferDurationTraces: "10s",
		BufferDurationLogs:   "0",
	}
	assert.Error(t, cfg.Validate())

	cfg = &Config{
		BufferDurationTraces: "10s",
		BufferDurationLogs:   "10s",
	}
	assert.NoError(t, cfg.Validate())
}
