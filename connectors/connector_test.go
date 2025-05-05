package otelprocessortests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/metadata"
)

func TestConsumeMetrics(t *testing.T) {
	// Create test fixtures
	lcon := &consumertest.LogsSink{}
	fakeClock := clockwork.NewFakeClock()
	cfg := &Config{
		FlushInterval: time.Nanosecond,
	}

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	// Create connector
	p, err := newConnector(logger, cfg, lcon, fakeClock)
	require.NoError(t, err)

	// Load test data
	dir := filepath.Join("testdata")
	md, err := golden.ReadMetrics(filepath.Join(dir, "metrics.yaml"))
	require.NoError(t, err)

	// Create test context
	ctx := metadata.NewIncomingContext(context.Background(), nil)
	err = p.Start(ctx, componenttest.NewNopHost())
	defer func() { sdErr := p.Shutdown(ctx); require.NoError(t, sdErr) }()
	require.NoError(t, err)

	err = p.ConsumeMetrics(ctx, md)
	assert.NoError(t, err)

	// Trigger flush.
	fakeClock.Advance(time.Nanosecond)
	require.Eventually(t, func() bool {
		return len(lcon.AllLogs()) > 0
	}, 1*time.Second, 10*time.Millisecond)

}
