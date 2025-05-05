// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelprocessortests // import "go.opentelemetry.io/collector/cmd/mdatagen/internal/sampleprocessor"

import (
	"context"

	"github.com/jonboulle/clockwork"
	"github.com/nicolastakashi/otelprocessortests/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

// NewFactory returns a receiver.Factory for sample receiver.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		func() component.Config { return &struct{}{} },
		connector.WithMetricsToLogs(createMetricsToLogsConnector, metadata.MetricsStability),
	)
}

func createMetricsToLogsConnector(ctx context.Context, settings connector.Settings, cfg component.Config, logsConsumer consumer.Logs) (connector.Metrics, error) {
	return newConnector(settings.Logger, cfg, logsConsumer, clockwork.FromContext(ctx))
}
