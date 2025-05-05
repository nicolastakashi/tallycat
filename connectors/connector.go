package otelprocessortests

import (
	"context"
	"sync"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricToLogsConnector struct {
	logger       *zap.Logger
	config       *Config
	logsConsumer consumer.Logs
	schemas      []MetricSchema
	schemasMutex sync.RWMutex
	done         chan struct{}
	started      bool
	clock        clockwork.Clock
	ticker       clockwork.Ticker
	shutdownOnce sync.Once
}

func newConnector(logger *zap.Logger, config component.Config, logsConsumer consumer.Logs, clock clockwork.Clock) (*metricToLogsConnector, error) {
	cfg := config.(*Config)
	return &metricToLogsConnector{
		logger:       logger,
		config:       cfg,
		logsConsumer: logsConsumer,
		schemas:      make([]MetricSchema, 0, 100), // Pre-allocate with reasonable capacity
		done:         make(chan struct{}),
		clock:        clock,
		ticker:       clock.NewTicker(cfg.FlushInterval),
	}, nil
}

func (p *metricToLogsConnector) Start(ctx context.Context, _ component.Host) error {
	p.logger.Info("Starting spanmetrics connector")

	p.started = true
	go func() {
		for {
			select {
			case <-p.done:
				return
			case <-p.ticker.Chan():
				if err := p.emitSchemas(ctx); err != nil {
					p.logger.Error("Failed to emit schemas", zap.Error(err))
				}
			}
		}
	}()

	return nil
}

func (p *metricToLogsConnector) emitSchemas(ctx context.Context) error {
	p.schemasMutex.RLock()
	defer p.schemasMutex.RUnlock()

	if len(p.schemas) == 0 {
		p.logger.Debug("No schemas to emit")
		return nil
	}

	p.logger.Debug("Emitting schemas",
		zap.Int("schema_count", len(p.schemas)),
		zap.Any("schemas", p.schemas))

	err := EmitSchemasAsLogs(ctx, p.schemas, p.logsConsumer)
	if err != nil {
		p.logger.Error("Failed to emit schemas as logs", zap.Error(err))
		return err
	}
	p.logger.Debug("Successfully emitted schemas as logs")
	return nil
}

// Capabilities implements the consumer interface.
func (p *metricToLogsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *metricToLogsConnector) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	// Extract schema metadata
	newSchemas := ExtractMetricSchemas(metrics)
	p.logger.Debug("Extracted schemas from metrics",
		zap.Int("schema_count", len(newSchemas)),
		zap.Any("schemas", newSchemas))

	// Update schemas slice
	p.schemasMutex.Lock()
	defer p.schemasMutex.Unlock()

	// Reuse existing slice if possible, otherwise create new one
	if cap(p.schemas) >= len(newSchemas) {
		p.schemas = p.schemas[:len(newSchemas)]
	} else {
		p.schemas = make([]MetricSchema, len(newSchemas))
	}
	copy(p.schemas, newSchemas)
	p.logger.Debug("Updated schemas in connector",
		zap.Int("total_schemas", len(p.schemas)),
		zap.Any("schemas", p.schemas))

	return nil
}

// EmitSchemasAsLogs converts multiple MetricSchemas to a single plog.Logs batch and sends it to the consumer
func EmitSchemasAsLogs(ctx context.Context, schemas []MetricSchema, consumer consumer.Logs) error {
	if len(schemas) == 0 {
		return nil
	}

	// Create a single logs batch
	logs := plog.NewLogs()

	// Group schemas by resource to share ResourceLogs blocks
	resourceLogs := make(map[string]int) // Map resource key to ResourceLogs index

	// Process each schema
	for _, schema := range schemas {
		// Get or create ResourceLogs for this schema's resource
		var rl plog.ResourceLogs
		resourceKey := schema.SchemaID // Using SchemaID as a unique key for the resource
		if idx, ok := resourceLogs[resourceKey]; ok {
			rl = logs.ResourceLogs().At(idx)
		} else {
			rl = logs.ResourceLogs().AppendEmpty()
			// Set resource attributes
			for k, v := range schema.Resource {
				rl.Resource().Attributes().PutStr(k, v)
			}
			resourceLogs[resourceKey] = logs.ResourceLogs().Len() - 1
		}

		// Get the log record from SchemaToLog
		schemaLogs := SchemaToLog(schema)
		if schemaLogs.ResourceLogs().Len() == 0 {
			continue
		}

		// Get the source log record
		sourceRL := schemaLogs.ResourceLogs().At(0)
		sourceSL := sourceRL.ScopeLogs().At(0)
		sourceLR := sourceSL.LogRecords().At(0)

		// Create a new scope logs in our batch
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(schema.ScopeName)

		// Create a new log record and copy the source
		lr := sl.LogRecords().AppendEmpty()
		sourceLR.CopyTo(lr)
	}

	// Send the batch to the consumer
	err := consumer.ConsumeLogs(ctx, logs)
	if err != nil {
		return err
	}
	return nil
}

func (p *metricToLogsConnector) Shutdown(context.Context) error {
	p.shutdownOnce.Do(func() {
		p.logger.Info("Shutting down metric to logs connector")
		if p.started {
			p.logger.Info("Stopping ticker")
			p.ticker.Stop()
			p.done <- struct{}{}
			p.started = false
		}
	})
	return nil
}
