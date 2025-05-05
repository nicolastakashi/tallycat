package otelprocessortests

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestExtractMetricSchemas(t *testing.T) {
	// Create a sample metrics payload
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.9.0")

	// Add resource attributes
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	rm.Resource().Attributes().PutStr("service.version", "1.0.0")
	rm.Resource().Attributes().PutInt("instance.id", 123)
	rm.Resource().Attributes().PutBool("is.production", true)

	// Add scope metrics
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://opentelemetry.io/schemas/1.9.0")
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0.0")
	sm.Scope().Attributes().PutStr("scope.attr", "scope-value")
	sm.Scope().Attributes().PutInt("scope.version", 42)

	// Add a gauge metric
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("test.gauge")
	gauge.SetUnit("ms")
	gauge.SetDescription("Test gauge metric")
	gauge.SetEmptyGauge()

	// Add data points with attributes
	dp1 := gauge.Gauge().DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.Timestamp(1234567890))
	dp1.SetDoubleValue(42.0)
	dp1.Attributes().PutStr("http.status_code", "200")
	dp1.Attributes().PutInt("http.method", 1)
	dp1.Attributes().PutBool("is.error", false)

	dp2 := gauge.Gauge().DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.Timestamp(1234567891))
	dp2.SetDoubleValue(43.0)
	dp2.Attributes().PutStr("http.status_code", "404")
	dp2.Attributes().PutStr("error.type", "not_found")
	dp2.Attributes().PutBool("is.error", true)

	// Extract schema metadata
	schemas := ExtractMetricSchemas(metrics)

	// Verify results
	assert.Equal(t, 1, len(schemas))
	schema := schemas[0]

	// Verify basic metadata
	assert.Equal(t, "metric", schema.SignalType)
	assert.Equal(t, "test-scope", schema.ScopeName)
	assert.Equal(t, "Gauge", schema.MetricType)
	assert.Equal(t, "ms", schema.Unit)
	assert.Equal(t, 2, schema.SeenCount) // Two data points
	assert.False(t, schema.IsMonotonic)
	assert.Equal(t, "unspecified", schema.Temporality)

	// Verify resource attributes are empty (we don't store values)
	assert.Empty(t, schema.Resource)

	// Verify fields
	fields := make(map[string]SchemaField)
	for _, f := range schema.Fields {
		fields[f.Name] = f
	}

	// Check resource fields
	assert.Contains(t, fields, "service.name")
	assert.Equal(t, "resource", fields["service.name"].Source)
	assert.Equal(t, "String", fields["service.name"].Type)
	assert.Empty(t, fields["service.name"].Example)

	assert.Contains(t, fields, "instance.id")
	assert.Equal(t, "resource", fields["instance.id"].Source)
	assert.Equal(t, "Int", fields["instance.id"].Type)
	assert.Empty(t, fields["instance.id"].Example)

	assert.Contains(t, fields, "is.production")
	assert.Equal(t, "resource", fields["is.production"].Source)
	assert.Equal(t, "Bool", fields["is.production"].Type)
	assert.Empty(t, fields["is.production"].Example)

	// Check scope fields
	assert.Contains(t, fields, "scope.attr")
	assert.Equal(t, "scope", fields["scope.attr"].Source)
	assert.Equal(t, "String", fields["scope.attr"].Type)
	assert.Empty(t, fields["scope.attr"].Example)

	assert.Contains(t, fields, "scope.version")
	assert.Equal(t, "scope", fields["scope.version"].Source)
	assert.Equal(t, "Int", fields["scope.version"].Type)
	assert.Empty(t, fields["scope.version"].Example)

	// Check data point fields
	assert.Contains(t, fields, "http.status_code")
	assert.Equal(t, "datapoint", fields["http.status_code"].Source)
	assert.Equal(t, "String", fields["http.status_code"].Type)
	assert.Empty(t, fields["http.status_code"].Example)

	assert.Contains(t, fields, "http.method")
	assert.Equal(t, "datapoint", fields["http.method"].Source)
	assert.Equal(t, "Int", fields["http.method"].Type)
	assert.Empty(t, fields["http.method"].Example)

	assert.Contains(t, fields, "error.type")
	assert.Equal(t, "datapoint", fields["error.type"].Source)
	assert.Equal(t, "String", fields["error.type"].Type)
	assert.Empty(t, fields["error.type"].Example)

	assert.Contains(t, fields, "is.error")
	assert.Equal(t, "datapoint", fields["is.error"].Source)
	assert.Equal(t, "Bool", fields["is.error"].Type)
	assert.Empty(t, fields["is.error"].Example)

	// Verify schema ID is deterministic
	schemaID := schema.SchemaID
	schemas2 := ExtractMetricSchemas(metrics)
	assert.Equal(t, schemaID, schemas2[0].SchemaID)

	// Verify schema ID includes all metadata
	assert.True(t, strings.Contains(schemaID, "metric"))
	assert.True(t, strings.Contains(schemaID, "test-scope"))
	assert.True(t, strings.Contains(schemaID, "Gauge"))
	assert.True(t, strings.Contains(schemaID, "ms"))
	assert.True(t, strings.Contains(schemaID, "unspecified"))
}

func TestSchemaToLog(t *testing.T) {
	// Create a sample schema
	schema := MetricSchema{
		BaseSchema: BaseSchema{
			SchemaID:   "test-schema-id",
			SignalType: "metric",
			ScopeName:  "test-scope",
			Resource: map[string]string{
				"service.name": "test-service",
			},
			Fields: []SchemaField{
				{
					Name:   "http.status_code",
					Type:   "String",
					Source: "datapoint",
				},
				{
					Name:   "http.method",
					Type:   "Int",
					Source: "datapoint",
				},
			},
			SeenCount: 42,
		},
		MetricType:  "Gauge",
		Unit:        "ms",
		IsMonotonic: false,
		Temporality: "unspecified",
	}

	// Convert to logs
	logs := SchemaToLog(schema)

	// Verify results
	assert.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)
	assert.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, 1, sl.LogRecords().Len())
	lr := sl.LogRecords().At(0)

	// Verify basic log record fields
	assert.Equal(t, "tallycat.schema.extracted", lr.Body().Str())
	assert.Equal(t, "INFO", lr.SeverityText())

	// Verify schema attributes
	attrs := lr.Attributes()
	id, exists := attrs.Get("tallycat.schema.id")
	assert.True(t, exists)
	assert.Equal(t, "test-schema-id", id.Str())

	signalType, exists := attrs.Get("tallycat.schema.signal_type")
	assert.True(t, exists)
	assert.Equal(t, "metric", signalType.Str())

	metricType, exists := attrs.Get("tallycat.schema.metric_type")
	assert.True(t, exists)
	assert.Equal(t, "Gauge", metricType.Str())

	unit, exists := attrs.Get("tallycat.schema.unit")
	assert.True(t, exists)
	assert.Equal(t, "ms", unit.Str())

	seenCount, exists := attrs.Get("tallycat.schema.seen_count")
	assert.True(t, exists)
	assert.Equal(t, int64(42), seenCount.Int())

	scopeName, exists := attrs.Get("tallycat.schema.scope_name")
	assert.True(t, exists)
	assert.Equal(t, "test-scope", scopeName.Str())

	// Verify field names
	fieldNames, exists := attrs.Get("tallycat.schema.field_names")
	assert.True(t, exists)
	assert.Equal(t, 2, fieldNames.Slice().Len())
	assert.Equal(t, "http.status_code", fieldNames.Slice().At(0).Str())
	assert.Equal(t, "http.method", fieldNames.Slice().At(1).Str())

	// Verify field types
	fieldTypes, exists := attrs.Get("tallycat.schema.field_types")
	assert.True(t, exists)
	statusCodeType, exists := fieldTypes.Map().Get("http.status_code")
	assert.True(t, exists)
	assert.Equal(t, "String", statusCodeType.Str())
	methodType, exists := fieldTypes.Map().Get("http.method")
	assert.True(t, exists)
	assert.Equal(t, "Int", methodType.Str())

	// Verify field sources
	fieldSources, exists := attrs.Get("tallycat.schema.field_sources")
	assert.True(t, exists)
	statusCodeSource, exists := fieldSources.Map().Get("http.status_code")
	assert.True(t, exists)
	assert.Equal(t, "datapoint", statusCodeSource.Str())
	methodSource, exists := fieldSources.Map().Get("http.method")
	assert.True(t, exists)
	assert.Equal(t, "datapoint", methodSource.Str())

	// Verify resource attributes
	resourceAttrs := rl.Resource().Attributes()
	serviceName, exists := resourceAttrs.Get("service.name")
	assert.True(t, exists)
	assert.Equal(t, "test-service", serviceName.Str())
}
