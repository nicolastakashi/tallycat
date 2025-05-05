package otelprocessortests

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SchemaField represents a single field in the schema
type SchemaField struct {
	Name              string // e.g. "http.status_code"
	Type              string // "string", "int", etc.
	Source            string // "resource", "attribute", "datapoint", "span", "body", etc.
	IsHighCardinality bool
	Example           string
}

// BaseSchema contains shared metadata across all schema types
type BaseSchema struct {
	SchemaID   string
	SignalType string
	ScopeName  string
	Resource   map[string]string
	Fields     []SchemaField
	SeenCount  int
}

// MetricSchema represents the schema for a metric
type MetricSchema struct {
	BaseSchema
	MetricType  string
	Unit        string
	IsMonotonic bool
	Temporality string // "delta", "cumulative", etc.
}

// LogSchema represents the schema for a log
type LogSchema struct {
	BaseSchema
	BodyType    string // "string", "json", etc.
	HasSeverity bool
}

// TraceSchema represents the schema for a trace
type TraceSchema struct {
	BaseSchema
	SpanKind  string // "client", "server", etc.
	HasStatus bool
	HasEvents bool
}

// ExtractMetricSchemas extracts schema metadata from metrics
func ExtractMetricSchemas(metrics pmetric.Metrics) []MetricSchema {
	// Map to track unique schemas by their ID
	schemaMap := make(map[string]*MetricSchema)

	// Process each resource metric
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		resourceAttrs := flattenAttributeTypes(rm.Resource().Attributes())

		// Process each scope metric
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scopeName := sm.Scope().Name()
			scopeAttrs := flattenAttributeTypes(sm.Scope().Attributes())

			// Process each metric
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)

				// Create schema fields from all sources
				fields := make([]SchemaField, 0)

				// Add resource attributes
				for name, attrType := range resourceAttrs {
					fields = append(fields, SchemaField{
						Name:   name,
						Type:   string(attrType),
						Source: "resource",
					})
				}

				// Add scope attributes
				for name, attrType := range scopeAttrs {
					fields = append(fields, SchemaField{
						Name:   name,
						Type:   string(attrType),
						Source: "scope",
					})
				}

				// Add metric attributes from data points
				dpAttrs := extractDataPointAttributeTypes(m)
				for name, attrType := range dpAttrs {
					fields = append(fields, SchemaField{
						Name:   name,
						Type:   string(attrType),
						Source: "datapoint",
					})
				}

				// Create schema metadata
				schema := &MetricSchema{
					BaseSchema: BaseSchema{
						SignalType: "metric",
						ScopeName:  scopeName,
						Resource:   make(map[string]string), // Empty map since we don't store values
						Fields:     fields,
						SeenCount:  1,
					},
					MetricType:  string(convertMetricType(m.Type())),
					Unit:        m.Unit(),
					IsMonotonic: isMonotonic(m),
					Temporality: string(convertTemporality(m)),
				}

				// Generate schema ID
				schema.SchemaID = generateSchemaID(schema)

				// Update or create schema in map
				if existing, ok := schemaMap[schema.SchemaID]; ok {
					existing.SeenCount++
				} else {
					schemaMap[schema.SchemaID] = schema
				}
			}
		}
	}

	// Convert map to slice
	result := make([]MetricSchema, 0, len(schemaMap))
	for _, schema := range schemaMap {
		result = append(result, *schema)
	}

	return result
}

// ExtractLogSchemas extracts schema metadata from logs
func ExtractLogSchemas(logs plog.Logs) []LogSchema {
	// TODO: Implement log schema extraction
	return nil
}

// ExtractTraceSchemas extracts schema metadata from traces
func ExtractTraceSchemas(traces ptrace.Traces) []TraceSchema {
	// TODO: Implement trace schema extraction
	return nil
}

// SchemaToLog converts a MetricSchema to OpenTelemetry logs
func SchemaToLog(schema MetricSchema) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	// Set basic log record fields
	lr.Body().SetStr("tallycat.schema.extracted")
	lr.SetSeverityText("INFO")

	// Set schema attributes
	attrs := lr.Attributes()
	attrs.PutStr("tallycat.schema.id", schema.SchemaID)
	attrs.PutStr("tallycat.schema.signal_type", "metric")
	attrs.PutStr("tallycat.schema.metric_type", schema.MetricType)
	attrs.PutStr("tallycat.schema.unit", schema.Unit)
	attrs.PutInt("tallycat.schema.seen_count", int64(schema.SeenCount))
	attrs.PutStr("tallycat.schema.scope_name", schema.ScopeName)

	// Set field names as a slice
	fieldNames := attrs.PutEmptySlice("tallycat.schema.field_names")
	for _, f := range schema.Fields {
		fieldNames.AppendEmpty().SetStr(f.Name)
	}

	// Set field types as a map
	fieldTypes := attrs.PutEmptyMap("tallycat.schema.field_types")
	for _, f := range schema.Fields {
		fieldTypes.PutStr(f.Name, f.Type)
	}

	// Set field sources as a map
	fieldSources := attrs.PutEmptyMap("tallycat.schema.field_sources")
	for _, f := range schema.Fields {
		fieldSources.PutStr(f.Name, f.Source)
	}

	// Set resource attributes
	resourceAttrs := rl.Resource().Attributes()
	for k, v := range schema.Resource {
		resourceAttrs.PutStr(k, v)
	}

	return logs
}

// flattenAttributeTypes converts pcommon.Map to a map of value types
func flattenAttributeTypes(attrs pcommon.Map) map[string]ValueType {
	result := make(map[string]ValueType, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = convertValueType(v.Type())
		return true
	})
	return result
}

// extractDataPointAttributeTypes extracts all unique attribute types from data points
func extractDataPointAttributeTypes(m pmetric.Metric) map[string]ValueType {
	attrs := make(map[string]ValueType)

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for i := 0; i < m.Gauge().DataPoints().Len(); i++ {
			mergeAttributeTypes(attrs, m.Gauge().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		for i := 0; i < m.Sum().DataPoints().Len(); i++ {
			mergeAttributeTypes(attrs, m.Sum().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		for i := 0; i < m.Histogram().DataPoints().Len(); i++ {
			mergeAttributeTypes(attrs, m.Histogram().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		for i := 0; i < m.ExponentialHistogram().DataPoints().Len(); i++ {
			mergeAttributeTypes(attrs, m.ExponentialHistogram().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		for i := 0; i < m.Summary().DataPoints().Len(); i++ {
			mergeAttributeTypes(attrs, m.Summary().DataPoints().At(i).Attributes())
		}
	}

	return attrs
}

// mergeAttributeTypes merges attribute types from a pcommon.Map into our type map
func mergeAttributeTypes(target map[string]ValueType, source pcommon.Map) {
	source.Range(func(k string, v pcommon.Value) bool {
		if _, exists := target[k]; !exists {
			target[k] = convertValueType(v.Type())
		}
		return true
	})
}

// ValueType represents the type of a value
type ValueType string

const (
	ValueTypeString ValueType = "String"
	ValueTypeInt    ValueType = "Int"
	ValueTypeDouble ValueType = "Double"
	ValueTypeBool   ValueType = "Bool"
	ValueTypeMap    ValueType = "Map"
	ValueTypeArray  ValueType = "Array"
)

// convertValueType converts pcommon.ValueType to our ValueType
func convertValueType(t pcommon.ValueType) ValueType {
	switch t {
	case pcommon.ValueTypeStr:
		return ValueTypeString
	case pcommon.ValueTypeInt:
		return ValueTypeInt
	case pcommon.ValueTypeDouble:
		return ValueTypeDouble
	case pcommon.ValueTypeBool:
		return ValueTypeBool
	case pcommon.ValueTypeMap:
		return ValueTypeMap
	case pcommon.ValueTypeSlice:
		return ValueTypeArray
	default:
		return ValueTypeString
	}
}

// convertMetricType converts pmetric.MetricType to string
func convertMetricType(t pmetric.MetricType) string {
	switch t {
	case pmetric.MetricTypeGauge:
		return "Gauge"
	case pmetric.MetricTypeSum:
		return "Sum"
	case pmetric.MetricTypeHistogram:
		return "Histogram"
	case pmetric.MetricTypeExponentialHistogram:
		return "ExponentialHistogram"
	case pmetric.MetricTypeSummary:
		return "Summary"
	default:
		return "Gauge"
	}
}

// isMonotonic determines if a metric is monotonic
func isMonotonic(m pmetric.Metric) bool {
	if m.Type() == pmetric.MetricTypeSum {
		return m.Sum().IsMonotonic()
	}
	return false
}

// convertTemporality converts metric aggregation temporality to string
func convertTemporality(m pmetric.Metric) string {
	switch m.Type() {
	case pmetric.MetricTypeSum:
		switch m.Sum().AggregationTemporality() {
		case pmetric.AggregationTemporalityDelta:
			return "delta"
		case pmetric.AggregationTemporalityCumulative:
			return "cumulative"
		default:
			return "unspecified"
		}
	case pmetric.MetricTypeHistogram:
		switch m.Histogram().AggregationTemporality() {
		case pmetric.AggregationTemporalityDelta:
			return "delta"
		case pmetric.AggregationTemporalityCumulative:
			return "cumulative"
		default:
			return "unspecified"
		}
	case pmetric.MetricTypeExponentialHistogram:
		switch m.ExponentialHistogram().AggregationTemporality() {
		case pmetric.AggregationTemporalityDelta:
			return "delta"
		case pmetric.AggregationTemporalityCumulative:
			return "cumulative"
		default:
			return "unspecified"
		}
	default:
		return "unspecified"
	}
}

// generateSchemaID creates a deterministic hash of the schema
func generateSchemaID(schema *MetricSchema) string {
	// Create a deterministic string representation of the schema
	var sb strings.Builder

	// Add metadata fields
	sb.WriteString(schema.SignalType)
	sb.WriteString("|")
	sb.WriteString(schema.ScopeName)
	sb.WriteString("|")
	sb.WriteString(schema.MetricType)
	sb.WriteString("|")
	sb.WriteString(schema.Unit)
	sb.WriteString("|")
	sb.WriteString(schema.Temporality)
	sb.WriteString("|")

	// Sort fields by name for deterministic output
	fieldNames := make([]string, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		fieldNames = append(fieldNames, f.Name)
	}
	sort.Strings(fieldNames)

	// Add sorted fields
	for _, name := range fieldNames {
		for _, f := range schema.Fields {
			if f.Name == name {
				sb.WriteString(name)
				sb.WriteString(":")
				sb.WriteString(f.Type)
				sb.WriteString(":")
				sb.WriteString(f.Source)
				sb.WriteString("|")
				break
			}
		}
	}

	// Create SHA-256 hash
	h := sha256.New()
	h.Write([]byte(sb.String()))
	return hex.EncodeToString(h.Sum(nil))
}
