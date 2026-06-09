package wide

import (
	"testing"
	"time"
)

func BenchmarkJSONMarshal(b *testing.B) {
	cols := []Column{
		{Name: "host", Type: ColumnString},
		{Name: "status", Type: ColumnInt64},
		{Name: "duration_ms", Type: ColumnFloat64},
		{Name: "error", Type: ColumnBool},
		{Name: "trace_id", Type: ColumnString},
	}
	values := []any{
		"web-01",
		int64(200),
		45.2,
		false,
		"trace-abc123",
	}
	row := &jsonRow{
		ts:     time.Now(),
		cols:   cols,
		values: values,
	}

	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = marshalMap(row)
		}
	})

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = marshalRaw(row)
		}
	})
}

func BenchmarkJSONMarshalWide(b *testing.B) {
	cols := []Column{
		{Name: "env", Type: ColumnString},
		{Name: "host", Type: ColumnString},
		{Name: "region", Type: ColumnString},
		{Name: "service", Type: ColumnString},
		{Name: "version", Type: ColumnString},
		{Name: "trace_id", Type: ColumnString},
		{Name: "span_id", Type: ColumnString},
		{Name: "user_agent", Type: ColumnString},
		{Name: "ip", Type: ColumnString},
		{Name: "method", Type: ColumnString},
		{Name: "path", Type: ColumnString},
		{Name: "status", Type: ColumnInt64},
		{Name: "duration_ms", Type: ColumnFloat64},
		{Name: "request_id", Type: ColumnString},
		{Name: "datacenter", Type: ColumnString},
		{Name: "metric", Type: ColumnString},
		{Name: "value", Type: ColumnFloat64},
		{Name: "log_line", Type: ColumnString},
		{Name: "error", Type: ColumnBool},
		{Name: "bytes_out", Type: ColumnInt64},
		{Name: "log_message", Type: ColumnString},
	}
	values := []any{
		"production",
		"web-01",
		"eu-west-1",
		"api",
		"1.4.2",
		"trace-12345",
		"span-12345",
		"Mozilla/5.0",
		"192.168.1.1",
		"GET",
		"/api/v1/events",
		int64(200),
		float64(45.5),
		"req-12345",
		"ams1",
		"http_request",
		float64(42),
		"GET /api/v1/events 200 45ms",
		false,
		int64(1024),
		"2026-03-09T14:23:45Z INFO GET /api/v1/events 200 45ms trace_id=trace-12345 span_id=span-12345 host=web-01 region=eu-west-1",
	}
	row := &jsonRow{
		ts:     time.Now(),
		cols:   cols,
		values: values,
	}

	b.Run("map/wide", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = marshalMap(row)
		}
	})

	b.Run("raw/wide", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = marshalRaw(row)
		}
	})
}
