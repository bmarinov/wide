package router

import (
	"bytes"
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	"github.com/bmarinov/sandbox-columnstore/internal/otlp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/encoding/protojson"
)

func newOTELMux(store *columnar.Store) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("POST /v1development/profiles", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		req := v1development.ProfilesData{}
		err = protojson.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		events := otlp.PivotProfiles(&req)
		for _, v := range events {
			err := store.Receive(r.Context(), v, nil)
			if err != nil {
				slog.Error("receiving profile", "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)

	}))

	mux.Handle("POST /v1/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		unmarshaler := &pmetric.JSONUnmarshaler{}
		md, err := unmarshaler.UnmarshalMetrics(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		events := pivotMetrics(md)
		for _, e := range events {
			err := store.Receive(r.Context(), e, nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.WriteHeader(http.StatusAccepted)
	}))

	return mux
}

func pivotMetrics(md pmetric.Metrics) []columnar.Event {
	// group fields by timestamp
	byTS := make(map[tsKey][]columnar.Field)

	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)

		rKey := resourceKey(rm)

		// extract resource attributes: service, host etc
		var resourceFields []columnar.Field
		rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
			resourceFields = append(resourceFields, columnar.Field{
				Name:  k,
				Value: v.AsString(),
			})
			return true
		})

		sms := rm.ScopeMetrics()
		for j := range sms.Len() {
			metrics := sms.At(j).Metrics()
			for k := range metrics.Len() {
				m := metrics.At(k)
				extractDataPoints(m, rKey, resourceFields, byTS)
			}
		}
	}

	// convert map to events
	events := make([]columnar.Event, 0, len(byTS))
	for key, fields := range byTS {
		ts := time.Unix(0, int64(key.ts)).UTC()
		events = append(events, columnar.Event{
			Timestamp: ts,
			Fields:    fields,
		})
	}
	return events
}

type tsKey struct {
	resource string
	ts       uint64
}

func resourceKey(rm pmetric.ResourceMetrics) string {
	attrs := make([]string, 0)
	rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
		attrs = append(attrs, k+"="+v.AsString())
		return true
	})
	sort.Strings(attrs)
	return strings.Join(attrs, ",")
}

func extractDataPoints(m pmetric.Metric, rKey string, resourceFields []columnar.Field, byTS map[tsKey][]columnar.Field) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := range dps.Len() {
			addDataPoint(m.Name(), dps.At(i), rKey, resourceFields, byTS)
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := range dps.Len() {
			addDataPoint(m.Name(), dps.At(i), rKey, resourceFields, byTS)
		}
	// case pmetric.MetricTypeHistogram:
	// 	dps := m.Histogram().DataPoints()
	// 	for i := range dps.Len() {
	// 		addHistogramDataPoint(m.Name(), dps.At(i), rKey, resourceFields, byTS)
	// 	}
	// case pmetric.MetricTypeExponentialHistogram:
	// 	dps := m.ExponentialHistogram().DataPoints()
	// 	for i := range dps.Len() {
	// 		addExponentialHistogramDataPoint(m.Name(), dps.At(i), rKey, resourceFields, byTS)
	// 	}
	// case pmetric.MetricTypeSummary:
	// 	dps := m.Summary().DataPoints()
	// 	for i := range dps.Len() {
	// 		addSummaryDataPoint(m.Name(), dps.At(i), rKey, resourceFields, byTS)
	// 	}
	default:
		// TODO: handle all types
		slog.Warn("unhandled metric type", "name", m.Name(), "type", m.Type())
	}
}

func addDataPoint(name string, dp pmetric.NumberDataPoint, rKey string, resourceFields []columnar.Field, byTS map[tsKey][]columnar.Field) {
	// build column name
	colName := name
	dp.Attributes().Range(func(k string, v pcommon.Value) bool {
		colName = name + "." + v.AsString()
		return true
	})

	// extract value
	var val any
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		val = dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		val = dp.IntValue()
	}

	// first time seeing this key, add resource fields
	key := tsKey{resource: rKey, ts: uint64(dp.Timestamp())}
	if _, exists := byTS[key]; !exists {
		byTS[key] = append([]columnar.Field{}, resourceFields...)
	}
	byTS[key] = append(byTS[key], columnar.Field{Name: colName, Value: val})
}

func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	if r.Header.Get("Content-Encoding") != "gzip" {
		return body, nil
	}

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	return io.ReadAll(gr)
}
