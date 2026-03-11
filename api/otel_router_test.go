package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsIngest(t *testing.T) {
	f, err := os.Open("./testdata/metrics_otlp.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s := columnar.New(t.Context(), columnar.Config{})
	mux := NewAppMux(s)

	request, err := http.NewRequest(http.MethodPost, "/v1/metrics", f)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
}

func TestGenerateFlatNDJSON(t *testing.T) {
	t.Skip("used only to generate matching data")

	data, err := os.ReadFile("./testdata/metrics_otlp.json")
	if err != nil {
		t.Fatal(err)
	}

	md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(data)
	if err != nil {
		t.Fatal(err)
	}

	events := pivotMetrics(md)

	f, err := os.Create("./testdata/metrics_flat.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, e := range events {
		row := map[string]any{"ts": e.Timestamp.Format(time.RFC3339Nano)}
		for _, field := range e.Fields {
			row[field.Name] = field.Value
		}
		if err := enc.Encode(row); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("wrote %d events to testdata/metrics_flat.ndjson", len(events))
}
