package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmarinov/wide/otlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestMetricsIngest(t *testing.T) {
	f, err := os.Open("./testdata/metrics_otlp.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	store := &fakeStore{}
	recorder := serve(t, store, http.MethodPost, "/v1/metrics", f, "application/json")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	if got := len(store.receivedEvents()); got != 7 {
		t.Errorf("expected 7 pivoted events (one per resource and timestamp), got %d", got)
	}
}

func TestProfilesIngest_ThreeSamplesLandInStore(t *testing.T) {
	req := otlp.BuildProfilesRequest([]*v1development.Sample{
		otlp.OneSample(fixedTS),
		otlp.OneSample(fixedTS + uint64(time.Second)),
		otlp.OneSample(fixedTS + 2*uint64(time.Second)),
	}, map[string]string{"service.name": "test-svc"})

	body, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	store := &fakeStore{}
	recorder := serve(t, store, http.MethodPost, "/v1development/profiles", bytes.NewReader(body), "application/json")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	if got := len(store.receivedEvents()); got != 3 {
		t.Errorf("expected 3 profile events, got %d", got)
	}
}

func TestProfilesIngest_InvalidBody_Returns400(t *testing.T) {
	store := &fakeStore{}
	recorder := serve(t, store, http.MethodPost, "/v1development/profiles", strings.NewReader("not json"), "application/json")

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected 400 got %d", recorder.Code)
	}
	if got := len(store.receivedEvents()); got != 0 {
		t.Errorf("expected nothing stored, got %d events", got)
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
