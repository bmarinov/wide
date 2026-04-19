package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	"github.com/bmarinov/sandbox-columnstore/internal/otlp"
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

	s := columnar.New(t.Context(), columnar.Config{})
	mux := NewAppMux(s)

	r, err := http.NewRequest(http.MethodPost, "/v1development/profiles", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, r)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	ack, wait := ackFn(t)
	_ = s.Receive(t.Context(), columnar.Event{Timestamp: time.Now()}, ack)
	wait()

	if got := s.Stats().BufRows; got != 4 { // 3 profile events + 1 sync sentinel
		t.Errorf("expected 4 rows (3 profile + 1 sentinel), got %d", got)
	}
}

func TestProfilesIngest_InvalidBody_Returns400(t *testing.T) {
	s := columnar.New(t.Context(), columnar.Config{})
	mux := NewAppMux(s)

	r, err := http.NewRequest(http.MethodPost, "/v1development/profiles", strings.NewReader("not json"))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, r)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected 400 got %d", recorder.Code)
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
