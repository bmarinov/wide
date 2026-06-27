package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
)

func BenchmarkIngestEndpoints(b *testing.B) {
	otlpData, err := os.ReadFile("./testdata/metrics_otlp.json")
	if err != nil {
		b.Fatal(err)
	}
	ndjsonData, err := os.ReadFile("./testdata/metrics_flat.ndjson")
	if err != nil {
		b.Fatal(err)
	}

	s := columnar.New(b.Context(), columnar.Config{})
	mux := NewAppMux(s)

	b.Run("otlp/metrics", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			req, _ := http.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(otlpData))
			req.Header.Set("Content-Type", "application/json")
			mux.ServeHTTP(httptest.NewRecorder(), req)
		}
	})

	b.Run("flat/events", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			req, _ := http.NewRequest(http.MethodPost, "/events", bytes.NewReader(ndjsonData))
			req.Header.Set("Content-Type", "application/x-ndjson")
			mux.ServeHTTP(httptest.NewRecorder(), req)
		}
	})
}
