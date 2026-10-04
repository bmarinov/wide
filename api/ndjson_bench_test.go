package api

import (
	"bytes"
	"os"
	"testing"

	"github.com/bmarinov/wide"
)

func BenchmarkParseNDJSON(b *testing.B) {
	lines, err := os.ReadFile("testdata/metrics_flat.ndjson")
	if err != nil {
		b.Fatal(err)
	}
	body := bytes.Repeat(lines, 100)

	// keep every event like a store does, so the benchmark pays for the fields it hands out
	kept := make([]wide.Event, 0, bytes.Count(body, []byte("\n")))
	b.ReportAllocs()
	for b.Loop() {
		kept = kept[:0]
		err := processNDJSON(bytes.NewReader(body), func(e wide.Event) error {
			kept = append(kept, e)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
