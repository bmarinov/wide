package router

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
)

func TestEventPost(t *testing.T) {
	f, err := os.Open("./testdata/event_post.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

	request, err := http.NewRequest(http.MethodPost, "/events", f)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")

	s := columnar.New(t.Context(), columnar.Config{})
	mux := NewAppMux(s)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Errorf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
}

func TestQueryPost(t *testing.T) {
	// seed
	s := columnar.New(t.Context(), columnar.Config{})
	tsRef := time.Now()
	seed := []columnar.Event{
		{
			Timestamp: tsRef,
			Fields:    []columnar.Field{{Name: "foo", Value: true}},
		},
		{
			Timestamp: tsRef.Add(time.Minute),
			Fields:    []columnar.Field{{Name: "bar", Value: float64(3.14)}},
		},
	}

	for _, v := range seed {
		ack, wait := ackFn(t)
		_ = s.Receive(t.Context(), v, ack)
		wait()
	}

	// query
	body := strings.NewReader(`{"limit": 100}`)
	from := tsRef.UTC().Format(time.RFC3339)
	to := tsRef.Add(10 * time.Minute).UTC().Format(time.RFC3339)
	request, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("/query?from=%s&to=%s", from, to),
		body,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")

	mux := NewAppMux(s)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var rows []map[string]any
	scanner := bufio.NewScanner(recorder.Body)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("invalid json line: %s: %v", scanner.Text(), err)
		}
		rows = append(rows, row)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows got %d", len(rows))
	}

	// assert timestamp present on every row
	for i, row := range rows {
		if _, ok := row["timestamp"]; !ok {
			t.Errorf("row %d missing timestamp", i)
		}
	}
}

func TestQueryPost_Json_Aggregation(t *testing.T) {
	s := columnar.New(t.Context(), columnar.Config{})
	tsRef := time.Now()

	seed := []columnar.Event{
		{
			Timestamp: tsRef,
			Fields: []columnar.Field{
				{Name: "host", Value: "a"},
				{Name: "duration_ms", Value: float64(100)},
			},
		},
		{
			Timestamp: tsRef,
			Fields: []columnar.Field{
				{Name: "host", Value: "a"},
				{Name: "duration_ms", Value: float64(150)},
			},
		},
		{
			Timestamp: tsRef,
			Fields: []columnar.Field{
				{Name: "host", Value: "b"},
				{Name: "duration_ms", Value: float64(200)},
			},
		},
	}
	for _, v := range seed {
		ack, wait := ackFn(t)
		_ = s.Receive(t.Context(), v, ack)
		wait()
	}

	body, err := json.Marshal(columnar.QueryParams{
		GroupBy: []string{"host"},
		Aggregations: []columnar.Aggregation{
			{Op: columnar.OpCount},
			{Op: columnar.OpAvg, Column: "duration_ms"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	from := tsRef.UTC().Format(time.RFC3339)
	to := tsRef.Add(10 * time.Minute).UTC().Format(time.RFC3339)

	request, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("/query/json?from=%s&to=%s", from, to),
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")

	mux := NewAppMux(s)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("http %d", recorder.Code)
	}

	var rows []map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&rows); err != nil {
		t.Fatalf("invalid json: %v, body: %s", err, recorder.Body.String())
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows got %d", len(rows))
	}

	byHost := make(map[string]map[string]any)
	for _, row := range rows {
		host, ok := row["host"].(string)
		if !ok {
			t.Fatal("host field not found")
		}
		byHost[host] = row
	}

	v, ok := byHost["a"]
	if !ok {
		t.Fatal()
	}
	aCount := v["COUNT"].(float64)
	if aCount != 2 {
		t.Errorf("expected 2 got %f", aCount)
	}
	aAvg := v["AVG(duration_ms)"].(float64)
	if aAvg != float64(125) {
		t.Errorf("unexpected avg %f", aAvg)
	}

	bCount, ok := byHost["b"]["COUNT"]
	if !ok || bCount.(float64) != 1 {
		t.Errorf("host b: expected COUNT=1 got %v", bCount)
	}
	if bAvg, ok := byHost["b"]["AVG(duration_ms)"]; !ok || bAvg.(float64) != 200 {
		t.Errorf("host b: expected avg 5 got %v", bAvg)
	}

	for _, row := range rows {
		if _, ok := row["ts"]; ok {
			t.Errorf("aggregate result should not have ts field, got %v", row["ts"])
		}
	}
}

func ackFn(t *testing.T) (func(error), func()) {
	t.Helper()
	done := make(chan error, 1)
	return func(err error) { done <- err },
		func() {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
}
