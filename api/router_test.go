package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bmarinov/wide"
)

func TestEventPost(t *testing.T) {
	f, err := os.Open("./testdata/event_post.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

	store := &fakeStore{}
	recorder := serve(t, store, http.MethodPost, "/events", f, "application/x-ndjson")

	if recorder.Code != http.StatusAccepted {
		t.Errorf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	tsRef := time.Date(2026, 3, 11, 16, 45, 51, 0, time.UTC)
	want := []wide.Event{
		{Timestamp: tsRef, Fields: []wide.Field{{Name: "route", Value: "/blap"}, {Name: "status", Value: int64(401)}}},
		{Timestamp: tsRef, Fields: []wide.Field{{Name: "host", Value: "localhost"}, {Name: "message", Value: "user foo bar"}}},
		{Timestamp: tsRef, Fields: []wide.Field{{Name: "trace_id", Value: "foo_123"}}},
	}
	if got := store.receivedEvents(); !reflect.DeepEqual(got, want) {
		t.Errorf("events\n got %v\nwant %v", got, want)
	}
}

func TestEventPost_LineWithoutTimestampGetsReceiveTime(t *testing.T) {
	body := `{"route": "/a"}` + "\n" +
		`{"ts": "2026-03-11T16:45:51.000Z", "route": "/b"}` + "\n" +
		`{"route": "/c"}` + "\n"

	store := &fakeStore{}
	before := time.Now()
	recorder := serve(t, store, http.MethodPost, "/events", strings.NewReader(body), "application/x-ndjson")
	after := time.Now()

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected %d got %d: %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	events := store.receivedEvents()
	if len(events) != 3 {
		t.Fatalf("expected 3 events handed to the store, got %d", len(events))
	}
	for _, i := range []int{0, 2} {
		if ts := events[i].Timestamp; ts.Before(before) || ts.After(after) {
			t.Errorf("event %d: timestamp got %v, want the receive time between %v and %v", i, ts, before, after)
		}
	}
	tsRef := time.Date(2026, 3, 11, 16, 45, 51, 0, time.UTC)
	if !events[1].Timestamp.Equal(tsRef) {
		t.Errorf("event 1: timestamp got %v, want %v", events[1].Timestamp, tsRef)
	}
}

func TestQueryPost(t *testing.T) {
	tsRef := time.Date(2026, 3, 11, 16, 45, 51, 0, time.UTC)
	store := &fakeStore{
		columns: []wide.Column{
			{Name: "foo", Type: wide.ColumnBool},
			{Name: "bar", Type: wide.ColumnFloat64},
		},
		rows: []fakeRow{
			{ts: tsRef, values: []any{true, nil}},
			{ts: tsRef.Add(time.Minute), values: []any{nil, 3.14}},
		},
	}

	from, to := tsRef, tsRef.Add(10*time.Minute)
	recorder := serve(t, store, http.MethodPost,
		fmt.Sprintf("/query?from=%s&to=%s", from.Format(time.RFC3339), to.Format(time.RFC3339)),
		strings.NewReader(`{"limit": 100}`), "application/json")

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	want := []map[string]any{
		{"ts": tsRef.Format(time.RFC3339Nano), "foo": true},
		{"ts": tsRef.Add(time.Minute).Format(time.RFC3339Nano), "bar": 3.14},
	}
	if got := decodeNDJSON(t, recorder.Body); !reflect.DeepEqual(got, want) {
		t.Errorf("rows\n got %v\nwant %v", got, want)
	}

	calls := store.queryCalls()
	if len(calls) != 1 {
		t.Fatalf("expected one query, got %d", len(calls))
	}
	if !calls[0].from.Equal(from) || !calls[0].to.Equal(to) {
		t.Errorf("range forwarded as %v..%v, want %v..%v", calls[0].from, calls[0].to, from, to)
	}
	if calls[0].params.Limit != 100 {
		t.Errorf("limit forwarded as %d, want 100", calls[0].params.Limit)
	}
}

func TestQueryPostJSON_AggregatedRows(t *testing.T) {
	store := &fakeStore{
		columns: []wide.Column{
			{Name: "host", Type: wide.ColumnString},
			{Name: "COUNT", Type: wide.ColumnFloat64},
			{Name: "AVG(duration_ms)", Type: wide.ColumnFloat64},
		},
		rows: []fakeRow{
			{values: []any{"a", float64(2), float64(125)}},
			{values: []any{"b", float64(1), float64(200)}},
		},
	}
	params := wide.QueryParams{
		GroupBy: []string{"host"},
		Aggregations: []wide.Aggregation{
			{Op: wide.OpCount},
			{Op: wide.OpAvg, Column: "duration_ms"},
		},
	}

	recorder := queryJSON(t, store, params)

	if recorder.Code != http.StatusOK {
		t.Fatalf("http %d: %s", recorder.Code, recorder.Body.String())
	}
	want := []map[string]any{
		{"host": "a", "COUNT": float64(2), "AVG(duration_ms)": float64(125)},
		{"host": "b", "COUNT": float64(1), "AVG(duration_ms)": float64(200)},
	}
	if got := decodeJSONRows(t, recorder.Body); !reflect.DeepEqual(got, want) {
		t.Errorf("rows\n got %v\nwant %v", got, want)
	}

	calls := store.queryCalls()
	if len(calls) != 1 {
		t.Fatalf("expected one query, got %d", len(calls))
	}
	if !reflect.DeepEqual(calls[0].params.GroupBy, params.GroupBy) || !reflect.DeepEqual(calls[0].params.Aggregations, params.Aggregations) {
		t.Errorf("params forwarded as %+v, want %+v", calls[0].params, params)
	}
}

func TestQueryPostJSON_BucketRowsCarryTimestamp(t *testing.T) {
	tsRef := time.Date(2026, 3, 11, 16, 45, 0, 0, time.UTC)
	store := &fakeStore{
		columns: []wide.Column{{Name: "COUNT", Type: wide.ColumnFloat64}},
		rows: []fakeRow{
			{ts: tsRef, values: []any{float64(2)}},
			{ts: tsRef.Add(time.Minute), values: []any{float64(1)}},
		},
	}
	params := wide.QueryParams{
		Window:       time.Minute,
		Aggregations: []wide.Aggregation{{Op: wide.OpCount}},
	}

	recorder := queryJSON(t, store, params)

	if recorder.Code != http.StatusOK {
		t.Fatalf("http %d: %s", recorder.Code, recorder.Body.String())
	}
	want := []map[string]any{
		{"ts": tsRef.Format(time.RFC3339Nano), "COUNT": float64(2)},
		{"ts": tsRef.Add(time.Minute).Format(time.RFC3339Nano), "COUNT": float64(1)},
	}
	if got := decodeJSONRows(t, recorder.Body); !reflect.DeepEqual(got, want) {
		t.Errorf("rows\n got %v\nwant %v", got, want)
	}
	if calls := store.queryCalls(); len(calls) != 1 || calls[0].params.Window != time.Minute {
		t.Errorf("window not forwarded: %+v", calls)
	}
}

func TestQueryPost_AggregatedRowsCarryNoTimestamp(t *testing.T) {
	store := &fakeStore{
		columns: []wide.Column{
			{Name: "host", Type: wide.ColumnString},
			{Name: "COUNT", Type: wide.ColumnFloat64},
			{Name: "AVG(duration_ms)", Type: wide.ColumnFloat64},
		},
		rows: []fakeRow{
			{values: []any{"a", float64(2), float64(125)}},
			{values: []any{"b", float64(1), float64(200)}},
		},
	}
	params := wide.QueryParams{
		GroupBy: []string{"host"},
		Aggregations: []wide.Aggregation{
			{Op: wide.OpCount},
			{Op: wide.OpAvg, Column: "duration_ms"},
		},
	}

	recorder := queryNDJSON(t, store, params)

	if recorder.Code != http.StatusOK {
		t.Fatalf("http %d: %s", recorder.Code, recorder.Body.String())
	}
	want := []map[string]any{
		{"host": "a", "COUNT": float64(2), "AVG(duration_ms)": float64(125)},
		{"host": "b", "COUNT": float64(1), "AVG(duration_ms)": float64(200)},
	}
	if got := decodeNDJSON(t, recorder.Body); !reflect.DeepEqual(want, got) {
		t.Errorf("rows\n expected %v\ngot %v", want, got)
	}
}

func TestQueryPost_AcceptHeaderSelectsTheResponseShape(t *testing.T) {
	tsRef := time.Date(2026, 3, 11, 16, 45, 51, 0, time.UTC)
	expect := []map[string]any{{"ts": tsRef.Format(time.RFC3339Nano), "COUNT": float64(2)}}

	tests := []struct {
		name        string
		accept      string
		contentType string
	}{
		{name: "no header", accept: "", contentType: "application/x-ndjson"},
		{name: "json", accept: "application/json", contentType: "application/json"},
		{name: "grafana default", accept: "application/json, text/plain, */*", contentType: "application/json"},
		{name: "any", accept: "*/*", contentType: "application/x-ndjson"},
		{name: "ndjson", accept: "application/x-ndjson", contentType: "application/x-ndjson"},
		{name: "json with parameter", accept: "application/json;q=0.9", contentType: "application/json"},
		{name: "json after another type", accept: "text/plain, application/json", contentType: "application/json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{
				columns: []wide.Column{{Name: "COUNT", Type: wide.ColumnFloat64}},
				rows:    []fakeRow{{ts: tsRef, values: []any{float64(2)}}},
			}
			recorder := queryAccept(t, store, tc.accept)

			if recorder.Code != http.StatusOK {
				t.Fatalf("http %d: %s", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != tc.contentType {
				t.Errorf("Content-Type expected %q got %q", tc.contentType, got)
			}
			if got := recorder.Header().Get("Vary"); got != "Accept" {
				t.Errorf("Vary expected Accept to indicate content negotiation, got %q", got)
			}
			var got []map[string]any
			if tc.contentType == "application/json" {
				got = decodeJSONRows(t, recorder.Body)
			} else {
				got = decodeNDJSON(t, recorder.Body)
			}
			if !reflect.DeepEqual(expect, got) {
				t.Errorf("rows\n expected %v\ngot %v", expect, got)
			}
		})
	}
}

func TestQuery_InvalidRequestIsRejectedBeforeTheStore(t *testing.T) {
	validRange := "from=2026-03-11T16:45:00Z&to=2026-03-11T16:55:00Z"
	tests := []struct {
		name  string
		query string
		body  string
	}{
		{name: "duplicate select column", query: validRange, body: `{"select": ["foo", "foo"]}`},
		{name: "group by without aggregation", query: validRange, body: `{"groupBy": ["host"]}`},
		{name: "unparseable from", query: "from=yesterday&to=2026-03-11T16:55:00Z", body: `{}`},
		{name: "missing to", query: "from=2026-03-11T16:45:00Z", body: `{}`},
		{name: "malformed body", query: validRange, body: `{"limit": `},
	}
	for _, path := range []string{"/query", "/query/json"} {
		for _, tc := range tests {
			t.Run(path+" "+tc.name, func(t *testing.T) {
				store := &fakeStore{}
				recorder := serve(t, store, http.MethodPost, path+"?"+tc.query, strings.NewReader(tc.body), "application/json")

				if recorder.Code != http.StatusBadRequest {
					t.Errorf("expected %d got %d", http.StatusBadRequest, recorder.Code)
				}
				if calls := store.queryCalls(); len(calls) != 0 {
					t.Errorf("store was queried %d times for an invalid request", len(calls))
				}
			})
		}
	}
}

func TestQuery_StoreErrorsMapToStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid query", err: fmt.Errorf("bad column: %w", wide.ErrInvalidQuery), want: http.StatusBadRequest},
		{name: "any other failure", err: errors.New("segment unreadable"), want: http.StatusInternalServerError},
	}
	for _, path := range []string{"/query", "/query/json"} {
		for _, tc := range tests {
			t.Run(path+" "+tc.name, func(t *testing.T) {
				store := &fakeStore{queryErr: tc.err}
				recorder := serve(t, store, http.MethodPost,
					path+"?from=2026-03-11T16:45:00Z&to=2026-03-11T16:55:00Z",
					strings.NewReader(`{"limit": 10}`), "application/json")

				if recorder.Code != tc.want {
					t.Errorf("expected %d got %d", tc.want, recorder.Code)
				}
			})
		}
	}
}

func queryJSON(t *testing.T, store Store, params wide.QueryParams) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, store, http.MethodPost,
		"/query/json?from=2026-03-11T16:45:00Z&to=2026-03-11T16:55:00Z",
		bytes.NewReader(body), "application/json")
}

// queryNDJSON posts params to /query without an Accept header, the NDJSON default.
func queryNDJSON(t *testing.T, store Store, params wide.QueryParams) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, store, http.MethodPost,
		"/query?from=2026-03-11T16:45:00Z&to=2026-03-11T16:55:00Z",
		bytes.NewReader(body), "application/json")
}

// queryAccept posts a query to /query with the given Accept header, none when empty.
func queryAccept(t *testing.T, store Store, accept string) *httptest.ResponseRecorder {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost,
		"/query?from=2026-03-11T16:45:00Z&to=2026-03-11T16:55:00Z",
		strings.NewReader(`{"limit": 10}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if accept != "" {
		request.Header.Set("Accept", accept)
	}
	recorder := httptest.NewRecorder()
	NewAppMux(store).ServeHTTP(recorder, request)
	return recorder
}

func decodeNDJSON(t *testing.T, r io.Reader) []map[string]any {
	t.Helper()
	var rows []map[string]any
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("invalid json line %q: %v", scanner.Text(), err)
		}
		rows = append(rows, row)
	}
	return rows
}

func decodeJSONRows(t *testing.T, r io.Reader) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := json.NewDecoder(r).Decode(&rows); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	return rows
}
