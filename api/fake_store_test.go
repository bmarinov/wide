package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/wide"
)

// fakeStore records what the handlers hand it and serves a canned result set.
type fakeStore struct {
	mu       sync.Mutex
	received []wide.Event
	queries  []queryCall

	columns  []wide.Column
	rows     []fakeRow
	queryErr error
}

type queryCall struct {
	from, to time.Time
	params   wide.QueryParams
}

type fakeRow struct {
	ts     time.Time
	values []any
}

func (f *fakeStore) Receive(_ context.Context, e wide.Event, ack func(error)) error {
	f.mu.Lock()
	f.received = append(f.received, e)
	f.mu.Unlock()
	if ack != nil {
		ack(nil)
	}
	return nil
}

func (f *fakeStore) Query(_ context.Context, from, to time.Time, q wide.QueryParams, sink wide.Sink) error {
	f.mu.Lock()
	f.queries = append(f.queries, queryCall{from: from, to: to, params: q})
	f.mu.Unlock()
	if f.queryErr != nil {
		return f.queryErr
	}
	sink.Schema(f.columns)
	for _, r := range f.rows {
		if !sink.Row(r.ts, r.values) {
			return nil
		}
	}
	return nil
}

func (f *fakeStore) receivedEvents() []wide.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wide.Event(nil), f.received...)
}

func (f *fakeStore) queryCalls() []queryCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queryCall(nil), f.queries...)
}

func serve(t *testing.T, store Store, method, target string, body io.Reader, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	request, err := http.NewRequest(method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()
	NewAppMux(store).ServeHTTP(recorder, request)
	return recorder
}
