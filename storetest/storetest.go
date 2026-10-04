// Package storetest exercises an api.Store against the behavior the API
// handlers and the Grafana data source rely on. A store that passes Run can sit
// behind api.NewAppMux.
//
// The suite waits for ingestion through the ack callback of Receive, so a store
// must call ack once an event is queryable.
package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/bmarinov/wide"
	"github.com/bmarinov/wide/api"
)

// Run runs the contract suite. newStore must return a fresh, empty store.
func Run(t *testing.T, newStore func(t *testing.T) api.Store) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, s api.Store)
	}{
		{"round trip keeps field values and types", roundTrip},
		{"the query range excludes events outside it", rangeBounds},
		{"rows carry only the fields the event had", sparseRows},
		{"select narrows rows to the chosen fields", selectFields},
		{"filters", filters},
		{"numeric filters compare by value across integer and float columns", numericFilterKinds},
		{"limit caps the number of rows", limit},
		{"aggregations", aggregations},
		{"aggregating a non-numeric column is an invalid query", invalidAggregation},
		{"group by", groupBy},
		{"window buckets an aggregation by time", window},
		{"ingest order does not change the result set", outOfOrder},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newStore(t))
		})
	}
}

func roundTrip(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	values := []struct {
		name  string
		value any
	}{
		{"message", "Hello!"},
		{"score", float64(35.1)},
		{"count", int64(242)},
		{"ready", true},
		{"zero", float64(0)},
		{"empty", ""},
		{"off", false},
	}
	var fields []wide.Field
	for _, v := range values {
		fields = append(fields, wide.Field{Name: v.name, Value: v.value})
	}
	receive(t, s, wide.Event{Timestamp: base, Fields: fields})

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{})
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !rows[0].Timestamp.Equal(base) {
		t.Errorf("timestamp: got %v, want %v", rows[0].Timestamp, base)
	}
	for _, v := range values {
		got, ok := field(rows[0], v.name)
		if !ok {
			t.Errorf("field %q missing", v.name)
			continue
		}
		if got != v.value {
			t.Errorf("field %q: got %v (%T), want %v (%T)", v.name, got, got, v.value, v.value)
		}
	}
}

func rangeBounds(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base.Add(-10 * time.Minute), Fields: []wide.Field{{Name: "n", Value: float64(1)}}},
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "n", Value: float64(2)}}},
		wide.Event{Timestamp: base.Add(10 * time.Minute), Fields: []wide.Field{{Name: "n", Value: float64(3)}}},
	)

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{})
	if len(rows) != 1 {
		t.Fatalf("expected 1 row inside the range, got %d", len(rows))
	}
	if got, _ := field(rows[0], "n"); got != float64(2) {
		t.Errorf("expected the event at the range centre, got n=%v", got)
	}
}

func sparseRows(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "id", Value: int64(1)}, {Name: "count", Value: int64(35)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "id", Value: int64(2)}, {Name: "score", Value: float64(35.4)}}},
	)

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for _, row := range rows {
		id, _ := field(row, "id")
		switch id {
		case int64(1):
			if v, ok := field(row, "score"); ok {
				t.Errorf("row 1 should not carry score, got %v", v)
			}
		case int64(2):
			if v, ok := field(row, "count"); ok {
				t.Errorf("row 2 should not carry count, got %v", v)
			}
		default:
			t.Errorf("row without id: %v", row.Fields)
		}
	}
}

func selectFields(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "status", Value: int64(200)}, {Name: "duration", Value: int64(350)}, {Name: "path", Value: "/hi"}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "status", Value: int64(500)}, {Name: "duration", Value: int64(1500)}, {Name: "log", Value: "ouch"}}},
	)

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{Select: []string{"status", "duration"}})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if len(row.Fields) != 2 {
			t.Errorf("expected exactly the two selected fields, got %v", row.Fields)
		}
		for _, name := range []string{"status", "duration"} {
			if _, ok := field(row, name); !ok {
				t.Errorf("selected field %q missing in %v", name, row.Fields)
			}
		}
	}
}

func filters(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "host", Value: "a"}, {Name: "code", Value: float64(200)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "host", Value: "b"}, {Name: "code", Value: float64(500)}}},
		wide.Event{Timestamp: base.Add(2 * time.Second), Fields: []wide.Field{{Name: "host", Value: "a"}, {Name: "code", Value: float64(404)}}},
		wide.Event{Timestamp: base.Add(3 * time.Second), Fields: []wide.Field{{Name: "host", Value: "c"}}},
	)

	cases := []struct {
		name    string
		filters []wide.Filter
		want    int
	}{
		{"eq on a string field", []wide.Filter{{Field: "host", Op: wide.EqOperator, Value: "a"}}, 2},
		{"exists", []wide.Filter{{Field: "code", Op: wide.ExistsOperator}}, 3},
		{"not_exists", []wide.Filter{{Field: "code", Op: wide.NotExistsOperator}}, 1},
		{"gt skips rows without the field", []wide.Filter{{Field: "code", Op: wide.GtOperator, Value: float64(300)}}, 2},
		{"lt", []wide.Filter{{Field: "code", Op: wide.LtOperator, Value: float64(300)}}, 1},
		{"gte is inclusive", []wide.Filter{{Field: "code", Op: wide.GteOperator, Value: float64(404)}}, 2},
		{"lte is inclusive", []wide.Filter{{Field: "code", Op: wide.LteOperator, Value: float64(404)}}, 2},
		{"filters combine with and", []wide.Filter{
			{Field: "host", Op: wide.EqOperator, Value: "a"},
			{Field: "code", Op: wide.GtOperator, Value: float64(300)},
		}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{Filters: tc.filters})
			if len(rows) != tc.want {
				t.Errorf("expected %d rows, got %d: %v", tc.want, len(rows), rows)
			}
		})
	}
}

func numericFilterKinds(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	// otlp int attributes, as_int points and profile sample values land as int64, doubles as float64.
	// JSON filter values always decode as float64.
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "http.response.status_code", Value: int64(200)}, {Name: "jvm.gc.duration", Value: float64(0.5)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "http.response.status_code", Value: int64(500)}, {Name: "jvm.gc.duration", Value: float64(2)}}},
		wide.Event{Timestamp: base.Add(2 * time.Second), Fields: []wide.Field{{Name: "http.response.status_code", Value: int64(404)}, {Name: "jvm.gc.duration", Value: float64(1)}}},
	)

	cases := []struct {
		name    string
		filters []wide.Filter
		want    int
	}{
		{"eq with a float value on an integer column", []wide.Filter{{Field: "http.response.status_code", Op: wide.EqOperator, Value: float64(500)}}, 1},
		{"gt with a float value on an integer column", []wide.Filter{{Field: "http.response.status_code", Op: wide.GtOperator, Value: float64(300)}}, 2},
		{"lte with a float value on an integer column", []wide.Filter{{Field: "http.response.status_code", Op: wide.LteOperator, Value: float64(404)}}, 2},
		{"eq with a fractional value matches no integer", []wide.Filter{{Field: "http.response.status_code", Op: wide.EqOperator, Value: float64(404.5)}}, 0},
		{"eq with an integer value on a float column", []wide.Filter{{Field: "jvm.gc.duration", Op: wide.EqOperator, Value: int64(2)}}, 1},
		{"gte with an integer value on a float column", []wide.Filter{{Field: "jvm.gc.duration", Op: wide.GteOperator, Value: int64(1)}}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{Filters: tc.filters})
			if len(rows) != tc.want {
				t.Errorf("expected %d rows, got %d: %v", tc.want, len(rows), rows)
			}
		})
	}
}

func limit(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "n", Value: float64(1)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "n", Value: float64(2)}}},
		wide.Event{Timestamp: base.Add(2 * time.Second), Fields: []wide.Field{{Name: "n", Value: float64(3)}}},
	)

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{Limit: 2})
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func aggregations(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "duration", Value: float64(10)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "duration", Value: float64(20)}}},
		wide.Event{Timestamp: base.Add(2 * time.Second), Fields: []wide.Field{{Name: "duration", Value: float64(30)}}},
	)
	from, to := base.Add(-time.Minute), base.Add(time.Minute)

	cases := []struct {
		agg  wide.Aggregation
		want float64
	}{
		{wide.Aggregation{Op: wide.OpCount}, 3},
		{wide.Aggregation{Op: wide.OpSum, Column: "duration"}, 60},
		{wide.Aggregation{Op: wide.OpAvg, Column: "duration"}, 20},
		{wide.Aggregation{Op: wide.OpMax, Column: "duration"}, 30},
		{wide.Aggregation{Op: wide.OpMin, Column: "duration"}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.agg.OutputName(), func(t *testing.T) {
			rows := query(t, s, from, to, wide.QueryParams{Aggregations: []wide.Aggregation{tc.agg}})
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			if !rows[0].Timestamp.IsZero() {
				t.Errorf("an unwindowed aggregation row carries no time, got %v", rows[0].Timestamp)
			}
			if got, _ := field(rows[0], tc.agg.OutputName()); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("several aggregations in one query", func(t *testing.T) {
		rows := query(t, s, from, to, wide.QueryParams{Aggregations: []wide.Aggregation{
			{Op: wide.OpCount},
			{Op: wide.OpAvg, Column: "duration"},
		}})
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if got, _ := field(rows[0], "COUNT"); got != float64(3) {
			t.Errorf("COUNT: got %v", got)
		}
		if got, _ := field(rows[0], "AVG(duration)"); got != float64(20) {
			t.Errorf("AVG(duration): got %v", got)
		}
	})

	t.Run("no events in range yields no rows", func(t *testing.T) {
		rows := query(t, s, base.Add(time.Hour), base.Add(2*time.Hour), wide.QueryParams{Aggregations: []wide.Aggregation{{Op: wide.OpCount}}})
		if len(rows) != 0 {
			t.Errorf("expected no rows, got %v", rows)
		}
	})

	t.Run("a column nobody has: SUM is zero, the others are absent", func(t *testing.T) {
		sum := wide.Aggregation{Op: wide.OpSum, Column: "missing"}
		rows := query(t, s, from, to, wide.QueryParams{Aggregations: []wide.Aggregation{sum}})
		if len(rows) != 1 {
			t.Fatalf("SUM: expected 1 row, got %d", len(rows))
		}
		if got, _ := field(rows[0], sum.OutputName()); got != float64(0) {
			t.Errorf("SUM of a missing column: got %v, want 0", got)
		}
		for _, op := range []wide.Op{wide.OpAvg, wide.OpMax, wide.OpMin} {
			agg := wide.Aggregation{Op: op, Column: "missing"}
			rows := query(t, s, from, to, wide.QueryParams{Aggregations: []wide.Aggregation{agg}})
			if len(rows) != 1 {
				t.Fatalf("%s: expected 1 row, got %d", op, len(rows))
			}
			if v, ok := field(rows[0], agg.OutputName()); ok {
				t.Errorf("%s of a missing column should be absent, got %v", op, v)
			}
		}
	})
}

func invalidAggregation(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s, wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "label", Value: "foo"}, {Name: "active", Value: true}}})

	for _, column := range []string{"label", "active"} {
		for _, op := range []wide.Op{wide.OpSum, wide.OpAvg, wide.OpMax, wide.OpMin} {
			t.Run(string(op)+" over "+column, func(t *testing.T) {
				err := s.Query(t.Context(), base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{
					Aggregations: []wide.Aggregation{{Op: op, Column: column}},
				}, &wide.CollectSink{})
				if !errors.Is(err, wide.ErrInvalidQuery) {
					t.Errorf("expected an error wrapping ErrInvalidQuery, got %v", err)
				}
			})
		}
	}
}

func groupBy(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "host", Value: "a"}, {Name: "d", Value: float64(10)}}},
		wide.Event{Timestamp: base.Add(time.Second), Fields: []wide.Field{{Name: "host", Value: "a"}, {Name: "d", Value: float64(20)}}},
		wide.Event{Timestamp: base.Add(2 * time.Second), Fields: []wide.Field{{Name: "host", Value: "b"}, {Name: "d", Value: float64(5)}}},
		wide.Event{Timestamp: base.Add(3 * time.Second), Fields: []wide.Field{{Name: "d", Value: float64(7)}}},
	)

	rows := query(t, s, base.Add(-time.Minute), base.Add(time.Minute), wide.QueryParams{
		GroupBy:      []string{"host"},
		Aggregations: []wide.Aggregation{{Op: wide.OpCount}, {Op: wide.OpSum, Column: "d"}},
	})
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups (a, b, and the rows without host), got %d: %v", len(rows), rows)
	}
	want := map[any][2]float64{"a": {2, 30}, "b": {1, 5}, nil: {1, 7}}
	for _, row := range rows {
		host, _ := field(row, "host")
		expected, ok := want[host]
		if !ok {
			t.Errorf("unexpected group %v", host)
			continue
		}
		if got, _ := field(row, "COUNT"); got != expected[0] {
			t.Errorf("group %v: COUNT got %v, want %v", host, got, expected[0])
		}
		if got, _ := field(row, "SUM(d)"); got != expected[1] {
			t.Errorf("group %v: SUM(d) got %v, want %v", host, got, expected[1])
		}
	}
}

func window(t *testing.T, s api.Store) {
	const bucket = time.Minute
	base := time.Now().UTC().Truncate(time.Hour)
	receive(t, s,
		wide.Event{Timestamp: base, Fields: []wide.Field{{Name: "v", Value: float64(3)}, {Name: "svc", Value: "a"}}},
		wide.Event{Timestamp: base.Add(3 * time.Second), Fields: []wide.Field{{Name: "v", Value: float64(9)}, {Name: "svc", Value: "b"}}},
		wide.Event{Timestamp: base.Add(75 * time.Second), Fields: []wide.Field{{Name: "v", Value: float64(15)}, {Name: "svc", Value: "a"}}},
		wide.Event{Timestamp: base.Add(121 * time.Second), Fields: []wide.Field{{Name: "v", Value: float64(12)}, {Name: "svc", Value: "a"}}},
		wide.Event{Timestamp: base.Add(179 * time.Second), Fields: []wide.Field{{Name: "v", Value: float64(24)}, {Name: "svc", Value: "b"}}},
	)
	from, to := base, base.Add(time.Hour)

	t.Run("one row per bucket, stamped with the bucket start", func(t *testing.T) {
		rows := query(t, s, from, to, wide.QueryParams{
			Window:       bucket,
			Aggregations: []wide.Aggregation{{Op: wide.OpAvg, Column: "v"}},
		})
		want := map[time.Duration]float64{0: 6, bucket: 15, 2 * bucket: 18}
		if len(rows) != len(want) {
			t.Fatalf("expected %d buckets, got %d: %v", len(want), len(rows), rows)
		}
		for _, row := range rows {
			expected, ok := want[row.Timestamp.Sub(base)]
			if !ok {
				t.Errorf("unexpected bucket at %v", row.Timestamp)
				continue
			}
			if got, _ := field(row, "AVG(v)"); got != expected {
				t.Errorf("bucket %v: got %v, want %v", row.Timestamp.Sub(base), got, expected)
			}
		}
	})

	t.Run("combined with group by: one row per bucket and group", func(t *testing.T) {
		rows := query(t, s, from, to, wide.QueryParams{
			Window:       bucket,
			GroupBy:      []string{"svc"},
			Aggregations: []wide.Aggregation{{Op: wide.OpSum, Column: "v"}},
		})
		type key struct {
			offset time.Duration
			svc    any
		}
		want := map[key]float64{
			{0, "a"}: 3, {0, "b"}: 9,
			{bucket, "a"}:     15,
			{2 * bucket, "a"}: 12, {2 * bucket, "b"}: 24,
		}
		if len(rows) != len(want) {
			t.Fatalf("expected %d rows, got %d: %v", len(want), len(rows), rows)
		}
		for _, row := range rows {
			svc, _ := field(row, "svc")
			k := key{row.Timestamp.Sub(base), svc}
			expected, ok := want[k]
			if !ok {
				t.Errorf("unexpected row for %+v", k)
				continue
			}
			if got, _ := field(row, "SUM(v)"); got != expected {
				t.Errorf("%+v: got %v, want %v", k, got, expected)
			}
		}
	})
}

func outOfOrder(t *testing.T, s api.Store) {
	base := time.Now().UTC().Truncate(time.Second)
	stamps := []time.Time{base.Add(5 * time.Second), base.Add(time.Second), base.Add(9 * time.Second)}
	for i, ts := range stamps {
		receive(t, s, wide.Event{Timestamp: ts, Fields: []wide.Field{{Name: "i", Value: int64(i)}}})
	}

	rows := query(t, s, base, base.Add(time.Minute), wide.QueryParams{})
	if len(rows) != len(stamps) {
		t.Fatalf("expected %d rows, got %d", len(stamps), len(rows))
	}
	seen := map[time.Time]bool{}
	for _, row := range rows {
		seen[row.Timestamp.UTC()] = true
	}
	for _, ts := range stamps {
		if !seen[ts] {
			t.Errorf("event at %v missing from the result", ts)
		}
	}
}

func receive(t *testing.T, s api.Store, events ...wide.Event) {
	t.Helper()
	for _, e := range events {
		done := make(chan error, 1)
		if err := s.Receive(t.Context(), e, func(err error) { done <- err }); err != nil {
			t.Fatalf("receive: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("receive: ack reported %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("receive: ack was never called")
		}
	}
}

func query(t *testing.T, s api.Store, from, to time.Time, q wide.QueryParams) []wide.Event {
	t.Helper()
	sink := &wide.CollectSink{}
	if err := s.Query(t.Context(), from, to, q, sink); err != nil {
		t.Fatalf("query: %v", err)
	}
	return sink.Result
}

func field(e wide.Event, name string) (any, bool) {
	for _, f := range e.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}
