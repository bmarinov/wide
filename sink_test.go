package wide

import (
	"bufio"
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestStreamingSink(t *testing.T) {
	t.Run("single row", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)

		sink.Schema([]Column{
			{
				Name: "host",
				Type: ColumnString,
			},
			{
				Name: "status",
				Type: ColumnInt64,
			},
			{
				Name: "test",
				Type: ColumnBool,
			},
		})

		next := sink.Row(time.Now(), []any{"localhost", 200, true})
		if !next {
			t.Error("unexpected stop")
		}

		var got map[string]any
		err := json.Unmarshal(b.Bytes(), &got)
		if err != nil {
			t.Fatal(err)
		}

		if got["host"] != "localhost" {
			t.Errorf("expected host=localhost got %v", got["host"])
		}
		if got["status"] != float64(200) {
			t.Errorf("expected status=200 got %v", got["status"])
		}
		if got["test"] != true {
			t.Errorf("expected test=true got %v", got["test"])
		}
	})
	t.Run("multiple delimited rows", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)

		sink.Schema([]Column{
			{
				Name: "host",
				Type: ColumnString,
			},
			{
				Name: "status",
				Type: ColumnInt64,
			},
		})
		_ = sink.Row(time.Now(), []any{"abc", 500})
		_ = sink.Row(time.Now(), []any{"def", 404})

		var rows []map[string]any
		scanner := bufio.NewScanner(&b)
		for scanner.Scan() {
			var row map[string]any
			err := json.Unmarshal(scanner.Bytes(), &row)
			if err != nil {
				t.Fatalf("reading row: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 2 {
			t.Errorf("expected 2 rows got %d", len(rows))
		}
	})
	t.Run("mismatched columns and values", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)
		sink.Schema([]Column{
			{
				Name: "foo",
				Type: ColumnString,
			},
			{
				Name: "bar",
				Type: ColumnBool,
			},
		})
		next := sink.Row(time.Now(), []any{"blap"})
		if next {
			t.Error("expected stop for mismatched values len")
		}
	})

	// --- transport/serialization: json-specific tests:

	t.Run("nil values", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)
		sink.Schema([]Column{
			{
				Name: "foo",
				Type: ColumnString,
			},
			{
				Name: "bar",
				Type: ColumnBool,
			},
		})
		next := sink.Row(time.Now(), []any{"here", nil})
		if !next {
			t.Error("unexpected stop")
		}

		var got map[string]any
		err := json.Unmarshal(b.Bytes(), &got)
		if err != nil {
			t.Fatal(err)
		}
		v, found := got["bar"]
		if found {
			t.Errorf("nil field should not appear in output row, got %v", v)
		}
	})
	t.Run("default values", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)
		columns := []Column{
			{
				Name: "a",
				Type: ColumnString,
			},
			{
				Name: "b",
				Type: ColumnBool,
			},
			{
				Name: "c",
				Type: ColumnFloat64,
			},
		}
		sink.Schema(columns)

		_ = sink.Row(time.Now(), []any{"", false, 0})

		var got map[string]any
		err := json.Unmarshal(b.Bytes(), &got)
		if err != nil {
			t.Fatal(err)
		}

		for _, column := range columns {
			_, found := got[column.Name]
			if !found {
				t.Errorf("expected map entry  with zero value for column %s", column.Name)
			}
		}
	})
	t.Run("ts key", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)
		sink.Schema([]Column{
			{
				Name: "foo",
				Type: ColumnString,
			},
		})
		tsRef := time.Date(2026, 3, 11, 16, 45, 51, 120_000_000, time.UTC)

		_ = sink.Row(tsRef, []any{"here"})

		var got map[string]any
		err := json.Unmarshal(b.Bytes(), &got)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"ts": "2026-03-11T16:45:51.12Z", "foo": "here"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("row\n expected %v\ngot %v", want, got)
		}
	})
	t.Run("zero time has no ts", func(t *testing.T) {
		var b bytes.Buffer
		sink := NewStreamingSink(&b)
		sink.Schema([]Column{
			{
				Name: "COUNT",
				Type: ColumnFloat64,
			},
		})

		_ = sink.Row(time.Time{}, []any{float64(2)})

		var got map[string]any
		err := json.Unmarshal(b.Bytes(), &got)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"COUNT": float64(2)}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("row\n expected %v\ngot %v", want, got)
		}
	})
}
