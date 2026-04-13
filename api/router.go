package router

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
)

func NewAppMux(store *columnar.Store) *http.ServeMux {
	otelMux := newOTELMux(store)
	eventMux := newEventsMux(store)

	appMux := http.NewServeMux()
	appMux.Handle("/v1/", otelMux)
	appMux.Handle("/", eventMux)
	appMux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	return appMux
}

func NewServer(mux *http.ServeMux, port int) *http.Server {
	srv := http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	return &srv
}

func newEventsMux(store *columnar.Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, err := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		to, err := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var params columnar.QueryParams
		err = json.NewDecoder(r.Body).Decode(&params)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := params.Validate(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")

		sink := columnar.NewStreamingSink(w)
		err = store.Query(r.Context(), from, to, params, sink)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	mux.Handle("POST /events", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := processNDJSON(r.Body, func(e columnar.Event) error {
			return store.Receive(r.Context(), e, nil)
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}))

	mux.Handle("POST /query/json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, err := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		to, err := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var params columnar.QueryParams
		err = json.NewDecoder(r.Body).Decode(&params)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := params.Validate(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		sink := &columnar.CollectSink{}
		_ = store.Query(r.Context(), from, to, params, sink)

		w.Header().Set("Content-Type", "application/json")

		rows := make([]map[string]any, 0, len(sink.Result))
		for _, e := range sink.Result {
			row := map[string]any{}
			if !e.Timestamp.IsZero() {
				row["ts"] = e.Timestamp.Format(time.RFC3339Nano)
			}
			for _, f := range e.Fields {
				row[f.Name] = f.Value
			}
			rows = append(rows, row)
		}
		err = json.NewEncoder(w).Encode(rows)
		if err != nil {
			slog.Error("encoding json", "err", err)
		}
	}))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	return mux
}

func processNDJSON(r io.Reader, handle func(columnar.Event) error) error {
	br := bufio.NewReaderSize(r, 64*1024)
	dec := json.NewDecoder(br)
	dec.UseNumber()

	ev := columnar.Event{
		Fields: make([]columnar.Field, 0, 64),
	}
	for {
		err := parseLine(dec, &ev)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		err = handle(ev)
		if err != nil {
			return err
		}
	}

}

func parseLine(dec *json.Decoder, dest *columnar.Event) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("unexpected token %v", t)
	}

	// TODO: measure
	dest.Fields = dest.Fields[:0]

	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("expected string for key got %T", keyToken)
		}

		valueToken, err := dec.Token()
		if err != nil {
			return err
		}

		if key == "ts" {
			switch v := valueToken.(type) {
			case string:
				dest.Timestamp, err = time.Parse(time.RFC3339Nano, v)
				if err != nil {
					return fmt.Errorf("parsing ts: %w", err)
				}
			case json.Number:
				ms, err := v.Int64()
				if err != nil {
					return fmt.Errorf("parsing ts as int: %w", err)
				}
				dest.Timestamp = time.UnixMilli(ms).UTC()
			default:
				return fmt.Errorf("unexpected ts type %T", valueToken)
			}
			continue
		}

		switch v := valueToken.(type) {
		case bool:
			dest.Fields = append(dest.Fields, columnar.Field{Name: key, Value: v})
		case string:
			dest.Fields = append(dest.Fields, columnar.Field{Name: key, Value: v})
		case json.Number:
			if i, err := v.Int64(); err == nil {
				dest.Fields = append(dest.Fields, columnar.Field{Name: key, Value: i})
			} else if f, err := v.Float64(); err == nil {
				dest.Fields = append(dest.Fields, columnar.Field{Name: key, Value: f})
			} else {
				return fmt.Errorf("field %q: unparseable number %q", key, v)
			}
		case nil:
		default:
			return fmt.Errorf("field %q: unexpected type %T", key, valueToken)
		}
	}
	// closing }
	if _, err := dec.Token(); err != nil {
		return err
	}
	return nil
}
