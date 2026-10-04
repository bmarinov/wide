package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bmarinov/wide"
)

// Receiver accepts events for storage. The receiver owns e and may keep it
// after Receive returns.
//
// When ack is not nil it is called with the result once the event has been applied.
type Receiver interface {
	Receive(ctx context.Context, e wide.Event, ack func(error)) error
}

// Querier streams the rows matching a query into sink.
type Querier interface {
	Query(ctx context.Context, from, to time.Time, q wide.QueryParams, sink wide.Sink) error
}

// Store is everything the API needs from a backing event store.
type Store interface {
	Receiver
	Querier
}

func NewAppMux(store Store) *http.ServeMux {
	otelMux := newOTELMux(store)
	eventMux := newEventsMux(store)

	appMux := http.NewServeMux()
	appMux.Handle("/v1/", otelMux)
	appMux.Handle("/v1development/", otelMux)
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

func newEventsMux(store Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Accept")
		acceptJSON := acceptsJSON(r.Header.Get("Accept"))
		serveQuery(w, r, store, acceptJSON)
	}))
	mux.Handle("POST /events", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received := time.Now().UTC()
		err := processNDJSON(r.Body, func(e wide.Event) error {
			if e.Timestamp.IsZero() {
				e.Timestamp = received
			}
			return store.Receive(r.Context(), e, nil)
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	// deprecated, endpoint used by the grafana plugin
	mux.Handle("POST /query/json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveQuery(w, r, store, true)
	}))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	return mux
}

// serveQuery writes the store query result to w.
func serveQuery(w http.ResponseWriter, r *http.Request, store Store, asJSONArray bool) {
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

	var params wide.QueryParams
	err = json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := params.Validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if asJSONArray {
		sink := &wide.CollectSink{}
		err = store.Query(r.Context(), from, to, params, sink)
		if err != nil {
			if errors.Is(err, wide.ErrInvalidQuery) {
				w.WriteHeader(http.StatusBadRequest)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = writeJSONArray(sink, w)
		if err != nil {
			slog.Error("encoding json", "err", err)
		}
	} else {
		w.Header().Set("Content-Type", "application/x-ndjson")

		sink := wide.NewStreamingSink(w)
		err = store.Query(r.Context(), from, to, params, sink)
		if err != nil {
			if errors.Is(err, wide.ErrInvalidQuery) {
				w.WriteHeader(http.StatusBadRequest)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}

// acceptsJSON returns true when the request header indicates json as an accepted response.
func acceptsJSON(acceptHeader string) bool {
	accept := strings.SplitSeq(acceptHeader, ",")

	for mediaRange := range accept {
		mediaType, _, _ := strings.Cut(strings.TrimSpace(mediaRange), ";")

		if strings.EqualFold(mediaType, "application/json") {
			return true
		}
	}
	return false
}

// writeJSONArray marshals the collected rows as a json array and writes it to w.
func writeJSONArray(sink *wide.CollectSink, w http.ResponseWriter) error {
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
	return json.NewEncoder(w).Encode(rows)
}

func processNDJSON(r io.Reader, handle func(wide.Event) error) error {
	br := bufio.NewReaderSize(r, 64*1024)
	dec := json.NewDecoder(br)
	dec.UseNumber()

	ev := wide.Event{
		Fields: make([]wide.Field, 0, 64),
	}
	for {
		err := parseLine(dec, &ev)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// the store may keep the event after Receive returns, so it gets its own fields
		e := ev
		e.Fields = slices.Clone(ev.Fields)
		err = handle(e)
		if err != nil {
			return err
		}
	}

}

func parseLine(dec *json.Decoder, dest *wide.Event) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("unexpected token %v", t)
	}

	dest.Timestamp = time.Time{}
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
			dest.Fields = append(dest.Fields, wide.Field{Name: key, Value: v})
		case string:
			dest.Fields = append(dest.Fields, wide.Field{Name: key, Value: v})
		case json.Number:
			if i, err := v.Int64(); err == nil {
				dest.Fields = append(dest.Fields, wide.Field{Name: key, Value: i})
			} else if f, err := v.Float64(); err == nil {
				dest.Fields = append(dest.Fields, wide.Field{Name: key, Value: f})
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
