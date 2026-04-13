package columnar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// CollectSink gathers the response in memory.
type CollectSink struct {
	cols   []Column
	Result []Event
}

// Schema implements [Sink].
func (c *CollectSink) Schema(columns []Column) {
	c.cols = columns
}

// Row implements [Sink].
func (c *CollectSink) Row(ts time.Time, values []any) bool {
	row := Event{
		Timestamp: ts,
	}

	for i, v := range c.cols {
		if values[i] != nil {
			row.Fields = append(row.Fields, Field{Name: v.Name, Value: values[i]})
		}
	}

	c.Result = append(c.Result, row)
	return true
}

var _ Sink = &CollectSink{}

type StreamingSink struct {
	columns []Column
	// writer is used by encoder, can replace later?
	// writer io.Writer
	// json encoder, might pull out later?
	json *json.Encoder
}

// Row implements [Sink].
func (s *StreamingSink) Row(ts time.Time, values []any) bool {
	err := s.json.Encode(&jsonRow{ts: ts, cols: s.columns, values: values})

	return err == nil
}

func (s *StreamingSink) Schema(columns []Column) {
	s.columns = columns
}

func NewStreamingSink(w io.Writer) *StreamingSink {
	return &StreamingSink{
		// writer: w,
		json: json.NewEncoder(w),
	}
}

var _ Sink = &StreamingSink{}

type jsonRow struct {
	ts     time.Time
	cols   []Column
	values []any
}

func (r *jsonRow) MarshalJSON() ([]byte, error) {
	return marshalRaw(r)
}

// TODO:  encoding/json/v2

func marshalRaw(row *jsonRow) ([]byte, error) {
	if len(row.cols) != len(row.values) {
		return nil, fmt.Errorf("keys and values arrays must have equal length")
	}

	var b bytes.Buffer
	b.WriteByte('{')
	b.Write([]byte(`"timestamp":"`))
	buf := row.ts.AppendFormat(b.AvailableBuffer(), time.RFC3339Nano)
	b.Write(buf)
	b.WriteByte('"')

	for i, v := range row.cols {
		if row.values[i] == nil {
			continue
		}
		b.WriteByte(',')

		b.WriteByte('"')
		b.WriteString(v.Name)
		b.WriteString(`":`)

		val, err := json.Marshal(row.values[i])
		if err != nil {
			return nil, err
		}
		b.Write(val)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// marshalMap
//
// Deprecated: only used in benchmark comparison.
func marshalMap(row *jsonRow) ([]byte, error) {
	if len(row.cols) != len(row.values) {
		return nil, fmt.Errorf("keys and values arrays must have equal length")
	}

	m := make(map[string]interface{}, len(row.cols))
	m["timestamp"] = row.ts
	for i, key := range row.cols {
		m[key.Name] = row.values[i]
	}

	return json.Marshal(m)
}
