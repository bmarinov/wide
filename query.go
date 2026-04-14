package columnar

import (
	"fmt"
	"time"
)

type QueryParams struct {
	Limit        int           `json:"limit"`
	Select       []string      `json:"select"`
	Filters      []Filter      `json:"filters"`
	Aggregations []Aggregation `json:"aggregations"`
	Window       time.Duration `json:"window"`
	GroupBy      []string      `json:"groupBy"`
}

// Aggregation query.
type Aggregation struct {
	Op     Op     `json:"op"`
	Column string `json:"column"`
}

func (a Aggregation) OutputName() string {
	if a.Op == OpCount {
		return string(OpCount)
	}
	return fmt.Sprintf("%s(%s)", a.Op, a.Column)
}

// Validate returns an error if the query parameters are invalid.
func (q QueryParams) Validate() error {
	seen := make(map[string]struct{}, len(q.Select))
	for _, col := range q.Select {
		if _, ok := seen[col]; ok {
			return fmt.Errorf("duplicate column in select: %q", col)
		}
		seen[col] = struct{}{}
	}
	return nil
}

type Filter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

// Column describes a named column and its type in a query result schema.
type Column struct {
	Name string
	Type ColumnType
}

// Sink receives query results. Schema is called once with the output column
// descriptors before any Row calls.
type Sink interface {
	Schema(columns []Column)
	Row(ts time.Time, values []any) (next bool)
}
