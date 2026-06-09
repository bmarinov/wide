package wide

import "time"

type ColumnType = string

const (
	ColumnBool    ColumnType = "bool"
	ColumnFloat64 ColumnType = "float64"
	ColumnInt64   ColumnType = "int64"
	ColumnString  ColumnType = "string"
	ColumnUnknown ColumnType = "unknown"
)

const (
	EqOperator        = "eq"
	ExistsOperator    = "exists"
	NotExistsOperator = "not_exists"
	GtOperator        = "gt"
	LtOperator        = "lt"
	GteOperator       = "gte"
	LteOperator       = "lte"
)

type Event struct {
	Timestamp time.Time
	Fields    []Field
}

type Field struct {
	Name  string
	Value any
}

type Op string

const (
	OpCount Op = "COUNT"
	OpMax   Op = "MAX"
	OpMin   Op = "MIN"
	OpSum   Op = "SUM"
	OpAvg   Op = "AVG"
)

// valid returns false for unknown aggregation operations.
func (op Op) valid() bool {
	switch op {
	case OpCount, OpSum, OpAvg, OpMax, OpMin:
		return true
	}
	return false
}
