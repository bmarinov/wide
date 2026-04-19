package columnar

import (
	"testing"
)

// FindField looks up a named field in an event. Useful in assertions.
func FindField(t *testing.T, e Event, name string) (any, bool) {
	t.Helper()
	for _, f := range e.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}
