package router

import (
	"fmt"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
)

// field names as constants: proto version change = change here first
const (
	fieldStack   = "profile.stack"
	fieldTraceID = "trace_id"
	fieldSpanID  = "span_id"
)

func pivotProfiles(data *v1development.ProfilesData) []columnar.Event {
	var result []columnar.Event
	for _, rProf := range data.GetResourceProfiles() {
		for _, scopeProf := range rProf.ScopeProfiles {
			for _, profile := range scopeProf.Profiles {
				st := profile.SampleType
				sType := data.Dictionary.StringTable[st.TypeStrindex]
				sUnit := data.Dictionary.StringTable[st.UnitStrindex]

				for _, sample := range profile.Samples {

					event := columnar.Event{
						Fields: []columnar.Field{
							{Name: sType + "_" + sUnit},
						}}
					if len(sample.TimestampsUnixNano) == 0 {
						event.Timestamp = time.Unix(0, int64(profile.TimeUnixNano)).UTC()
					} else {
						event.Timestamp = time.Unix(0, int64(sample.TimestampsUnixNano[0])).UTC()
					}
					for _, attrKV := range rProf.Resource.Attributes {
						event.Fields = append(event.Fields, columnar.Field{
							Name:  attrKV.Key,
							Value: anyValue(attrKV.Value, data.Dictionary),
						})
					}
					// frames
					// link -> trace/span id

					result = append(result, event)
				}
			}
		}
	}

	return result
}

// anyValue converts an OTel AnyValue to a plain Go value.
// StringValueStrindex (profiling-specific) is resolved against the string table.
func anyValue(v *commonv1.AnyValue, dict *v1development.ProfilesDictionary) any {
	if v == nil {
		return nil
	}
	switch vt := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return vt.StringValue
	case *commonv1.AnyValue_StringValueStrindex:
		return dictStr(dict, vt.StringValueStrindex)
	case *commonv1.AnyValue_IntValue:
		return vt.IntValue
	case *commonv1.AnyValue_BoolValue:
		return vt.BoolValue
	case *commonv1.AnyValue_DoubleValue:
		return vt.DoubleValue
	case *commonv1.AnyValue_BytesValue:
		return vt.BytesValue
	default:
		return fmt.Sprintf("%v", v)
	}
}

// dictStr safely indexes into the string table. Returns "" for index 0 (sentinel).
func dictStr(dict *v1development.ProfilesDictionary, idx int32) string {
	if dict == nil || idx == 0 || int(idx) >= len(dict.StringTable) {
		return ""
	}
	return dict.StringTable[idx]
}
