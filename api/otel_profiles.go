package router

import (
	"encoding/hex"
	"fmt"
	"strings"
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

const (
	stackDelim  = ';'
	nativeFrame = "<unknown>"
)

func pivotProfiles(data *v1development.ProfilesData) []columnar.Event {
	if data.Dictionary == nil {
		// invalid data
		return nil
	}

	var result []columnar.Event
	for _, rProf := range data.GetResourceProfiles() {
		for _, scopeProf := range rProf.ScopeProfiles {
			for _, profile := range scopeProf.Profiles {

				if profile.SampleType == nil {
					continue
				}

				st := profile.SampleType
				sType := data.Dictionary.StringTable[st.TypeStrindex]
				sUnit := data.Dictionary.StringTable[st.UnitStrindex]

				for _, sample := range profile.Samples {

					var baseFields []columnar.Field
					if rProf.Resource == nil {
						// no attributes
						baseFields = make([]columnar.Field, 0, 2)
					} else {
						// capacity = attrs + name/value + stack:
						baseFields = make([]columnar.Field, 0, 2+len(rProf.Resource.Attributes))
						for _, attrKV := range rProf.Resource.Attributes {
							key := attrKV.GetKey()
							if key == "" {
								key = dictStr(data.Dictionary, attrKV.KeyStrindex)
							}
							val := anyValue(attrKV.Value, data.Dictionary)
							baseFields = append(baseFields, columnar.Field{
								Name:  key,
								Value: val,
							})
						}
					}

					// frames
					if sample.StackIndex > 0 &&
						len(data.Dictionary.StackTable) > int(sample.StackIndex) {
						stack := data.Dictionary.StackTable[sample.StackIndex]
						var frames []string

						for _, locIdx := range stack.LocationIndices {
							loc := dictLookup(data.Dictionary.LocationTable, locIdx)
							if loc == nil {
								continue
							}
							if len(loc.Lines) == 0 {
								frames = append(frames, nativeFrame)
							} else {
								for _, stackLine := range loc.Lines {
									fn := dictLookup(data.Dictionary.FunctionTable, stackLine.FunctionIndex)
									if fn == nil {
										continue
									}
									fnName := dictStr(data.Dictionary, fn.NameStrindex)
									// todo: null check
									frames = append(frames, fnName)
								}
							}
						}
						baseFields = append(baseFields,
							columnar.Field{Name: fieldStack, Value: strings.Join(frames, string(stackDelim))})
					}

					link := dictLookup(data.Dictionary.LinkTable, sample.LinkIndex)
					if link != nil {
						if len(link.TraceId) > 0 {
							baseFields = append(baseFields, columnar.Field{
								Name:  fieldTraceID,
								Value: hex.EncodeToString(link.TraceId),
							})
						}
						if len(link.SpanId) > 0 {
							baseFields = append(baseFields, columnar.Field{
								Name:  fieldSpanID,
								Value: hex.EncodeToString(link.SpanId),
							})
						}
					}

					if len(sample.TimestampsUnixNano) > 0 && len(sample.Values) == 0 {
						// ts-only shape
						for _, sampleTS := range sample.TimestampsUnixNano {
							fields := make([]columnar.Field, len(baseFields)+1)
							copy(fields, baseFields)
							fields[len(fields)-1] = columnar.Field{Name: sType + "_" + sUnit, Value: int64(1)}
							result = append(result, columnar.Event{
								Timestamp: time.Unix(0, int64(sampleTS)).UTC(),
								Fields:    fields,
							})
						}
					} else if len(sample.TimestampsUnixNano) == len(sample.Values) &&
						len(sample.TimestampsUnixNano) > 0 {
						// zip
						for i, sampleTS := range sample.TimestampsUnixNano {
							row := columnar.Event{
								Timestamp: time.Unix(0, int64(sampleTS)).UTC(),
								Fields:    make([]columnar.Field, len(baseFields)+1),
							}
							copy(row.Fields, baseFields)
							row.Fields[len(row.Fields)-1] = columnar.Field{Name: sType + "_" + sUnit, Value: sample.Values[i]}
							result = append(result, row)
						}
					} else if len(sample.Values) == 1 && len(sample.TimestampsUnixNano) == 0 {
						// aggregated
						result = append(result, columnar.Event{
							Timestamp: time.Unix(0, int64(profile.TimeUnixNano)).UTC(),
							Fields: append(baseFields, columnar.Field{
								Name:  sType + "_" + sUnit,
								Value: sample.Values[0],
							}),
						})
					} else {
						// unknown shape
						// slog.Error()
					}
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

// dictLookup safely indexes into a lookup table. Returns "" for index 0 (sentinel).
func dictLookup[T any](lookupTable []T, idx int32) T {
	if idx == 0 || lookupTable == nil || int(idx) >= len(lookupTable) {
		var zero T
		return zero
	}
	return lookupTable[idx]
}
