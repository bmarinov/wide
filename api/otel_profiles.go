package router

import (
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	collpb "go.opentelemetry.io/proto/slim/otlp/collector/profiles/v1development"
)

// field names as constants: proto version change = change here first
const (
	fieldStack      = "profile.stack"
	fieldValue      = "profile.value"
	fieldSampleType = "profile.sample_type"
	fieldSampleUnit = "profile.sample_unit"
	fieldTraceID    = "trace_id"
	fieldSpanID     = "span_id"
)

func pivotProfiles(req *collpb.ExportProfilesServiceRequest) []columnar.Event {
	var result []columnar.Event
	for _, prof := range req.GetResourceProfiles() {
		for _, scopeProf := range prof.ScopeProfiles {
			for _, profile := range scopeProf.Profiles {
				st := profile.SampleType
				sType := req.Dictionary.StringTable[st.TypeStrindex]
				sUnit := req.Dictionary.StringTable[st.UnitStrindex]

				for _, sample := range profile.Samples {
					// timestamps are collapsed to t0:
					ts := sample.TimestampsUnixNano[0]

					for _, sampleVal := range sample.Values {
						// dummy code
						_ = sampleVal
					}

					result = append(result, columnar.Event{
						Timestamp: time.Unix(0, int64(ts)).UTC(),
						Fields: []columnar.Field{
							{
								Name:  sType + "_" + sUnit,
								Value: sample.Values[0],
							},
						},
					})
				}
			}
		}
	}

	return result
}
