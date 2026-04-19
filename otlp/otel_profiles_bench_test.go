package otlp

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// buildBenchRequest builds a ProfilesData with nFrames locations per stack,
// nSamples ts-only samples, and nAttrs resource attributes, matching the
// shape seen in real eBPF payloads (one function per location, string-table
// encoded keys/values, TimestampsUnixNano set, Values empty).
func buildBenchRequest(nFrames, nSamples, nAttrs int) *v1development.ProfilesData {
	strs := []string{
		"",        // sentinel=0
		"samples", // 1
		"count",   // 2
	}
	intern := func(s string) int32 {
		for i, v := range strs {
			if v == s {
				return int32(i)
			}
		}
		idx := int32(len(strs))
		strs = append(strs, s)
		return idx
	}

	// FunctionTable: sentinel + nFrames
	fns := make([]*v1development.Function, 1, nFrames+1)
	fns[0] = &v1development.Function{}
	for i := range nFrames {
		nameIdx := intern(fmt.Sprintf("pkg.Func%d", i))
		fns = append(fns, &v1development.Function{NameStrindex: nameIdx})
	}

	// LocationTable: sentinel + nFrames (one function per location)
	locs := make([]*v1development.Location, 1, nFrames+1)
	locs[0] = &v1development.Location{}
	locIndices := make([]int32, nFrames)
	for i := range nFrames {
		fnIdx := int32(i + 1)
		locIdx := int32(i + 1)
		locs = append(locs, &v1development.Location{
			Lines: []*v1development.Line{{FunctionIndex: fnIdx}},
		})
		locIndices[i] = locIdx
	}

	// StackTable: sentinel + one stack referencing all locations
	stacks := []*v1development.Stack{
		{},
		{LocationIndices: locIndices},
	}

	// Resource attrs: key and value both string-table encoded
	attrs := make([]*commonv1.KeyValue, nAttrs)
	for i := range nAttrs {
		attrs[i] = &commonv1.KeyValue{
			KeyStrindex: intern(fmt.Sprintf("resource.attr%d", i)),
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValueStrindex{
					StringValueStrindex: intern(fmt.Sprintf("value%d", i)),
				},
			},
		}
	}

	samples := make([]*v1development.Sample, nSamples)
	for i := range nSamples {
		samples[i] = &v1development.Sample{
			StackIndex:         1,
			TimestampsUnixNano: []uint64{fixedTS + uint64(i)*1_000_000_000},
		}
	}

	return &v1development.ProfilesData{
		Dictionary: &v1development.ProfilesDictionary{
			StringTable:   strs,
			FunctionTable: fns,
			LocationTable: locs,
			StackTable:    stacks,
		},
		ResourceProfiles: []*v1development.ResourceProfiles{
			{
				Resource: &resourcev1.Resource{Attributes: attrs},
				ScopeProfiles: []*v1development.ScopeProfiles{
					{Profiles: []*v1development.Profile{{
						SampleType: &v1development.ValueType{TypeStrindex: 1, UnitStrindex: 2},
						Samples:    samples,
					}}},
				},
			},
		},
	}
}

var benchDepths = []struct {
	name   string
	frames int
}{
	{"shallow/5", 5},
	{"mid/15", 15},
	{"deep/30", 30},
}

// BenchmarkPivotProfiles_TsOnly measures the ts-only (eBPF) hot path with
// one sample per call across realistic stack depths.
func BenchmarkPivotProfiles_TsOnly(b *testing.B) {
	for _, tc := range benchDepths {
		req := buildBenchRequest(tc.frames, 1, 3)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				PivotProfiles(req)
			}
		})
	}
}

// BenchmarkPivotProfiles_TsOnlyBatch measures throughput with 100 samples
// per call, closer to a real ingest burst.
func BenchmarkPivotProfiles_TsOnlyBatch(b *testing.B) {
	for _, tc := range benchDepths {
		req := buildBenchRequest(tc.frames, 100, 3)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				PivotProfiles(req)
			}
		})
	}
}
