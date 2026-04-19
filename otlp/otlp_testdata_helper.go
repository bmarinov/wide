package otlp

import (
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Dict layout for buildRequest: named indices are compile-checked cross-references.
// Update both the constant and the table slice when the layout changes.
const (
	// StringTable: [0] is always "" (proto sentinel for string indices)
	strSamples  int32 = 1
	strCount    int32 = 2
	strMainMain int32 = 3
	strGoexit   int32 = 4
	strMainGo   int32 = 5

	// FunctionTable: [0] is the null sentinel (Function{})
	fnMainMain int32 = 1
	fnGoexit   int32 = 2

	// LocationTable: [0] is the null sentinel (Location{})
	locMainMain int32 = 1
	locGoexit   int32 = 2

	// StackTable: [0] is the null sentinel (Stack{}); StackIndex 0 means "no stack"
	stkKnownCall int32 = 1 // leaf-first: main.main;runtime.goexit
)

// knownStack is the call stack every buildRequest profile resolves to.
// Leaf-first, semicolon-separated (the pprof collapsed format).
const knownStack = "main.main;runtime.goexit"

// knownSampleCol is the value column name derived from the buildRequest dictionary.
const knownSampleCol = "samples_count"

// BuildProfilesRequest constructs a minimal but complete ProfilesData whose encoding
// matches what protojson.Unmarshal produces from real eBPF profiler payloads:
// all strings go through the shared dictionary, attribute keys and values are
// stored as StringTable indices (KeyStrindex / StringValueStrindex), never inline.
func BuildProfilesRequest(samples []*v1development.Sample, resourceAttrs map[string]string) *v1development.ProfilesData {
	strs := []string{
		"",               // strSentinel=0
		"samples",        // strSamples=1
		"count",          // strCount=2
		"main.main",      // strMainMain=3
		"runtime.goexit", // strGoexit=4
		"main.go",        // strMainGo=5
	}

	// intern appends s to strs if not present and returns its index.
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

	// Resource attrs: key and string value both go through the string table,
	// matching the KeyStrindex / StringValueStrindex encoding protojson produces.
	attrs := make([]*commonv1.KeyValue, 0, len(resourceAttrs))
	for k, v := range resourceAttrs {
		attrs = append(attrs, &commonv1.KeyValue{
			KeyStrindex: intern(k),
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValueStrindex{StringValueStrindex: intern(v)},
			},
		})
	}

	if samples == nil {
		samples = []*v1development.Sample{OneSample(1_000_000_000)}
	}

	return &v1development.ProfilesData{
		Dictionary: &v1development.ProfilesDictionary{
			StringTable: strs,
			FunctionTable: []*v1development.Function{
				{}, // sentinel=0
				{NameStrindex: strMainMain, FilenameStrindex: strMainGo}, // fnMainMain=1
				{NameStrindex: strGoexit, FilenameStrindex: strMainGo},   // fnGoexit=2
			},
			LocationTable: []*v1development.Location{
				{}, // sentinel=0
				{Lines: []*v1development.Line{{FunctionIndex: fnMainMain}}}, // locMainMain=1
				{Lines: []*v1development.Line{{FunctionIndex: fnGoexit}}},   // locGoexit=2
			},
			StackTable: []*v1development.Stack{
				{}, // sentinel=0
				{LocationIndices: []int32{locMainMain, locGoexit}}, // stkKnownCall=1
			},
		},
		ResourceProfiles: []*v1development.ResourceProfiles{
			{
				Resource: &resourcev1.Resource{Attributes: attrs},
				ScopeProfiles: []*v1development.ScopeProfiles{
					{Profiles: []*v1development.Profile{{
						SampleType: &v1development.ValueType{TypeStrindex: strSamples, UnitStrindex: strCount},
						Samples:    samples,
					}}},
				},
			},
		},
	}
}

// OneSample returns a Sample pointing at stkKnownCall with a single timestamp
// and no explicit value, the eBPF shape. The implicit count per observation is 1.
func OneSample(timestampNano uint64) *v1development.Sample {
	return &v1development.Sample{
		StackIndex:         stkKnownCall,
		TimestampsUnixNano: []uint64{timestampNano},
	}
}
