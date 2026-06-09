package otlp

import (
	"github.com/bmarinov/sandbox-columnstore/internal/wide/widetest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/encoding/protojson"
)

var fixedTS = uint64(time.Now().Truncate(time.Microsecond).UnixNano())

// malformed or missing data must not panic
func TestPivotProfiles_Stability(t *testing.T) {
	tests := []struct {
		name       string
		req        func() *v1development.ProfilesData
		wantEvents int
	}{
		{
			name: "nil dictionary yields no events",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.Dictionary = nil
				return req
			},
			wantEvents: 0,
		},
		{
			name: "nil sample type yields no events",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].SampleType = nil
				return req
			},
			wantEvents: 0,
		},
		{
			name: "nil sample type in one profile does not suppress events from sibling profiles",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				invalid := &v1development.Profile{SampleType: nil, Samples: []*v1development.Sample{OneSample(fixedTS)}}
				req.ResourceProfiles[0].ScopeProfiles[0].Profiles = append(
					[]*v1development.Profile{invalid},
					req.ResourceProfiles[0].ScopeProfiles[0].Profiles...,
				)
				return req
			},
			wantEvents: 1,
		},
		{
			name: "nil resource still yields event without attributes",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.ResourceProfiles[0].Resource = nil
				return req
			},
			wantEvents: 1,
		},
		{
			name: "nil stack table still yields event without stack field",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.Dictionary.StackTable = nil
				return req
			},
			wantEvents: 1,
		},
		{
			name: "stack index beyond table length still yields event without stack field",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].Samples[0].StackIndex = 99
				return req
			},
			wantEvents: 1,
		},
		{
			name: "location index beyond table length still yields event with partial stack",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.Dictionary.StackTable[stkKnownCall].LocationIndices = []int32{99}
				return req
			},
			wantEvents: 1,
		},
		{
			name: "function index beyond table length still yields event with partial stack",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
				req.Dictionary.LocationTable[locMainMain].Lines[0].FunctionIndex = 99
				return req
			},
			wantEvents: 1,
		},
		{
			name: "link index beyond table length still yields event without trace fields",
			req: func() *v1development.ProfilesData {
				req := BuildProfilesRequest([]*v1development.Sample{{
					StackIndex:         stkKnownCall,
					TimestampsUnixNano: []uint64{fixedTS},
					LinkIndex:          99,
				}}, nil)
				return req
			},
			wantEvents: 1,
		},
		{
			name: "mismatched timestamps and values produces zero events",
			req: func() *v1development.ProfilesData {
				return BuildProfilesRequest([]*v1development.Sample{{
					StackIndex:         stkKnownCall,
					TimestampsUnixNano: []uint64{fixedTS, fixedTS + 1},
					Values:             []int64{10}, // length mismatch: unknown shape
				}}, nil)
			},
			wantEvents: 0,
		},
		{
			name: "sample with no timestamps and no values produces zero events",
			req: func() *v1development.ProfilesData {
				return BuildProfilesRequest([]*v1development.Sample{{StackIndex: stkKnownCall}}, nil)
			},
			wantEvents: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			events := PivotProfiles(tc.req())
			if len(events) != tc.wantEvents {
				t.Errorf("got %d events, want %d", len(events), tc.wantEvents)
			}
		})
	}
}

// --- Sentinel: index 0 in any table means "unset"; field absent from event, no crash ---

func TestPivotProfiles_StackIndexZeroProducesEventWithNoStackField(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{{
		StackIndex:         0, // sentinel, "no stack recorded"
		TimestampsUnixNano: []uint64{fixedTS},
	}}, nil)

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := widetest.FindField(t, events[0], fieldStack); ok && v != "" {
		t.Errorf("expected no %q field for sentinel StackIndex, got %q", fieldStack, v)
	}
}

func TestPivotProfiles_UnresolvableLocationProducesEmptyStackField(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
	req.Dictionary.StackTable[stkKnownCall] = &v1development.Stack{
		LocationIndices: []int32{0}, // sentinel location only
	}

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := widetest.FindField(t, events[0], fieldStack); ok && v != "" {
		t.Errorf("expected empty %q for sentinel location, got %q", fieldStack, v)
	}
}

func TestPivotProfiles_FrameWithNoFunctionNameOmittedFromStack(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
	locWithSentinelFn := int32(len(req.Dictionary.LocationTable))
	req.Dictionary.LocationTable = append(req.Dictionary.LocationTable,
		&v1development.Location{Lines: []*v1development.Line{{FunctionIndex: 0}}},
	)
	req.Dictionary.StackTable[stkKnownCall] = &v1development.Stack{
		LocationIndices: []int32{locWithSentinelFn, locMainMain},
	}

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	v, ok := widetest.FindField(t, events[0], fieldStack)
	if !ok || v == "" {
		t.Fatalf("expected non-empty %q field, got %v", fieldStack, v)
	}
	if v.(string) != "main.main" {
		t.Errorf("sentinel fn frame must be omitted: got %q, want %q", v, "main.main")
	}
}

func TestPivotProfiles_SampleWithNoLinkedTraceContextEmitsNoTraceOrSpanFields(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if _, ok := widetest.FindField(t, events[0], fieldTraceID); ok {
		t.Errorf("expected no %q field when LinkIndex is unset", fieldTraceID)
	}
	if _, ok := widetest.FindField(t, events[0], fieldSpanID); ok {
		t.Errorf("expected no %q field when LinkIndex is unset", fieldSpanID)
	}
}

// --- Mapping: correct values extracted from valid, well-formed data ---

func TestPivotProfiles_OneSampleProducesOneEvent(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil))

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := widetest.FindField(t, events[0], fieldStack); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldStack)
	}
	if v, ok := widetest.FindField(t, events[0], knownSampleCol); !ok || v != int64(1) {
		t.Errorf("expected %q = 1 (implicit count), got %v", knownSampleCol, v)
	}
}

func TestPivotProfiles_StackResolvesToKnownCallChain(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil))

	v, ok := widetest.FindField(t, events[0], fieldStack)
	if !ok {
		t.Fatalf("missing %q field", fieldStack)
	}
	if v != knownStack {
		t.Errorf("stack: got %q, want %q", v, knownStack)
	}
}

func TestPivotProfiles_MultipleTimestampsWithoutValuesProducesOneEventPerTimestamp(t *testing.T) {
	sample := &v1development.Sample{
		StackIndex:         stkKnownCall,
		TimestampsUnixNano: []uint64{fixedTS, fixedTS + 1_000_000_000, fixedTS + 2_000_000_000},
	}
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{sample}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events (one per timestamp), got %d", len(events))
	}
	for i, want := range []uint64{fixedTS, fixedTS + 1_000_000_000, fixedTS + 2_000_000_000} {
		wantT := time.Unix(0, int64(want)).UTC()
		if !events[i].Timestamp.Equal(wantT) {
			t.Errorf("event %d: timestamp got %v, want %v", i, events[i].Timestamp, wantT)
		}
	}
}

func TestPivotProfiles_PairedTimestampsAndValuesProduceOneEventPerPair(t *testing.T) {
	sample := &v1development.Sample{
		StackIndex:         stkKnownCall,
		TimestampsUnixNano: []uint64{fixedTS, fixedTS + uint64(time.Minute), fixedTS + 2*uint64(time.Minute)},
		Values:             []int64{25, 33, 424},
	}
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{sample}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events (one per timestamp), got %d", len(events))
	}
	for i := range len(events) {
		expectedTime := time.Unix(0, int64(sample.TimestampsUnixNano[i])).UTC()
		expectedVal := sample.Values[i]
		if !events[i].Timestamp.Equal(expectedTime) {
			t.Errorf("event %d: timestamp got %v, want %v", i, events[i].Timestamp, expectedTime)
		}
		actualV, got := widetest.FindField(t, events[i], knownSampleCol)
		if !got {
			t.Fatalf("field %s not found on %v", knownSampleCol, events[i])
		}
		if actualV != expectedVal {
			t.Errorf("event %d: val got %v, want %v", i, actualV, expectedVal)
		}
	}
}

func TestPivotProfiles_MultipleSamplesProduceOneEventEach(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{
		OneSample(fixedTS),
		OneSample(fixedTS + 1_000_000_000),
		OneSample(fixedTS + 2_000_000_000),
	}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestPivotProfiles_ResourceAttributesFlowToEveryEvent(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest(
		[]*v1development.Sample{OneSample(fixedTS), OneSample(fixedTS + 1_000_000_000)},
		map[string]string{
			"process.executable.name": "columnstore",
			"service.name":            "sandbox-columnstore",
		},
	))

	for i, e := range events {
		if v, ok := widetest.FindField(t, e, "process.executable.name"); !ok || v != "columnstore" {
			t.Errorf("event %d: process.executable.name: got %v, want columnstore", i, v)
		}
		if v, ok := widetest.FindField(t, e, "service.name"); !ok || v != "sandbox-columnstore" {
			t.Errorf("event %d: service.name: got %v, want sandbox-columnstore", i, v)
		}
	}
}

func TestPivotProfiles_SampleTimestampUsedWhenPresent(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil))

	want := time.Unix(0, int64(fixedTS)).UTC()
	if !events[0].Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", events[0].Timestamp, want)
	}
}

func TestPivotProfiles_UsesProfileTimeWhenSampleCarriesNoTimestamp(t *testing.T) {
	expectedValue := int64(35)
	req := BuildProfilesRequest([]*v1development.Sample{{StackIndex: stkKnownCall, Values: []int64{int64(expectedValue)}}}, nil)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].TimeUnixNano = fixedTS

	events := PivotProfiles(req)
	if len(events) != 1 {
		t.Fatalf("expected 1 event got %d", len(events))
	}

	parsed := events[0]
	want := time.Unix(0, int64(fixedTS)).UTC()
	if !parsed.Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", parsed.Timestamp, want)
	}
	v, found := widetest.FindField(t, parsed, knownSampleCol)
	if !found {
		t.Fatalf("expected field %s not found", knownSampleCol)
	}
	if expectedValue != v {
		t.Errorf("expected profile value %v got %v", expectedValue, v)
	}
}

func TestPivotProfiles_ZeroSamplesProducesZeroEvents(t *testing.T) {
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{}, nil))

	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestPivotProfiles_LinkIndexResolvesTraceAndSpanID(t *testing.T) {
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7}
	req := BuildProfilesRequest([]*v1development.Sample{{StackIndex: stkKnownCall, TimestampsUnixNano: []uint64{fixedTS}, LinkIndex: 1}}, nil)
	req.Dictionary.LinkTable = []*v1development.Link{
		{},                                 // sentinel=0
		{TraceId: traceID, SpanId: spanID}, // real link=1
	}

	events := PivotProfiles(req)

	expectedTraceID := "0102030405060708090a0b0c0d0e0f10"
	if v, ok := widetest.FindField(t, events[0], fieldTraceID); !ok || v != expectedTraceID {
		t.Errorf("expected trace_id %s got %v", expectedTraceID, v)
	}
	expectedSpanID := "a0a1a2a3a4a5a6a7"
	if v, ok := widetest.FindField(t, events[0], fieldSpanID); !ok || v != expectedSpanID {
		t.Errorf("expected span_id %s got %v", expectedSpanID, v)
	}
}

func TestPivotProfiles_UnsymbolizedLocationProducedNoFrameInStack(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)
	locUnsymbolized := int32(len(req.Dictionary.LocationTable))
	req.Dictionary.LocationTable = append(req.Dictionary.LocationTable,
		&v1development.Location{Lines: nil},
	)
	stkUnsymbolized := int32(len(req.Dictionary.StackTable))
	req.Dictionary.StackTable = append(req.Dictionary.StackTable,
		&v1development.Stack{LocationIndices: []int32{locUnsymbolized, locGoexit}},
	)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].Samples[0].StackIndex = stkUnsymbolized

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	v, ok := widetest.FindField(t, events[0], fieldStack)
	if !ok {
		t.Fatalf("missing %q field", fieldStack)
	}
	want := nativeFrame + ";" + "runtime.goexit"
	if v != want {
		t.Errorf("stack: got %q, want %q", v, want)
	}
}

func TestPivotProfiles_InlinedFramesAllAppearInStack(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{OneSample(fixedTS)}, nil)

	strInlined := int32(len(req.Dictionary.StringTable))
	req.Dictionary.StringTable = append(req.Dictionary.StringTable, "inlined.func")
	strOuter := int32(len(req.Dictionary.StringTable))
	req.Dictionary.StringTable = append(req.Dictionary.StringTable, "outer.func")

	fnInlined := int32(len(req.Dictionary.FunctionTable))
	req.Dictionary.FunctionTable = append(req.Dictionary.FunctionTable,
		&v1development.Function{NameStrindex: strInlined},
	)
	fnOuter := int32(len(req.Dictionary.FunctionTable))
	req.Dictionary.FunctionTable = append(req.Dictionary.FunctionTable,
		&v1development.Function{NameStrindex: strOuter},
	)

	locInlined := int32(len(req.Dictionary.LocationTable))
	req.Dictionary.LocationTable = append(req.Dictionary.LocationTable,
		&v1development.Location{Lines: []*v1development.Line{
			{FunctionIndex: fnInlined},
			{FunctionIndex: fnOuter},
		}},
	)
	stkInlined := int32(len(req.Dictionary.StackTable))
	req.Dictionary.StackTable = append(req.Dictionary.StackTable,
		&v1development.Stack{LocationIndices: []int32{locInlined, locGoexit}},
	)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].Samples[0].StackIndex = stkInlined

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	v, ok := widetest.FindField(t, events[0], fieldStack)
	if !ok {
		t.Fatalf("missing %q field", fieldStack)
	}
	want := "inlined.func;outer.func;runtime.goexit"
	if v != want {
		t.Errorf("stack: got %q, want %q", v, want)
	}
}

func TestPivotProfiles_MultipleTimestampsWithoutValuesProduceIndependentEvents(t *testing.T) {
	sample := &v1development.Sample{
		StackIndex:         stkKnownCall,
		TimestampsUnixNano: []uint64{fixedTS, fixedTS + 1_000_000_000},
	}
	events := PivotProfiles(BuildProfilesRequest([]*v1development.Sample{sample}, nil))
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	events[0].Fields[len(events[0].Fields)-1].Value = int64(999)
	v, ok := widetest.FindField(t, events[1], knownSampleCol)
	if !ok {
		t.Fatalf("field %s missing on event 1", knownSampleCol)
	}
	if v != int64(1) {
		t.Errorf("mutating event[0] corrupted event[1]: got %v, want 1", v)
	}
}

func TestPivotProfiles_AggregatedSampleValueAppearsInEvent(t *testing.T) {
	req := BuildProfilesRequest([]*v1development.Sample{{StackIndex: stkKnownCall, Values: []int64{42}}}, nil)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].TimeUnixNano = fixedTS

	events := PivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	v, ok := widetest.FindField(t, events[0], knownSampleCol)
	if !ok || v != int64(42) {
		t.Errorf("%s: got %v, want 42", knownSampleCol, v)
	}
}

// TODO: test oracle for data driven tests

func TestPivotProfiles_Testdata(t *testing.T) {
	files, _ := filepath.Glob("testdata/profiles/*.json")
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			body, _ := os.ReadFile(f)
			req := &v1development.ProfilesData{}
			if err := protojson.Unmarshal(body, req); err != nil {
				t.Fatal(err)
			}
			events := PivotProfiles(req)
			if len(events) == 0 {
				t.Fatal("expected at least one event")
			}

			for i, event := range events {
				if event.Timestamp.IsZero() {
					t.Errorf("event with zero ts: idx %d, data: %v", i, event)
				}
				_, found := widetest.FindField(t, event, fieldStack)
				if !found {
					t.Errorf("expected field %v not found", fieldStack)
				}
			}
		})
	}
}
