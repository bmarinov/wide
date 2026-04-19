package router

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Dict layout for buildRequest.
const (
	// StringTable
	strSamples  int32 = 1
	strCount    int32 = 2
	strMainMain int32 = 3
	strGoexit   int32 = 4
	strMainGo   int32 = 5

	// FunctionTable
	fnMainMain int32 = 1
	fnGoexit   int32 = 2

	// LocationTable
	locMainMain int32 = 1
	locGoexit   int32 = 2

	// StackTable
	stkKnownCall int32 = 1 // leaf-first: main.main;runtime.goexit
)

// knownStack is the call stack every buildRequest profile resolves to.
// Leaf-first, semicolon-separated (the pprof collapsed format).
const knownStack = "main.main;runtime.goexit"

// knownSampleCol is the value column name derived from the buildRequest dictionary.
const knownSampleCol = "samples_count"

// buildRequest constructs a minimal but complete ProfilesData whose encoding
// matches what protojson.Unmarshal produces from real eBPF profiler payloads:
// all strings go through the shared dictionary, attribute keys and values are
// stored as StringTable indices (KeyStrindex / StringValueStrindex), never inline.
func buildRequest(samples []*v1development.Sample, resourceAttrs map[string]string) *v1development.ProfilesData {
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
		samples = []*v1development.Sample{oneSample(1_000_000_000)}
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
				{}, // stkSentinel=0: null, StackIndex 0 means "no stack"
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

// oneSample returns a Sample pointing at stkKnownCall (the deterministic
// knownStack) with a single timestamp and no explicit value, the eBPF shape.
// The implicit count per observation is 1.
func oneSample(timestampNano uint64) *v1development.Sample {
	return &v1development.Sample{
		StackIndex:         stkKnownCall,
		TimestampsUnixNano: []uint64{timestampNano},
	}
}

// findField looks up a named field in an event. Useful in assertions.
func findField(t *testing.T, e columnar.Event, name string) (any, bool) {
	t.Helper()
	for _, f := range e.Fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

var fixedTS = uint64(time.Now().Truncate(time.Microsecond).UnixNano())

func TestPivotProfiles_OneSampleProducesOneEvent(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil))

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := findField(t, events[0], fieldStack); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldStack)
	}
	if v, ok := findField(t, events[0], knownSampleCol); !ok || v != int64(1) {
		t.Errorf("expected %q = 1 (implicit count), got %v", knownSampleCol, v)
	}
}

func TestPivotProfiles_StackResolvesToKnownCallChain(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil))

	v, ok := findField(t, events[0], fieldStack)
	if !ok {
		t.Fatalf("missing %q field", fieldStack)
	}
	if v != knownStack {
		t.Errorf("stack: got %q, want %q", v, knownStack)
	}
}

func TestPivotProfiles_TsOnlyMultipleTimestampsInOneSample(t *testing.T) {
	// One sample carrying three timestamps, the ts-only shape allows this.
	// Each timestamp must expand to its own event.
	sample := &v1development.Sample{
		StackIndex:         0,
		TimestampsUnixNano: []uint64{fixedTS, fixedTS + 1_000_000_000, fixedTS + 2_000_000_000},
	}
	events := pivotProfiles(buildRequest([]*v1development.Sample{sample}, nil))

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

func TestPivotProfiles_ZipShapeMultiTsValues(t *testing.T) {
	sample := &v1development.Sample{
		StackIndex:         0,
		TimestampsUnixNano: []uint64{fixedTS, fixedTS + uint64(time.Minute), fixedTS + 2*uint64(time.Minute)},
		Values:             []int64{25, 33, 424},
	}
	events := pivotProfiles(buildRequest([]*v1development.Sample{sample}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events (one per timestamp), got %d", len(events))
	}
	for i := range len(events) {
		expectedTime := time.Unix(0, int64(sample.TimestampsUnixNano[i])).UTC()
		expectedVal := sample.Values[i]
		if !events[i].Timestamp.Equal(expectedTime) {
			t.Errorf("event %d: timestamp got %v, want %v", i, events[i].Timestamp, expectedTime)
		}
		actualV, got := findField(t, events[i], knownSampleCol)
		if !got {
			t.Fatalf("field %s not found on %v", knownSampleCol, events[i])
		}
		if actualV != expectedVal {
			t.Errorf("event %d: val got %v, want %v", i, actualV, expectedVal)
		}
	}
}

func TestPivotProfiles_MultipleSamplesProduceOneEventEach(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{
		oneSample(fixedTS),
		oneSample(fixedTS + 1_000_000_000),
		oneSample(fixedTS + 2_000_000_000),
	}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestPivotProfiles_ResourceAttributesFlowToEveryEvent(t *testing.T) {
	events := pivotProfiles(buildRequest(
		[]*v1development.Sample{oneSample(fixedTS), oneSample(fixedTS + 1_000_000_000)},
		map[string]string{
			"process.executable.name": "columnstore",
			"service.name":            "sandbox-columnstore",
		},
	))

	for i, e := range events {
		if v, ok := findField(t, e, "process.executable.name"); !ok || v != "columnstore" {
			t.Errorf("event %d: process.executable.name: got %v, want columnstore", i, v)
		}
		if v, ok := findField(t, e, "service.name"); !ok || v != "sandbox-columnstore" {
			t.Errorf("event %d: service.name: got %v, want sandbox-columnstore", i, v)
		}
	}
}

func TestPivotProfiles_SampleTimestampUsedWhenPresent(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil))

	want := time.Unix(0, int64(fixedTS)).UTC()
	if !events[0].Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", events[0].Timestamp, want)
	}
}

func TestPivotProfiles_ProfileTimeFallsBackWhenSampleHasNoTimestamp(t *testing.T) {
	// sample with no TimestampsUnixNano, must fall back to Profile.TimeUnixNano
	req := buildRequest([]*v1development.Sample{{StackIndex: 0, Values: []int64{1}}}, nil)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].TimeUnixNano = fixedTS

	events := pivotProfiles(req)

	want := time.Unix(0, int64(fixedTS)).UTC()
	if !events[0].Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", events[0].Timestamp, want)
	}
}

func TestPivotProfiles_StackIndexZeroWithEmptyStackTableProducesEmptyStackField(t *testing.T) {
	// StackIndex 0 is the null sentinel when no StackTable is present.
	// The event must still be produced; fieldStack must be "" or absent, not a panic.
	req := buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil)
	req.Dictionary.StackTable = nil

	events := pivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := findField(t, events[0], fieldStack); ok && v != "" {
		t.Errorf("expected empty %q field, got %q", fieldStack, v)
	}
}

func TestPivotProfiles_ZeroSamplesProducesZeroEvents(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{}, nil))

	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestPivotProfiles_LinkIndexResolvesTraceAndSpanID(t *testing.T) {
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7}
	req := buildRequest([]*v1development.Sample{{StackIndex: stkKnownCall, TimestampsUnixNano: []uint64{fixedTS}, LinkIndex: 1}}, nil)
	req.Dictionary.LinkTable = []*v1development.Link{
		{},                                 // sentinel=0
		{TraceId: traceID, SpanId: spanID}, // real link=1
	}

	events := pivotProfiles(req)

	if v, ok := findField(t, events[0], fieldTraceID); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldTraceID)
	}
	if v, ok := findField(t, events[0], fieldSpanID); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldSpanID)
	}
}

func TestPivotProfiles_LocationSentinelProducesEmptyStack(t *testing.T) {
	// LocationTable[0] is the null sentinel, a stack referencing only loc=0
	// must not panic and must produce an empty (or absent) stack field.
	req := buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil)
	req.Dictionary.StackTable[stkKnownCall] = &v1development.Stack{
		LocationIndices: []int32{0}, // sentinel location only
	}

	events := pivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := findField(t, events[0], fieldStack); ok && v != "" {
		t.Errorf("expected empty %q for sentinel location, got %q", fieldStack, v)
	}
}

func TestPivotProfiles_FunctionSentinelFrameOmittedFromStack(t *testing.T) {
	// FunctionTable[0] is the null sentinel, a location whose FunctionIndex=0
	// must contribute no frame to the collapsed stack string.
	req := buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil)
	// Append a new location pointing at the function sentinel.
	locWithSentinelFn := int32(len(req.Dictionary.LocationTable))
	req.Dictionary.LocationTable = append(req.Dictionary.LocationTable,
		&v1development.Location{Lines: []*v1development.Line{{FunctionIndex: 0}}},
	)
	// Stack: sentinel-fn frame first, then a real location.
	req.Dictionary.StackTable[stkKnownCall] = &v1development.Stack{
		LocationIndices: []int32{locWithSentinelFn, locMainMain},
	}

	events := pivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	v, ok := findField(t, events[0], fieldStack)
	if !ok || v == "" {
		t.Fatalf("expected non-empty %q field, got %v", fieldStack, v)
	}
	stack, _ := v.(string)
	if stack != "main.main" {
		t.Errorf("sentinel fn frame must be omitted: got %q, want %q", stack, "main.main")
	}
}

func TestPivotProfiles_LinkIndexZeroMeansNoLink(t *testing.T) {
	// LinkIndex defaults to 0, the null sentinel. No trace/span fields must be emitted.
	req := buildRequest([]*v1development.Sample{oneSample(fixedTS)}, nil)
	// LinkTable absent; sample.LinkIndex == 0 by default.

	events := pivotProfiles(req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if _, ok := findField(t, events[0], fieldTraceID); ok {
		t.Errorf("expected no %q field for sentinel LinkIndex", fieldTraceID)
	}
	if _, ok := findField(t, events[0], fieldSpanID); ok {
		t.Errorf("expected no %q field for sentinel LinkIndex", fieldSpanID)
	}
}

func TestPivotProfiles_Testdata(t *testing.T) {
	files, _ := filepath.Glob("testdata/profiles/*.json")
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			body, _ := os.ReadFile(f)
			req := &v1development.ProfilesData{}
			if err := protojson.Unmarshal(body, req); err != nil {
				t.Fatal(err)
			}
			events := pivotProfiles(req)
			if len(events) == 0 {
				t.Fatal("expected at least one event")
			}

			// len > 0
			// every event: stack field non-empty
			// every event: value field present and is int64
			// every event: timestamp not zero
		})
	}
}
