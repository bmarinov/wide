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

// knownStack is the call stack every buildRequest profile resolves to.
// Leaf-first, semicolon-separated (the pprof collapsed format).
// Change this constant if you change the dictionaries below.
const knownStack = "main.main;runtime.goexit"

// buildRequest constructs a minimal but complete ProfilesData
// with fully wired dictionaries so the resolved call stack is deterministic.
//
// In v0.2.0 the ProfilesDictionary lives at the REQUEST level (not per-profile).
// All profiles and samples in the request share this single dictionary.
//
// Dictionary layout:
//
//	StringTable:   0=""  1="cpu"  2="nanoseconds"  3="main.main"  4="runtime.goexit"  5="main.go"
//	FunctionTable: 0->str[3]="main.main"   1->str[4]="runtime.goexit"
//	LocationTable: 0->func[0]   1->func[1]
//	StackTable:    0->locs[0,1]   (leaf=loc0=main.main, root=loc1=runtime.goexit)
//
// Call chain for any sample with StackIndex=0:
//
//	StackTable[0].LocationIndices=[0,1]
//	-> LocationTable[0].Lines[0].FunctionIndex=0 -> StringTable[3] = "main.main"
//	-> LocationTable[1].Lines[0].FunctionIndex=1 -> StringTable[4] = "runtime.goexit"
//	-> "main.main;runtime.goexit"
func buildRequest(samples []*v1development.Sample, resourceAttrs map[string]string) *v1development.ProfilesData {
	dict := &v1development.ProfilesDictionary{
		StringTable: []string{
			"",               // 0: pprof convention, index 0 is always empty
			"cpu",            // 1: sample type name
			"nanoseconds",    // 2: sample unit
			"main.main",      // 3: leaf function name
			"runtime.goexit", // 4: root function name
			"main.go",        // 5: source file
		},
		FunctionTable: []*v1development.Function{
			{NameStrindex: 3, FilenameStrindex: 5}, // 0: main.main
			{NameStrindex: 4, FilenameStrindex: 5}, // 1: runtime.goexit
		},
		LocationTable: []*v1development.Location{
			{Lines: []*v1development.Line{{FunctionIndex: 0}}}, // 0 -> main.main
			{Lines: []*v1development.Line{{FunctionIndex: 1}}}, // 1 -> runtime.goexit
		},
		StackTable: []*v1development.Stack{
			{LocationIndices: []int32{0, 1}}, // 0: leaf-first -> main.main;runtime.goexit
		},
	}

	if samples == nil {
		samples = []*v1development.Sample{oneSample(10_000, 1_000_000_000)}
	}

	profile := &v1development.Profile{
		SampleType: &v1development.ValueType{TypeStrindex: 1, UnitStrindex: 2},
		Samples:    samples,
	}

	if resourceAttrs == nil {
		resourceAttrs = map[string]string{}
	}
	attrs := make([]*commonv1.KeyValue, 0, len(resourceAttrs))
	for k, v := range resourceAttrs {
		attrs = append(attrs, &commonv1.KeyValue{
			Key:   k,
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
		})
	}

	return &v1development.ProfilesData{
		Dictionary: dict,
		ResourceProfiles: []*v1development.ResourceProfiles{
			{
				Resource: &resourcev1.Resource{Attributes: attrs},
				ScopeProfiles: []*v1development.ScopeProfiles{
					{Profiles: []*v1development.Profile{profile}},
				},
			},
		},
	}
}

// oneSample returns a Sample pointing at StackTable[0] (the deterministic
// knownStack) with the given CPU value and nanosecond timestamp.
func oneSample(valueNanos int64, timestampNano uint64) *v1development.Sample {
	return &v1development.Sample{
		StackIndex:         0,
		Values:             []int64{valueNanos},
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
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(50_000_000, fixedTS)}, nil))

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if v, ok := findField(t, events[0], fieldStack); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldStack)
	}
	if _, ok := findField(t, events[0], fieldValue); !ok {
		t.Errorf("expected %q field", fieldValue)
	}
	if v, ok := findField(t, events[0], fieldSampleType); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldSampleType)
	}
	if v, ok := findField(t, events[0], fieldSampleUnit); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldSampleUnit)
	}
}

func TestPivotProfiles_StackResolvesToKnownCallChain(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(50_000_000, fixedTS)}, nil))

	v, ok := findField(t, events[0], fieldStack)
	if !ok {
		t.Fatalf("missing %q field", fieldStack)
	}
	if v != knownStack {
		t.Errorf("stack: got %q, want %q", v, knownStack)
	}
}

func TestPivotProfiles_MultipleSamplesProduceOneEventEach(t *testing.T) {
	events := pivotProfiles(buildRequest([]*v1development.Sample{
		oneSample(50_000_000, fixedTS),
		oneSample(12_500_000, fixedTS+1_000_000_000),
		oneSample(75_000_000, fixedTS+2_000_000_000),
	}, nil))

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestPivotProfiles_ResourceAttributesFlowToEveryEvent(t *testing.T) {
	events := pivotProfiles(buildRequest(
		[]*v1development.Sample{oneSample(50_000_000, fixedTS), oneSample(25_000_000, fixedTS+1_000_000_000)},
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
	events := pivotProfiles(buildRequest([]*v1development.Sample{oneSample(50_000_000, fixedTS)}, nil))

	want := time.Unix(0, int64(fixedTS)).UTC()
	if !events[0].Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", events[0].Timestamp, want)
	}
}

func TestPivotProfiles_ProfileTimeFallsBackWhenSampleHasNoTimestamp(t *testing.T) {
	// sample with no TimestampsUnixNano, must fall back to Profile.TimeUnixNano
	req := buildRequest([]*v1development.Sample{{StackIndex: 0, Values: []int64{50_000_000}}}, nil)
	req.ResourceProfiles[0].ScopeProfiles[0].Profiles[0].TimeUnixNano = fixedTS

	events := pivotProfiles(req)

	want := time.Unix(0, int64(fixedTS)).UTC()
	if !events[0].Timestamp.Equal(want) {
		t.Errorf("timestamp: got %v, want %v", events[0].Timestamp, want)
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
	req := buildRequest([]*v1development.Sample{{StackIndex: 0, Values: []int64{50_000_000}, LinkIndex: 0}}, nil)
	req.Dictionary.LinkTable = []*v1development.Link{{TraceId: traceID, SpanId: spanID}}

	events := pivotProfiles(req)

	if v, ok := findField(t, events[0], fieldTraceID); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldTraceID)
	}
	if v, ok := findField(t, events[0], fieldSpanID); !ok || v == "" {
		t.Errorf("expected non-empty %q field", fieldSpanID)
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
			// events := pivotProfiles(req)
			// len > 0
			// every event: stack field non-empty
			// every event: value field present and is int64
			// every event: timestamp not zero
		})
	}
}
