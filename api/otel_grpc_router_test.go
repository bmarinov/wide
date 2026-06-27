package api

import (
	"github.com/bmarinov/sandbox-columnstore/internal/wide"
	"testing"
	"time"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	"github.com/bmarinov/sandbox-columnstore/internal/otlp"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
)

var fixedTS = uint64(time.Now().Truncate(time.Microsecond).UnixNano())

func TestGRPCProfilesExport_SamplesLandInStore(t *testing.T) {
	req := otlp.BuildProfilesRequest([]*v1development.Sample{
		otlp.OneSample(fixedTS),
		otlp.OneSample(fixedTS + uint64(time.Second)),
		otlp.OneSample(fixedTS + 2*uint64(time.Second)),
	}, map[string]string{"service.name": "test-svc"})

	s := columnar.New(t.Context(), columnar.Config{})
	srv := &profilesGRPCServer{store: s}

	if _, err := srv.Export(t.Context(), toGRPCRequest(req)); err != nil {
		t.Fatalf("export returned error: %v", err)
	}

	ack, wait := ackFn(t)
	_ = s.Receive(t.Context(), wide.Event{Timestamp: time.Now()}, ack)
	wait()

	if got := s.Stats().BufRows; got != 4 { // 3 profile events + 1 sync sentinel
		t.Errorf("expected 4 rows (3 profile + 1 sentinel), got %d", got)
	}
}

func TestGRPCProfilesExport_EmptyPayload_ReturnsNoError(t *testing.T) {
	s := columnar.New(t.Context(), columnar.Config{})
	srv := &profilesGRPCServer{store: s}

	if _, err := srv.Export(t.Context(), &collectorv1.ExportProfilesServiceRequest{}); err != nil {
		t.Fatalf("empty payload returned error: %v", err)
	}
}

func TestGRPCProfilesExport_NilDictionary_StoresNoEvents(t *testing.T) {
	req := otlp.BuildProfilesRequest([]*v1development.Sample{otlp.OneSample(fixedTS)}, nil)
	req.Dictionary = nil

	s := columnar.New(t.Context(), columnar.Config{})
	srv := &profilesGRPCServer{store: s}

	if _, err := srv.Export(t.Context(), toGRPCRequest(req)); err != nil {
		t.Fatalf("nil dictionary returned error: %v", err)
	}

	ack, wait := ackFn(t)
	_ = s.Receive(t.Context(), wide.Event{Timestamp: time.Now()}, ack)
	wait()

	if got := s.Stats().BufRows; got != 1 { // sentinel only, no profile events
		t.Errorf("expected 1 row (sentinel only), got %d", got)
	}
}

// toGRPCRequest converts ProfilesData to the grpc wire format.
func toGRPCRequest(req *v1development.ProfilesData) *collectorv1.ExportProfilesServiceRequest {
	return &collectorv1.ExportProfilesServiceRequest{
		ResourceProfiles: req.ResourceProfiles,
		Dictionary:       req.Dictionary,
	}
}
