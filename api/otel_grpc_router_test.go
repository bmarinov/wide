package api

import (
	"testing"
	"time"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"

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

	store := &fakeStore{}
	srv := &profilesGRPCServer{store: store}

	if _, err := srv.Export(t.Context(), toGRPCRequest(req)); err != nil {
		t.Fatalf("export returned error: %v", err)
	}
	if got := len(store.receivedEvents()); got != 3 {
		t.Errorf("expected 3 profile events, got %d", got)
	}
}

func TestGRPCProfilesExport_EmptyPayload_ReturnsNoError(t *testing.T) {
	store := &fakeStore{}
	srv := &profilesGRPCServer{store: store}

	if _, err := srv.Export(t.Context(), &collectorv1.ExportProfilesServiceRequest{}); err != nil {
		t.Fatalf("empty payload returned error: %v", err)
	}
	if got := len(store.receivedEvents()); got != 0 {
		t.Errorf("expected nothing stored, got %d events", got)
	}
}

func TestGRPCProfilesExport_NilDictionary_StoresNoEvents(t *testing.T) {
	req := otlp.BuildProfilesRequest([]*v1development.Sample{otlp.OneSample(fixedTS)}, nil)
	req.Dictionary = nil

	store := &fakeStore{}
	srv := &profilesGRPCServer{store: store}

	if _, err := srv.Export(t.Context(), toGRPCRequest(req)); err != nil {
		t.Fatalf("nil dictionary returned error: %v", err)
	}
	if got := len(store.receivedEvents()); got != 0 {
		t.Errorf("expected nothing stored, got %d events", got)
	}
}

// toGRPCRequest converts ProfilesData to the grpc wire format.
func toGRPCRequest(req *v1development.ProfilesData) *collectorv1.ExportProfilesServiceRequest {
	return &collectorv1.ExportProfilesServiceRequest{
		ResourceProfiles: req.ResourceProfiles,
		Dictionary:       req.Dictionary,
	}
}
