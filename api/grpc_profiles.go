package router

import (
	"context"
	"fmt"
	"net"
	"os"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"

	"github.com/bmarinov/sandbox-columnstore/internal/columnar"
	"go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

// profilesGRPCServer receives OTLP profiles over gRPC.
type profilesGRPCServer struct {
	store *columnar.Store
	collectorv1.UnimplementedProfilesServiceServer
}

func newProfilesGRPCServer(s *columnar.Store) *profilesGRPCServer {
	return &profilesGRPCServer{
		store: s,
	}
}

func (s *profilesGRPCServer) Export(ctx context.Context, req *collectorv1.ExportProfilesServiceRequest) (*collectorv1.ExportProfilesServiceResponse, error) {
	events := pivotProfiles(&v1development.ProfilesData{
		ResourceProfiles: req.ResourceProfiles,
		Dictionary:       req.Dictionary,
	})
	for _, e := range events {
		err := s.store.Receive(ctx, e, nil)
		if err != nil {
			return nil, fmt.Errorf("storing profile event: %w", err)
		}
	}

	return &collectorv1.ExportProfilesServiceResponse{}, nil
}

func NewGRPCServer(store *columnar.Store, port int) (*grpc.Server, net.Listener, error) {
	var opts []grpc.ServerOption
	if os.Getenv("PROFILES_DEBUG_DUMP") == "true" {
		opts = append(opts, grpc.UnaryInterceptor(debugDumpInterceptor))
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc listen: %w", err)
	}

	srv := grpc.NewServer(opts...)
	collectorv1.RegisterProfilesServiceServer(srv, newProfilesGRPCServer(store))
	return srv, lis, nil
}
