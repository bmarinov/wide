package router

import (
	"log/slog"

	collpb "go.opentelemetry.io/proto/slim/otlp/collector/profiles/v1development"
)

func pivotProfiles(req *collpb.ExportProfilesServiceRequest) {
	for _, prof := range req.GetResourceProfiles() {
		for _, scopeProf := range prof.ScopeProfiles {
			slog.Info("scope prof", "sp", scopeProf.Scope.Attributes)
		}
	}

	return
}
