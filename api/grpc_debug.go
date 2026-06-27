package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func debugDumpInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	msg, ok := req.(proto.Message)
	if !ok {
		return resp, errors.New("failed to convert req to proto.Message")
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		slog.Warn("failed to marshal pb message to file ", "err", err)
		return resp, err
	}

	fname := fmt.Sprintf("/tmp/profiles_%d.pb", time.Now().UnixNano())
	err = os.WriteFile(fname, b, 0o644)
	if err != nil {
		slog.Warn("profiles dump failed", "err", err)
	} else {
		slog.Info("profiles dumped", "file", fname, "bytes", len(b))
	}

	return resp, err
}
