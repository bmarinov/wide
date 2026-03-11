package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

func NewServer() {
	mux := http.NewServeMux()
	mux.Handle("POST /query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

	}))
}

func run(m *http.ServeMux, ctx context.Context) {
	srv := http.Server{Addr: ":8080", Handler: m}

	go func() {
		err := srv.ListenAndServe()

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("unexpected server error", "err", err)
		}
	}()

	<-ctx.Done()
	timeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(timeout)
}
