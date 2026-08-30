package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	nethttp "net/http"

	"github.com/satyamsipah/ledger-core/internal/config"
)

// Server runs an HTTP listener with a shutdown that waits for in-flight
// requests.
type Server struct {
	server   *nethttp.Server
	logger   *slog.Logger
	name     string
	shutdown config.HTTP
}

// NewServer builds a listener with every timeout set explicitly.
//
// Go's zero-value Server has no timeouts at all, which means one slow client
// can hold a connection indefinitely. For a payments API that is a denial of
// service with no attacker required.
func NewServer(name string, cfg config.HTTP, handler nethttp.Handler, logger *slog.Logger) *Server {
	return &Server{
		server: &nethttp.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		logger:   logger,
		name:     name,
		shutdown: cfg,
	}
}

// Run serves until ctx is cancelled, then drains in-flight requests within the
// configured shutdown budget.
//
// Draining matters more here than in most services: a request cut off
// mid-flight has an open database transaction behind it, and the difference
// between a clean rollback and an abandoned one is whether the next deploy
// waits a few seconds.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		s.logger.Info("http server listening",
			slog.String("listener", s.name),
			slog.String("addr", s.server.Addr))
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			errCh <- fmt.Errorf("serve %s: %w", s.name, err)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Deliberately built from context.Background(): ctx is already cancelled,
	// and deriving the shutdown deadline from it would abort the drain
	// instantly, which is the opposite of what shutdown is for.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdown.ShutdownTimeout)
	defer cancel()

	s.logger.Info("http server shutting down",
		slog.String("listener", s.name),
		slog.Duration("grace", s.shutdown.ShutdownTimeout))

	//nolint:contextcheck // shutdownCtx is deliberately not derived from ctx: ctx
	// is already cancelled by the time we get here, so inheriting it would abort
	// the drain instantly. See the comment above.
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown %s: %w", s.name, err)
	}
	return <-errCh
}
