package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/ledatu/csar-core/health"
	"github.com/ledatu/csar-core/httpserver"
	corestorage "github.com/ledatu/csar-core/storage"
	"github.com/ledatu/csar-core/tlsx"
	"github.com/ledatu/csar-s3/internal/config"
	"github.com/ledatu/csar-s3/internal/httpapi"
	"github.com/ledatu/csar-s3/internal/service"
	"github.com/redis/go-redis/v9"
)

// Version is set at build time via ldflags.
var Version = "dev"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("s3 service failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load(ctx, logger)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	scopeNames := cfg.ScopeNames()
	sort.Strings(scopeNames)

	runtimes := make(map[string]service.ScopeRuntime, len(scopeNames))
	for _, name := range scopeNames {
		signer, err := corestorage.NewS3Signer(cfg.SignerConfig(name))
		if err != nil {
			return fmt.Errorf("create signer for %s: %w", name, err)
		}

		backend, err := service.NewS3Backend(cfg.BackendConfig(name))
		if err != nil {
			return fmt.Errorf("create s3 backend for %s: %w", name, err)
		}

		runtimes[name] = service.ScopeRuntime{
			Policy:  cfg.ScopePolicy(name),
			Objects: backend,
			Signer:  signer,
		}
	}

	intents, closeIntents, err := newIntentRepository(ctx, cfg.IntentStore, logger)
	if err != nil {
		return fmt.Errorf("create intent repository: %w", err)
	}
	defer closeIntents()

	svc, err := service.New(runtimes, intents)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", httpapi.New(svc))
	mux.Handle("GET /health", health.Handler(Version))

	rc := health.NewReadinessChecker(Version, true)
	rc.Register("http_server", health.TCPDialCheck(cfg.ListenAddr, time.Second))

	var healthSidecar *health.Sidecar
	if cfg.ProbeSidecar.Addr != "" {
		healthSidecar, err = health.NewSidecar(health.SidecarConfig{
			Addr:      cfg.ProbeSidecar.Addr,
			Version:   Version,
			Readiness: rc,
			Logger:    logger.With("component", "health"),
		})
		if err != nil {
			return fmt.Errorf("create health sidecar: %w", err)
		}
		go func() {
			if err := healthSidecar.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("health sidecar error", "error", err)
			}
		}()
	}

	var tlsCfg *tlsx.ServerConfig
	if cfg.TLS.IsEnabled() {
		tlsCfg = &tlsx.ServerConfig{
			CertFile:     cfg.TLS.CertFile,
			KeyFile:      cfg.TLS.KeyFile,
			ClientCAFile: cfg.TLS.ClientCAFile,
			MinVersion:   cfg.TLS.MinVersion,
		}
	}

	server, err := httpserver.New(&httpserver.Config{
		Addr:            cfg.ListenAddr,
		Handler:         mux,
		TLS:             tlsCfg,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, logger)
	if err != nil {
		return fmt.Errorf("create http server: %w", err)
	}

	if err := server.Run(ctx); err != nil {
		if healthSidecar != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			_ = healthSidecar.Shutdown(shutdownCtx)
		}
		return fmt.Errorf("run server: %w", err)
	}
	return nil
}

func newIntentRepository(ctx context.Context, cfg config.IntentStoreConfig, logger *slog.Logger) (service.IntentRepository, func(), error) {
	switch cfg.Type {
	case config.IntentStoreTypeMemory:
		logger.Info("using in-memory intent repository")
		return service.NewMemoryIntentRepository(), func() {}, nil
	case config.IntentStoreTypeRedis:
		opts, err := redis.ParseURL(cfg.Redis.URL.Plaintext())
		if err != nil {
			return nil, nil, err
		}
		client := redis.NewClient(opts)
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := client.Ping(pingCtx).Err(); err != nil {
			closeRedisClient(logger, client)
			return nil, nil, err
		}
		repo, err := service.NewRedisIntentRepository(client, cfg.Redis.KeyPrefix, cfg.RetentionBuffer)
		if err != nil {
			closeRedisClient(logger, client)
			return nil, nil, err
		}
		logger.Info("using redis intent repository", "key_prefix", cfg.Redis.KeyPrefix)
		return repo, func() { closeRedisClient(logger, client) }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported intent store type %q", cfg.Type)
	}
}

func closeRedisClient(logger *slog.Logger, client *redis.Client) {
	if err := client.Close(); err != nil {
		logger.Warn("close redis client", "error", err)
	}
}
