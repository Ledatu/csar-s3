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
	ctx := context.Background()

	cfg, err := config.Load(ctx, logger)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	scopeNames := cfg.ScopeNames()
	sort.Strings(scopeNames)

	runtimes := make(map[string]service.ScopeRuntime, len(scopeNames))
	for _, name := range scopeNames {
		signer, err := corestorage.NewS3Signer(cfg.SignerConfig(name))
		if err != nil {
			logger.Error("create signer", "scope", name, "error", err)
			os.Exit(1)
		}

		backend, err := service.NewS3Backend(cfg.BackendConfig(name))
		if err != nil {
			logger.Error("create s3 backend", "scope", name, "error", err)
			os.Exit(1)
		}

		runtimes[name] = service.ScopeRuntime{
			Policy:  cfg.ScopePolicy(name),
			Objects: backend,
			Signer:  signer,
		}
	}

	intents, closeIntents, err := newIntentRepository(ctx, cfg.IntentStore, logger)
	if err != nil {
		logger.Error("create intent repository", "error", err)
		os.Exit(1)
	}
	defer closeIntents()

	svc, err := service.New(runtimes, intents)
	if err != nil {
		closeIntents()
		logger.Error("create service", "error", err)
		os.Exit(1)
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
			closeIntents()
			logger.Error("create health sidecar", "error", err)
			os.Exit(1)
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
		closeIntents()
		logger.Error("create http server", "error", err)
		os.Exit(1)
	}

	if err := server.Run(ctx); err != nil {
		if healthSidecar != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			_ = healthSidecar.Shutdown(shutdownCtx)
		}
		closeIntents()
		logger.Error("run server", "error", err)
		os.Exit(1)
	}
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
