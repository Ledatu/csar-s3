package config

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/ledatu/csar-core/configload"
	"github.com/ledatu/csar-core/configutil"
	corestorage "github.com/ledatu/csar-core/storage"
	"github.com/ledatu/csar-core/ycloud"
	"github.com/ledatu/csar-s3/internal/service"
	"gopkg.in/yaml.v3"
)

// Config is the top-level csar-s3 service configuration.
type Config struct {
	ListenAddr      string                         `yaml:"listen_addr"`
	ShutdownTimeout time.Duration                  `yaml:"shutdown_timeout"`
	TLS             configutil.TLSSection          `yaml:"tls"`
	ProbeSidecar    configutil.ProbeSidecarSection `yaml:",inline"`
	Scopes          map[string]ScopeConfig         `yaml:"scopes"`
}

// ScopeConfig describes one logical storage scope and its dedicated credentials.
type ScopeConfig struct {
	Bucket              string            `yaml:"bucket"`
	Endpoint            string            `yaml:"endpoint"`
	Region              string            `yaml:"region"`
	KeyPrefix           string            `yaml:"key_prefix"`
	UsePathStyle        bool              `yaml:"use_path_style"`
	MaxUploadBytes      int64             `yaml:"max_upload_bytes"`
	AllowedContentTypes []string          `yaml:"allowed_content_types"`
	DefaultIntentTTL    time.Duration     `yaml:"default_intent_ttl"`
	MaxIntentTTL        time.Duration     `yaml:"max_intent_ttl"`
	DefaultReadLinkTTL  time.Duration     `yaml:"default_read_link_ttl"`
	MaxReadLinkTTL      time.Duration     `yaml:"max_read_link_ttl"`
	Auth                ycloud.AuthConfig `yaml:"auth"`
}

// Load reads config using the shared config-source flags used across Go services.
func Load(ctx context.Context, logger *slog.Logger) (*Config, error) {
	flags := configload.NewSourceFlags()
	flags.RegisterFlags(flag.CommandLine)
	flag.Parse()

	params := flags.SourceParams()
	return configload.LoadInitial(ctx, &params, logger, Parse)
}

// Parse decodes YAML config and applies env expansion plus defaults.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}

	configutil.ExpandEnvInStruct(reflect.ValueOf(&cfg).Elem())
	applyDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate rejects incomplete startup state before the service begins serving.
func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}
	if err := c.TLS.Validate(); err != nil {
		return err
	}
	if len(c.Scopes) == 0 {
		return fmt.Errorf("scopes must contain at least one entry")
	}

	for name, scope := range c.Scopes {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("scope name is required")
		}
		if scope.Bucket == "" {
			return fmt.Errorf("scopes.%s.bucket is required", name)
		}
		if scope.MaxUploadBytes <= 0 {
			return fmt.Errorf("scopes.%s.max_upload_bytes must be greater than zero", name)
		}
		if scope.DefaultIntentTTL <= 0 {
			return fmt.Errorf("scopes.%s.default_intent_ttl must be greater than zero", name)
		}
		if scope.MaxIntentTTL < scope.DefaultIntentTTL {
			return fmt.Errorf("scopes.%s.max_intent_ttl must be >= scopes.%s.default_intent_ttl", name, name)
		}
		if scope.DefaultReadLinkTTL <= 0 {
			return fmt.Errorf("scopes.%s.default_read_link_ttl must be greater than zero", name)
		}
		if scope.MaxReadLinkTTL < scope.DefaultReadLinkTTL {
			return fmt.Errorf("scopes.%s.max_read_link_ttl must be >= scopes.%s.default_read_link_ttl", name, name)
		}
		if scope.Auth.AuthMode == "" {
			scope.Auth.AuthMode = "static"
			c.Scopes[name] = scope
		}
		if scope.Auth.AuthMode != "static" {
			return fmt.Errorf("scopes.%s.auth.auth_mode=%q is not supported yet; csar-s3 currently requires static S3 credentials for presigning", name, scope.Auth.AuthMode)
		}
		if scope.Auth.AccessKeyID.IsEmpty() || scope.Auth.SecretAccessKey.IsEmpty() {
			return fmt.Errorf("scopes.%s.auth requires access_key_id and secret_access_key", name)
		}
	}
	return nil
}

// ScopeNames returns the configured scope names.
func (c *Config) ScopeNames() []string {
	names := make([]string, 0, len(c.Scopes))
	for name := range c.Scopes {
		names = append(names, name)
	}
	return names
}

// SignerConfig returns the shared presigner configuration for one scope.
func (c *Config) SignerConfig(name string) corestorage.S3SignerConfig {
	scope := c.Scopes[name]
	return corestorage.S3SignerConfig{
		Endpoint:        scope.Endpoint,
		Region:          scope.Region,
		UsePathStyle:    scope.UsePathStyle,
		AccessKeyID:     scope.Auth.AccessKeyID,
		SecretAccessKey: scope.Auth.SecretAccessKey,
	}
}

// BackendConfig returns the static-credential S3 client configuration for one scope.
func (c *Config) BackendConfig(name string) service.S3BackendConfig {
	scope := c.Scopes[name]
	return service.S3BackendConfig{
		Bucket:          scope.Bucket,
		Endpoint:        scope.Endpoint,
		Region:          scope.Region,
		UsePathStyle:    scope.UsePathStyle,
		AccessKeyID:     scope.Auth.AccessKeyID,
		SecretAccessKey: scope.Auth.SecretAccessKey,
	}
}

// ScopePolicy returns the service-layer limits and key-scoping settings for one scope.
func (c *Config) ScopePolicy(name string) service.ScopePolicy {
	scope := c.Scopes[name]
	allowed := make(map[string]struct{}, len(scope.AllowedContentTypes))
	for _, value := range scope.AllowedContentTypes {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		allowed[value] = struct{}{}
	}
	return service.ScopePolicy{
		Name:                name,
		Bucket:              scope.Bucket,
		KeyPrefix:           scope.KeyPrefix,
		MaxUploadBytes:      scope.MaxUploadBytes,
		AllowedContentTypes: allowed,
		DefaultIntentTTL:    scope.DefaultIntentTTL,
		MaxIntentTTL:        scope.MaxIntentTTL,
		DefaultReadLinkTTL:  scope.DefaultReadLinkTTL,
		MaxReadLinkTTL:      scope.MaxReadLinkTTL,
	}
}

func applyDefaults(cfg *Config) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8087"
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	cfg.ProbeSidecar = cfg.ProbeSidecar.WithDefault("127.0.0.1:9088")
	for name, scope := range cfg.Scopes {
		if scope.Endpoint == "" {
			scope.Endpoint = "https://storage.yandexcloud.net"
		}
		if scope.Region == "" {
			scope.Region = "ru-central1"
		}
		if scope.MaxUploadBytes == 0 {
			scope.MaxUploadBytes = 32 << 20
		}
		if scope.DefaultIntentTTL == 0 {
			scope.DefaultIntentTTL = 15 * time.Minute
		}
		if scope.MaxIntentTTL == 0 {
			scope.MaxIntentTTL = time.Hour
		}
		if scope.DefaultReadLinkTTL == 0 {
			scope.DefaultReadLinkTTL = 15 * time.Minute
		}
		if scope.MaxReadLinkTTL == 0 {
			scope.MaxReadLinkTTL = 15 * time.Minute
		}
		cfg.Scopes[name] = scope
	}
}
