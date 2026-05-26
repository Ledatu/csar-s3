package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	csarerrors "github.com/ledatu/csar-core/errors"
	corestorage "github.com/ledatu/csar-core/storage"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultRedisIntentKeyPrefix       = "csar-s3:intents:"
	DefaultRedisIntentRetentionBuffer = 5 * time.Minute
)

// RedisIntentRepository stores upload intents in Redis with expiry-derived TTLs.
type RedisIntentRepository struct {
	client          *redis.Client
	keyPrefix       string
	retentionBuffer time.Duration
}

// NewRedisIntentRepository creates a Redis-backed intent repository.
// The Redis client lifecycle remains owned by the caller.
func NewRedisIntentRepository(client *redis.Client, keyPrefix string, retentionBuffer time.Duration) (*RedisIntentRepository, error) {
	if client == nil {
		return nil, fmt.Errorf("redis intent repository requires a redis client")
	}
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = DefaultRedisIntentKeyPrefix
	}
	if retentionBuffer <= 0 {
		retentionBuffer = DefaultRedisIntentRetentionBuffer
	}
	return &RedisIntentRepository{
		client:          client,
		keyPrefix:       keyPrefix,
		retentionBuffer: retentionBuffer,
	}, nil
}

// Save inserts a new intent record.
func (r *RedisIntentRepository) Save(ctx context.Context, intent corestorage.UploadIntent) error {
	return r.put(ctx, intent)
}

// Get fetches an existing intent record.
func (r *RedisIntentRepository) Get(ctx context.Context, id string) (corestorage.UploadIntent, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return corestorage.UploadIntent{}, csarerrors.NotFound("upload intent not found")
	}
	data, err := r.client.Get(ctx, r.key(id)).Bytes()
	if err == redis.Nil {
		return corestorage.UploadIntent{}, csarerrors.NotFound("upload intent %q not found", id)
	}
	if err != nil {
		return corestorage.UploadIntent{}, csarerrors.Internal(err)
	}
	var intent corestorage.UploadIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		return corestorage.UploadIntent{}, csarerrors.Internal(fmt.Errorf("decode upload intent %q: %w", id, err))
	}
	return cloneIntent(intent), nil
}

// Put replaces an existing intent record.
func (r *RedisIntentRepository) Put(ctx context.Context, intent corestorage.UploadIntent) error {
	return r.put(ctx, intent)
}

func (r *RedisIntentRepository) put(ctx context.Context, intent corestorage.UploadIntent) error {
	intent.ID = strings.TrimSpace(intent.ID)
	if intent.ID == "" {
		return fmt.Errorf("upload intent id is required")
	}
	ttl, err := r.ttl(intent)
	if err != nil {
		return err
	}
	data, err := json.Marshal(cloneIntent(intent))
	if err != nil {
		return fmt.Errorf("encode upload intent %q: %w", intent.ID, err)
	}
	if err := r.client.Set(ctx, r.key(intent.ID), data, ttl).Err(); err != nil {
		return csarerrors.Internal(err)
	}
	return nil
}

func (r *RedisIntentRepository) ttl(intent corestorage.UploadIntent) (time.Duration, error) {
	if intent.ExpiresAt.IsZero() {
		return 0, fmt.Errorf("upload intent %q expires_at is required", intent.ID)
	}
	ttl := time.Until(intent.ExpiresAt.Add(r.retentionBuffer))
	if ttl <= 0 {
		return time.Second, nil
	}
	return ttl, nil
}

func (r *RedisIntentRepository) key(id string) string {
	return r.keyPrefix + id
}
