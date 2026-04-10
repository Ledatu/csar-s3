package service

import (
	"context"
	"fmt"
	"io"
	"mime"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	csarerrors "github.com/ledatu/csar-core/errors"
	corestorage "github.com/ledatu/csar-core/storage"
)

const storageKeySeparator = ":"

// ObjectStore is the storage backend used by the service layer.
type ObjectStore interface {
	PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (corestorage.ObjectInfo, error)
	DeleteObject(ctx context.Context, key string) error
	HeadObject(ctx context.Context, key string) (corestorage.ObjectInfo, error)
}

// Signer mints direct-upload and direct-download requests.
type Signer interface {
	PresignPut(ctx context.Context, in corestorage.PresignPutInput) (corestorage.PresignedRequest, error)
	PresignGet(ctx context.Context, in corestorage.PresignGetInput) (corestorage.PresignedRequest, error)
}

// IntentRepository stores upload-intent state.
type IntentRepository interface {
	Save(ctx context.Context, intent corestorage.UploadIntent) error
	Get(ctx context.Context, id string) (corestorage.UploadIntent, error)
	Put(ctx context.Context, intent corestorage.UploadIntent) error
}

// ScopePolicy defines one logical upload/read namespace.
type ScopePolicy struct {
	Name                string
	Bucket              string
	KeyPrefix           string
	MaxUploadBytes      int64
	AllowedContentTypes map[string]struct{}
	DefaultIntentTTL    time.Duration
	MaxIntentTTL        time.Duration
	DefaultReadLinkTTL  time.Duration
	MaxReadLinkTTL      time.Duration
}

// ScopeRuntime wires the policy plus concrete object store and signer.
type ScopeRuntime struct {
	Policy  ScopePolicy
	Objects ObjectStore
	Signer  Signer
}

// MintUploadIntentRequest creates a scope-bound direct-upload intent.
type MintUploadIntentRequest struct {
	Scope         string
	Filename      string
	ContentType   string
	ContentLength int64
	Metadata      map[string]string
	TTL           time.Duration
}

// IssueReadLinkRequest creates a signed read link for an opaque storage key.
type IssueReadLinkRequest struct {
	StorageKey          string
	ResponseFilename    string
	ResponseContentType string
	TTL                 time.Duration
}

// Service implements the csar-s3 storage flow across multiple logical scopes.
type Service struct {
	scopes  map[string]ScopeRuntime
	intents IntentRepository
}

// New wires the service dependencies.
func New(scopes map[string]ScopeRuntime, intents IntentRepository) (*Service, error) {
	if len(scopes) == 0 {
		return nil, fmt.Errorf("service: at least one scope is required")
	}
	if intents == nil {
		return nil, fmt.Errorf("service: intents repository is required")
	}

	normalized := make(map[string]ScopeRuntime, len(scopes))
	seenKeys := make(map[string]string, len(scopes))
	for name, runtime := range scopes {
		scopeName := strings.TrimSpace(name)
		if scopeName == "" {
			scopeName = strings.TrimSpace(runtime.Policy.Name)
		}
		if scopeName == "" {
			return nil, fmt.Errorf("service: scope name is required")
		}
		if runtime.Objects == nil || runtime.Signer == nil {
			return nil, fmt.Errorf("service: scope %q requires objects and signer", scopeName)
		}
		runtime.Policy.Name = scopeName
		if strings.TrimSpace(runtime.Policy.Bucket) == "" {
			return nil, fmt.Errorf("service: scope %q bucket is required", scopeName)
		}
		if runtime.Policy.MaxUploadBytes <= 0 {
			return nil, fmt.Errorf("service: scope %q max_upload_bytes must be greater than zero", scopeName)
		}
		if runtime.Policy.DefaultIntentTTL <= 0 || runtime.Policy.MaxIntentTTL <= 0 {
			return nil, fmt.Errorf("service: scope %q intent TTLs must be greater than zero", scopeName)
		}
		if runtime.Policy.MaxIntentTTL < runtime.Policy.DefaultIntentTTL {
			return nil, fmt.Errorf("service: scope %q max_intent_ttl must be >= default_intent_ttl", scopeName)
		}
		if runtime.Policy.DefaultReadLinkTTL <= 0 || runtime.Policy.MaxReadLinkTTL <= 0 {
			return nil, fmt.Errorf("service: scope %q read-link TTLs must be greater than zero", scopeName)
		}
		if runtime.Policy.MaxReadLinkTTL < runtime.Policy.DefaultReadLinkTTL {
			return nil, fmt.Errorf("service: scope %q max_read_link_ttl must be >= default_read_link_ttl", scopeName)
		}
		dedupeKey := runtime.Policy.Bucket + "|" + strings.Trim(runtime.Policy.KeyPrefix, "/")
		if existing, ok := seenKeys[dedupeKey]; ok {
			return nil, fmt.Errorf("service: scopes %q and %q share the same bucket/key_prefix", existing, scopeName)
		}
		seenKeys[dedupeKey] = scopeName
		normalized[scopeName] = runtime
	}

	return &Service{scopes: normalized, intents: intents}, nil
}

// PutObject performs a server-side upload into the selected scope bucket.
func (s *Service) PutObject(ctx context.Context, scopeName, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (corestorage.ObjectInfo, error) {
	_, scope, err := s.scope(scopeName)
	if err != nil {
		return corestorage.ObjectInfo{}, err
	}
	if size <= 0 {
		return corestorage.ObjectInfo{}, csarerrors.Validation("content length must be greater than zero")
	}
	if size > scope.Policy.MaxUploadBytes {
		return corestorage.ObjectInfo{}, csarerrors.Validation("content length exceeds scope max_upload_bytes")
	}
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if err := validateScopeContentType(scope.Policy, contentType); err != nil {
		return corestorage.ObjectInfo{}, err
	}

	resolvedKey, err := resolveScopeObjectKey(scope.Policy, key)
	if err != nil {
		return corestorage.ObjectInfo{}, err
	}
	return scope.Objects.PutObject(ctx, resolvedKey, body, size, contentType, corestorage.CanonicalMetadata(metadata))
}

// DeleteObject removes an object addressed by opaque storage_key.
func (s *Service) DeleteObject(ctx context.Context, storageKey string) error {
	scopeName, objectKey, err := ParseStorageKey(storageKey)
	if err != nil {
		return err
	}
	_, scope, err := s.scope(scopeName)
	if err != nil {
		return err
	}
	return scope.Objects.DeleteObject(ctx, objectKey)
}

// MintUploadIntent creates a pending upload intent plus presigned PUT request.
func (s *Service) MintUploadIntent(ctx context.Context, in MintUploadIntentRequest) (corestorage.UploadIntent, string, error) {
	_, scope, err := s.scope(in.Scope)
	if err != nil {
		return corestorage.UploadIntent{}, "", err
	}

	contentType := strings.TrimSpace(strings.ToLower(in.ContentType))
	if err := validateScopeContentType(scope.Policy, contentType); err != nil {
		return corestorage.UploadIntent{}, "", err
	}
	if in.ContentLength <= 0 {
		return corestorage.UploadIntent{}, "", csarerrors.Validation("content length must be greater than zero")
	}
	if in.ContentLength > scope.Policy.MaxUploadBytes {
		return corestorage.UploadIntent{}, "", csarerrors.Validation("content length exceeds scope max_upload_bytes")
	}

	ttl := in.TTL
	if ttl == 0 {
		ttl = scope.Policy.DefaultIntentTTL
	}
	if ttl > scope.Policy.MaxIntentTTL {
		return corestorage.UploadIntent{}, "", csarerrors.Validation("ttl exceeds configured max_intent_ttl")
	}

	relativeKey, err := generatedRelativeKey(in.Filename, contentType, in.Metadata)
	if err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Validation("%v", err)
	}
	resolvedKey, err := resolveScopeObjectKey(scope.Policy, relativeKey)
	if err != nil {
		return corestorage.UploadIntent{}, "", err
	}

	input := corestorage.MintUploadIntentInput{
		Scope:         scope.Policy.Name,
		Filename:      strings.TrimSpace(in.Filename),
		ContentType:   contentType,
		ContentLength: in.ContentLength,
		Metadata:      corestorage.CanonicalMetadata(in.Metadata),
		TTL:           ttl,
	}
	if err := corestorage.ValidateMintUploadIntentInput(input); err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Validation("%v", err)
	}

	now := time.Now().UTC()
	upload, err := scope.Signer.PresignPut(ctx, corestorage.PresignPutInput{
		Bucket:      scope.Policy.Bucket,
		Key:         resolvedKey,
		ContentType: input.ContentType,
		Metadata:    input.Metadata,
		Expires:     input.TTL,
	})
	if err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Internal(err)
	}

	intent := corestorage.UploadIntent{
		ID:            corestorage.NewUploadIntentID(),
		Status:        corestorage.UploadIntentStatusPending,
		Object:        corestorage.ObjectInfo{Bucket: scope.Policy.Bucket, Key: resolvedKey, ContentType: input.ContentType},
		ContentLength: input.ContentLength,
		Metadata:      cloneStringMap(input.Metadata),
		Upload:        cloneSignedRequest(upload),
		CreatedAt:     now,
		ExpiresAt:     now.Add(input.TTL),
	}
	if err := s.intents.Save(ctx, intent); err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Internal(err)
	}

	storageKey, err := EncodeStorageKey(scope.Policy.Name, resolvedKey)
	if err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Internal(err)
	}
	return intent, storageKey, nil
}

// GetUploadIntent returns the tracked intent state.
func (s *Service) GetUploadIntent(ctx context.Context, id string) (corestorage.UploadIntent, error) {
	intent, err := s.intents.Get(ctx, id)
	if err != nil {
		return corestorage.UploadIntent{}, err
	}
	return s.refreshIntentStatus(ctx, intent)
}

// FinalizeUploadIntent verifies object existence and marks the intent complete.
func (s *Service) FinalizeUploadIntent(ctx context.Context, id string) (corestorage.UploadIntent, string, error) {
	intent, err := s.GetUploadIntent(ctx, id)
	if err != nil {
		return corestorage.UploadIntent{}, "", err
	}
	if intent.Status == corestorage.UploadIntentStatusExpired {
		return corestorage.UploadIntent{}, "", csarerrors.Conflict("upload intent %q has expired", id)
	}

	scopeName, scope, err := s.scopeForObject(intent.Object)
	if err != nil {
		return corestorage.UploadIntent{}, "", err
	}

	object, err := scope.Objects.HeadObject(ctx, intent.Object.Key)
	if err != nil {
		return corestorage.UploadIntent{}, "", err
	}
	if intent.ContentLength > 0 && object.Size > 0 && object.Size != intent.ContentLength {
		return corestorage.UploadIntent{}, "", csarerrors.Conflict("uploaded object size does not match intent")
	}
	if intent.Object.ContentType != "" && object.ContentType != "" && !strings.EqualFold(intent.Object.ContentType, object.ContentType) {
		return corestorage.UploadIntent{}, "", csarerrors.Conflict("uploaded object content type does not match intent")
	}

	now := time.Now().UTC()
	intent.Status = corestorage.UploadIntentStatusFinalized
	intent.Object = object
	intent.FinalizedAt = &now
	if err := s.intents.Put(ctx, intent); err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Internal(err)
	}

	storageKey, err := EncodeStorageKey(scopeName, object.Key)
	if err != nil {
		return corestorage.UploadIntent{}, "", csarerrors.Internal(err)
	}
	return intent, storageKey, nil
}

// IssueReadLink creates a signed GET request for an existing object.
func (s *Service) IssueReadLink(ctx context.Context, in IssueReadLinkRequest) (corestorage.ObjectInfo, corestorage.ReadLink, error) {
	scopeName, objectKey, err := ParseStorageKey(in.StorageKey)
	if err != nil {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, err
	}
	_, scope, err := s.scope(scopeName)
	if err != nil {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, err
	}

	ttl := in.TTL
	if ttl == 0 {
		ttl = scope.Policy.DefaultReadLinkTTL
	}
	if ttl > scope.Policy.MaxReadLinkTTL {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, csarerrors.Validation("ttl exceeds configured max_read_link_ttl")
	}

	input := corestorage.IssueReadLinkInput{
		Bucket:              scope.Policy.Bucket,
		Key:                 objectKey,
		ResponseFilename:    in.ResponseFilename,
		ResponseContentType: in.ResponseContentType,
		TTL:                 ttl,
	}
	if err := corestorage.ValidateIssueReadLinkInput(input); err != nil {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, csarerrors.Validation("%v", err)
	}

	object, err := scope.Objects.HeadObject(ctx, input.Key)
	if err != nil {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, err
	}
	request, err := scope.Signer.PresignGet(ctx, corestorage.PresignGetInput{
		Bucket:              input.Bucket,
		Key:                 input.Key,
		ResponseFilename:    input.ResponseFilename,
		ResponseContentType: input.ResponseContentType,
		Expires:             input.TTL,
	})
	if err != nil {
		return corestorage.ObjectInfo{}, corestorage.ReadLink{}, csarerrors.Internal(err)
	}

	return object, corestorage.ReadLink{URL: request.URL, Headers: cloneStringMap(request.Headers), ExpiresAt: request.ExpiresAt}, nil
}

func (s *Service) scope(name string) (string, ScopeRuntime, error) {
	name = strings.TrimSpace(name)
	scope, ok := s.scopes[name]
	if !ok {
		return "", ScopeRuntime{}, csarerrors.NotFound("scope %q not found", name)
	}
	return name, scope, nil
}

func (s *Service) scopeForObject(object corestorage.ObjectInfo) (string, ScopeRuntime, error) {
	var (
		matchedName string
		matched     ScopeRuntime
		found       bool
	)
	for name, scope := range s.scopes {
		if object.Bucket != scope.Policy.Bucket {
			continue
		}
		if !keyMatchesPrefix(object.Key, scope.Policy.KeyPrefix) {
			continue
		}
		if found {
			return "", ScopeRuntime{}, csarerrors.Internal(fmt.Errorf("object %q matches multiple scopes", object.Key))
		}
		matchedName = name
		matched = scope
		found = true
	}
	if !found {
		return "", ScopeRuntime{}, csarerrors.NotFound("scope for object %q not found", object.Key)
	}
	return matchedName, matched, nil
}

func validateScopeContentType(scope ScopePolicy, contentType string) error {
	if len(scope.AllowedContentTypes) == 0 {
		return nil
	}
	if _, ok := scope.AllowedContentTypes[strings.ToLower(strings.TrimSpace(contentType))]; !ok {
		return csarerrors.Validation("content type is not allowed for scope %q", scope.Name)
	}
	return nil
}

func resolveScopeObjectKey(scope ScopePolicy, key string) (string, error) {
	resolvedKey, err := corestorage.JoinObjectKey(scope.KeyPrefix, key)
	if err != nil {
		return "", csarerrors.Validation("%v", err)
	}
	return resolvedKey, nil
}

func keyMatchesPrefix(key, prefix string) bool {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return true
	}
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}

func generatedRelativeKey(filename, contentType string, metadata map[string]string) (string, error) {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(filename)))
	if ext == "" {
		if byType, _ := mime.ExtensionsByType(contentType); len(byType) > 0 {
			ext = byType[0]
		}
	}
	if ext == "" {
		ext = ".bin"
	}

	name := uuid.NewString() + ext
	owner := strings.TrimSpace(metadata["owner_user_id"])
	if owner == "" {
		return name, nil
	}
	if strings.Contains(owner, "/") {
		return "", fmt.Errorf("owner_user_id must not contain '/'")
	}
	if err := corestorage.ValidateObjectKey(owner); err != nil {
		return "", err
	}
	return path.Join(owner, name), nil
}

// EncodeStorageKey builds the opaque key returned to external callers.
func EncodeStorageKey(scope, objectKey string) (string, error) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return "", fmt.Errorf("scope is required")
	}
	if strings.Contains(scope, storageKeySeparator) || strings.Contains(scope, "/") {
		return "", fmt.Errorf("scope contains unsupported characters")
	}
	if err := corestorage.ValidateObjectKey(objectKey); err != nil {
		return "", err
	}
	return scope + storageKeySeparator + objectKey, nil
}

// ParseStorageKey resolves the logical scope and object key from an opaque storage key.
func ParseStorageKey(storageKey string) (string, string, error) {
	storageKey = strings.TrimSpace(storageKey)
	scope, objectKey, ok := strings.Cut(storageKey, storageKeySeparator)
	if !ok || scope == "" || objectKey == "" {
		return "", "", csarerrors.Validation("storage_key is invalid")
	}
	if err := corestorage.ValidateObjectKey(objectKey); err != nil {
		return "", "", csarerrors.Validation("%v", err)
	}
	return scope, objectKey, nil
}

func (s *Service) refreshIntentStatus(ctx context.Context, intent corestorage.UploadIntent) (corestorage.UploadIntent, error) {
	if intent.Status == corestorage.UploadIntentStatusPending && time.Now().UTC().After(intent.ExpiresAt) {
		intent.Status = corestorage.UploadIntentStatusExpired
		if err := s.intents.Put(ctx, intent); err != nil {
			return corestorage.UploadIntent{}, csarerrors.Internal(err)
		}
	}
	return intent, nil
}

// MemoryIntentRepository is a minimal scaffold for intent lifecycle tracking.
type MemoryIntentRepository struct {
	mu      sync.RWMutex
	intents map[string]corestorage.UploadIntent
}

// NewMemoryIntentRepository creates the in-memory intent store.
func NewMemoryIntentRepository() *MemoryIntentRepository {
	return &MemoryIntentRepository{intents: make(map[string]corestorage.UploadIntent)}
}

// Save inserts a new intent record.
func (r *MemoryIntentRepository) Save(_ context.Context, intent corestorage.UploadIntent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intents[intent.ID] = cloneIntent(intent)
	return nil
}

// Get fetches an existing intent record.
func (r *MemoryIntentRepository) Get(_ context.Context, id string) (corestorage.UploadIntent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	intent, ok := r.intents[id]
	if !ok {
		return corestorage.UploadIntent{}, csarerrors.NotFound("upload intent %q not found", id)
	}
	return cloneIntent(intent), nil
}

// Put replaces an existing intent record.
func (r *MemoryIntentRepository) Put(_ context.Context, intent corestorage.UploadIntent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intents[intent.ID] = cloneIntent(intent)
	return nil
}

func cloneIntent(intent corestorage.UploadIntent) corestorage.UploadIntent {
	intent.Metadata = cloneStringMap(intent.Metadata)
	intent.Upload = cloneSignedRequest(intent.Upload)
	return intent
}

func cloneSignedRequest(req corestorage.SignedRequest) corestorage.SignedRequest {
	req.Headers = cloneStringMap(req.Headers)
	return req
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
