package service

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	corestorage "github.com/ledatu/csar-core/storage"
)

type fakeObjectStore struct {
	bucket  string
	objects map[string]corestorage.ObjectInfo
}

func (f *fakeObjectStore) PutObject(_ context.Context, key string, _ io.Reader, size int64, contentType string, _ map[string]string) (corestorage.ObjectInfo, error) {
	ref := corestorage.ObjectInfo{
		Bucket:      f.bucket,
		Key:         key,
		ContentType: contentType,
		Size:        size,
		ETag:        `"etag"`,
	}
	if f.objects == nil {
		f.objects = make(map[string]corestorage.ObjectInfo)
	}
	f.objects[key] = ref
	return ref, nil
}

func (f *fakeObjectStore) DeleteObject(_ context.Context, key string) error {
	delete(f.objects, key)
	return nil
}

func (f *fakeObjectStore) HeadObject(_ context.Context, key string) (corestorage.ObjectInfo, error) {
	return f.objects[key], nil
}

type fakeSigner struct {
	put corestorage.PresignedRequest
	get corestorage.PresignedRequest
}

func (f fakeSigner) PresignPut(_ context.Context, _ corestorage.PresignPutInput) (corestorage.PresignedRequest, error) {
	return f.put, nil
}

func (f fakeSigner) PresignGet(_ context.Context, _ corestorage.PresignGetInput) (corestorage.PresignedRequest, error) {
	return f.get, nil
}

func newTestService(t *testing.T) (*Service, *fakeObjectStore) {
	t.Helper()

	store := &fakeObjectStore{bucket: "avatar-bucket", objects: make(map[string]corestorage.ObjectInfo)}
	svc, err := New(map[string]ScopeRuntime{
		"authn-avatars": {
			Policy: ScopePolicy{
				Name:                "authn-avatars",
				Bucket:              "avatar-bucket",
				KeyPrefix:           "profiles/avatars",
				MaxUploadBytes:      10 << 20,
				AllowedContentTypes: map[string]struct{}{"image/png": {}},
				DefaultIntentTTL:    15 * time.Minute,
				MaxIntentTTL:        time.Hour,
				DefaultReadLinkTTL:  5 * time.Minute,
				MaxReadLinkTTL:      15 * time.Minute,
			},
			Objects: store,
			Signer: fakeSigner{
				put: corestorage.PresignedRequest{Method: "PUT", URL: "https://signed-put", Headers: map[string]string{"Content-Type": "image/png"}},
				get: corestorage.PresignedRequest{Method: "GET", URL: "https://signed-get"},
			},
		},
	}, NewMemoryIntentRepository())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return svc, store
}

func TestMintAndFinalizeUploadIntent(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)

	intent, storageKey, err := svc.MintUploadIntent(context.Background(), MintUploadIntentRequest{
		Scope:         "authn-avatars",
		Filename:      "avatar.png",
		ContentType:   "image/png",
		ContentLength: 5,
		Metadata:      map[string]string{"owner_user_id": "user-1"},
	})
	if err != nil {
		t.Fatalf("MintUploadIntent returned error: %v", err)
	}
	if intent.Object.Key == "" || intent.Object.Bucket != "avatar-bucket" {
		t.Fatalf("unexpected intent object: %#v", intent.Object)
	}
	if !strings.HasPrefix(intent.Object.Key, "profiles/avatars/user-1/") {
		t.Fatalf("expected owner-prefixed key, got %q", intent.Object.Key)
	}
	if !strings.HasSuffix(intent.Object.Key, ".png") {
		t.Fatalf("expected .png key, got %q", intent.Object.Key)
	}
	if !strings.HasPrefix(storageKey, "authn-avatars:") {
		t.Fatalf("unexpected storage key %q", storageKey)
	}

	if _, err := svc.PutObject(context.Background(), "authn-avatars", "user-1/manual.png", bytes.NewReader([]byte("hello")), 5, "image/png", nil); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}

	if _, err := svc.PutObject(context.Background(), "authn-avatars", intent.Object.Key[len("profiles/avatars/"):], bytes.NewReader([]byte("hello")), 5, "image/png", nil); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}

	finalized, finalizedStorageKey, err := svc.FinalizeUploadIntent(context.Background(), intent.ID)
	if err != nil {
		t.Fatalf("FinalizeUploadIntent returned error: %v", err)
	}
	if finalized.Status != corestorage.UploadIntentStatusFinalized {
		t.Fatalf("expected finalized status, got %q", finalized.Status)
	}
	if finalized.FinalizedAt == nil {
		t.Fatalf("expected finalized timestamp")
	}
	if finalizedStorageKey == "" {
		t.Fatalf("expected finalized storage key")
	}
}

func TestMintUploadIntentUsesExactObjectKey(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)

	intent, storageKey, err := svc.MintUploadIntent(context.Background(), MintUploadIntentRequest{
		Scope:         "authn-avatars",
		ObjectKey:     "users/user-1/photos/photo.png",
		ContentType:   "image/png",
		ContentLength: 5,
	})
	if err != nil {
		t.Fatalf("MintUploadIntent returned error: %v", err)
	}

	wantKey := "profiles/avatars/users/user-1/photos/photo.png"
	if intent.Object.Key != wantKey {
		t.Fatalf("expected object key %q, got %q", wantKey, intent.Object.Key)
	}
	if storageKey != "authn-avatars:"+wantKey {
		t.Fatalf("unexpected storage key %q", storageKey)
	}
}

func TestMintUploadIntentUsesKeyPrefix(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)

	intent, storageKey, err := svc.MintUploadIntent(context.Background(), MintUploadIntentRequest{
		Scope:         "authn-avatars",
		Filename:      "photo.png",
		KeyPrefix:     "users/user-1/photos",
		ContentType:   "image/png",
		ContentLength: 5,
	})
	if err != nil {
		t.Fatalf("MintUploadIntent returned error: %v", err)
	}

	wantPrefix := "profiles/avatars/users/user-1/photos/"
	if !strings.HasPrefix(intent.Object.Key, wantPrefix) {
		t.Fatalf("expected key prefix %q, got %q", wantPrefix, intent.Object.Key)
	}
	if !strings.HasSuffix(intent.Object.Key, ".png") {
		t.Fatalf("expected .png key, got %q", intent.Object.Key)
	}
	if storageKey != "authn-avatars:"+intent.Object.Key {
		t.Fatalf("unexpected storage key %q", storageKey)
	}
}

func TestMintUploadIntentRejectsConflictingKeyFields(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)

	_, _, err := svc.MintUploadIntent(context.Background(), MintUploadIntentRequest{
		Scope:         "authn-avatars",
		ObjectKey:     "users/user-1/photo.png",
		KeyPrefix:     "users/user-1/photos",
		ContentType:   "image/png",
		ContentLength: 5,
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestMintUploadIntentRejectsInvalidKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		objectKey string
		keyPrefix string
	}{
		{name: "object leading slash", objectKey: "/users/user-1/photo.png"},
		{name: "object trailing slash", objectKey: "users/user-1/"},
		{name: "object dotdot", objectKey: "users/../photo.png"},
		{name: "object empty segment", objectKey: "users//photo.png"},
		{name: "prefix leading slash", keyPrefix: "/users/user-1/photos"},
		{name: "prefix trailing slash", keyPrefix: "users/user-1/photos/"},
		{name: "prefix dotdot", keyPrefix: "users/../photos"},
		{name: "prefix empty segment", keyPrefix: "users//photos"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)
			_, _, err := svc.MintUploadIntent(context.Background(), MintUploadIntentRequest{
				Scope:         "authn-avatars",
				ObjectKey:     tt.objectKey,
				KeyPrefix:     tt.keyPrefix,
				ContentType:   "image/png",
				ContentLength: 5,
			})
			if err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestIssueReadLinkUsesOpaqueStorageKey(t *testing.T) {
	t.Parallel()

	svc, store := newTestService(t)
	store.objects["profiles/avatars/user-1/report.png"] = corestorage.ObjectInfo{
		Bucket:      "avatar-bucket",
		Key:         "profiles/avatars/user-1/report.png",
		ContentType: "image/png",
		Size:        10,
	}

	object, link, err := svc.IssueReadLink(context.Background(), IssueReadLinkRequest{StorageKey: "authn-avatars:profiles/avatars/user-1/report.png"})
	if err != nil {
		t.Fatalf("IssueReadLink returned error: %v", err)
	}
	if object.Key != "profiles/avatars/user-1/report.png" {
		t.Fatalf("unexpected object key %q", object.Key)
	}
	if link.URL != "https://signed-get" {
		t.Fatalf("unexpected signed link %#v", link)
	}
}
