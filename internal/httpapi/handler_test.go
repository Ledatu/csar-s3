package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corestorage "github.com/ledatu/csar-core/storage"
	"github.com/ledatu/csar-s3/api"
	"github.com/ledatu/csar-s3/internal/service"
)

type handlerFakeObjectStore struct {
	bucket  string
	objects map[string]corestorage.ObjectInfo
}

func (f *handlerFakeObjectStore) PutObject(_ context.Context, key string, _ io.Reader, size int64, contentType string, _ map[string]string) (corestorage.ObjectInfo, error) {
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

func (f *handlerFakeObjectStore) DeleteObject(_ context.Context, key string) error {
	delete(f.objects, key)
	return nil
}

func (f *handlerFakeObjectStore) HeadObject(_ context.Context, key string) (corestorage.ObjectInfo, error) {
	return f.objects[key], nil
}

type handlerFakeSigner struct {
	put      corestorage.PresignedRequest
	get      corestorage.PresignedRequest
	putInput corestorage.PresignPutInput
}

func (f *handlerFakeSigner) PresignPut(_ context.Context, in corestorage.PresignPutInput) (corestorage.PresignedRequest, error) {
	f.putInput = in
	return f.put, nil
}

func (f *handlerFakeSigner) PresignGet(_ context.Context, _ corestorage.PresignGetInput) (corestorage.PresignedRequest, error) {
	return f.get, nil
}

type browserMintHTTPResponse struct {
	Intent api.BrowserUploadIntent `json:"intent"`
}

type browserFinalizeHTTPResponse struct {
	Intent     api.BrowserUploadIntent `json:"intent"`
	StorageKey string                  `json:"storage_key"`
}

func newBrowserHandlerForTest(t *testing.T) (http.Handler, *handlerFakeObjectStore, *handlerFakeSigner) {
	t.Helper()

	store := &handlerFakeObjectStore{bucket: "knowledge-bucket", objects: make(map[string]corestorage.ObjectInfo)}
	signer := &handlerFakeSigner{
		put: corestorage.PresignedRequest{Method: "PUT", URL: "https://signed-browser-put", Headers: map[string]string{"Content-Type": "image/png"}},
		get: corestorage.PresignedRequest{Method: "GET", URL: "https://signed-browser-get"},
	}
	svc, err := service.New(map[string]service.ScopeRuntime{
		"knowledge-media": {
			Policy: service.ScopePolicy{
				Name:                "knowledge-media",
				Bucket:              "knowledge-bucket",
				KeyPrefix:           "knowledge",
				MaxUploadBytes:      10 << 20,
				AllowedContentTypes: map[string]struct{}{"image/png": {}},
				DefaultIntentTTL:    5 * time.Minute,
				MaxIntentTTL:        15 * time.Minute,
				DefaultReadLinkTTL:  2 * time.Minute,
				MaxReadLinkTTL:      5 * time.Minute,
				BrowserProfiles: map[string]service.BrowserProfilePolicy{
					"knowledge-editor-media": {
						Name:           "knowledge-editor-media",
						Enabled:        true,
						RequireSubject: true,
						KeyTemplate:    "uploads/{subject}/{uuid}{ext}",
						AllowedOperations: map[string]struct{}{
							"upload_intent": {},
							"inspect":       {},
							"finalize":      {},
						},
						AllowedMetadataKeys: map[string]struct{}{
							"article_id": {},
							"purpose":    {},
						},
						DefaultMetadata: map[string]string{"source": "browser"},
					},
				},
			},
			Objects: store,
			Signer:  signer,
		},
	}, service.NewMemoryIntentRepository())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return New(svc), store, signer
}

func executeJSON(handler http.Handler, method, path, subject, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if subject != "" {
		req.Header.Set("X-Gateway-Subject", subject)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func mintBrowserIntentOverHTTP(t *testing.T, handler http.Handler, subject string) browserMintHTTPResponse {
	t.Helper()

	rec := executeJSON(handler, http.MethodPost, "/v1/browser/storage/knowledge-editor-media/upload-intents", subject, `{
		"filename": "hero.png",
		"content_type": "image/png",
		"content_length": 42,
		"metadata": {
			"purpose": "hero",
			"article_id": "article-123"
		}
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("mint status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp browserMintHTTPResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	return resp
}

func TestBrowserMintUploadIntentExtractsGatewaySubject(t *testing.T) {
	t.Parallel()

	handler, _, signer := newBrowserHandlerForTest(t)
	resp := mintBrowserIntentOverHTTP(t, handler, "subject-1")

	if resp.Intent.Profile != "knowledge-editor-media" {
		t.Fatalf("expected browser profile, got %q", resp.Intent.Profile)
	}
	if !strings.HasPrefix(signer.putInput.Key, "knowledge/uploads/subject-1/") {
		t.Fatalf("expected key generated from X-Gateway-Subject, got %q", signer.putInput.Key)
	}
	if !strings.HasSuffix(signer.putInput.Key, ".png") {
		t.Fatalf("expected .png generated key, got %q", signer.putInput.Key)
	}
	if got := signer.putInput.Metadata["csar_browser_subject"]; got != "subject-1" {
		t.Fatalf("expected gateway subject metadata, got %q in %#v", got, signer.putInput.Metadata)
	}
}

func TestBrowserMintUploadIntentRejectsDangerousOverrideFields(t *testing.T) {
	t.Parallel()

	handler, _, _ := newBrowserHandlerForTest(t)
	rec := executeJSON(handler, http.MethodPost, "/v1/browser/storage/knowledge-editor-media/upload-intents", "subject-1", `{
		"filename": "hero.png",
		"content_type": "image/png",
		"content_length": 42,
		"scope": "authn-avatars",
		"object_key": "evil/manual.png",
		"key_prefix": "evil",
		"ttl_seconds": 86400
	}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected dangerous override fields to be rejected, got status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestBrowserMintUploadIntentRequiresGatewaySubject(t *testing.T) {
	t.Parallel()

	handler, _, _ := newBrowserHandlerForTest(t)
	rec := executeJSON(handler, http.MethodPost, "/v1/browser/storage/knowledge-editor-media/upload-intents", "", `{
		"filename": "hero.png",
		"content_type": "image/png",
		"content_length": 42
	}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected missing gateway subject to be rejected, got status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestBrowserFinalizeUsesStoredIntentState(t *testing.T) {
	t.Parallel()

	handler, store, signer := newBrowserHandlerForTest(t)
	minted := mintBrowserIntentOverHTTP(t, handler, "subject-1")
	mintedKey := signer.putInput.Key
	store.objects[mintedKey] = corestorage.ObjectInfo{
		Bucket:      "knowledge-bucket",
		Key:         mintedKey,
		ContentType: "image/png",
		Size:        42,
	}

	path := "/v1/browser/storage/knowledge-editor-media/upload-intents/" + minted.Intent.ID + "/finalize"
	rec := executeJSON(handler, http.MethodPost, path, "subject-1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("finalize status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp browserFinalizeHTTPResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode finalize response: %v", err)
	}
	if want := "knowledge-media:" + mintedKey; resp.StorageKey != want {
		t.Fatalf("finalize used client state, got storage key %q want %q", resp.StorageKey, want)
	}
	if resp.Intent.Status != corestorage.UploadIntentStatusFinalized {
		t.Fatalf("expected finalized browser intent, got %q", resp.Intent.Status)
	}
}
