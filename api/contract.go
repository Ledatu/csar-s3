package api

import (
	"time"

	"github.com/ledatu/csar-core/storage"
)

const (
	// Server-side upload uses the raw request body and optional metadata headers.
	RoutePutObject = "PUT /v1/scopes/{scope}/objects/{key...}"
	// Server-side delete removes an object addressed by opaque storage_key.
	RouteDeleteObject = "DELETE /v1/objects/{storage_key...}"
	// Upload-intent minting returns a presigned PUT request.
	RouteMintUploadIntent = "POST /v1/upload-intents"
	// Upload-intent inspection returns the latest tracked state for one intent.
	RouteInspectUploadIntent = "GET /v1/upload-intents/{intent_id}"
	// Upload-intent finalize verifies the object in storage and marks the intent complete.
	RouteFinalizeUploadIntent = "POST /v1/upload-intents/{intent_id}/finalize"
	// Signed read-link issuance returns a presigned GET request.
	RouteIssueReadLink = "POST /v1/read-links"
	// Browser upload-intent minting derives scope, key, metadata, and TTL from a profile.
	RouteMintBrowserUploadIntent = "POST /v1/browser/storage/{profile}/upload-intents"
	// Browser upload-intent inspection is profile and subject bound.
	RouteInspectBrowserUploadIntent = "GET /v1/browser/storage/{profile}/upload-intents/{intent_id}"
	// Browser upload-intent finalize verifies the profile and subject-bound intent.
	RouteFinalizeBrowserUploadIntent = "POST /v1/browser/storage/{profile}/upload-intents/{intent_id}/finalize"

	ServerPutMetadataHeaderPrefix = "X-CSAR-S3-Meta-"
)

// MintUploadIntentRequest creates a presigned client upload.
type MintUploadIntentRequest struct {
	Scope         string            `json:"scope"`
	Filename      string            `json:"filename,omitempty"`
	ObjectKey     string            `json:"object_key,omitempty"`
	KeyPrefix     string            `json:"key_prefix,omitempty"`
	ContentType   string            `json:"content_type"`
	ContentLength int64             `json:"content_length"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	TTLSeconds    int64             `json:"ttl_seconds,omitempty"`
}

type MintUploadIntentResponse struct {
	Intent     storage.UploadIntent `json:"intent"`
	StorageKey string               `json:"storage_key"`
}

type InspectUploadIntentResponse struct {
	Intent storage.UploadIntent `json:"intent"`
}

type FinalizeUploadIntentResponse struct {
	Intent     storage.UploadIntent `json:"intent"`
	StorageKey string               `json:"storage_key"`
}

// IssueReadLinkRequest creates a presigned GET for an existing object.
type IssueReadLinkRequest struct {
	StorageKey          string `json:"storage_key"`
	ResponseFilename    string `json:"response_filename,omitempty"`
	ResponseContentType string `json:"response_content_type,omitempty"`
	TTLSeconds          int64  `json:"ttl_seconds,omitempty"`
}

type IssueReadLinkResponse struct {
	Object storage.ObjectInfo `json:"object"`
	Link   storage.ReadLink   `json:"link"`
}

type PutObjectResponse struct {
	Object storage.ObjectInfo `json:"object"`
}

type DeleteObjectResponse struct {
	StorageKey string `json:"storage_key"`
	Deleted    bool   `json:"deleted"`
}

// MintBrowserUploadIntentRequest is the browser-safe upload intent request.
type MintBrowserUploadIntentRequest struct {
	Filename      string            `json:"filename,omitempty"`
	ContentType   string            `json:"content_type"`
	ContentLength int64             `json:"content_length"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// BrowserUploadIntent is the reduced browser-facing view of an upload intent.
type BrowserUploadIntent struct {
	ID            string                     `json:"id"`
	Profile       string                     `json:"profile"`
	Status        storage.UploadIntentStatus `json:"status"`
	ContentType   string                     `json:"content_type,omitempty"`
	ContentLength int64                      `json:"content_length"`
	Metadata      map[string]string          `json:"metadata,omitempty"`
	Upload        storage.SignedRequest      `json:"upload,omitempty"`
	CreatedAt     time.Time                  `json:"created_at"`
	ExpiresAt     time.Time                  `json:"expires_at"`
	FinalizedAt   *time.Time                 `json:"finalized_at,omitempty"`
	Object        *BrowserUploadIntentObject `json:"object,omitempty"`
}

// BrowserUploadIntentObject omits bucket and object key details from browser responses.
type BrowserUploadIntentObject struct {
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ETag        string `json:"etag,omitempty"`
}

type MintBrowserUploadIntentResponse struct {
	Intent BrowserUploadIntent `json:"intent"`
}

type InspectBrowserUploadIntentResponse struct {
	Intent BrowserUploadIntent `json:"intent"`
}

type FinalizeBrowserUploadIntentResponse struct {
	Intent     BrowserUploadIntent `json:"intent"`
	StorageKey string              `json:"storage_key"`
}
