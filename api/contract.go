package api

import "github.com/ledatu/csar-core/storage"

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

	ServerPutMetadataHeaderPrefix = "X-CSAR-S3-Meta-"
)

// MintUploadIntentRequest creates a presigned client upload.
type MintUploadIntentRequest struct {
	Scope         string            `json:"scope"`
	Filename      string            `json:"filename,omitempty"`
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
