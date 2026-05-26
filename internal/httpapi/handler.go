package httpapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/gatewayctx"
	"github.com/ledatu/csar-core/httpx"
	corestorage "github.com/ledatu/csar-core/storage"
	"github.com/ledatu/csar-s3/api"
	"github.com/ledatu/csar-s3/internal/service"
)

type handler struct {
	svc *service.Service
}

// New returns the concrete HTTP contract for csar-s3.
func New(svc *service.Service) http.Handler {
	h := &handler{svc: svc}

	mux := http.NewServeMux()
	mux.HandleFunc(api.RoutePutObject, h.handlePutObject)
	mux.HandleFunc("PUT /svc/s3/v1/scopes/{scope}/objects/{key...}", h.handlePutObject)
	mux.HandleFunc(api.RouteDeleteObject, h.handleDeleteObject)
	mux.HandleFunc("DELETE /svc/s3/v1/objects/{storage_key...}", h.handleDeleteObject)
	mux.HandleFunc(api.RouteMintUploadIntent, h.handleMintUploadIntent)
	mux.HandleFunc("POST /svc/s3/v1/upload-intents", h.handleMintUploadIntent)
	mux.HandleFunc(api.RouteInspectUploadIntent, h.handleInspectUploadIntent)
	mux.HandleFunc("GET /svc/s3/v1/upload-intents/{intent_id}", h.handleInspectUploadIntent)
	mux.HandleFunc(api.RouteFinalizeUploadIntent, h.handleFinalizeUploadIntent)
	mux.HandleFunc("POST /svc/s3/v1/upload-intents/{intent_id}/finalize", h.handleFinalizeUploadIntent)
	mux.HandleFunc(api.RouteIssueReadLink, h.handleIssueReadLink)
	mux.HandleFunc("POST /svc/s3/v1/read-links", h.handleIssueReadLink)
	mux.HandleFunc(api.RouteMintBrowserUploadIntent, h.handleMintBrowserUploadIntent)
	mux.HandleFunc(api.RouteInspectBrowserUploadIntent, h.handleInspectBrowserUploadIntent)
	mux.HandleFunc(api.RouteFinalizeBrowserUploadIntent, h.handleFinalizeBrowserUploadIntent)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return mux
}

func (h *handler) handlePutObject(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer func() { _ = body.Close() }()

	payload, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httpx.WriteError(w, csarerrors.Validation("request body exceeds scope max_upload_bytes"))
			return
		}
		httpx.WriteError(w, csarerrors.Internal(err))
		return
	}
	if len(payload) == 0 {
		httpx.WriteError(w, csarerrors.Validation("request body is required"))
		return
	}

	object, err := h.svc.PutObject(
		r.Context(),
		r.PathValue("scope"),
		r.PathValue("key"),
		bytes.NewReader(payload),
		int64(len(payload)),
		r.Header.Get("Content-Type"),
		extractMetadataHeaders(r.Header),
	)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, api.PutObjectResponse{Object: object})
}

func (h *handler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	storageKey := r.PathValue("storage_key")
	if err := h.svc.DeleteObject(r.Context(), storageKey); err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, api.DeleteObjectResponse{StorageKey: storageKey, Deleted: true})
}

func (h *handler) handleMintUploadIntent(w http.ResponseWriter, r *http.Request) {
	var req api.MintUploadIntentRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	intent, storageKey, err := h.svc.MintUploadIntent(r.Context(), service.MintUploadIntentRequest{
		Scope:         req.Scope,
		Filename:      req.Filename,
		ObjectKey:     req.ObjectKey,
		KeyPrefix:     req.KeyPrefix,
		ContentType:   req.ContentType,
		ContentLength: req.ContentLength,
		Metadata:      req.Metadata,
		TTL:           ttlFromSeconds(req.TTLSeconds),
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, api.MintUploadIntentResponse{Intent: intent, StorageKey: storageKey})
}

func (h *handler) handleInspectUploadIntent(w http.ResponseWriter, r *http.Request) {
	intent, err := h.svc.GetUploadIntent(r.Context(), r.PathValue("intent_id"))
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, api.InspectUploadIntentResponse{Intent: intent})
}

func (h *handler) handleFinalizeUploadIntent(w http.ResponseWriter, r *http.Request) {
	intent, storageKey, err := h.svc.FinalizeUploadIntent(r.Context(), r.PathValue("intent_id"))
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, api.FinalizeUploadIntentResponse{Intent: intent, StorageKey: storageKey})
}

func (h *handler) handleIssueReadLink(w http.ResponseWriter, r *http.Request) {
	var req api.IssueReadLinkRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	object, link, err := h.svc.IssueReadLink(r.Context(), service.IssueReadLinkRequest{
		StorageKey:          req.StorageKey,
		ResponseFilename:    req.ResponseFilename,
		ResponseContentType: req.ResponseContentType,
		TTL:                 ttlFromSeconds(req.TTLSeconds),
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, api.IssueReadLinkResponse{Object: object, Link: link})
}

func (h *handler) handleMintBrowserUploadIntent(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	if err := service.ValidateBrowserProfileName(profile); err != nil {
		httpx.WriteError(w, csarerrors.Validation("%v", err))
		return
	}

	var req api.MintBrowserUploadIntentRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	intent, err := h.svc.MintBrowserUploadIntent(r.Context(), service.MintBrowserUploadIntentRequest{
		Profile:       profile,
		Subject:       browserSubject(r),
		Filename:      req.Filename,
		ContentType:   req.ContentType,
		ContentLength: req.ContentLength,
		Metadata:      req.Metadata,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, api.MintBrowserUploadIntentResponse{Intent: browserAPIIntent(profile, intent)})
}

func (h *handler) handleInspectBrowserUploadIntent(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	if err := service.ValidateBrowserProfileName(profile); err != nil {
		httpx.WriteError(w, csarerrors.Validation("%v", err))
		return
	}

	intent, err := h.svc.GetBrowserUploadIntent(r.Context(), service.GetBrowserUploadIntentRequest{
		Profile: profile,
		Subject: browserSubject(r),
		ID:      r.PathValue("intent_id"),
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, api.InspectBrowserUploadIntentResponse{Intent: browserAPIIntent(profile, intent)})
}

func (h *handler) handleFinalizeBrowserUploadIntent(w http.ResponseWriter, r *http.Request) {
	profile := r.PathValue("profile")
	if err := service.ValidateBrowserProfileName(profile); err != nil {
		httpx.WriteError(w, csarerrors.Validation("%v", err))
		return
	}
	if err := requireEmptyBody(r); err != nil {
		httpx.WriteError(w, err)
		return
	}

	intent, storageKey, err := h.svc.FinalizeBrowserUploadIntent(r.Context(), service.FinalizeBrowserUploadIntentRequest{
		Profile: profile,
		Subject: browserSubject(r),
		ID:      r.PathValue("intent_id"),
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, api.FinalizeBrowserUploadIntentResponse{
		Intent:     browserAPIIntent(profile, intent),
		StorageKey: storageKey,
	})
}

func extractMetadataHeaders(header http.Header) map[string]string {
	var metadata map[string]string
	for key, values := range header {
		if !strings.HasPrefix(http.CanonicalHeaderKey(key), http.CanonicalHeaderKey(api.ServerPutMetadataHeaderPrefix)) {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		name := strings.TrimPrefix(http.CanonicalHeaderKey(key), http.CanonicalHeaderKey(api.ServerPutMetadataHeaderPrefix))
		metadata[strings.ToLower(name)] = strings.Join(values, ", ")
	}
	return metadata
}

func ttlFromSeconds(seconds int64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func browserSubject(r *http.Request) string {
	if id, ok := gatewayctx.FromContext(r.Context()); ok {
		return strings.TrimSpace(id.Subject)
	}
	return strings.TrimSpace(gatewayctx.FromRequest(r).Subject)
}

func requireEmptyBody(r *http.Request) error {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return csarerrors.Validation("request body must be empty")
		}
		return csarerrors.Internal(err)
	}
	if len(body) > 0 {
		return csarerrors.Validation("request body must be empty")
	}
	return nil
}

func browserAPIIntent(profile string, intent corestorage.UploadIntent) api.BrowserUploadIntent {
	out := api.BrowserUploadIntent{
		ID:            intent.ID,
		Profile:       profile,
		Status:        intent.Status,
		ContentType:   intent.Object.ContentType,
		ContentLength: intent.ContentLength,
		Metadata:      service.BrowserVisibleMetadata(intent.Metadata),
		Upload:        intent.Upload,
		CreatedAt:     intent.CreatedAt,
		ExpiresAt:     intent.ExpiresAt,
		FinalizedAt:   intent.FinalizedAt,
	}
	if intent.Status == corestorage.UploadIntentStatusFinalized {
		out.Object = &api.BrowserUploadIntentObject{
			ContentType: intent.Object.ContentType,
			Size:        intent.Object.Size,
			ETag:        intent.Object.ETag,
		}
	}
	return out
}
