package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/secret"
	corestorage "github.com/ledatu/csar-core/storage"
)

// S3BackendConfig configures the static-credential S3 client used by csar-s3.
type S3BackendConfig struct {
	Bucket          string
	Endpoint        string
	Region          string
	UsePathStyle    bool
	AccessKeyID     secret.Secret
	SecretAccessKey secret.Secret
}

// S3Backend implements server-side object operations against S3-compatible storage.
type S3Backend struct {
	bucket string
	client *s3.Client
}

// NewS3Backend creates the concrete storage backend used by the service.
func NewS3Backend(cfg S3BackendConfig) (*S3Backend, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("s3 backend: bucket is required")
	}
	if cfg.AccessKeyID.IsEmpty() || cfg.SecretAccessKey.IsEmpty() {
		return nil, fmt.Errorf("s3 backend: static access key credentials are required")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "https://storage.yandexcloud.net"
	}
	if cfg.Region == "" {
		cfg.Region = "ru-central1"
	}

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(cfg.Endpoint),
		Region:       cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID.Plaintext(),
			cfg.SecretAccessKey.Plaintext(),
			"",
		),
		UsePathStyle: cfg.UsePathStyle,
	})

	return &S3Backend{
		bucket: cfg.Bucket,
		client: client,
	}, nil
}

// PutObject uploads the object body and then returns the freshly written object state.
func (b *S3Backend) PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (corestorage.ObjectInfo, error) {
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}

	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
		Metadata:      metadata,
	})
	if err != nil {
		return corestorage.ObjectInfo{}, csarerrors.Internal(err)
	}

	return b.HeadObject(ctx, key)
}

// DeleteObject removes the object. Missing objects are treated as already deleted.
func (b *S3Backend) DeleteObject(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return csarerrors.Internal(err)
	}
	return nil
}

// HeadObject fetches the canonical object metadata from S3.
func (b *S3Backend) HeadObject(ctx context.Context, key string) (corestorage.ObjectInfo, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return corestorage.ObjectInfo{}, csarerrors.NotFound("object %q not found", key)
		}
		return corestorage.ObjectInfo{}, csarerrors.Internal(err)
	}

	ref := corestorage.ObjectInfo{
		Bucket:      b.bucket,
		Key:         key,
		ContentType: aws.ToString(out.ContentType),
		Size:        aws.ToInt64(out.ContentLength),
		VersionID:   aws.ToString(out.VersionId),
	}
	if out.ETag != nil {
		ref.ETag = aws.ToString(out.ETag)
	}
	if out.LastModified != nil {
		ts := out.LastModified.UTC()
		ref.LastModified = &ts
	}
	return ref, nil
}

func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}

var _ ObjectStore = (*S3Backend)(nil)
