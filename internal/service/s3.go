package service

import (
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
)

type S3Service interface {
	storage.S3
}

type s3Service struct {
	storage storage.Storage
}

// News3Service 创建一个新的S3服务实例
func NewS3Service(storage storage.Storage) (S3Service, error) {
	return &s3Service{storage: storage}, nil
}

// CreateBucket 创建一个新的存储桶
func (s *s3Service) CreateBucket(bucket string) error {
	return s.storage.CreateBucket(bucket)
}

// DeleteBucket 删除一个存储桶
func (s *s3Service) DeleteBucket(bucket string) error {
	return s.storage.DeleteBucket(bucket)
}

// BucketExists 检查存储桶是否存在
func (s *s3Service) BucketExists(bucket string) (bool, error) {
	return s.storage.BucketExists(bucket)
}

// ListBuckets 列出所有存储桶
func (s *s3Service) ListBuckets() ([]types.BucketInfo, error) {
	return s.storage.ListBuckets()
}

// GetObject 获取对象
func (s *s3Service) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	return s.storage.GetObject(bucket, key)
}

// PutObject 存储对象
func (s *s3Service) PutObject(bucket string, object *types.S3ObjectData) error {
	return s.storage.PutObject(bucket, object)
}

// DeleteObject 删除对象
func (s *s3Service) DeleteObject(bucket, key string) error {
	return s.storage.DeleteObject(bucket, key)
}

// ListObjects 列出存储桶中的对象
func (s *s3Service) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	return s.storage.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

// CreateMultipartUpload 创建分段上传
func (s *s3Service) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	return s.storage.CreateMultipartUpload(bucket, key, contentType, metadata)
}

// UploadPart 上传分段
func (s *s3Service) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	return s.storage.UploadPart(bucket, key, uploadID, partNumber, data)
}

// CompleteMultipartUpload 完成分段上传
func (s *s3Service) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	return s.storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
}

// AbortMultipartUpload 中止分段上传
func (s *s3Service) AbortMultipartUpload(bucket, key, uploadID string) error {
	return s.storage.AbortMultipartUpload(bucket, key, uploadID)
}
