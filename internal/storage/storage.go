package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/elastic-io/haven/internal/types"
)

// S3Storage 定义了基本的S3存储操作接口
type Storage interface {
	S3
	// 关闭存储
	Close() error
}

const (
	// 分段上传相关常量
	MaxMultipartLifetime = 7 * 24 * time.Hour // 7天，与S3一致
	CleanupInterval      = 1 * time.Hour      // 清理过期上传的间隔
)

type S3 interface {
	// 桶操作
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]types.BucketInfo, error)
	BucketExists(bucket string) (bool, error)

	// 基本对象操作
	GetObject(bucket, key string) (*types.S3ObjectData, error)
	PutObject(bucket string, object *types.S3ObjectData) error
	DeleteObject(bucket, key string) error
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error)

	// 分段上传
	CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error)
	UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error)
	AbortMultipartUpload(bucket, key, uploadID string) error
}

type backend func(string) (Storage, error)

var Backends = map[string]backend{}

func BackendRegister(name string, be backend) {
	if _, ok := Backends[name]; ok {
		panic(fmt.Errorf("backend %s already registered", name))
	}
	Backends[name] = be
}

func NewStorage(engine, path string) (Storage, error) {
	if backend, ok := Backends[engine]; ok {
		return backend(path)
	}
	return nil, fmt.Errorf("backend %s not found", engine)
}

// generateUploadID 生成唯一的上传ID
func GenerateUploadID(bucket, key string) string {
	now := time.Now().UnixNano()
	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%d", bucket, key, now)))
	return hex.EncodeToString(hash[:])
}

// calculateETag 计算数据的MD5哈希作为ETag
func CalculateETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash[:]))
}

// calculateMultipartETag 计算分段上传的最终ETag
// S3兼容的格式: "{md5-of-all-etags}-{number-of-parts}"
func CalculateMultipartETag(etags []string) string {
	// 移除每个ETag的引号
	cleanETags := make([]string, len(etags))
	for i, etag := range etags {
		cleanETags[i] = strings.Trim(etag, "\"")
	}

	// 连接所有ETag并计算MD5
	combined := strings.Join(cleanETags, "")
	hash := md5.Sum([]byte(combined))

	return fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(hash[:]), len(etags))
}

/*
// generateUploadID 生成唯一的上传ID
func generateUploadID(bucket, key string) string {
	now := time.Now().UnixNano()
	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%d", bucket, key, now)))
	return hex.EncodeToString(hash[:])
}

// calculateETag 计算数据的MD5哈希作为ETag
func calculateETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash[:]))
}

// calculateMultipartETag 计算分段上传的最终ETag
// S3兼容的格式: "{md5-of-all-etags}-{number-of-parts}"
func calculateMultipartETag(etags []string) string {
	// 移除每个ETag的引号
	cleanETags := make([]string, len(etags))
	for i, etag := range etags {
		cleanETags[i] = strings.Trim(etag, "\"")
	}

	// 连接所有ETag并计算MD5
	combined := strings.Join(cleanETags, "")
	hash := md5.Sum([]byte(combined))

	return fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(hash[:]), len(etags))
}
*/
