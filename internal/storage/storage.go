package storage

import (
	"fmt"

	"github.com/elastic-io/haven/internal/types"
)

// S3Storage 定义了基本的S3存储操作接口
type Storage interface {
	S3
	// 关闭存储
	Close() error
}

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
