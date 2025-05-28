package storage

import (
    "fmt"
)

// S3Storage 定义了基本的S3存储操作接口
type S3 interface {
    // 基本对象操作
    PutObject(bucket, key string, data []byte, metadata map[string]string) error
    GetObject(bucket, key string) ([]byte, map[string]string, error)
    DeleteObject(bucket, key string) error
    ListObjects(bucket, prefix string) ([]string, error)
    
    // 桶操作
    CreateBucket(bucket string) error
    DeleteBucket(bucket string) error
    ListBuckets() ([]string, error)
    BucketExists(bucket string) (bool, error)
    
    // 关闭存储
    Close() error
}

type backend func(string) (S3, error)
var Backends = map[string]backend{}

func BackendRegister(name string, be backend) {
    if _, ok := Backends[name]; ok {
        panic(fmt.Errorf("backend %s already registered", name))
    }
    Backends[name] = be
}

func NewStorage(engine, path string) (S3, error) {
    if backend, ok := Backends[engine]; ok {
        return backend(path)
    }
    return nil,  fmt.Errorf("backend %s not found", engine)
}