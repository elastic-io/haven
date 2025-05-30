package types

import (
	"time"
)

// MultipartPart 表示分段上传的一个部分
//
//go:generate easyjson -all person.go
type MultipartPart struct {
	PartNumber int
	ETag       string
}

// multipartUploadInfo 存储分段上传的元数据
//
//go:generate easyjson -all person.go
type MultipartUploadInfo struct {
	Bucket      string
	Key         string
	UploadID    string
	ContentType string
	Metadata    map[string]string
	CreatedAt   time.Time
}

//go:generate easyjson -all person.go
type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int
}

// BucketInfo 表示存储桶的元数据
//
//go:generate easyjson -all person.go
type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

// S3ObjectInfo 表示 S3 对象的元数据
//
//go:generate easyjson -all person.go
type S3ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// S3ObjectData 表示 S3 对象的完整数据
//
//go:generate easyjson -all person.go
type S3ObjectData struct {
	Key          string
	Data         []byte
	ContentType  string
	LastModified time.Time
	ETag         string
	Metadata     map[string]string
	Size         int64
}
