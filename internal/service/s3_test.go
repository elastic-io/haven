package service

import (
	"errors"
	"testing"
	"time"

	"github.com/elastic-io/haven/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// 创建一个模拟的存储接口
type S3MockStorage struct {
	mock.Mock
}

// 桶操作
func (m *S3MockStorage) BucketExists(bucket string) (bool, error) {
	args := m.Called(bucket)
	return args.Bool(0), args.Error(1)
}

func (m *S3MockStorage) CreateBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *S3MockStorage) DeleteBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *S3MockStorage) ListBuckets() ([]types.BucketInfo, error) {
	args := m.Called()
	if buckets := args.Get(0); buckets != nil {
		return buckets.([]types.BucketInfo), args.Error(1)
	}
	return nil, args.Error(1)
}

// 基本对象操作
func (m *S3MockStorage) PutObject(bucket string, object *types.S3ObjectData) error {
	args := m.Called(bucket, object)
	return args.Error(0)
}

func (m *S3MockStorage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	args := m.Called(bucket, key)
	if obj := args.Get(0); obj != nil {
		return obj.(*types.S3ObjectData), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *S3MockStorage) DeleteObject(bucket, key string) error {
	args := m.Called(bucket, key)
	return args.Error(0)
}

func (m *S3MockStorage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	args := m.Called(bucket, prefix, marker, delimiter, maxKeys)
	var objects []types.S3ObjectInfo
	var commonPrefixes []string

	if objs := args.Get(0); objs != nil {
		objects = objs.([]types.S3ObjectInfo)
	}

	if prefixes := args.Get(1); prefixes != nil {
		commonPrefixes = prefixes.([]string)
	}

	return objects, commonPrefixes, args.Error(2)
}

// 分段上传
func (m *S3MockStorage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	args := m.Called(bucket, key, contentType, metadata)
	return args.String(0), args.Error(1)
}

func (m *S3MockStorage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	args := m.Called(bucket, key, uploadID, partNumber, data)
	return args.String(0), args.Error(1)
}

func (m *S3MockStorage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	args := m.Called(bucket, key, uploadID, parts)
	return args.String(0), args.Error(1)
}

func (m *S3MockStorage) AbortMultipartUpload(bucket, key, uploadID string) error {
	args := m.Called(bucket, key, uploadID)
	return args.Error(0)
}

func (m *S3MockStorage) Close() error {
	return nil
}

// 测试用例
func TestCreateBucket(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 测试成功场景
	mockStorage.On("CreateBucket", "test-bucket").Return(nil)
	err := service.CreateBucket("test-bucket")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("CreateBucket", "error-bucket").Return(errors.New("bucket creation failed"))
	err = service.CreateBucket("error-bucket")
	assert.Error(t, err)
	assert.Equal(t, "bucket creation failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteBucket(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 测试成功场景
	mockStorage.On("DeleteBucket", "test-bucket").Return(nil)
	err := service.DeleteBucket("test-bucket")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("DeleteBucket", "error-bucket").Return(errors.New("bucket deletion failed"))
	err = service.DeleteBucket("error-bucket")
	assert.Error(t, err)
	assert.Equal(t, "bucket deletion failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestBucketExists(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 测试存在场景
	mockStorage.On("BucketExists", "existing-bucket").Return(true, nil)
	exists, err := service.BucketExists("existing-bucket")
	assert.NoError(t, err)
	assert.True(t, exists)
	mockStorage.AssertExpectations(t)

	// 测试不存在场景
	mockStorage.On("BucketExists", "non-existing-bucket").Return(false, nil)
	exists, err = service.BucketExists("non-existing-bucket")
	assert.NoError(t, err)
	assert.False(t, exists)
	mockStorage.AssertExpectations(t)

	// 测试错误场景
	mockStorage.On("BucketExists", "error-bucket").Return(false, errors.New("bucket check failed"))
	exists, err = service.BucketExists("error-bucket")
	assert.Error(t, err)
	assert.False(t, exists)
	assert.Equal(t, "bucket check failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestListBuckets(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	buckets := []types.BucketInfo{
		{Name: "bucket1", CreationDate: time.Now()},
		{Name: "bucket2", CreationDate: time.Now()},
	}

	// 测试成功场景
	mockStorage.On("ListBuckets").Return(buckets, nil)
	result, err := service.ListBuckets()
	assert.NoError(t, err)
	assert.Equal(t, buckets, result)
	mockStorage.AssertExpectations(t)

	// 测试错误场景
	mockStorage.On("ListBuckets").Return([]types.BucketInfo{}, errors.New("list buckets failed"))
	result, err = service.ListBuckets()
	assert.Error(t, err)
	assert.Empty(t, result)
	assert.Equal(t, "list buckets failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestGetObject(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	object := &types.S3ObjectData{
		Key:         "test-key",
		Data:        []byte("test data"),
		ContentType: "text/plain",
		ETag:        "test-etag",
	}

	// 测试成功场景
	mockStorage.On("GetObject", "test-bucket", "test-key").Return(object, nil)
	result, err := service.GetObject("test-bucket", "test-key")
	assert.NoError(t, err)
	assert.Equal(t, object, result)
	mockStorage.AssertExpectations(t)

	// 测试对象不存在场景
	mockStorage.On("GetObject", "test-bucket", "non-existing-key").Return(nil, errors.New("object not found"))
	result, err = service.GetObject("test-bucket", "non-existing-key")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, "object not found", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestPutObject(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	object := &types.S3ObjectData{
		Key:         "test-key",
		Data:        []byte("test data"),
		ContentType: "text/plain",
		ETag:        "test-etag",
	}

	// 测试成功场景
	mockStorage.On("PutObject", "test-bucket", object).Return(nil)
	err := service.PutObject("test-bucket", object)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("PutObject", "error-bucket", object).Return(errors.New("put object failed"))
	err = service.PutObject("error-bucket", object)
	assert.Error(t, err)
	assert.Equal(t, "put object failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteObject(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 测试成功场景
	mockStorage.On("DeleteObject", "test-bucket", "test-key").Return(nil)
	err := service.DeleteObject("test-bucket", "test-key")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("DeleteObject", "error-bucket", "error-key").Return(errors.New("delete object failed"))
	err = service.DeleteObject("error-bucket", "error-key")
	assert.Error(t, err)
	assert.Equal(t, "delete object failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestListObjects(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	objects := []types.S3ObjectInfo{
		{Key: "key1", Size: 100, LastModified: time.Now(), ETag: "etag1"},
		{Key: "key2", Size: 200, LastModified: time.Now(), ETag: "etag2"},
	}
	commonPrefixes := []string{"prefix1/", "prefix2/"}

	// 测试成功场景
	mockStorage.On("ListObjects", "test-bucket", "prefix", "marker", "/", 1000).
		Return(objects, commonPrefixes, nil)
	resultObjects, resultPrefixes, err := service.ListObjects("test-bucket", "prefix", "marker", "/", 1000)
	assert.NoError(t, err)
	assert.Equal(t, objects, resultObjects)
	assert.Equal(t, commonPrefixes, resultPrefixes)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("ListObjects", "error-bucket", "", "", "", 0).
		Return([]types.S3ObjectInfo{}, []string{}, errors.New("list objects failed"))
	resultObjects, resultPrefixes, err = service.ListObjects("error-bucket", "", "", "", 0)
	assert.Error(t, err)
	assert.Empty(t, resultObjects)
	assert.Empty(t, resultPrefixes)
	assert.Equal(t, "list objects failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestCreateMultipartUpload(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	metadata := map[string]string{"key1": "value1"}

	// 测试成功场景
	mockStorage.On("CreateMultipartUpload", "test-bucket", "test-key", "text/plain", metadata).
		Return("upload-id-123", nil)
	uploadID, err := service.CreateMultipartUpload("test-bucket", "test-key", "text/plain", metadata)
	assert.NoError(t, err)
	assert.Equal(t, "upload-id-123", uploadID)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("CreateMultipartUpload", "error-bucket", "error-key", "text/plain", metadata).
		Return("", errors.New("create multipart upload failed"))
	uploadID, err = service.CreateMultipartUpload("error-bucket", "error-key", "text/plain", metadata)
	assert.Error(t, err)
	assert.Empty(t, uploadID)
	assert.Equal(t, "create multipart upload failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestUploadPart(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	data := []byte("part data")

	// 测试成功场景
	mockStorage.On("UploadPart", "test-bucket", "test-key", "upload-id-123", 1, data).
		Return("etag-part-1", nil)
	etag, err := service.UploadPart("test-bucket", "test-key", "upload-id-123", 1, data)
	assert.NoError(t, err)
	assert.Equal(t, "etag-part-1", etag)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("UploadPart", "error-bucket", "error-key", "error-upload-id", 2, data).
		Return("", errors.New("upload part failed"))
	etag, err = service.UploadPart("error-bucket", "error-key", "error-upload-id", 2, data)
	assert.Error(t, err)
	assert.Empty(t, etag)
	assert.Equal(t, "upload part failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestCompleteMultipartUpload(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 准备测试数据
	parts := []types.MultipartPart{
		{PartNumber: 1, ETag: "etag-part-1"},
		{PartNumber: 2, ETag: "etag-part-2"},
	}

	// 测试成功场景
	mockStorage.On("CompleteMultipartUpload", "test-bucket", "test-key", "upload-id-123", parts).
		Return("final-etag", nil)
	etag, err := service.CompleteMultipartUpload("test-bucket", "test-key", "upload-id-123", parts)
	assert.NoError(t, err)
	assert.Equal(t, "final-etag", etag)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("CompleteMultipartUpload", "error-bucket", "error-key", "error-upload-id", parts).
		Return("", errors.New("complete multipart upload failed"))
	etag, err = service.CompleteMultipartUpload("error-bucket", "error-key", "error-upload-id", parts)
	assert.Error(t, err)
	assert.Empty(t, etag)
	assert.Equal(t, "complete multipart upload failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestAbortMultipartUpload(t *testing.T) {
	mockStorage := new(S3MockStorage)
	service, _ := NewS3Service(mockStorage)

	// 测试成功场景
	mockStorage.On("AbortMultipartUpload", "test-bucket", "test-key", "upload-id-123").Return(nil)
	err := service.AbortMultipartUpload("test-bucket", "test-key", "upload-id-123")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)

	// 测试失败场景
	mockStorage.On("AbortMultipartUpload", "error-bucket", "error-key", "error-upload-id").
		Return(errors.New("abort multipart upload failed"))
	err = service.AbortMultipartUpload("error-bucket", "error-key", "error-upload-id")
	assert.Error(t, err)
	assert.Equal(t, "abort multipart upload failed", err.Error())
	mockStorage.AssertExpectations(t)
}
