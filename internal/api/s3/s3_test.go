package s3

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// 创建一个模拟的 S3 服务
type MockS3Service struct {
	mock.Mock
}

func (m *MockS3Service) ListBuckets() ([]types.BucketInfo, error) {
	args := m.Called()
	return args.Get(0).([]types.BucketInfo), args.Error(1)
}

func (m *MockS3Service) CreateBucket(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *MockS3Service) DeleteBucket(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *MockS3Service) BucketExists(name string) (bool, error) {
	args := m.Called(name)
	return args.Bool(0), args.Error(1)
}

func (m *MockS3Service) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	args := m.Called(bucket, prefix, marker, delimiter, maxKeys)
	return args.Get(0).([]types.S3ObjectInfo), args.Get(1).([]string), args.Error(2)
}

func (m *MockS3Service) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	args := m.Called(bucket, key)
	return args.Get(0).(*types.S3ObjectData), args.Error(1)
}

func (m *MockS3Service) PutObject(bucket string, object *types.S3ObjectData) error {
	args := m.Called(bucket, object)
	return args.Error(0)
}

func (m *MockS3Service) DeleteObject(bucket, key string) error {
	args := m.Called(bucket, key)
	return args.Error(0)
}

func (m *MockS3Service) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	args := m.Called(bucket, key, contentType, metadata)
	return args.String(0), args.Error(1)
}

func (m *MockS3Service) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	args := m.Called(bucket, key, uploadID, partNumber, data)
	return args.String(0), args.Error(1)
}

func (m *MockS3Service) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	args := m.Called(bucket, key, uploadID, parts)
	return args.String(0), args.Error(1)
}

func (m *MockS3Service) AbortMultipartUpload(bucket, key, uploadID string) error {
	// 调用m.Called方法，传入bucket、key和uploadID参数，模拟S3服务的AbortMultipartUpload操作
	args := m.Called(bucket, key, uploadID)
	// 返回模拟调用的第一个错误值，如果没有错误则返回nil
	return args.Error(0)
}

// 设置测试环境
func setupTest() (*fiber.App, *S3API, *MockS3Service) {
	app := fiber.New()
	mockService := new(MockS3Service)
	log.Init("", "debug")
	api := &S3API{
		name:    "s3",
		service: mockService,
	}
	api.RegisterRoutes(app)
	return app, api, mockService
}

// 测试列出所有存储桶
func TestHandleS3Base(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	buckets := []types.BucketInfo{
		{Name: "bucket1", CreationDate: time.Now()},
		{Name: "bucket2", CreationDate: time.Now()},
	}
	mockService.On("ListBuckets").Return(buckets, nil)

	// 创建请求
	req := httptest.NewRequest("GET", "/s3/", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	// 验证响应内容
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "<Name>bucket1</Name>")
	assert.Contains(t, string(body), "<Name>bucket2</Name>")

	mockService.AssertExpectations(t)
}

// 测试创建存储桶
func TestHandleS3Bucket(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("CreateBucket", "testbucket").Return(nil)

	// 创建请求
	req := httptest.NewRequest("PUT", "/s3/testbucket", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试创建已存在的存储桶
func TestHandleS3BucketAlreadyExists(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("CreateBucket", "testbucket").Return(fmt.Errorf("bucket already exists"))

	// 创建请求
	req := httptest.NewRequest("PUT", "/s3/testbucket", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusConflict, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试删除存储桶
func TestHandleS3DeleteBucket(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("DeleteBucket", "testbucket").Return(nil)

	// 创建请求
	req := httptest.NewRequest("DELETE", "/s3/testbucket", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusNoContent, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试列出存储桶内容
func TestHandleS3ListBucket(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("BucketExists", "testbucket").Return(true, nil)

	objects := []types.S3ObjectInfo{
		{Key: "object1", LastModified: time.Now(), ETag: "\"etag1\"", Size: 100},
		{Key: "object2", LastModified: time.Now(), ETag: "\"etag2\"", Size: 200},
	}
	commonPrefixes := []string{"prefix1/", "prefix2/"}

	mockService.On("ListObjects", "testbucket", "", "", "", 1000).Return(objects, commonPrefixes, nil)

	// 创建请求
	req := httptest.NewRequest("GET", "/s3/testbucket/", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	// 验证响应内容
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "<Key>object1</Key>")
	assert.Contains(t, string(body), "<Key>object2</Key>")
	assert.Contains(t, string(body), "<Prefix>prefix1/</Prefix>")

	mockService.AssertExpectations(t)
}

// 测试获取对象
func TestHandleS3GetObject(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("BucketExists", "testbucket").Return(true, nil)

	objectData := types.S3ObjectData{
		Key:          "testkey",
		Data:         []byte("test content"),
		ContentType:  "text/plain",
		LastModified: time.Now(),
		ETag:         "\"etag123\"",
		Metadata:     map[string]string{"custom": "value"},
	}

	mockService.On("GetObject", "testbucket", "testkey").Return(objectData, nil)

	// 创建请求
	req := httptest.NewRequest("GET", "/s3/testbucket/testkey", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/plain", resp.Header.Get("Content-Type"))
	assert.Equal(t, "\"etag123\"", resp.Header.Get("ETag"))
	assert.Equal(t, "value", resp.Header.Get("x-amz-meta-custom"))

	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "test content", string(body))

	mockService.AssertExpectations(t)
}

// 测试上传对象
func TestHandleS3PutObject(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("BucketExists", "testbucket").Return(true, nil)
	mockService.On("PutObject", "testbucket", mock.AnythingOfType("service.S3ObjectData")).Return(nil)

	// 创建请求
	content := []byte("test content")
	req := httptest.NewRequest("PUT", "/s3/testbucket/testkey", bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-meta-custom", "value")

	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试删除对象
func TestHandleS3DeleteObject(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("BucketExists", "testbucket").Return(true, nil)
	mockService.On("DeleteObject", "testbucket", "testkey").Return(nil)

	// 创建请求
	req := httptest.NewRequest("DELETE", "/s3/testbucket/testkey", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusNoContent, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试创建分段上传
func TestHandleS3CreateMultipartUpload(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("BucketExists", "testbucket").Return(true, nil)
	mockService.On("CreateMultipartUpload", "testbucket", "testkey", "text/plain", mock.AnythingOfType("map[string]string")).Return("upload-id-123", nil)

	// 创建请求
	req := httptest.NewRequest("POST", "/s3/testbucket/testkey?uploads", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-meta-custom", "value")

	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	// 验证响应内容
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "<UploadId>upload-id-123</UploadId>")

	mockService.AssertExpectations(t)
}

// 测试上传分段
func TestHandleS3UploadPart(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	content := []byte("part content")
	mockService.On("UploadPart", "testbucket", "testkey", "upload-id-123", 1, content).Return("\"etag-part1\"", nil)

	// 创建请求
	req := httptest.NewRequest("PUT", "/s3/testbucket/testkey?partNumber=1&uploadId=upload-id-123", bytes.NewReader(content))
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, "\"etag-part1\"", resp.Header.Get("ETag"))

	mockService.AssertExpectations(t)
}

// 测试完成分段上传
func TestHandleS3CompleteMultipartUpload(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("CompleteMultipartUpload", "testbucket", "testkey", "upload-id-123", mock.AnythingOfType("[]service.MultipartPart")).Return("\"final-etag\"", nil)

	// 创建请求体
	xmlBody := `
	<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>"etag-part1"</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>"etag-part2"</ETag>
		</Part>
	</CompleteMultipartUpload>
	`

	req := httptest.NewRequest("POST", "/s3/testbucket/testkey?uploadId=upload-id-123", strings.NewReader(xmlBody))
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	// 验证响应内容
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "<ETag>\"final-etag\"</ETag>")

	mockService.AssertExpectations(t)
}

// 测试中止分段上传
func TestHandleS3AbortMultipartUpload(t *testing.T) {
	app, _, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("AbortMultipartUpload", "testbucket", "testkey", "upload-id-123").Return(nil)

	// 创建请求
	req := httptest.NewRequest("DELETE", "/s3/testbucket/testkey?uploadId=upload-id-123", nil)
	resp, err := app.Test(req)

	// 断言
	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusNoContent, resp.StatusCode)

	mockService.AssertExpectations(t)
}
