// registry_test.go
package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// 创建一个模拟的 RegistryService
type MockRegistryService struct {
	mock.Mock
}

func (m *MockRegistryService) PutManifest(repository, reference string, data []byte, contentType string) error {
	args := m.Called(repository, reference, data, contentType)
	return args.Error(0)
}

func (m *MockRegistryService) GetManifest(repository, reference string) ([]byte, string, error) {
	args := m.Called(repository, reference)
	return args.Get(0).([]byte), args.String(1), args.Error(2)
}

func (m *MockRegistryService) DeleteManifest(repository, reference string) error {
	args := m.Called(repository, reference)
	return args.Error(0)
}

func (m *MockRegistryService) PutBlob(repository, digest string, data []byte) error {
	args := m.Called(repository, digest, data)
	return args.Error(0)
}

func (m *MockRegistryService) GetBlob(repository, digest string) ([]byte, error) {
	args := m.Called(repository, digest)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockRegistryService) DeleteBlob(repository, digest string) error {
	args := m.Called(repository, digest)
	return args.Error(0)
}

func (m *MockRegistryService) ListBlobs() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockRegistryService) ListManifests() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockRegistryService) GetManifestReferences(digest string) ([]string, error) {
	args := m.Called(digest)
	return args.Get(0).([]string), args.Error(1)
}

// 设置测试环境
func setupTest() (*fiber.App, *MockRegistryService) {
	app := fiber.New()
	mockService := new(MockRegistryService)

	log.Init("", "debug")

	api := RegistryAPI{
		name:    "registry",
		service: mockService,
	}

	api.RegisterRoutes(app)

	return app, mockService
}

// 测试 API 基础路由
func TestHandleAPIBase(t *testing.T) {
	app, _ := setupTest()

	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "registry/2.0", resp.Header.Get("Docker-Distribution-API-Version"))
}

// 测试开始上传
func TestHandleStartUpload(t *testing.T) {
	app, _ := setupTest()

	req := httptest.NewRequest(http.MethodPost, "/v2/test-repo/blobs/uploads/", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Location"))
	assert.NotEmpty(t, resp.Header.Get("Docker-Upload-UUID"))
}

// 测试推送清单
func TestHandlePushManifest(t *testing.T) {
	app, mockService := setupTest()

	manifestData := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)
	contentType := "application/vnd.docker.distribution.manifest.v2+json"

	// 设置模拟服务的预期行为
	mockService.On("PutManifest", "test-repo", "latest", manifestData, contentType).Return(nil)

	req := httptest.NewRequest(http.MethodPut, "/v2/test-repo/manifests/latest", bytes.NewReader(manifestData))
	req.Header.Set("Content-Type", contentType)

	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Docker-Content-Digest"))

	mockService.AssertExpectations(t)
}

// 测试拉取清单
func TestHandlePullManifest(t *testing.T) {
	app, mockService := setupTest()

	manifestData := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)
	contentType := "application/vnd.docker.distribution.manifest.v2+json"

	// 设置模拟服务的预期行为
	mockService.On("GetManifest", "test-repo", "latest").Return(manifestData, contentType, nil)

	req := httptest.NewRequest(http.MethodGet, "/v2/test-repo/manifests/latest", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, contentType, resp.Header.Get("Content-Type"))
	assert.NotEmpty(t, resp.Header.Get("Docker-Content-Digest"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, manifestData, body)

	mockService.AssertExpectations(t)
}

// 测试HEAD请求清单
func TestHandleHeadManifest(t *testing.T) {
	app, mockService := setupTest()

	manifestData := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)
	contentType := "application/vnd.docker.distribution.manifest.v2+json"

	// 设置模拟服务的预期行为
	mockService.On("GetManifest", "test-repo", "latest").Return(manifestData, contentType, nil)

	req := httptest.NewRequest(http.MethodHead, "/v2/test-repo/manifests/latest", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, contentType, resp.Header.Get("Content-Type"))
	assert.NotEmpty(t, resp.Header.Get("Docker-Content-Digest"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body) // HEAD请求不应返回内容

	mockService.AssertExpectations(t)
}

// 测试Blob分块上传
func TestHandleBlobUploadPatch(t *testing.T) {
	app, _ := setupTest()

	// 首先创建上传
	postReq := httptest.NewRequest(http.MethodPost, "/v2/test-repo/blobs/uploads/", nil)
	postResp, err := app.Test(postReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, postResp.StatusCode)

	// 获取上传UUID
	location := postResp.Header.Get("Location")
	uuid := strings.TrimPrefix(location, "/v2/test-repo/blobs/uploads/")

	// 执行PATCH请求
	blobData := []byte("test blob data")
	patchReq := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(blobData))
	patchResp, err := app.Test(patchReq)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, patchResp.StatusCode)
	assert.Equal(t, fmt.Sprintf("0-%d", len(blobData)-1), patchResp.Header.Get("Range"))
	assert.Equal(t, uuid, patchResp.Header.Get("Docker-Upload-UUID"))
}

// 测试推送Blob
func TestHandlePushBlob(t *testing.T) {
	app, mockService := setupTest()

	// 首先创建上传
	postReq := httptest.NewRequest(http.MethodPost, "/v2/test-repo/blobs/uploads/", nil)
	postResp, err := app.Test(postReq)
	require.NoError(t, err)

	// 获取上传UUID
	location := postResp.Header.Get("Location")

	// 执行PATCH请求
	blobData := []byte("test blob data")
	patchReq := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(blobData))
	_, err = app.Test(patchReq)
	require.NoError(t, err)

	// 计算摘要
	digest := "sha256:" + utils.ComputeSHA256(blobData)

	// 设置模拟服务的预期行为
	mockService.On("PutBlob", "test-repo", digest, blobData).Return(nil)

	// 执行PUT请求完成上传
	putReq := httptest.NewRequest(http.MethodPut, location+"?digest="+digest, nil)
	putResp, err := app.Test(putReq)

	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, putResp.StatusCode)
	assert.Equal(t, digest, putResp.Header.Get("Docker-Content-Digest"))

	mockService.AssertExpectations(t)
}

// 测试拉取Blob
func TestHandlePullBlob(t *testing.T) {
	app, mockService := setupTest()

	blobData := []byte("test blob data")
	digest := "sha256:" + utils.ComputeSHA256(blobData)

	// 设置模拟服务的预期行为
	mockService.On("GetBlob", "test-repo", digest).Return(blobData, nil)

	req := httptest.NewRequest(http.MethodGet, "/v2/test-repo/blobs/"+digest, nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, digest, resp.Header.Get("Docker-Content-Digest"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, blobData, body)

	mockService.AssertExpectations(t)
}

// 测试HEAD请求Blob
func TestHandleHeadBlob(t *testing.T) {
	app, mockService := setupTest()

	blobData := []byte("test blob data")
	digest := "sha256:" + utils.ComputeSHA256(blobData)

	// 设置模拟服务的预期行为
	mockService.On("GetBlob", "test-repo", digest).Return(blobData, nil)

	req := httptest.NewRequest(http.MethodHead, "/v2/test-repo/blobs/"+digest, nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, digest, resp.Header.Get("Docker-Content-Digest"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body) // HEAD请求不应返回内容

	mockService.AssertExpectations(t)
}

// 测试删除清单
func TestHandleDeleteManifest(t *testing.T) {
	app, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("DeleteManifest", "test-repo", "latest").Return(nil)

	req := httptest.NewRequest(http.MethodDelete, "/v2/test-repo/manifests/latest", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试删除Blob
func TestHandleDeleteBlob(t *testing.T) {
	app, mockService := setupTest()

	digest := "sha256:1234567890abcdef"

	// 设置模拟服务的预期行为
	mockService.On("DeleteBlob", "test-repo", digest).Return(nil)

	req := httptest.NewRequest(http.MethodDelete, "/v2/test-repo/blobs/"+digest, nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试垃圾回收
func TestHandleGarbageCollection(t *testing.T) {
	app, mockService := setupTest()

	blobs := []string{"repo1:sha256:blob1", "repo2:sha256:blob2"}

	// 设置模拟服务的预期行为
	mockService.On("ListBlobs").Return(blobs, nil)
	mockService.On("GetManifestReferences", "sha256:blob1").Return([]string{}, nil)
	mockService.On("GetManifestReferences", "sha256:blob2").Return([]string{"manifest1"}, nil)
	mockService.On("DeleteBlob", "repo1", "sha256:blob1").Return(nil)

	req := httptest.NewRequest(http.MethodPost, "/v2/_gc", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, float64(1), result["deleted_blobs"])

	mockService.AssertExpectations(t)
}

// 测试列出清单
func TestHandleListManifests(t *testing.T) {
	app, mockService := setupTest()

	manifests := []string{"repo1:tag1", "repo1:tag2", "repo2:latest"}

	// 设置模拟服务的预期行为
	mockService.On("ListManifests").Return(manifests, nil)

	req := httptest.NewRequest(http.MethodGet, "/v2/_debug/manifests", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)

	manifestsResult, ok := result["manifests"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(manifestsResult))

	mockService.AssertExpectations(t)
}

// 测试错误情况 - 清单不存在
func TestHandlePullManifestNotFound(t *testing.T) {
	app, mockService := setupTest()

	// 设置模拟服务的预期行为
	mockService.On("GetManifest", "test-repo", "nonexistent").Return([]byte{}, "", fmt.Errorf("manifest not found"))

	req := httptest.NewRequest(http.MethodGet, "/v2/test-repo/manifests/nonexistent", nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试错误情况 - Blob不存在
func TestHandlePullBlobNotFound(t *testing.T) {
	app, mockService := setupTest()

	digest := "sha256:nonexistent"

	// 设置模拟服务的预期行为
	mockService.On("GetBlob", "test-repo", digest).Return(nil, fmt.Errorf("blob not found"))

	req := httptest.NewRequest(http.MethodGet, "/v2/test-repo/blobs/"+digest, nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试错误情况 - 删除被引用的Blob
func TestHandleDeleteBlobStillReferenced(t *testing.T) {
	app, mockService := setupTest()

	digest := "sha256:referenced"

	// 设置模拟服务的预期行为
	mockService.On("DeleteBlob", "test-repo", digest).Return(fmt.Errorf("blob still referenced by manifests"))

	req := httptest.NewRequest(http.MethodDelete, "/v2/test-repo/blobs/"+digest, nil)
	resp, err := app.Test(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	mockService.AssertExpectations(t)
}

// 测试中间件路径解析
func TestDockerRegistryPathMiddleware(t *testing.T) {
	api := RegistryAPI{name: "registry"}

	testCases := []struct {
		path       string
		method     string
		repository string
		reference  string
		digest     string
		uuid       string
	}{
		{
			path:       "/v2/test-repo/manifests/latest",
			method:     "GET",
			repository: "test-repo",
			reference:  "latest",
		},
		{
			path:       "/v2/test-repo/blobs/sha256:1234",
			method:     "GET",
			repository: "test-repo",
			digest:     "sha256:1234",
		},
		{
			path:       "/v2/test-repo/blobs/uploads/uuid-123",
			method:     "PUT",
			repository: "test-repo",
			uuid:       "uuid-123",
		},
		{
			path:       "/v2/org/repo/manifests/v1.0",
			method:     "GET",
			repository: "org/repo",
			reference:  "v1.0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			app := fiber.New()

			// 添加中间件和测试处理程序
			app.Use(func(c *fiber.Ctx) error {
				return api.registryPathMiddleware(c)
			})

			app.All("*", func(c *fiber.Ctx) error {
				repo := c.Locals("repository")
				ref := c.Locals("reference")
				digest := c.Locals("digest")
				uuid := c.Locals("uuid")

				result := map[string]interface{}{}

				if repo != nil {
					result["repository"] = repo
				}
				if ref != nil {
					result["reference"] = ref
				}
				if digest != nil {
					result["digest"] = digest
				}
				if uuid != nil {
					result["uuid"] = uuid
				}

				return c.JSON(result)
			})

			req := httptest.NewRequest(tc.method, tc.path, nil)
			resp, err := app.Test(req)

			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var result map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&result)
			require.NoError(t, err)

			if tc.repository != "" {
				assert.Equal(t, tc.repository, result["repository"])
			}
			if tc.reference != "" {
				assert.Equal(t, tc.reference, result["reference"])
			}
			if tc.digest != "" {
				assert.Equal(t, tc.digest, result["digest"])
			}
			if tc.uuid != "" {
				assert.Equal(t, tc.uuid, result["uuid"])
			}
		})
	}
}
