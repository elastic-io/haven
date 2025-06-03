package service

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// 创建一个模拟的存储接口
type RegistryMockStorage struct {
	mock.Mock
}

// 桶操作
func (m *RegistryMockStorage) BucketExists(bucket string) (bool, error) {
	args := m.Called(bucket)
	return args.Bool(0), args.Error(1)
}

func (m *RegistryMockStorage) CreateBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *RegistryMockStorage) DeleteBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *RegistryMockStorage) ListBuckets() ([]types.BucketInfo, error) {
	args := m.Called()
	if buckets := args.Get(0); buckets != nil {
		return buckets.([]types.BucketInfo), args.Error(1)
	}
	return nil, args.Error(1)
}

// 基本对象操作
func (m *RegistryMockStorage) PutObject(bucket string, object *types.S3ObjectData) error {
	args := m.Called(bucket, object)
	return args.Error(0)
}

func (m *RegistryMockStorage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	args := m.Called(bucket, key)
	if obj := args.Get(0); obj != nil {
		return obj.(*types.S3ObjectData), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *RegistryMockStorage) DeleteObject(bucket, key string) error {
	args := m.Called(bucket, key)
	return args.Error(0)
}

func (m *RegistryMockStorage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
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
func (m *RegistryMockStorage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	args := m.Called(bucket, key, contentType, metadata)
	return args.String(0), args.Error(1)
}

func (m *RegistryMockStorage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	args := m.Called(bucket, key, uploadID, partNumber, data)
	return args.String(0), args.Error(1)
}

func (m *RegistryMockStorage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	args := m.Called(bucket, key, uploadID, parts)
	return args.String(0), args.Error(1)
}

func (m *RegistryMockStorage) AbortMultipartUpload(bucket, key, uploadID string) error {
	args := m.Called(bucket, key, uploadID)
	return args.Error(0)
}

func (m *RegistryMockStorage) Close() error {
	return nil
}

func TestNewRegistryService(t *testing.T) {
	mockStorage := new(RegistryMockStorage)

	// 测试桶已存在的情况
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()
	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)
	assert.NotNil(t, service)

	// 测试桶不存在需要创建的情况
	mockStorage.On("BucketExists", registryBucket).Return(false, nil).Once()
	mockStorage.On("CreateBucket", registryBucket).Return(nil).Once()
	service, err = NewRegistryService(mockStorage)
	assert.NoError(t, err)
	assert.NotNil(t, service)

	// 测试检查桶存在时出错的情况
	mockStorage.On("BucketExists", registryBucket).Return(false, errors.New("storage error")).Once()
	service, err = NewRegistryService(mockStorage)
	assert.Error(t, err)
	assert.Nil(t, service)

	// 测试创建桶时出错的情况
	mockStorage.On("BucketExists", registryBucket).Return(false, nil).Once()
	mockStorage.On("CreateBucket", registryBucket).Return(errors.New("bucket creation error")).Once()
	service, err = NewRegistryService(mockStorage)
	assert.Error(t, err)
	assert.Nil(t, service)

	mockStorage.AssertExpectations(t)
}

func TestInit(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	// 测试有效配置
	cfg := &config.Config{
		MaxMultipart: "10M",
		ChunkLength:  "1M",
	}
	err = service.Init(cfg)
	assert.NoError(t, err)
	assert.Equal(t, 10*1024*1024, service.multipartThreshold)
	assert.Equal(t, 1*1024*1024, service.chunkLength)

	// 测试无效的 MaxMultipart 配置
	cfg = &config.Config{
		MaxMultipart: "invalid",
		ChunkLength:  "1M",
	}
	err = service.Init(cfg)
	assert.Error(t, err)

	// 测试无效的 ChunkLength 配置
	cfg = &config.Config{
		MaxMultipart: "10M",
		ChunkLength:  "invalid",
	}
	err = service.Init(cfg)
	assert.Error(t, err)

	mockStorage.AssertExpectations(t)
}

func TestPutManifest(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	repository := "test/repo"
	reference := "latest"
	contentType := "application/vnd.docker.distribution.manifest.v2+json"
	data := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)

	// 计算预期的摘要
	digest := "sha256:" + utils.ComputeSHA256(data)

	// 模拟存储操作
	mockStorage.On("PutObject", registryBucket, mock.MatchedBy(func(obj *types.S3ObjectData) bool {
		return obj.Key == fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	})).Return(nil).Once()

	mockStorage.On("PutObject", registryBucket, mock.MatchedBy(func(obj *types.S3ObjectData) bool {
		return obj.Key == fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)
	})).Return(nil).Once()

	// 执行测试
	err = service.PutManifest(repository, reference, data, contentType)
	assert.NoError(t, err)

	// 测试存储失败的情况
	mockStorage.On("PutObject", registryBucket, mock.MatchedBy(func(obj *types.S3ObjectData) bool {
		return obj.Key == fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	})).Return(errors.New("storage error")).Once()

	err = service.PutManifest(repository, reference, data, contentType)
	assert.Error(t, err)

	mockStorage.AssertExpectations(t)
}

func TestGetManifest(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	repository := "test/repo"
	reference := "latest"
	contentType := "application/vnd.docker.distribution.manifest.v2+json"
	data := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)

	// 创建清单对象
	manifest := &types.Manifest{
		ContentType: contentType,
		Data:        data,
	}
	manifestData, _ := manifest.MarshalJSON()

	// 模拟直接通过键查找成功的情况
	key := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: manifestData,
	}, nil).Once()

	// 执行测试
	resultData, resultContentType, err := service.GetManifest(repository, reference)
	assert.NoError(t, err)
	assert.Equal(t, data, resultData)
	assert.Equal(t, contentType, resultContentType)

	// 测试通过摘要查找的情况
	digest := "sha256:" + utils.ComputeSHA256(data)

	// 模拟直接查找失败
	mockStorage.On("GetObject", registryBucket, fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)).
		Return(nil, errors.New("not found")).Once()

	// 模拟列出所有清单
	prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
	mockStorage.On("ListObjects", registryBucket, prefix, "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: fmt.Sprintf("%s/%s/tag1", repository, manifestsPath)},
		{Key: fmt.Sprintf("%s/%s/tag2", repository, manifestsPath)},
	}, "", nil).Once()

	// 模拟获取第一个清单失败
	mockStorage.On("GetObject", registryBucket, fmt.Sprintf("%s/%s/tag1", repository, manifestsPath)).
		Return(nil, errors.New("error")).Once()

	// 模拟获取第二个清单成功，且摘要匹配
	mockStorage.On("GetObject", registryBucket, fmt.Sprintf("%s/%s/tag2", repository, manifestsPath)).
		Return(&types.S3ObjectData{Data: manifestData}, nil).Once()

	// 执行测试
	resultData, resultContentType, err = service.GetManifest(repository, digest)
	assert.NoError(t, err)
	assert.Equal(t, data, resultData)
	assert.Equal(t, contentType, resultContentType)

	// 测试找不到清单的情况
	mockStorage.On("GetObject", registryBucket, key).Return(nil, errors.New("not found")).Once()
	mockStorage.On("ListObjects", registryBucket, prefix, "", "", 1000).Return([]types.S3ObjectInfo{}, "", nil).Once()

	resultData, resultContentType, err = service.GetManifest(repository, reference)
	assert.Error(t, err)
	assert.Nil(t, resultData)
	assert.Empty(t, resultContentType)

	mockStorage.AssertExpectations(t)
}

func TestDeleteManifest(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	repository := "test/repo"
	reference := "latest"
	contentType := "application/vnd.docker.distribution.manifest.v2+json"
	data := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`)

	// 创建清单对象
	manifest := &types.Manifest{
		ContentType: contentType,
		Data:        data,
	}
	manifestData, _ := manifest.MarshalJSON()
	digest := "sha256:" + utils.ComputeSHA256(data)

	// 模拟获取清单
	key := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: manifestData,
	}, nil).Once()

	// 模拟列出所有清单
	prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
	mockStorage.On("ListObjects", registryBucket, prefix, "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)},
		{Key: fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)},
	}, "", nil).Once()

	// 模拟获取清单内容
	mockStorage.On("GetObject", registryBucket, fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)).
		Return(&types.S3ObjectData{Data: manifestData}, nil).Once()
	mockStorage.On("GetObject", registryBucket, fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)).
		Return(&types.S3ObjectData{Data: manifestData}, nil).Once()

	// 模拟删除操作
	mockStorage.On("DeleteObject", registryBucket, fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)).
		Return(nil).Once()
	mockStorage.On("DeleteObject", registryBucket, fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)).
		Return(nil).Once()

	// 执行测试
	err = service.DeleteManifest(repository, reference)
	assert.NoError(t, err)

	// 测试找不到清单的情况
	mockStorage.On("GetObject", registryBucket, key).Return(nil, errors.New("not found")).Once()

	err = service.DeleteManifest(repository, reference)
	assert.Error(t, err)

	mockStorage.AssertExpectations(t)
}

func TestPutBlob(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	// 设置分块上传阈值
	service.multipartThreshold = 10 * 1024 * 1024 // 10MB
	service.chunkLength = 1 * 1024 * 1024         // 1MB

	repository := "test/repo"
	digest := "sha256:abc123"

	// 测试小文件直接上传
	smallData := bytes.Repeat([]byte("a"), 1024) // 1KB

	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()
	mockStorage.On("PutObject", registryBucket, mock.MatchedBy(func(obj *types.S3ObjectData) bool {
		return obj.Key == fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
	})).Return(nil).Once()

	err = service.PutBlob(repository, digest, smallData)
	assert.NoError(t, err)

	// 测试桶不存在的情况
	mockStorage.On("BucketExists", registryBucket).Return(false, nil).Once()
	mockStorage.On("CreateBucket", registryBucket).Return(nil).Once()
	mockStorage.On("PutObject", registryBucket, mock.MatchedBy(func(obj *types.S3ObjectData) bool {
		return obj.Key == fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
	})).Return(nil).Once()

	err = service.PutBlob(repository, digest, smallData)
	assert.NoError(t, err)

	// 测试检查桶存在时出错
	mockStorage.On("BucketExists", registryBucket).Return(false, errors.New("storage error")).Once()

	err = service.PutBlob(repository, digest, smallData)
	assert.Error(t, err)

	mockStorage.AssertExpectations(t)
}

func TestGetBlob(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	repository := "test/repo"
	digest := "sha256:abc123"
	data := []byte("test blob data")

	// 测试普通blob获取
	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: data,
	}, nil).Once()

	resultData, err := service.GetBlob(repository, digest)
	assert.NoError(t, err)
	assert.Equal(t, data, resultData)

	// 测试分块blob获取
	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: []byte{},
		Metadata: map[string]string{
			"chunked":  "true",
			"manifest": key + ".manifest",
		},
	}, nil).Once()

	// 模拟获取清单
	mm := &types.MultiManifest{
		TotalChunks: 2,
		Digest:      digest,
		Size:        len(data),
	}
	manifestData, _ := mm.MarshalJSON()

	mockStorage.On("GetObject", registryBucket, key+".manifest").Return(&types.S3ObjectData{
		Key:  key + ".manifest",
		Data: manifestData,
	}, nil).Once()

	// 模拟获取块
	mockStorage.On("GetObject", registryBucket, key+".part0").Return(&types.S3ObjectData{
		Key:  key + ".part0",
		Data: data[:len(data)/2],
	}, nil).Once()

	mockStorage.On("GetObject", registryBucket, key+".part1").Return(&types.S3ObjectData{
		Key:  key + ".part1",
		Data: data[len(data)/2:],
	}, nil).Once()

	resultData, err = service.GetBlob(repository, digest)
	assert.Error(t, err) // 应该失败，因为摘要不匹配

	// 测试找不到blob的情况
	mockStorage.On("GetObject", registryBucket, key).Return(nil, errors.New("not found")).Once()

	resultData, err = service.GetBlob(repository, digest)
	assert.Error(t, err)
	assert.Nil(t, resultData)

	mockStorage.AssertExpectations(t)
}

func TestDeleteBlob(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	repository := "test/repo"
	digest := "sha256:abc123"

	// 测试 blob 被引用的情况
	// 模拟 GetManifestReferences 返回引用
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: "repo1/manifests/tag1"},
	}, []string{}, nil).Once()

	v2Manifest := types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: digest,
		},
	}
	v2Data, _ := v2Manifest.MarshalJSON()
	manifest1 := &types.Manifest{
		ContentType: "application/vnd.docker.distribution.manifest.v2+json",
		Data:        v2Data,
	}
	manifest1Data, _ := manifest1.MarshalJSON()

	mockStorage.On("GetObject", registryBucket, "repo1/manifests/tag1").Return(&types.S3ObjectData{
		Key:  "repo1/manifests/tag1",
		Data: manifest1Data,
	}, nil).Once()

	err = service.DeleteBlob(repository, digest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blob is still referenced")

	// 测试正常删除普通 blob
	// 模拟 GetManifestReferences 返回空引用
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).Return([]types.S3ObjectInfo{}, []string{}, nil).Once()

	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: []byte("test"),
	}, nil).Once()

	mockStorage.On("DeleteObject", registryBucket, key).Return(nil).Once()

	err = service.DeleteBlob(repository, digest)
	assert.NoError(t, err)

	// 测试删除分块 blob
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).Return([]types.S3ObjectInfo{}, []string{}, nil).Once()

	mockStorage.On("GetObject", registryBucket, key).Return(&types.S3ObjectData{
		Key:  key,
		Data: []byte{},
		Metadata: map[string]string{
			"chunked":  "true",
			"manifest": key + ".manifest",
		},
	}, nil).Once()

	// 模拟获取清单
	mm := &types.MultiManifest{
		TotalChunks: 2,
		Digest:      digest,
		Size:        10,
	}
	manifestData, _ := mm.MarshalJSON()

	mockStorage.On("GetObject", registryBucket, key+".manifest").Return(&types.S3ObjectData{
		Key:  key + ".manifest",
		Data: manifestData,
	}, nil).Once()

	// 模拟删除块和清单
	mockStorage.On("DeleteObject", registryBucket, key+".part0").Return(nil).Once()
	mockStorage.On("DeleteObject", registryBucket, key+".part1").Return(nil).Once()
	mockStorage.On("DeleteObject", registryBucket, key+".manifest").Return(nil).Once()
	mockStorage.On("DeleteObject", registryBucket, key).Return(nil).Once()

	err = service.DeleteBlob(repository, digest)
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestListBlobs(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	// 模拟列出所有blob
	mockStorage.On("ListObjects", registryBucket, blobsPath+"/", "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: "repo1/blobs/sha256:123"},
		{Key: "repo2/blobs/sha256:456"},
	}, []string{}, nil).Once()

	blobs, err := service.ListBlobs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(blobs))
	assert.Contains(t, blobs, "repo1/blobs/sha256:123")
	assert.Contains(t, blobs, "repo2/blobs/sha256:456")

	// 测试列出失败的情况
	mockStorage.On("ListObjects", registryBucket, blobsPath+"/", "", "", 1000).
		Return(nil, nil, errors.New("list error")).Once()

	blobs, err = service.ListBlobs()
	assert.Error(t, err)
	assert.Nil(t, blobs)

	mockStorage.AssertExpectations(t)
}

func TestListManifests(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	// 模拟列出所有对象
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: "repo1/manifests/tag1"},
		{Key: "repo1/manifests/tag2"},
		{Key: "repo2/manifests/latest"},
		{Key: "repo1/blobs/sha256:123"},
		{Key: "repo1/manifests/tag1.manifest"}, // 应该被过滤掉
	}, []string{}, nil).Once()

	manifests, err := service.ListManifests()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(manifests))
	assert.Contains(t, manifests, "repo1:tag1")
	assert.Contains(t, manifests, "repo1:tag2")
	assert.Contains(t, manifests, "repo2:latest")

	// 测试列出失败的情况
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).
		Return(nil, nil, errors.New("list error")).Once()

	manifests, err = service.ListManifests()
	assert.Error(t, err)
	assert.Nil(t, manifests)

	mockStorage.AssertExpectations(t)
}

func TestGetManifestReferences(t *testing.T) {
	mockStorage := new(RegistryMockStorage)
	mockStorage.On("BucketExists", registryBucket).Return(true, nil).Once()

	service, err := NewRegistryService(mockStorage)
	assert.NoError(t, err)

	blobDigest := "sha256:abc123"

	// 模拟列出所有清单
	mockStorage.On("ListObjects", registryBucket, "", "", "", 1000).Return([]types.S3ObjectInfo{
		{Key: "repo1/manifests/tag1"},
		{Key: "repo2/manifests/latest"},
	}, []string{}, nil).Once()

	// 模拟获取第一个清单内容 - 包含对blob的引用
	v2Manifest := types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: blobDigest,
		},
	}
	v2Data, _ := v2Manifest.MarshalJSON()

	mockStorage.On("GetObject", registryBucket, "repo1/manifests/tag1").Return(&types.S3ObjectData{
		Key:  "repo1/manifests/tag1",
		Data: v2Data,
	}, nil).Once()

	// 模拟获取第二个清单内容 - 不包含对blob的引用
	v2Manifest2 := types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: "sha256:other",
		},
	}
	v2Data2, _ := v2Manifest2.MarshalJSON()

	mockStorage.On("GetObject", registryBucket, "repo2/manifests/latest").Return(&types.S3ObjectData{
		Key:  "repo2/manifests/latest",
		Data: v2Data2,
	}, nil).Once()

	refs, err := service.GetManifestReferences(blobDigest)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(refs))
	assert.Contains(t, refs, "repo1/manifests/tag1")

	mockStorage.AssertExpectations(t)
}

func TestContainsBlobReference(t *testing.T) {
	blobDigest := "sha256:abc123"

	// 测试V2清单格式 - 配置引用
	v2Manifest := types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: blobDigest,
		},
	}
	v2Data, _ := v2Manifest.MarshalJSON()

	assert.True(t, containsBlobReference(v2Data, blobDigest))

	// 测试V2清单格式 - 层引用
	v2Manifest = types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: "sha256:other",
		},
		Layers: []types.ManifestV2Config{
			{Digest: "sha256:layer1"},
			{Digest: blobDigest},
		},
	}
	v2Data, _ = v2Manifest.MarshalJSON()

	assert.True(t, containsBlobReference(v2Data, blobDigest))

	// 测试V2清单格式 - 无引用
	v2Manifest = types.ManifestV2{
		SchemaVersion: 2,
		Config: types.ManifestV2Config{
			Digest: "sha256:other",
		},
		Layers: []types.ManifestV2Config{
			{Digest: "sha256:layer1"},
			{Digest: "sha256:layer2"},
		},
	}
	v2Data, _ = v2Manifest.MarshalJSON()

	assert.False(t, containsBlobReference(v2Data, blobDigest))

	// 测试V1清单格式
	v1Manifest := types.ManifestV1{
		SchemaVersion: 1,
		FSLayers: []types.ManifestV1Config{
			{BlobSum: "sha256:layer1"},
			{BlobSum: blobDigest},
		},
	}
	v1Data, _ := v1Manifest.MarshalJSON()

	assert.True(t, containsBlobReference(v1Data, blobDigest))
}
