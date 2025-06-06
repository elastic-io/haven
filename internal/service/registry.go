package service

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
)

const (
	registryBucket = "registry"
	manifestsPath  = "manifests"
	blobsPath      = "blobs"
)

type RegistryService interface {
	PutManifest(repository, reference string, data []byte, contentType string) error
	GetManifest(repository, reference string) ([]byte, string, error)
	DeleteManifest(repository, reference string) error
	PutBlob(repository, digest string, data []byte) error
	GetBlob(repository, digest string) ([]byte, error)
	DeleteBlob(repository, digest string) error
	ListBlobs() ([]string, error)
	ListManifests() ([]string, error)
	GetManifestReferences(blobDigest string) ([]string, error)
	PutBlobFromReader(repository, digest string, reader io.Reader, size int64) error
}

// registryService 提供Docker Registry的业务逻辑
type registryService struct {
	// 定义分块上传的阈值，超过此大小的blob将使用分块上传
	multipartThreshold int
	// 分块上传
	chunkLength int
	storage     storage.Storage
}

// NewregistryService 创建一个新的Registry服务
func NewRegistryService(storage storage.Storage) (*registryService, error) {
	// 确保registry桶存在
	exists, err := storage.BucketExists(registryBucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := storage.CreateBucket(registryBucket); err != nil {
			return nil, err
		}
	}

	return &registryService{storage: storage}, nil
}

func (r *registryService) Init(c *config.Config) error {
	var err error

	mml := len(c.MaxMultipart)
	r.multipartThreshold, err = utils.ParseSize(c.MaxMultipart[0:mml-1], c.MaxMultipart[mml-1:])
	if err != nil {
		return err
	}

	cll := len(c.ChunkLength)
	r.chunkLength, err = utils.ParseSize(c.ChunkLength[0:cll-1], c.ChunkLength[cll-1:])
	if err != nil {
		return err
	}
	return nil
}

// PutManifest 存储镜像清单
func (r *registryService) PutManifest(repository, reference string, data []byte, contentType string) error {
	log.Logger.Info("Registry: Putting manifest for ", repository, ":", reference, " size: ", len(data), " bytes")

	// 计算清单的摘要
	digest := "sha256:" + utils.ComputeSHA256(data)
	log.Logger.Info("Manifest digest: ", digest)

	// 准备存储数据
	m := &types.Manifest{
		ContentType: contentType,
		Data:        data,
	}

	value, err := m.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal manifest data: %w", err)
	}

	// 存储按标签索引的清单
	tagKey := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)

	// 使用新的存储层API存储对象
	tagObject := &types.S3ObjectData{
		Key:          tagKey,
		Data:         value,
		ContentType:  "application/json",
		LastModified: time.Now(),
		ETag:         "\"" + utils.ComputeSHA256(value) + "\"",
		Metadata:     nil,
	}

	if err := r.storage.PutObject(registryBucket, tagObject); err != nil {
		return fmt.Errorf("failed to store manifest by tag: %w", err)
	}

	// 同时存储按摘要索引的清单
	digestKey := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)

	// 使用新的存储层API存储对象
	digestObject := &types.S3ObjectData{
		Key:          digestKey,
		Data:         value,
		ContentType:  "application/json",
		LastModified: time.Now(),
		ETag:         "\"" + utils.ComputeSHA256(value) + "\"",
		Metadata:     nil,
	}

	if err := r.storage.PutObject(registryBucket, digestObject); err != nil {
		return fmt.Errorf("failed to store manifest by digest: %w", err)
	}

	log.Logger.Info("Successfully stored manifest ", repository, ":", reference, " with digest ", digest)
	return nil
}

// GetManifest 获取镜像清单
func (r *registryService) GetManifest(repository, reference string) ([]byte, string, error) {
	log.Logger.Info("Registry: Getting manifest for ", repository, ":", reference)

	// 首先尝试直接通过完整键查找
	key := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	log.Logger.Info("Looking for key: ", key)

	objectData, err := r.storage.GetObject(registryBucket, key)
	if err == nil {
		m := &types.Manifest{}
		if err := m.UnmarshalJSON(objectData.Data); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal manifest data: %w", err)
		}

		log.Logger.Info("Found manifest, size: ", len(m.Data), " bytes")
		return m.Data, m.ContentType, nil
	}

	// 如果是通过摘要查询但路径不匹配，尝试查找所有清单
	if strings.HasPrefix(reference, "sha256:") {
		log.Logger.Info("Searching for manifest by digest: ", reference)

		// 列出所有清单
		prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
		objects, _, err := r.storage.ListObjects(registryBucket, prefix, "", "", 1000)
		if err != nil {
			return nil, "", err
		}

		// 检查每个清单
		for _, obj := range objects {
			objectData, err := r.storage.GetObject(registryBucket, obj.Key)
			if err != nil {
				continue
			}

			m := &types.Manifest{}
			if err := m.UnmarshalJSON(objectData.Data); err != nil {
				log.Logger.Info("Warning: Failed to unmarshal manifest ", obj.Key, ": ", err)
				continue
			}

			// 计算清单的摘要
			digest := "sha256:" + utils.ComputeSHA256(m.Data)

			// 检查摘要是否匹配
			if digest == reference {
				log.Logger.Info("Found manifest with matching digest: ", obj.Key)
				return m.Data, m.ContentType, nil
			}
		}
	}

	// 列出所有可用的清单，以便调试
	prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
	objects, _, _ := r.storage.ListObjects(registryBucket, prefix, "", "", 1000)
	log.Logger.Info("Available manifests:")
	for _, obj := range objects {
		log.Logger.Info("  - ", obj.Key)
	}

	return nil, "", fmt.Errorf("manifest not found: %s:%s", repository, reference)
}

// DeleteManifest 删除镜像清单
func (r *registryService) DeleteManifest(repository, reference string) error {
	log.Logger.Info("Registry: Deleting manifest for ", repository, ":", reference)

	// 首先获取清单数据
	data, _, err := r.GetManifest(repository, reference)
	if err != nil {
		return err
	}

	// 如果是通过摘要查询，直接使用该摘要
	var digest string
	if strings.HasPrefix(reference, "sha256:") {
		digest = reference
	} else {
		// 否则计算摘要
		digest = "sha256:" + utils.ComputeSHA256(data)
	}

	// 查找实际存储的清单路径
	prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
	objects, _, err := r.storage.ListObjects(registryBucket, prefix, "", "", 1000)
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}

	// 标记是否找到并删除了清单
	manifestFound := false

	// 检查每个清单
	for _, obj := range objects {
		objectData, err := r.storage.GetObject(registryBucket, obj.Key)
		if err != nil {
			continue
		}

		m := &types.Manifest{}
		if err := m.UnmarshalJSON(objectData.Data); err != nil {
			log.Logger.Info("Warning: Failed to unmarshal manifest ", obj.Key, ": ", err)
			continue
		}

		// 计算清单的摘要
		currentDigest := "sha256:" + utils.ComputeSHA256(m.Data)

		// 检查摘要是否匹配
		if currentDigest == digest || (reference != digest && strings.HasSuffix(obj.Key, reference)) {
			log.Logger.Info("Deleting manifest: ", obj.Key)
			if err := r.storage.DeleteObject(registryBucket, obj.Key); err != nil {
				return fmt.Errorf("failed to delete manifest %s: %w", obj.Key, err)
			}
			manifestFound = true
		}
	}

	if !manifestFound {
		return fmt.Errorf("manifest not found for deletion: %s:%s", repository, reference)
	}

	log.Logger.Info("Successfully deleted manifest ", repository, ":", reference, " with digest ", digest)

	return nil
}

// PutBlob 存储一个blob
func (r *registryService) PutBlob(repository, digest string, data []byte) error {
	log.Logger.Info("Registry: Putting blob ", digest, " for repository ", repository, " size: ", len(data), " bytes")

	// 确保registry桶存在
	exists, err := r.storage.BucketExists(registryBucket)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		log.Logger.Info("Creating registry bucket as it doesn't exist")
		if err := r.storage.CreateBucket(registryBucket); err != nil {
			return fmt.Errorf("failed to create registry bucket: %w", err)
		}
	}

	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)

	// 对于小文件，直接使用PutObject
	if len(data) < r.multipartThreshold {
		log.Logger.Info("Using direct upload for small blob: ", digest)
		blobObject := &types.S3ObjectData{
			Key:          key,
			Data:         data,
			ContentType:  "application/octet-stream",
			LastModified: time.Now(),
			ETag:         "\"" + utils.ComputeSHA256(data) + "\"",
			Metadata:     nil,
		}

		return r.storage.PutObject(registryBucket, blobObject)
	}

	// 对于大文件，使用分块写入方式，而不是一次性加载整个文件
	log.Logger.Info("Using chunked upload for large blob: ", digest)

	// 使用分块直接写入
	return r.chunkedDirectUpload(registryBucket, key, data, digest)
}

// chunkedDirectUpload 使用分块方式直接上传
func (r *registryService) chunkedDirectUpload(bucket, key string, data []byte, digest string) error {
	log.Logger.Info("Using chunked direct upload for blob: ", digest)

	totalChunks := (len(data) + r.chunkLength - 1) / r.chunkLength

	for i := 0; i < totalChunks; i++ {
		start := i * r.chunkLength
		end := (i + 1) * r.chunkLength
		if end > len(data) {
			end = len(data)
		}

		chunkData := data[start:end]
		chunkKey := fmt.Sprintf("%s.part%d", key, i)

		log.Logger.Info("Uploading chunk ", i+1, " of ", totalChunks, " for blob ", digest, " (size: ", len(chunkData), " bytes)")

		chunkObject := &types.S3ObjectData{
			Key:          chunkKey,
			Data:         chunkData,
			ContentType:  "application/octet-stream",
			LastModified: time.Now(),
			ETag:         "\"" + utils.ComputeSHA256(chunkData) + "\"",
			Metadata:     nil,
		}

		if err := r.storage.PutObject(bucket, chunkObject); err != nil {
			// 清理已上传的块
			for j := 0; j < i; j++ {
				cleanupKey := fmt.Sprintf("%s.part%d", key, j)
				r.storage.DeleteObject(bucket, cleanupKey)
			}
			return fmt.Errorf("failed to upload chunk %d: %w", i+1, err)
		}
	}

	// 创建一个清单文件，记录所有块
	mm := &types.MultiManifest{
		TotalChunks: totalChunks,
		Digest:      digest,
		Size:        len(data),
	}

	manifestData, err := mm.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to create chunk manifest: %w", err)
	}

	manifestObject := &types.S3ObjectData{
		Key:          key + ".manifest",
		Data:         manifestData,
		ContentType:  "application/json",
		LastModified: time.Now(),
		ETag:         "\"" + utils.ComputeSHA256(manifestData) + "\"",
		Metadata:     nil,
	}

	if err := r.storage.PutObject(bucket, manifestObject); err != nil {
		return fmt.Errorf("failed to upload chunk manifest: %w", err)
	}

	// 创建一个空的主对象，指向清单
	emptyObject := &types.S3ObjectData{
		Key:          key,
		Data:         []byte{},
		ContentType:  "application/octet-stream",
		LastModified: time.Now(),
		ETag:         "\"" + digest + "\"",
		Metadata: map[string]string{
			"chunked":  "true",
			"manifest": key + ".manifest",
		},
	}

	if err := r.storage.PutObject(bucket, emptyObject); err != nil {
		return fmt.Errorf("failed to create main object: %w", err)
	}

	log.Logger.Info("Successfully uploaded blob ", digest, " using chunked direct upload")
	return nil
}

// GetBlob 获取一个blob
func (r *registryService) GetBlob(repository, digest string) ([]byte, error) {
	log.Logger.Info("Registry: Getting blob ", digest, " for repository ", repository)

	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
	objectData, err := r.storage.GetObject(registryBucket, key)
	if err != nil {
		return nil, err
	}

	// 检查是否是分块上传的对象
	if chunked, ok := objectData.Metadata["chunked"]; ok && chunked == "true" {
		return r.getChunkedBlob(registryBucket, key, objectData)
	}

	return objectData.Data, nil
}

// getChunkedBlob 获取分块上传的blob
func (r *registryService) getChunkedBlob(bucket, key string, mainObject *types.S3ObjectData) ([]byte, error) {
	manifestKey, ok := mainObject.Metadata["manifest"]
	if !ok {
		return nil, fmt.Errorf("chunked object missing manifest reference")
	}

	// 获取清单
	manifestObj, err := r.storage.GetObject(bucket, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk manifest: %w", err)
	}

	mm := &types.MultiManifest{}
	if err := mm.UnmarshalJSON(manifestObj.Data); err != nil {
		return nil, fmt.Errorf("failed to parse chunk manifest: %w", err)
	}

	// 分配足够的空间
	result := make([]byte, mm.Size)
	var offset int

	// 读取所有块并组装
	for i := 0; i < mm.TotalChunks; i++ {
		chunkKey := fmt.Sprintf("%s.part%d", key, i)
		chunkObj, err := r.storage.GetObject(bucket, chunkKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get chunk %d: %w", i, err)
		}

		// 复制数据到结果中
		copy(result[offset:], chunkObj.Data)
		offset += len(chunkObj.Data)
	}

	// 验证摘要
	actualDigest := "sha256:" + utils.ComputeSHA256(result)
	if actualDigest != mm.Digest {
		return nil, fmt.Errorf("blob digest mismatch: expected %s, got %s", mm.Digest, actualDigest)
	}

	return result, nil
}

// DeleteBlob 删除一个blob
func (r *registryService) DeleteBlob(repository, digest string) error {
	log.Logger.Info("Registry: Deleting blob ", digest, " for repository ", repository)

	// 首先检查是否有清单引用此blob
	refs, err := r.GetManifestReferences(digest)
	if err != nil {
		return fmt.Errorf("failed to check manifest references: %w", err)
	}

	if len(refs) > 0 {
		return fmt.Errorf("blob is still referenced by %d manifests", len(refs))
	}

	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)

	// 获取对象以检查是否是分块对象
	objectData, err := r.storage.GetObject(registryBucket, key)
	if err == nil {
		// 检查是否是分块上传的对象
		if chunked, ok := objectData.Metadata["chunked"]; ok && chunked == "true" {
			// 删除所有相关的块
			manifestKey, ok := objectData.Metadata["manifest"]
			if ok {
				// 获取清单
				manifestObj, err := r.storage.GetObject(registryBucket, manifestKey)
				if err == nil {
					mm := types.MultiManifest{}
					if mm.UnmarshalJSON(manifestObj.Data) == nil {
						// 删除所有块
						for i := 0; i < mm.TotalChunks; i++ {
							chunkKey := fmt.Sprintf("%s.part%d", key, i)
							r.storage.DeleteObject(registryBucket, chunkKey)
						}
					}
				}

				// 删除清单
				r.storage.DeleteObject(registryBucket, manifestKey)
			}
		}
	}

	// 删除主对象
	return r.storage.DeleteObject(registryBucket, key)
}

// ListBlobs 列出所有blob
func (r *registryService) ListBlobs() ([]string, error) {
	log.Logger.Info("Registry: Listing all blobs")

	// 列出所有blob对象
	prefix := blobsPath + "/"
	objects, _, err := r.storage.ListObjects(registryBucket, prefix, "", "", 1000)
	if err != nil {
		return nil, err
	}

	blobs := make([]string, len(objects))
	for i, obj := range objects {
		blobs[i] = obj.Key
	}

	return blobs, nil
}

// ListManifests 列出所有清单
func (r *registryService) ListManifests() ([]string, error) {
	log.Logger.Info("Registry: Listing all manifests")

	// 查找所有对象
	objects, _, err := r.storage.ListObjects(registryBucket, "", "", "", 1000)
	if err != nil {
		return nil, err
	}

	match := "/" + manifestsPath + "/"

	var manifests []string
	for _, obj := range objects {
		// 检查是否是清单文件 (包含 "/manifests/" 但不是 ".manifest" 结尾的文件)
		if strings.Contains(obj.Key, match) && !strings.HasSuffix(obj.Key, ".manifest") {
			// 提取仓库名和标签
			parts := strings.Split(obj.Key, match)
			if len(parts) == 2 {
				repo := parts[0]
				tag := parts[1]
				manifests = append(manifests, repo+":"+tag)
			}
		}
	}

	return manifests, nil
}

// GetManifestReferences 获取引用某个blob的所有manifest
func (r *registryService) GetManifestReferences(blobDigest string) ([]string, error) {
	log.Logger.Info("Registry: Getting manifest references for blob ", blobDigest)

	var references []string

	// 列出所有清单
	manifests, err := r.ListManifests()
	if err != nil {
		return nil, err
	}

	// 检查每个清单是否引用了指定的blob
	for _, manifestKey := range manifests {
		// 从路径中提取repository和reference
		parts := strings.Split(manifestKey, "/")
		if len(parts) < 3 {
			continue
		}

		repository := parts[0]
		reference := parts[2]

		// 获取清单数据
		manifestData, _, err := r.GetManifest(repository, reference)
		if err != nil {
			continue
		}

		// 解析清单内容，检查是否引用了指定的blob
		if containsBlobReference(manifestData, blobDigest) {
			references = append(references, manifestKey)
		}
	}

	return references, nil
}

// PutBlobFromReader 从读取器流式存储blob
func (r *registryService) PutBlobFromReader(repository, digest string, reader io.Reader, size int64) error {
	log.Logger.Info("Registry: Putting blob ", digest, " for repository ", repository, " size: ", size, " bytes")

	// 确保registry桶存在
	exists, err := r.storage.BucketExists(registryBucket)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		log.Logger.Info("Creating registry bucket as it doesn't exist")
		if err := r.storage.CreateBucket(registryBucket); err != nil {
			return fmt.Errorf("failed to create registry bucket: %w", err)
		}
	}

	key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)

	// 对于小文件，直接使用PutObject
	if size < int64(r.multipartThreshold) {
		log.Logger.Info("Using direct upload for small blob: ", digest)

		// 读取所有数据
		data, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to read data: %w", err)
		}

		blobObject := &types.S3ObjectData{
			Key:          key,
			Data:         data,
			ContentType:  "application/octet-stream",
			LastModified: time.Now(),
			ETag:         "\"" + utils.ComputeSHA256(data) + "\"",
			Metadata:     nil,
		}

		return r.storage.PutObject(registryBucket, blobObject)
	}

	// 对于大文件，使用分块上传
	log.Logger.Info("Using chunked upload for large blob: ", digest)
	return r.chunkedStreamUpload(registryBucket, key, reader, size, digest)
}

// chunkedStreamUpload 使用分块方式流式上传
func (r *registryService) chunkedStreamUpload(bucket, key string, reader io.Reader, size int64, digest string) error {
	log.Logger.Info("Using chunked stream upload for blob: ", digest)

	// 计算总块数
	totalChunks := (size + int64(r.chunkLength) - 1) / int64(r.chunkLength)
	log.Logger.Info("Total chunks: ", totalChunks, " for blob ", digest)

	// 创建临时目录存储分块
	tempDir, err := os.MkdirTemp("", "blob-chunks-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// 读取和上传分块
	buffer := make([]byte, r.chunkLength)
	for i := int64(0); i < totalChunks; i++ {
		// 读取一个块
		bytesRead, err := io.ReadFull(reader, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("failed to read chunk %d: %w", i+1, err)
		}

		// 如果是最后一块，可能不满
		chunkData := buffer[:bytesRead]
		chunkKey := fmt.Sprintf("%s.part%d", key, i)

		log.Logger.Info("Uploading chunk ", i+1, " of ", totalChunks, " for blob ", digest, " (size: ", len(chunkData), " bytes)")

		chunkObject := &types.S3ObjectData{
			Key:          chunkKey,
			Data:         chunkData,
			ContentType:  "application/octet-stream",
			LastModified: time.Now(),
			ETag:         "\"" + utils.ComputeSHA256(chunkData) + "\"",
			Metadata:     nil,
		}

		if err := r.storage.PutObject(bucket, chunkObject); err != nil {
			// 清理已上传的块
			for j := int64(0); j < i; j++ {
				cleanupKey := fmt.Sprintf("%s.part%d", key, j)
				r.storage.DeleteObject(bucket, cleanupKey)
			}
			return fmt.Errorf("failed to upload chunk %d: %w", i+1, err)
		}
	}

	// 创建一个清单文件，记录所有块
	mm := &types.MultiManifest{
		TotalChunks: int(totalChunks),
		Digest:      digest,
		Size:        int(size),
	}

	manifestData, err := mm.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to create chunk manifest: %w", err)
	}

	manifestObject := &types.S3ObjectData{
		Key:          key + ".manifest",
		Data:         manifestData,
		ContentType:  "application/json",
		LastModified: time.Now(),
		ETag:         "\"" + utils.ComputeSHA256(manifestData) + "\"",
		Metadata:     nil,
	}

	if err := r.storage.PutObject(bucket, manifestObject); err != nil {
		return fmt.Errorf("failed to upload chunk manifest: %w", err)
	}

	// 创建一个空的主对象，指向清单
	emptyObject := &types.S3ObjectData{
		Key:          key,
		Data:         []byte{},
		ContentType:  "application/octet-stream",
		LastModified: time.Now(),
		ETag:         "\"" + digest + "\"",
		Metadata: map[string]string{
			"chunked":  "true",
			"manifest": key + ".manifest",
		},
	}

	if err := r.storage.PutObject(bucket, emptyObject); err != nil {
		return fmt.Errorf("failed to create main object: %w", err)
	}

	log.Logger.Info("Successfully uploaded blob ", digest, " using chunked stream upload")
	return nil
}

// containsBlobReference 检查清单是否引用了指定的blob
func containsBlobReference(manifestData []byte, blobDigest string) bool {
	// 首先尝试解析为V2格式
	v2 := types.ManifestV2{}
	if err := v2.UnmarshalJSON(manifestData); err == nil {
		if v2.SchemaVersion == 2 {
			// 检查config blob
			if v2.Config.Digest == blobDigest {
				return true
			}

			// 检查layers
			for _, layer := range v2.Layers {
				if layer.Digest == blobDigest {
					return true
				}
			}
			return false
		}
	}

	// 如果不是V2格式，尝试解析为V1格式
	v1 := types.ManifestV1{}
	if err := v1.UnmarshalJSON(manifestData); err == nil {
		if v1.SchemaVersion == 1 {
			// 处理v1清单格式
			for _, layer := range v1.FSLayers {
				if layer.BlobSum == blobDigest {
					return true
				}
			}
		}
	}

	return false
}
