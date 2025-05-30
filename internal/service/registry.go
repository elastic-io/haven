package service

import (
	"encoding/json"
	"fmt"
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
}

// registryService 提供Docker Registry的业务逻辑
type registryService struct {
	// 定义分段上传的阈值，超过此大小的blob将使用分段上传
	multipartThreshold int
	// 每个分段的大小
	partSize int
	storage  storage.Storage
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

	psl := len(c.PartSize)
	r.partSize, err = utils.ParseSize(c.PartSize[0:psl-1], c.PartSize[psl-1:])
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
	storedManifest := struct {
		ContentType string `json:"content_type"`
		Data        []byte `json:"data"`
	}{
		ContentType: contentType,
		Data:        data,
	}

	value, err := json.Marshal(storedManifest)
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
		// 找到了直接匹配的清单
		var storedManifest struct {
			ContentType string `json:"content_type"`
			Data        []byte `json:"data"`
		}

		if err := json.Unmarshal(objectData.Data, &storedManifest); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal manifest data: %w", err)
		}

		log.Logger.Info("Found manifest, size: ", len(storedManifest.Data), " bytes")
		return storedManifest.Data, storedManifest.ContentType, nil
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

			var storedManifest struct {
				ContentType string `json:"content_type"`
				Data        []byte `json:"data"`
			}

			if err := json.Unmarshal(objectData.Data, &storedManifest); err != nil {
				log.Logger.Info("Warning: Failed to unmarshal manifest ", obj.Key, ": ", err)
				continue
			}

			// 计算清单的摘要
			digest := "sha256:" + utils.ComputeSHA256(storedManifest.Data)

			// 检查摘要是否匹配
			if digest == reference {
				log.Logger.Info("Found manifest with matching digest: ", obj.Key)
				return storedManifest.Data, storedManifest.ContentType, nil
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

	// 首先获取清单数据，以便找到其摘要
	data, _, err := r.GetManifest(repository, reference)
	if err != nil {
		return err
	}

	// 删除按标签索引的清单
	tagKey := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
	if err := r.storage.DeleteObject(registryBucket, tagKey); err != nil {
		return fmt.Errorf("failed to delete manifest by tag: %w", err)
	}

	// 删除按摘要索引的清单
	digest := "sha256:" + utils.ComputeSHA256(data)
	digestKey := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)
	if err := r.storage.DeleteObject(registryBucket, digestKey); err != nil {
		return fmt.Errorf("failed to delete manifest by digest: %w", err)
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

	// 尝试分段上传
	if r.tryMultipartUpload(registryBucket, key, data, digest) {
		return nil
	}

	// 如果分段上传失败，使用分块直接写入
	return r.chunkedDirectUpload(registryBucket, key, data, digest)
}

// tryMultipartUpload 尝试使用分段上传
// 返回是否成功
func (r *registryService) tryMultipartUpload(bucket, key string, data []byte, digest string) bool {
	log.Logger.Info("Attempting multipart upload for blob: ", digest)

	// 初始化分段上传
	uploadID, err := r.storage.CreateMultipartUpload(bucket, key, "application/octet-stream", nil)
	if err != nil {
		log.Logger.Warn("Multipart upload initialization failed: ", err)
		return false
	}

	// 分段上传数据
	var parts []types.MultipartPart
	totalParts := (len(data) + r.partSize - 1) / r.partSize // 向上取整

	for i := 0; i < totalParts; i++ {
		start := i * r.partSize
		end := (i + 1) * r.partSize
		if end > len(data) {
			end = len(data)
		}

		partNumber := i + 1 // 分段号从1开始
		partData := data[start:end]

		log.Logger.Info("Uploading part ", partNumber, " of ", totalParts, " for blob ", digest, " (size: ", len(partData), " bytes)")

		etag, err := r.storage.UploadPart(bucket, key, uploadID, partNumber, partData)
		if err != nil {
			// 上传失败，中止整个上传
			log.Logger.Warn("Part upload failed: ", err)
			abortErr := r.storage.AbortMultipartUpload(bucket, key, uploadID)
			if abortErr != nil {
				log.Logger.Warn("Failed to abort multipart upload: ", abortErr)
			}
			return false
		}

		parts = append(parts, types.MultipartPart{
			PartNumber: partNumber,
			ETag:       etag,
		})
	}

	// 完成分段上传
	_, err = r.storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		log.Logger.Warn("Failed to complete multipart upload: ", err)

		// 尝试中止失败的上传
		abortErr := r.storage.AbortMultipartUpload(bucket, key, uploadID)
		if abortErr != nil {
			log.Logger.Warn("Failed to abort multipart upload: ", abortErr)
		}
		return false
	}

	log.Logger.Info("Successfully uploaded blob ", digest, " using multipart upload")
	return true
}

// chunkedDirectUpload 使用分块方式直接上传
func (r *registryService) chunkedDirectUpload(bucket, key string, data []byte, digest string) error {
	log.Logger.Info("Using chunked direct upload for blob: ", digest)

	// 使用较小的块大小进行分块上传
	chunkSize := 1 * 1024 * 1024 // 1MB
	totalChunks := (len(data) + chunkSize - 1) / chunkSize

	for i := 0; i < totalChunks; i++ {
		start := i * chunkSize
		end := (i + 1) * chunkSize
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
	manifest := struct {
		TotalChunks int    `json:"total_chunks"`
		Digest      string `json:"digest"`
		Size        int    `json:"size"`
	}{
		TotalChunks: totalChunks,
		Digest:      digest,
		Size:        len(data),
	}

	manifestData, err := json.Marshal(manifest)
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

	var manifest struct {
		TotalChunks int    `json:"total_chunks"`
		Digest      string `json:"digest"`
		Size        int    `json:"size"`
	}

	if err := json.Unmarshal(manifestObj.Data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse chunk manifest: %w", err)
	}

	// 分配足够的空间
	result := make([]byte, manifest.Size)
	var offset int

	// 读取所有块并组装
	for i := 0; i < manifest.TotalChunks; i++ {
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
	if actualDigest != manifest.Digest {
		return nil, fmt.Errorf("blob digest mismatch: expected %s, got %s", manifest.Digest, actualDigest)
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
					var manifest struct {
						TotalChunks int `json:"total_chunks"`
					}

					if json.Unmarshal(manifestObj.Data, &manifest) == nil {
						// 删除所有块
						for i := 0; i < manifest.TotalChunks; i++ {
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

	// 列出所有清单对象
	prefix := manifestsPath + "/"
	objects, _, err := r.storage.ListObjects(registryBucket, prefix, "", "", 1000)
	if err != nil {
		return nil, err
	}

	manifests := make([]string, len(objects))
	for i, obj := range objects {
		manifests[i] = obj.Key
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

// containsBlobReference 检查清单是否引用了指定的blob
func containsBlobReference(manifestData []byte, blobDigest string) bool {
	// 解析清单内容，检查是否引用了指定的blob
	var manifest map[string]interface{}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return false // 跳过无法解析的清单
	}

	// 检查清单类型
	schemaVersion, ok := manifest["schemaVersion"].(float64)
	if !ok {
		return false
	}

	// 根据不同的清单版本检查blob引用
	if schemaVersion == 2 {
		// 检查config blob
		if config, ok := manifest["config"].(map[string]interface{}); ok {
			if digest, ok := config["digest"].(string); ok && digest == blobDigest {
				return true
			}
		}

		// 检查layers
		if layers, ok := manifest["layers"].([]interface{}); ok {
			for _, layer := range layers {
				if layerObj, ok := layer.(map[string]interface{}); ok {
					if digest, ok := layerObj["digest"].(string); ok && digest == blobDigest {
						return true
					}
				}
			}
		}
	} else if schemaVersion == 1 {
		// 处理v1清单格式
		if fsLayers, ok := manifest["fsLayers"].([]interface{}); ok {
			for _, layer := range fsLayers {
				if layerObj, ok := layer.(map[string]interface{}); ok {
					if blobSum, ok := layerObj["blobSum"].(string); ok && blobSum == blobDigest {
						return true
					}
				}
			}
		}
	}

	return false
}