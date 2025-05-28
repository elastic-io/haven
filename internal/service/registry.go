package service

import (
    "encoding/json"
    "fmt"
    "log"
    "strings"
    
    "github.com/elastic-io/haven/internal/storage"
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
    storage storage.S3
}

// NewregistryService 创建一个新的Registry服务
func NewRegistryService(storage storage.S3) (RegistryService, error) {
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

// PutManifest 存储镜像清单
func (r *registryService) PutManifest(repository, reference string, data []byte, contentType string) error {
    log.Printf("Registry: Putting manifest for %s:%s, size: %d bytes", repository, reference, len(data))
    
    // 计算清单的摘要
    digest := "sha256:" + utils.ComputeSHA256(data)
    log.Printf("Manifest digest: %s", digest)
    
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
    if err := r.storage.PutObject(registryBucket, tagKey, value, nil); err != nil {
        return fmt.Errorf("failed to store manifest by tag: %w", err)
    }
    
    // 同时存储按摘要索引的清单
    digestKey := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, digest)
    if err := r.storage.PutObject(registryBucket, digestKey, value, nil); err != nil {
        return fmt.Errorf("failed to store manifest by digest: %w", err)
    }
    
    log.Printf("Successfully stored manifest %s:%s with digest %s", repository, reference, digest)
    return nil
}

// GetManifest 获取镜像清单
func (r *registryService) GetManifest(repository, reference string) ([]byte, string, error) {
    log.Printf("Registry: Getting manifest for %s:%s", repository, reference)
    
    // 首先尝试直接通过完整键查找
    key := fmt.Sprintf("%s/%s/%s", repository, manifestsPath, reference)
    log.Printf("Looking for key: %s", key)
    
    data, _, err := r.storage.GetObject(registryBucket, key)
    if err == nil {
        // 找到了直接匹配的清单
        var storedManifest struct {
            ContentType string `json:"content_type"`
            Data        []byte `json:"data"`
        }
        
        if err := json.Unmarshal(data, &storedManifest); err != nil {
            return nil, "", fmt.Errorf("failed to unmarshal manifest data: %w", err)
        }
        
        log.Printf("Found manifest, size: %d bytes", len(storedManifest.Data))
        return storedManifest.Data, storedManifest.ContentType, nil
    }
    
    // 如果是通过摘要查询但路径不匹配，尝试查找所有清单
    if strings.HasPrefix(reference, "sha256:") {
        log.Printf("Searching for manifest by digest: %s", reference)
        
        // 列出所有清单
        prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
        manifests, err := r.storage.ListObjects(registryBucket, prefix)
        if err != nil {
            return nil, "", err
        }
        
        // 检查每个清单
        for _, manifestKey := range manifests {
            manifestData, _, err := r.storage.GetObject(registryBucket, manifestKey)
            if err != nil {
                continue
            }
            
            var storedManifest struct {
                ContentType string `json:"content_type"`
                Data        []byte `json:"data"`
            }
            
            if err := json.Unmarshal(manifestData, &storedManifest); err != nil {
                log.Printf("Warning: Failed to unmarshal manifest %s: %v", manifestKey, err)
                continue
            }
            
            // 计算清单的摘要
            digest := "sha256:" + utils.ComputeSHA256(storedManifest.Data)
            
            // 检查摘要是否匹配
            if digest == reference {
                log.Printf("Found manifest with matching digest: %s", manifestKey)
                return storedManifest.Data, storedManifest.ContentType, nil
            }
        }
    }
    
    // 列出所有可用的清单，以便调试
    prefix := fmt.Sprintf("%s/%s/", repository, manifestsPath)
    manifests, _ := r.storage.ListObjects(registryBucket, prefix)
    log.Printf("Available manifests:")
    for _, m := range manifests {
        log.Printf("  - %s", m)
    }
    
    return nil, "", fmt.Errorf("manifest not found: %s:%s", repository, reference)
}

// DeleteManifest 删除镜像清单
func (r *registryService) DeleteManifest(repository, reference string) error {
    log.Printf("Registry: Deleting manifest for %s:%s", repository, reference)
    
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
    
    log.Printf("Successfully deleted manifest %s:%s with digest %s", repository, reference, digest)
    return nil
}

// PutBlob 存储一个blob
func (r *registryService) PutBlob(repository, digest string, data []byte) error {
    log.Printf("Registry: Putting blob %s for repository %s, size: %d bytes", digest, repository, len(data))
    
    key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
    return r.storage.PutObject(registryBucket, key, data, nil)
}

// GetBlob 获取一个blob
func (r *registryService) GetBlob(repository, digest string) ([]byte, error) {
    log.Printf("Registry: Getting blob %s for repository %s", digest, repository)
    
    key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
    data, _, err := r.storage.GetObject(registryBucket, key)
    return data, err
}

// DeleteBlob 删除一个blob
func (r *registryService) DeleteBlob(repository, digest string) error {
    log.Printf("Registry: Deleting blob %s for repository %s", digest, repository)
    
    // 首先检查是否有清单引用此blob
    refs, err := r.GetManifestReferences(digest)
    if err != nil {
        return fmt.Errorf("failed to check manifest references: %w", err)
    }
    
    if len(refs) > 0 {
        return fmt.Errorf("blob is still referenced by %d manifests", len(refs))
    }
    
    key := fmt.Sprintf("%s/%s/%s", repository, blobsPath, digest)
    return r.storage.DeleteObject(registryBucket, key)
}

// ListBlobs 列出所有blob
func (r *registryService) ListBlobs() ([]string, error) {
    log.Printf("Registry: Listing all blobs")
    
    // 列出所有blob对象
    blobs, err := r.storage.ListObjects(registryBucket, "/"+blobsPath+"/")
    if err != nil {
        return nil, err
    }
    
    return blobs, nil
}

// ListManifests 列出所有清单
func (r *registryService) ListManifests() ([]string, error) {
    log.Printf("Registry: Listing all manifests")
    
    // 列出所有清单对象
    manifests, err := r.storage.ListObjects(registryBucket, "/"+manifestsPath+"/")
    if err != nil {
        return nil, err
    }
    
    return manifests, nil
}

// GetManifestReferences 获取引用某个blob的所有manifest
func (r *registryService) GetManifestReferences(blobDigest string) ([]string, error) {
    log.Printf("Registry: Getting manifest references for blob %s", blobDigest)
    
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