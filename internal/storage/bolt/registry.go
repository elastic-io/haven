package bolt
/*
import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic-io/haven/internal/utils"
	"go.etcd.io/bbolt"
)

func (s *BoltBackend) ListManifests() ([]string, error) {
    var manifests []string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(manifestsBucket))
        if b == nil {
            return fmt.Errorf("manifests bucket not found")
        }
        
        c := b.Cursor()
        for k, _ := c.First(); k != nil; k, _ = c.Next() {
            manifests = append(manifests, string(k))
        }
        
        return nil
    })
    
    return manifests, err
}

// GetManifest 获取镜像清单
func (s *BoltBackend) GetManifest(repository, reference string) ([]byte, string, error) {
    log.Printf("Storage: Getting manifest for %s:%s", repository, reference)
    
    var manifest []byte
    var contentType string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(manifestsBucket))
        
        // 首先尝试直接通过完整键查找
        key := repository + ":" + reference
        log.Printf("Storage: Looking for key: %s", key)
        
        value := bucket.Get([]byte(key))
        if value != nil {
            // 找到了直接匹配的清单
            log.Printf("Storage: Found manifest, size: %d bytes", len(value))
            
            // 解析存储的清单数据
            var storedManifest struct {
                ContentType string `json:"content_type"`
                Data        []byte `json:"data"`
            }
            
            if err := json.Unmarshal(value, &storedManifest); err != nil {
                return fmt.Errorf("failed to unmarshal manifest data: %w", err)
            }
            
            manifest = storedManifest.Data
            contentType = storedManifest.ContentType
            return nil
        }
        
        // 如果是通过摘要查询，尝试查找所有清单，检查它们的摘要
        if strings.HasPrefix(reference, "sha256:") {
            log.Printf("Searching for manifest by digest: %s", reference)
            
            // 遍历所有清单
            c := bucket.Cursor()
            for k, v := c.First(); k != nil; k, v = c.Next() {
                keyStr := string(k)
                if strings.HasPrefix(keyStr, repository+":") {
                    // 解析存储的清单数据
                    var storedManifest struct {
                        ContentType string `json:"content_type"`
                        Data        []byte `json:"data"`
                    }
                    
                    if err := json.Unmarshal(v, &storedManifest); err != nil {
                        log.Printf("Warning: Failed to unmarshal manifest %s: %v", keyStr, err)
                        continue
                    }
                    
                    // 计算清单的摘要
                    digest := "sha256:" + utils.ComputeSHA256(storedManifest.Data)
                    
                    // 检查摘要是否匹配
                    if digest == reference {
                        log.Printf("Found manifest with matching digest: %s", keyStr)
                        manifest = storedManifest.Data
                        contentType = storedManifest.ContentType
                        return nil
                    }
                }
            }
        }
        
        // 列出所有可用的清单，以便调试
        log.Printf("Available manifests:")
        c := bucket.Cursor()
        for k, _ := c.First(); k != nil; k, _ = c.Next() {
            log.Printf("  - %s", string(k))
        }
        
        return fmt.Errorf("manifest not found")
    })
    
    if err != nil {
        log.Printf("Storage error: %v", err)
        return nil, "", err
    }
    
    return manifest, contentType, nil
}

// PutManifest 存储镜像清单
func (s *BoltBackend) PutManifest(repository, reference string, manifest []byte, contentType string) error {
    log.Printf("Storage: Putting manifest for %s:%s, size: %d bytes", repository, reference, len(manifest))
    
    // 计算清单的摘要
    digest := "sha256:" + utils.ComputeSHA256(manifest)
    log.Printf("Manifest digest: %s", digest)
    
    // 准备存储数据
    storedManifest := struct {
        ContentType string `json:"content_type"`
        Data        []byte `json:"data"`
    }{
        ContentType: contentType,
        Data:        manifest,
    }
    
    value, err := json.Marshal(storedManifest)
    if err != nil {
        return fmt.Errorf("failed to marshal manifest data: %w", err)
    }
    
    return s.db.Update(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(manifestsBucket))
        
        // 存储按标签索引的清单
        tagKey := repository + ":" + reference
        if err := bucket.Put([]byte(tagKey), value); err != nil {
            return fmt.Errorf("failed to store manifest by tag: %w", err)
        }
        
        // 同时存储按摘要索引的清单
        digestKey := repository + ":" + digest
        if err := bucket.Put([]byte(digestKey), value); err != nil {
            return fmt.Errorf("failed to store manifest by digest: %w", err)
        }
        
        return nil
    })
}

// GetBlob 获取一个 blob，支持分块读取大文件
func (s *BoltBackend) GetBlob(repository, digest string) ([]byte, error) {
    log.Printf("Getting blob %s for repository %s", digest, repository)
    
    var data []byte
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(blobsBucket))
        key := repository + ":" + digest
        
        value := bucket.Get([]byte(key))
        if value == nil {
            return fmt.Errorf("blob not found: %s", digest)
        }
        
        // 检查是否是元数据格式
        var metaData map[string]interface{}
        if err := json.Unmarshal(value, &metaData); err == nil {
            // 成功解析为JSON，检查是否包含必要的元数据字段
            if _, hasSize := metaData["size"]; hasSize {
                if _, hasChunks := metaData["chunks"]; hasChunks {
                    // 这是一个大文件的元数据，需要读取所有块
                    chunks, ok := metaData["chunks"].(float64)
                    if !ok {
                        return fmt.Errorf("invalid chunks format in metadata")
                    }
                    
                    size, ok := metaData["size"].(float64)
                    if !ok {
                        return fmt.Errorf("invalid size format in metadata")
                    }
                    
                    log.Printf("Reading chunked blob: %d chunks, total size: %.0f bytes", int(chunks), size)
                    
                    // 预分配足够的空间
                    data = make([]byte, 0, int(size))
                    
                    // 读取所有块
                    for i := 0; i < int(chunks); i++ {
                        chunkKey := fmt.Sprintf("%s:%s:chunk:%d", repository, digest, i)
                        chunkData := bucket.Get([]byte(chunkKey))
                        if chunkData == nil {
                            return fmt.Errorf("chunk %d not found", i)
                        }
                        
                        data = append(data, chunkData...)
                    }
                    
                    return nil
                }
            }
        }
        
        // 不是元数据或者元数据格式不正确，假设这是一个直接存储的小文件
        log.Printf("Reading direct blob, size: %d bytes", len(value))
        data = make([]byte, len(value))
        copy(data, value)
        return nil
    })
    
    if err != nil {
        log.Printf("Error in GetBlob: %v", err)
        return nil, err
    }
    
    return data, nil
}

// PutBlob 存储一个 blob，使用分块存储处理大文件
func (s *BoltBackend) PutBlob(repository, digest string, data []byte) error {
    log.Printf("Storing blob %s for repository %s, size: %d bytes", digest, repository, len(data))
    
    // 对于大文件，我们需要分块存储
    const maxChunkSize = 10 * 1024 * 1024 // 10MB 块大小
    
    // 如果文件小于最大块大小，直接存储
    if len(data) <= maxChunkSize {
        log.Printf("Storing as direct blob (small file)")
        return s.db.Update(func(tx *bbolt.Tx) error {
            bucket := tx.Bucket([]byte(blobsBucket))
            key := repository + ":" + digest
            return bucket.Put([]byte(key), data)
        })
    }
    
    // 对于大文件，我们需要分块存储
    chunks := (len(data) + maxChunkSize - 1) / maxChunkSize
    log.Printf("Storing as chunked blob: %d chunks", chunks)
    
    // 首先存储元数据
    err := s.db.Update(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(blobsBucket))
        key := repository + ":" + digest
        
        // 存储元数据，指示这是一个大文件
        metaData := map[string]interface{}{
            "size": len(data),
            "chunks": chunks,
            "is_chunked": true, // 明确标记这是分块存储
        }
        
        metaBytes, err := json.Marshal(metaData)
        if err != nil {
            return fmt.Errorf("failed to marshal metadata: %w", err)
        }
        
        return bucket.Put([]byte(key), metaBytes)
    })
    
    if err != nil {
        log.Printf("Error storing metadata: %v", err)
        return err
    }
    
    // 分块存储实际数据
    for i := 0; i < chunks; i++ {
        start := i * maxChunkSize
        end := start + maxChunkSize
        if end > len(data) {
            end = len(data)
        }
        
        chunkData := data[start:end]
        chunkKey := fmt.Sprintf("%s:%s:chunk:%d", repository, digest, i)
        
        log.Printf("Storing chunk %d/%d, size: %d bytes", i+1, chunks, len(chunkData))
        
        err := s.db.Update(func(tx *bbolt.Tx) error {
            bucket := tx.Bucket([]byte(blobsBucket))
            return bucket.Put([]byte(chunkKey), chunkData)
        })
        
        if err != nil {
            log.Printf("Error storing chunk %d: %v", i, err)
            return fmt.Errorf("failed to store chunk %d: %w", i, err)
        }
    }
    
    log.Printf("Successfully stored chunked blob %s", digest)
    return nil
}

// DeleteManifest 删除镜像清单
func (s *BoltBackend) DeleteManifest(repository, reference string) error {
    log.Printf("Storage: Deleting manifest for %s:%s", repository, reference)
    
    var manifestData []byte
    var manifestDigest string
    
    // 首先获取清单数据，以便找到其摘要
    err := s.db.View(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(manifestsBucket))
        key := repository + ":" + reference
        
        value := bucket.Get([]byte(key))
        if value == nil {
            return fmt.Errorf("manifest not found: %s:%s", repository, reference)
        }
        
        var storedManifest struct {
            ContentType string `json:"content_type"`
            Data        []byte `json:"data"`
        }
        
        if err := json.Unmarshal(value, &storedManifest); err != nil {
            return fmt.Errorf("failed to unmarshal manifest data: %w", err)
        }
        
        manifestData = storedManifest.Data
        manifestDigest = "sha256:" + utils.ComputeSHA256(manifestData)
        
        return nil
    })
    
    if err != nil {
        return err
    }
    
    // 然后删除清单
    return s.db.Update(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(manifestsBucket))
        
        // 删除按标签索引的清单
        tagKey := repository + ":" + reference
        if err := bucket.Delete([]byte(tagKey)); err != nil {
            return fmt.Errorf("failed to delete manifest by tag: %w", err)
        }
        
        // 删除按摘要索引的清单
        digestKey := repository + ":" + manifestDigest
        if err := bucket.Delete([]byte(digestKey)); err != nil {
            return fmt.Errorf("failed to delete manifest by digest: %w", err)
        }
        
        return nil
    })
}

// DeleteBlob 删除Blob数据
func (s *BoltBackend) DeleteBlob(repository, digest string) error {
    log.Printf("Storage: Deleting blob %s for repository %s", digest, repository)
    
    // 首先检查是否有清单引用此blob
    refs, err := s.GetManifestReferences(digest)
    if err != nil {
        return fmt.Errorf("failed to check manifest references: %w", err)
    }
    
    if len(refs) > 0 {
        return fmt.Errorf("blob is still referenced by %d manifests", len(refs))
    }
    
    // 检查blob是否存在，以及是否是分块存储
    var isChunked bool
    var chunks int
    
    err = s.db.View(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(blobsBucket))
        key := repository + ":" + digest
        
        value := bucket.Get([]byte(key))
        if value == nil {
            return fmt.Errorf("blob not found: %s", digest)
        }
        
        // 检查是否是元数据格式
        var metaData map[string]interface{}
        if err := json.Unmarshal(value, &metaData); err == nil {
            if _, hasChunks := metaData["chunks"]; hasChunks {
                isChunked = true
                chunks = int(metaData["chunks"].(float64))
            }
        }
        
        return nil
    })
    
    if err != nil {
        return err
    }
    
    // 删除blob数据
    return s.db.Update(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(blobsBucket))
        key := repository + ":" + digest
        
        // 删除主键
        if err := bucket.Delete([]byte(key)); err != nil {
            return fmt.Errorf("failed to delete blob: %w", err)
        }
        
        // 如果是分块存储，删除所有块
        if isChunked {
            for i := 0; i < chunks; i++ {
                chunkKey := fmt.Sprintf("%s:%s:chunk:%d", repository, digest, i)
                if err := bucket.Delete([]byte(chunkKey)); err != nil {
                    return fmt.Errorf("failed to delete chunk %d: %w", i, err)
                }
            }
        }
        
        return nil
    })
}

// GetManifestReferences 获取引用某个blob的所有manifest
func (s *BoltBackend) GetManifestReferences(blobDigest string) ([]string, error) {
    var references []string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        manifestBucket := tx.Bucket([]byte(manifestsBucket))
        
        // 遍历所有清单
        return manifestBucket.ForEach(func(k, v []byte) error {
            var storedManifest struct {
                ContentType string `json:"content_type"`
                Data        []byte `json:"data"`
            }
            
            if err := json.Unmarshal(v, &storedManifest); err != nil {
                return nil // 跳过无法解析的条目
            }
            
            // 解析清单内容，检查是否引用了指定的blob
            var manifest map[string]interface{}
            if err := json.Unmarshal(storedManifest.Data, &manifest); err != nil {
                return nil // 跳过无法解析的清单
            }
            
            // 检查清单类型
            schemaVersion, ok := manifest["schemaVersion"].(float64)
            if !ok {
                return nil
            }
            
            // 根据不同的清单版本检查blob引用
            if schemaVersion == 2 {
                // 检查config blob
                if config, ok := manifest["config"].(map[string]interface{}); ok {
                    if digest, ok := config["digest"].(string); ok && digest == blobDigest {
                        references = append(references, string(k))
                        return nil
                    }
                }
                
                // 检查layers
                if layers, ok := manifest["layers"].([]interface{}); ok {
                    for _, layer := range layers {
                        if layerObj, ok := layer.(map[string]interface{}); ok {
                            if digest, ok := layerObj["digest"].(string); ok && digest == blobDigest {
                                references = append(references, string(k))
                                return nil
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
                                references = append(references, string(k))
                                return nil
                            }
                        }
                    }
                }
            }
            
            return nil
        })
    })
    
    if err != nil {
        return nil, err
    }
    
    return references, nil
}

// ListBlobs 列出所有blob
func (s *BoltBackend) ListBlobs() ([]string, error) {
    var blobs []string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        bucket := tx.Bucket([]byte(blobsBucket))
        if bucket == nil {
            return fmt.Errorf("blobs bucket not found")
        }
        
        c := bucket.Cursor()
        for k, _ := c.First(); k != nil; k, _ = c.Next() {
            blobs = append(blobs, string(k))
        }
        
        return nil
    })
    
    return blobs, err
}
    */