package bolt

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/elastic-io/haven/internal/storage"
	"go.etcd.io/bbolt"
)

func init() {
	storage.BackendRegister("bolt", NewBoltS3Storage)
}

const (
    // 保留原有的桶定义，用于内部实现
    dataBucket     = "data"
    metadataBucket = "metadata"
)

// BoltS3Storage 实现了S3Storage接口，使用BoltDB作为后端
type BoltS3Storage struct {
    db *bbolt.DB
}

// NewBoltS3Storage 创建一个新的BoltDB存储实例
func NewBoltS3Storage(path string) (storage.S3, error) {
    db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        return nil, err
    }
    
    // 初始化内部使用的桶
    err = db.Update(func(tx *bbolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte(dataBucket))
        if err != nil {
            return err
        }
        _, err = tx.CreateBucketIfNotExists([]byte(metadataBucket))
        return err
    })
    
    if err != nil {
        db.Close()
        return nil, err
    }
    
    return &BoltS3Storage{db: db}, nil
}

// Close 关闭数据库连接
func (s *BoltS3Storage) Close() error {
    return s.db.Close()
}

// CreateBucket 创建一个新的桶
func (s *BoltS3Storage) CreateBucket(bucket string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        dataBkt := tx.Bucket([]byte(dataBucket))
        _, err := dataBkt.CreateBucketIfNotExists([]byte(bucket))
        if err != nil {
            return err
        }
        
        metaBkt := tx.Bucket([]byte(metadataBucket))
        _, err = metaBkt.CreateBucketIfNotExists([]byte(bucket))
        return err
    })
}

// DeleteBucket 删除一个桶
func (s *BoltS3Storage) DeleteBucket(bucket string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        dataBkt := tx.Bucket([]byte(dataBucket))
        if err := dataBkt.DeleteBucket([]byte(bucket)); err != nil {
            return err
        }
        
        metaBkt := tx.Bucket([]byte(metadataBucket))
        return metaBkt.DeleteBucket([]byte(bucket))
    })
}

// ListBuckets 列出所有桶
func (s *BoltS3Storage) ListBuckets() ([]string, error) {
    var buckets []string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        dataBkt := tx.Bucket([]byte(dataBucket))
        if dataBkt == nil {
            return nil // 如果数据桶不存在，返回空列表
        }
        
        return dataBkt.ForEachBucket(func(name []byte) error {
            buckets = append(buckets, string(name))
            return nil
        })
    })
    
    return buckets, err
}

// BucketExists 检查桶是否存在
func (s *BoltS3Storage) BucketExists(bucket string) (bool, error) {
    var exists bool
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        dataBkt := tx.Bucket([]byte(dataBucket))
        b := dataBkt.Bucket([]byte(bucket))
        exists = (b != nil)
        return nil
    })
    
    return exists, err
}

// PutObject 存储对象
func (s *BoltS3Storage) PutObject(bucket, key string, data []byte, metadata map[string]string) error {
    log.Printf("Storage: Putting object %s in bucket %s, size: %d bytes", key, bucket, len(data))
    
    // 对于大文件，我们需要分块存储
    const maxChunkSize = 10 * 1024 * 1024 // 10MB 块大小
    
    return s.db.Update(func(tx *bbolt.Tx) error {
        // 获取数据桶
        dataBkt := tx.Bucket([]byte(dataBucket))
        if dataBkt == nil {
            return fmt.Errorf("data bucket not found")
        }
        
        bucketBkt := dataBkt.Bucket([]byte(bucket))
        if bucketBkt == nil {
            var err error
            bucketBkt, err = dataBkt.CreateBucket([]byte(bucket))
            if err != nil {
                return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
            }
        }
        
        // 如果文件小于最大块大小，直接存储
        if len(data) <= maxChunkSize {
            if err := bucketBkt.Put([]byte(key), data); err != nil {
                return err
            }
        } else {
            // 对于大文件，分块存储
            chunks := (len(data) + maxChunkSize - 1) / maxChunkSize
            log.Printf("Storing as chunked object: %d chunks", chunks)
            
            // 存储元数据，指示这是一个大文件
            chunkMeta := map[string]interface{}{
                "size":       len(data),
                "chunks":     chunks,
                "is_chunked": true,
            }
            
            metaBytes, err := json.Marshal(chunkMeta)
            if err != nil {
                return fmt.Errorf("failed to marshal chunk metadata: %w", err)
            }
            
            if err := bucketBkt.Put([]byte(key), metaBytes); err != nil {
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
                chunkKey := fmt.Sprintf("%s:chunk:%d", key, i)
                
                if err := bucketBkt.Put([]byte(chunkKey), chunkData); err != nil {
                    return fmt.Errorf("failed to store chunk %d: %w", i, err)
                }
            }
        }
        
        // 存储元数据
        if len(metadata) > 0 {
            metaBkt := tx.Bucket([]byte(metadataBucket))
            if metaBkt == nil {
                return fmt.Errorf("metadata bucket not found")
            }
            
            bucketMetaBkt := metaBkt.Bucket([]byte(bucket))
            if bucketMetaBkt == nil {
                var err error
                bucketMetaBkt, err = metaBkt.CreateBucket([]byte(bucket))
                if err != nil {
                    return fmt.Errorf("failed to create metadata bucket %s: %w", bucket, err)
                }
            }
            
            metaData, err := json.Marshal(metadata)
            if err != nil {
                return err
            }
            
            return bucketMetaBkt.Put([]byte(key), metaData)
        }
        
        return nil
    })
}

// GetObject 获取对象
func (s *BoltS3Storage) GetObject(bucket, key string) ([]byte, map[string]string, error) {
    log.Printf("Storage: Getting object %s from bucket %s", key, bucket)
    
    var data []byte
    var metadata map[string]string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        // 获取数据
        dataBkt := tx.Bucket([]byte(dataBucket))
        if dataBkt == nil {
            return fmt.Errorf("data bucket not found")
        }
        
        bucketBkt := dataBkt.Bucket([]byte(bucket))
        if bucketBkt == nil {
            return fmt.Errorf("bucket not found: %s", bucket)
        }
        
        value := bucketBkt.Get([]byte(key))
        if value == nil {
            return fmt.Errorf("key not found: %s", key)
        }
        
        // 检查是否是分块存储的大文件
        var chunkMeta map[string]interface{}
        if err := json.Unmarshal(value, &chunkMeta); err == nil {
            if isChunked, ok := chunkMeta["is_chunked"].(bool); ok && isChunked {
                chunks, ok := chunkMeta["chunks"].(float64)
                if !ok {
                    return fmt.Errorf("invalid chunks format in metadata")
                }
                
                size, ok := chunkMeta["size"].(float64)
                if !ok {
                    return fmt.Errorf("invalid size format in metadata")
                }
                
                log.Printf("Reading chunked object: %d chunks, total size: %.0f bytes", int(chunks), size)
                
                // 预分配足够的空间
                data = make([]byte, 0, int(size))
                
                // 读取所有块
                for i := 0; i < int(chunks); i++ {
                    chunkKey := fmt.Sprintf("%s:chunk:%d", key, i)
                    chunkData := bucketBkt.Get([]byte(chunkKey))
                    if chunkData == nil {
                        return fmt.Errorf("chunk %d not found", i)
                    }
                    
                    data = append(data, chunkData...)
                }
                
                // 继续获取元数据
            } else {
                // 不是分块存储，直接使用值
                data = make([]byte, len(value))
                copy(data, value)
            }
        } else {
            // 解析失败，假设这是普通数据
            data = make([]byte, len(value))
            copy(data, value)
        }
        
        // 获取元数据
        metaBkt := tx.Bucket([]byte(metadataBucket))
        if metaBkt != nil {
            bucketMetaBkt := metaBkt.Bucket([]byte(bucket))
            if bucketMetaBkt != nil {
                metaValue := bucketMetaBkt.Get([]byte(key))
                if metaValue != nil {
                    if err := json.Unmarshal(metaValue, &metadata); err != nil {
                        return err
                    }
                }
            }
        }
        
        return nil
    })
    
    return data, metadata, err
}

// DeleteObject 删除对象
func (s *BoltS3Storage) DeleteObject(bucket, key string) error {
    log.Printf("Storage: Deleting object %s from bucket %s", key, bucket)
    
    return s.db.Update(func(tx *bbolt.Tx) error {
        // 首先检查是否是分块存储的大文件
        dataBkt := tx.Bucket([]byte(dataBucket))
        if dataBkt == nil {
            return fmt.Errorf("data bucket not found")
        }
        
        bucketBkt := dataBkt.Bucket([]byte(bucket))
        if bucketBkt == nil {
            return fmt.Errorf("bucket not found: %s", bucket)
        }
        
        value := bucketBkt.Get([]byte(key))
        if value == nil {
            return fmt.Errorf("key not found: %s", key)
        }
        
        // 检查是否是分块存储的大文件
        var chunkMeta map[string]interface{}
        isChunked := false
        chunks := 0
        
        if err := json.Unmarshal(value, &chunkMeta); err == nil {
            if chunked, ok := chunkMeta["is_chunked"].(bool); ok && chunked {
                isChunked = true
                chunks = int(chunkMeta["chunks"].(float64))
            }
        }
        
        // 删除主键
        if err := bucketBkt.Delete([]byte(key)); err != nil {
            return err
        }
        
        // 如果是分块存储，删除所有块
        if isChunked {
            for i := 0; i < chunks; i++ {
                chunkKey := fmt.Sprintf("%s:chunk:%d", key, i)
                if err := bucketBkt.Delete([]byte(chunkKey)); err != nil {
                    return fmt.Errorf("failed to delete chunk %d: %w", i, err)
                }
            }
        }
        
        // 删除元数据
        metaBkt := tx.Bucket([]byte(metadataBucket))
        if metaBkt != nil {
            bucketMetaBkt := metaBkt.Bucket([]byte(bucket))
            if bucketMetaBkt != nil {
                return bucketMetaBkt.Delete([]byte(key))
            }
        }
        
        return nil
    })
}

// ListObjects 列出桶中的对象
func (s *BoltS3Storage) ListObjects(bucket, prefix string) ([]string, error) {
    log.Printf("Storage: Listing objects in bucket %s with prefix %s", bucket, prefix)
    
    var keys []string
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        dataBkt := tx.Bucket([]byte(dataBucket))
        if dataBkt == nil {
            return fmt.Errorf("data bucket not found")
        }
        
        bucketBkt := dataBkt.Bucket([]byte(bucket))
        if bucketBkt == nil {
            return fmt.Errorf("bucket not found: %s", bucket)
        }
        
        c := bucketBkt.Cursor()
        prefixBytes := []byte(prefix)
        
        for k, _ := c.Seek(prefixBytes); k != nil && hasPrefix(k, prefixBytes); k, _ = c.Next() {
            // 跳过分块存储的块
            if !isChunkKey(string(k)) {
                keys = append(keys, string(k))
            }
        }
        
        return nil
    })
    
    return keys, err
}

// hasPrefix 检查字节切片是否有指定前缀
func hasPrefix(s, prefix []byte) bool {
    if len(s) < len(prefix) {
        return false
    }
    for i := 0; i < len(prefix); i++ {
        if s[i] != prefix[i] {
            return false
        }
    }
    return true
}

// isChunkKey 检查键是否是分块存储的块
func isChunkKey(key string) bool {
    return strings.Contains(key, ":chunk:")
}

/*
import (
	"time"

	"github.com/elastic-io/haven/internal/storage"
	"go.etcd.io/bbolt"
)

const (
	manifestsBucket = "manifests"
	blobsBucket     = "blobs"
	metadataBucket  = "metadata"
)

func init() {
	backend := BoltBackend{name: "bolt"}
	storage.BackendRegister(backend.name, &backend)
}

type BoltBackend struct {
	name string
	db *bbolt.DB
}

func (s *BoltBackend) Init(path string) (storage.Storage, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range []string{manifestsBucket, blobsBucket, metadataBucket} {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &BoltBackend{db: db}, nil
}

func (s *BoltBackend) Close() error {
	return s.db.Close()
}
	*/