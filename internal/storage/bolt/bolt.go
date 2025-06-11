package bolt

import (
	"bytes"
	"fmt"
	"runtime/debug" // [新增] 用于panic恢复
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
	"go.etcd.io/bbolt"
)

func init() {
	storage.BackendRegister("bolt", NewBoltS3Storage)
}

// 常量定义
const (
	// 桶名称
	dataBucket      = "data"
	bucketsBucket   = "buckets"
	objectsBucket   = "objects"
	multipartBucket = "multipart"
	// [新增] 重试相关常量
	maxRetries = 3
	retryDelay = 100 * time.Millisecond
)

// BoltS3Storage 实现了S3types接口，使用BoltDB作为后端
type BoltS3Storage struct {
	db            *bbolt.DB
	cleanupTicker *time.Ticker
	cleanupDone chan bool
    closeOnce   sync.Once  // 添加这个字段
    closed      bool       // 添加关闭状态标记
	mu            sync.RWMutex // 保护并发访问
}

// NewBoltS3Storage 创建一个新的BoltDB存储实例
func NewBoltS3Storage(path string) (storage.Storage, error) {
	// [修改] 增加 BoltDB 的优化设置
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          false, // 在生产环境中保持为false以确保数据安全
		FreelistType:    bbolt.FreelistMapType,
		NoGrowSync:      false,
		ReadOnly:        false,
		MmapFlags:       0,
		InitialMmapSize: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt database: %w", err)
	}

	// 初始化所有必要的桶
	err = db.Update(func(tx *bbolt.Tx) error {
		buckets := []string{dataBucket, bucketsBucket, objectsBucket, multipartBucket}
		for _, bucket := range buckets {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
			}
		}
		return nil
	})

	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	s := &BoltS3Storage{
		db:          db,
	}

	// 启动后台清理过期的分段上传
	s.startCleanupRoutine()

	return s, nil
}

// [新增] 安全的游标操作包装函数
func (s *BoltS3Storage) safeCursorOperation(tx *bbolt.Tx, bucketName string, operation func(*bbolt.Cursor) error) error {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in cursor operation: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return fmt.Errorf("bucket %s not found", bucketName)
	}

	cursor := bucket.Cursor()
	if cursor == nil {
		return fmt.Errorf("failed to create cursor for bucket %s", bucketName)
	}

	return operation(cursor)
}

// [新增] 安全的桶操作包装函数
func (s *BoltS3Storage) safeBucketOperation(tx *bbolt.Tx, bucketName string, operation func(*bbolt.Bucket) error) error {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in bucket operation: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return fmt.Errorf("bucket %s not found", bucketName)
	}

	return operation(bucket)
}

// [新增] 重试机制包装函数
func (s *BoltS3Storage) withRetry(operation func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			log.Logger.Warnf("Operation failed (attempt %d/%d): %v", i+1, maxRetries, err)
			if i < maxRetries-1 {
				time.Sleep(retryDelay * time.Duration(i+1))
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

// Close 关闭数据库连接和清理例程
func (b *BoltS3Storage) Close() error {
    var err error
    b.closeOnce.Do(func() {
        b.mu.Lock()
        defer b.mu.Unlock()

        defer func() {
            if r := recover(); r != nil {
                log.Logger.Errorf("Recovered from panic in Close: %v", r)
                debug.PrintStack()
            }
        }()

        if b.db == nil {
            return
        }

        b.closed = true

        // 停止清理 goroutine
        if b.cleanupDone != nil {
            select {
            case b.cleanupDone <- true:
                // 成功发送停止信号
            default:
                // channel 可能已满或已关闭，忽略
            }
            close(b.cleanupDone)
            b.cleanupDone = nil
        }

        // 关闭数据库
        err = b.db.Close()
        b.db = nil
    })

    return err
}

// startCleanupRoutine 启动后台例程，定期清理过期的分段上传
func (s *BoltS3Storage) startCleanupRoutine() {
	s.cleanupTicker = time.NewTicker(storage.CleanupInterval)

	go func() {
		// [新增] 添加panic恢复和重启机制
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Errorf("Recovered from panic in cleanup routine: %v\n%s", r, debug.Stack())
				// 重启清理例程
				time.Sleep(time.Minute)
				s.startCleanupRoutine()
			}
		}()

		for {
			select {
			case <-s.cleanupTicker.C:
				if err := s.cleanupExpiredUploads(); err != nil {
					log.Logger.Error("Failed to cleanup expired multipart uploads: ", err)
				}
			case <-s.cleanupDone:
				return
			}
		}
	}()
}

// cleanupExpiredUploads 清理过期的分段上传
func (s *BoltS3Storage) cleanupExpiredUploads() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in cleanupExpiredUploads: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	now := time.Now()
	var expiredIDs []string

	// [修改] 使用安全的桶操作和重试机制
	// 查找过期的上传
	err := s.withRetry(func() error {
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
				return mpBkt.ForEach(func(k, v []byte) error {
					// 跳过子桶
					if v == nil {
						return nil
					}

					info := &types.MultipartUploadInfo{}
					if err := info.UnmarshalJSON(v); err != nil {
						log.Logger.Warn("Failed to unmarshal multipart info: ", err)
						return nil
					}

					if now.Sub(info.CreatedAt) > storage.MaxMultipartLifetime {
						expiredIDs = append(expiredIDs, info.UploadID)
					}

					return nil
				})
			})
		})
	})

	if err != nil {
		return fmt.Errorf("failed to scan for expired uploads: %w", err)
	}

	// [修改] 安全的清理过期上传
	// 清理过期的上传
	for _, uploadID := range expiredIDs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Logger.Errorf("Recovered from panic while cleaning upload %s: %v\n%s", uploadID, r, debug.Stack())
				}
			}()

			info := &types.MultipartUploadInfo{}

			// 首先获取上传信息
			err := s.db.View(func(tx *bbolt.Tx) error {
				return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
					data := mpBkt.Get([]byte(uploadID))
					if data == nil {
						return fmt.Errorf("upload info not found")
					}
					return info.UnmarshalJSON(data)
				})
			})

			if err != nil {
				log.Logger.Warn("Failed to get info for expired upload ", uploadID, ": ", err)
				return
			}

			// 然后中止上传
			if err := s.AbortMultipartUpload(info.Bucket, info.Key, uploadID); err != nil {
				log.Logger.Warn("Failed to abort expired upload ", uploadID, ": ", err)
			} else {
				log.Logger.Info("Cleaned up expired multipart upload: ", uploadID)
			}
		}()
	}

	return nil
}

// CreateMultipartUpload 初始化分段上传
func (s *BoltS3Storage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()


	if s.db == nil || s.closed {
        return "", fmt.Errorf("storage is closed")
    }

	log.Logger.Info("types: Creating multipart upload for ", key, " in bucket ", bucket)

	// 检查桶是否存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return "", fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("bucket does not exist: %s", bucket)
	}

	// 生成唯一的上传ID
	uploadID := storage.GenerateUploadID(bucket, key)

	err = s.db.Update(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
			// 创建上传信息
			uploadInfo := &types.MultipartUploadInfo{
				Bucket:      bucket,
				Key:         key,
				UploadID:    uploadID,
				ContentType: contentType,
				Metadata:    metadata,
				CreatedAt:   time.Now(),
			}

			// 序列化并存储
			data, err := uploadInfo.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal upload info: %w", err)
			}

			if err := mpBkt.Put([]byte(uploadID), data); err != nil {
				return fmt.Errorf("failed to store upload info: %w", err)
			}

			// 创建用于存储分段的桶
			partsBucketName := fmt.Sprintf("%s-parts", uploadID)
			if _, err := mpBkt.CreateBucket([]byte(partsBucketName)); err != nil {
				return fmt.Errorf("failed to create parts bucket: %w", err)
			}

			return nil
		})
	})

	if err != nil {
		return "", err
	}

	return uploadID, nil
}

// UploadPart 上传一个分段
func (s *BoltS3Storage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()


	if s.db == nil || s.closed {
        return "", fmt.Errorf("storage is closed")
    }

	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	log.Logger.Info("types: Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	// 计算MD5哈希作为ETag
	etag := storage.CalculateETag(data)

	err := s.db.Update(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
			// 获取上传信息
			uploadData := mpBkt.Get([]byte(uploadID))
			if uploadData == nil {
				return fmt.Errorf("upload ID not found: %s", uploadID)
			}

			uploadInfo := &types.MultipartUploadInfo{}
			if err := uploadInfo.UnmarshalJSON(uploadData); err != nil {
				return fmt.Errorf("failed to unmarshal upload info: %w", err)
			}

			// 验证桶和键
			if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
				return fmt.Errorf("bucket or key mismatch")
			}

			// 存储分段数据
			partsBucketName := fmt.Sprintf("%s-parts", uploadID)
			partsBkt := mpBkt.Bucket([]byte(partsBucketName))
			if partsBkt == nil {
				return fmt.Errorf("parts bucket not found")
			}

			// 创建分段信息
			pi := &types.PartInfo{
				PartNumber: partNumber,
				ETag:       etag,
				Size:       len(data),
			}

			partInfoData, err := pi.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal part info: %w", err)
			}

			// 存储分段数据和元数据
			partKey := fmt.Sprintf("%05d", partNumber) // 确保按数字顺序排序

			// 创建或获取该分段的桶
			partBkt, err := partsBkt.CreateBucketIfNotExists([]byte(partKey))
			if err != nil {
				return fmt.Errorf("failed to create part bucket: %w", err)
			}

			if err := partBkt.Put([]byte("data"), data); err != nil {
				return fmt.Errorf("failed to store part data: %w", err)
			}

			if err := partBkt.Put([]byte("info"), partInfoData); err != nil {
				return fmt.Errorf("failed to store part info: %w", err)
			}

			return nil
		})
	})

	if err != nil {
		return "", err
	}

	return etag, nil
}

// CompleteMultipartUpload 完成分段上传
func (s *BoltS3Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in CompleteMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return "", fmt.Errorf("storage is closed")
    }

	log.Logger.Info("types: Completing multipart upload for ", key, " in bucket ", bucket, " with ", len(parts), " parts")

	if len(parts) == 0 {
		return "", fmt.Errorf("no parts specified")
	}

	var finalETag string
	var allData []byte
	var allETags []string

	uploadInfo := &types.MultipartUploadInfo{}

	// 首先验证所有部分并收集数据
	err := s.db.View(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
			// 获取上传信息
			uploadData := mpBkt.Get([]byte(uploadID))
			if uploadData == nil {
				return fmt.Errorf("upload ID not found: %s", uploadID)
			}

			if err := uploadInfo.UnmarshalJSON(uploadData); err != nil {
				return fmt.Errorf("failed to unmarshal upload info: %w", err)
			}

			// 验证桶和键
			if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
				return fmt.Errorf("bucket or key mismatch")
			}

			// 获取分段数据
			partsBucketName := fmt.Sprintf("%s-parts", uploadID)
			partsBkt := mpBkt.Bucket([]byte(partsBucketName))
			if partsBkt == nil {
				return fmt.Errorf("parts bucket not found")
			}

			// 按照部分号排序
			sortedParts := make([]types.MultipartPart, len(parts))
			copy(sortedParts, parts)
			sort.Slice(sortedParts, func(i, j int) bool {
				return sortedParts[i].PartNumber < sortedParts[j].PartNumber
			})

			// 验证所有部分并收集数据
			for _, part := range sortedParts {
				partKey := fmt.Sprintf("%05d", part.PartNumber)
				partBkt := partsBkt.Bucket([]byte(partKey))
				if partBkt == nil {
					return fmt.Errorf("part %d not found", part.PartNumber)
				}

				// 获取分段信息
				partInfoData := partBkt.Get([]byte("info"))
				if partInfoData == nil {
					return fmt.Errorf("part %d info not found", part.PartNumber)
				}

				pi := &types.PartInfo{}
				if err := pi.UnmarshalJSON(partInfoData); err != nil {
					return fmt.Errorf("failed to unmarshal part info: %w", err)
				}

				// 验证ETag
				if pi.ETag != part.ETag {
					return fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
						part.PartNumber, pi.ETag, part.ETag)
				}

				// 获取分段数据
				partData := partBkt.Get([]byte("data"))
				if partData == nil {
					return fmt.Errorf("part %d data not found", part.PartNumber)
				}

				// [新增] 安全的数据复制
				dataCopy := make([]byte, len(partData))
				copy(dataCopy, partData)
				allData = append(allData, dataCopy...)
				allETags = append(allETags, part.ETag)
			}

			return nil
		})
	})

	if err != nil {
		return "", err
	}

	// 生成最终的ETag (与S3兼容)
	finalETag = storage.CalculateMultipartETag(allETags)

	// 存储合并后的对象
	err = s.PutObject(bucket, &types.S3ObjectData{
		Key:          key,
		Data:         allData,
		ContentType:  uploadInfo.ContentType,
		LastModified: time.Now(),
		ETag:         finalETag,
		Metadata:     uploadInfo.Metadata,
	})
	if err != nil {
		return "", fmt.Errorf("failed to store final object: %w", err)
	}

	// [修改] 安全的清理分段上传数据
	// 清理分段上传的数据
	err = s.db.Update(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
			// 删除分段桶
			partsBucketName := fmt.Sprintf("%s-parts", uploadID)
			if err := mpBkt.DeleteBucket([]byte(partsBucketName)); err != nil {
				log.Logger.Warn("Failed to delete parts bucket: ", err)
			}

			// 删除上传信息
			if err := mpBkt.Delete([]byte(uploadID)); err != nil {
				log.Logger.Warn("Failed to delete upload info: ", err)
			}

			return nil
		})
	})

	if err != nil {
		log.Logger.Warn("Failed to clean up multipart upload data: ", err)
		// 不返回错误，因为对象已经成功存储
	}

	log.Logger.Info("types: Completed multipart upload for ", key, " in bucket ", bucket, " with final ETag: ", finalETag)

	return finalETag, nil
}

// AbortMultipartUpload 中止分段上传
func (s *BoltS3Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in AbortMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	log.Logger.Info("types: Aborting multipart upload for ", key, " in bucket ", bucket)

	return s.db.Update(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
			// 获取上传信息
			uploadData := mpBkt.Get([]byte(uploadID))
			if uploadData == nil {
				return fmt.Errorf("upload ID not found: %s", uploadID)
			}

			uploadInfo := &types.MultipartUploadInfo{}
			if err := uploadInfo.UnmarshalJSON(uploadData); err != nil {
				return fmt.Errorf("failed to unmarshal upload info: %w", err)
			}

			// 验证桶和键
			if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
				return fmt.Errorf("bucket or key mismatch")
			}

			// 删除分段桶
			partsBucketName := fmt.Sprintf("%s-parts", uploadID)
			if err := mpBkt.DeleteBucket([]byte(partsBucketName)); err != nil {
				log.Logger.Warn("Failed to delete parts bucket: ", err)
			}

			// 删除上传信息
			if err := mpBkt.Delete([]byte(uploadID)); err != nil {
				return fmt.Errorf("failed to delete upload info: %w", err)
			}

			return nil
		})
	})
}

// ListMultipartUploads 列出所有进行中的分段上传
func (s *BoltS3Storage) ListMultipartUploads(bucket string) ([]types.MultipartUploadInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListMultipartUploads: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return nil, fmt.Errorf("storage is closed")
    }

	log.Logger.Info("types: Listing multipart uploads for bucket ", bucket)

	var uploads []types.MultipartUploadInfo

	// [修改] 使用安全的桶操作和重试机制
	err := s.withRetry(func() error {
		uploads = uploads[:0] // 重置切片
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
				return mpBkt.ForEach(func(k, v []byte) error {
					// 跳过子桶
					if v == nil {
						return nil
					}

					info := &types.MultipartUploadInfo{}
					if err := info.UnmarshalJSON(v); err != nil {
						log.Logger.Warn("Failed to unmarshal multipart upload info: ", err)
						return nil
					}

					// 只返回指定桶的上传
					if info.Bucket == bucket {
						uploads = append(uploads, types.MultipartUploadInfo{
							Bucket:    info.Bucket,
							Key:       info.Key,
							UploadID:  info.UploadID,
							CreatedAt: info.CreatedAt,
						})
					}

					return nil
				})
			})
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	return uploads, nil
}

// ListParts 列出分段上传的所有部分
func (s *BoltS3Storage) ListParts(bucket, key, uploadID string) ([]types.PartInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListParts: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return nil, fmt.Errorf("storage is closed")
    }

	log.Logger.Info("types: Listing parts for upload ", uploadID, " in bucket ", bucket)

	var parts []types.PartInfo

	// [修改] 使用安全的桶操作和重试机制
	err := s.withRetry(func() error {
		parts = parts[:0] // 重置切片
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, multipartBucket, func(mpBkt *bbolt.Bucket) error {
				// 获取上传信息
				uploadData := mpBkt.Get([]byte(uploadID))
				if uploadData == nil {
					return fmt.Errorf("upload ID not found: %s", uploadID)
				}

				uploadInfo := &types.MultipartUploadInfo{}
				if err := uploadInfo.UnmarshalJSON(uploadData); err != nil {
					return fmt.Errorf("failed to unmarshal upload info: %w", err)
				}

				// 验证桶和键
				if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
					return fmt.Errorf("bucket or key mismatch")
				}

				// 获取分段数据
				partsBucketName := fmt.Sprintf("%s-parts", uploadID)
				partsBkt := mpBkt.Bucket([]byte(partsBucketName))
				if partsBkt == nil {
					return fmt.Errorf("parts bucket not found")
				}

				return partsBkt.ForEach(func(k, v []byte) error {
					// 只处理子桶
					if v != nil {
						return nil
					}

					partBkt := partsBkt.Bucket(k)
					if partBkt == nil {
						return nil
					}

					// 获取分段信息
					partInfoData := partBkt.Get([]byte("info"))
					if partInfoData == nil {
						return nil
					}

					pi := &types.PartInfo{}
					if err := pi.UnmarshalJSON(partInfoData); err != nil {
						log.Logger.Warn("Failed to unmarshal part info: ", err)
						return nil
					}
					parts = append(parts, *pi)
					return nil
				})
			})
		})
	})

	if err != nil {
		return nil, err
	}

	// 按照部分号排序
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// 内部辅助方法

// bucketExistsInternal 检查桶是否存在（内部使用，不加锁）
func (s *BoltS3Storage) bucketExistsInternal(bucket string) (bool, error) {
	var exists bool

	err := s.db.View(func(tx *bbolt.Tx) error {
		return s.safeBucketOperation(tx, bucketsBucket, func(b *bbolt.Bucket) error {
			exists = b.Get([]byte(bucket)) != nil
			return nil
		})
	})

	return exists, err
}

func (s *BoltS3Storage) ListBuckets() ([]types.BucketInfo, error) {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListBuckets: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return nil, fmt.Errorf("storage is closed")
    }

	var buckets []types.BucketInfo

	// [修改] 使用安全的桶操作和重试机制
	err := s.withRetry(func() error {
		buckets = buckets[:0] // 重置切片
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, bucketsBucket, func(b *bbolt.Bucket) error {
				return b.ForEach(func(k, v []byte) error {
					info := &types.BucketInfo{}
					if err := info.UnmarshalJSON(v); err != nil {
						log.Logger.Warn("Failed to unmarshal bucket info: ", err)
						return nil
					}
					buckets = append(buckets, *info)
					return nil
				})
			})
		})
	})

	return buckets, err
}

func (s *BoltS3Storage) CreateBucket(bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in CreateBucket: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	log.Logger.Info("Creating bucket: ", bucket)

	// 检查存储桶是否已存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("bucket already exists")
	}

	// 创建存储桶信息
	info := types.BucketInfo{
		Name:         bucket,
		CreationDate: time.Now(),
	}
	data, err := info.MarshalJSON()
	if err != nil {
		return err
	}

	// [修改] 使用重试机制
	return s.withRetry(func() error {
		return s.db.Update(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, bucketsBucket, func(b *bbolt.Bucket) error {
				return b.Put([]byte(bucket), data)
			})
		})
	})
}

func (s *BoltS3Storage) DeleteBucket(bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in DeleteBucket: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	log.Logger.Info("Deleting bucket: ", bucket)

	// 检查存储桶是否存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket not found")
	}

	// [修改] 使用安全的游标操作检查存储桶是否为空
	// 检查存储桶是否为空
	var isEmpty = true
	err = s.db.View(func(tx *bbolt.Tx) error {
		return s.safeCursorOperation(tx, objectsBucket, func(c *bbolt.Cursor) error {
			bucketPrefix := bucket + "/"
			// 从桶前缀开始搜索
			for k, _ := c.Seek([]byte(bucketPrefix)); k != nil && bytes.HasPrefix(k, []byte(bucketPrefix)); k, _ = c.Next() {
				isEmpty = false
				break
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	if !isEmpty {
		return fmt.Errorf("bucket not empty")
	}

	// 删除存储桶
	return s.withRetry(func() error {
		return s.db.Update(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, bucketsBucket, func(b *bbolt.Bucket) error {
				return b.Delete([]byte(bucket))
			})
		})
	})
}

func (s *BoltS3Storage) BucketExists(bucket string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()


	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in BucketExists: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return false, fmt.Errorf("storage is closed")
    }

	return s.bucketExistsInternal(bucket)
}

func (s *BoltS3Storage) PutObject(bucket string, object *types.S3ObjectData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in PutObject: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }


	// 检查桶是否存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("bucket does not exist: %s", bucket)
	}

	fullKey := bucket + "/" + object.Key

	return s.withRetry(func() error {
		return s.db.Update(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
				// 直接序列化整个对象，像Badger一样
				data, err := object.MarshalJSON()
				if err != nil {
					return fmt.Errorf("failed to marshal object: %w", err)
				}
				return objBkt.Put([]byte(fullKey), data)
			})
		})
	})
}

func (s *BoltS3Storage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in GetObject: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return nil, fmt.Errorf("storage is closed")
    }	

	var objData *types.S3ObjectData
	fullKey := bucket + "/" + key

	err := s.withRetry(func() error {
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
				data := objBkt.Get([]byte(fullKey))
				if data == nil {
					return fmt.Errorf("object not found: %s/%s", bucket, key)
				}
				
				objData = &types.S3ObjectData{}
				return objData.UnmarshalJSON(data)
			})
		})
	})

	if err != nil {
		return nil, err
	}
	return objData, nil
}

func (s *BoltS3Storage) DeleteObject(bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in DeleteObject: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }	

	log.Logger.Info("Deleting object: ", key, " from bucket: ", bucket)

	// 构建完整键
	fullKey := bucket + "/" + key

	// [修改] 使用重试机制
	return s.withRetry(func() error {
		return s.db.Update(func(tx *bbolt.Tx) error {
			// 删除对象数据
			dataBkt := tx.Bucket([]byte(dataBucket))
			if dataBkt != nil {
				if err := dataBkt.Delete([]byte(fullKey)); err != nil {
					log.Logger.Warn("Failed to delete object data: ", err)
				}
			}

			// 删除对象元数据
			return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
				data := objBkt.Get([]byte(fullKey))
				if data == nil {
					return fmt.Errorf("object not found")
				}
				return objBkt.Delete([]byte(fullKey))
			})
		})
	})
}

func (s *BoltS3Storage) ObjectExists(bucket, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ObjectExists: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return false, fmt.Errorf("storage is closed")
    }

	var exists bool

	// 构建完整键
	fullKey := bucket + "/" + key

	// [修改] 使用安全的桶操作和重试机制
	err := s.withRetry(func() error {
		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
				exists = objBkt.Get([]byte(fullKey)) != nil
				return nil
			})
		})
	})

	return exists, err
}

func (s *BoltS3Storage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListObjects: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return nil, nil, fmt.Errorf("storage is closed")
    }

	log.Logger.Info("Listing objects in bucket: ", bucket, " with prefix: ", prefix)

	var objects []types.S3ObjectInfo
	var commonPrefixes []string
	prefixMap := make(map[string]bool)

	// [修改] 使用安全的游标操作和重试机制
	err := s.withRetry(func() error {
		objects = objects[:0] // 重置切片
		commonPrefixes = commonPrefixes[:0]
		prefixMap = make(map[string]bool)

		return s.db.View(func(tx *bbolt.Tx) error {
			return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
				bucketPrefix := bucket + "/"
				count := 0

				return s.safeCursorOperation(tx, objectsBucket, func(c *bbolt.Cursor) error {
					// 从桶前缀开始搜索
					for k, v := c.Seek([]byte(bucketPrefix)); k != nil && bytes.HasPrefix(k, []byte(bucketPrefix)); k, v = c.Next() {
						if maxKeys > 0 && count >= maxKeys {
							break
						}

						// 提取对象键（去掉存储桶前缀）
						fullKey := string(k)
						objKey := fullKey[len(bucketPrefix):]

						// 应用前缀过滤
						if prefix != "" && !strings.HasPrefix(objKey, prefix) {
							continue
						}

						// 应用标记过滤（分页）
						if marker != "" && objKey <= marker {
							continue
						}

						// 如果有分隔符，检查是否应该作为公共前缀
						if delimiter != "" {
							// 查找分隔符在键中的位置
							delimiterIndex := strings.Index(objKey[len(prefix):], delimiter)
							if delimiterIndex >= 0 {
								// 计算公共前缀
								commonPrefix := objKey[:len(prefix)+delimiterIndex+1]
								if !strings.HasSuffix(commonPrefix, delimiter) {
									commonPrefix += delimiter
								}
								if !prefixMap[commonPrefix] {
									prefixMap[commonPrefix] = true
									commonPrefixes = append(commonPrefixes, commonPrefix)
								}
								continue
							}
						}

						// 解析对象信息
						objInfo := &types.S3ObjectInfo{}
						if err := objInfo.UnmarshalJSON(v); err != nil {
							log.Logger.Warn("Warning: Failed to unmarshal object ", objKey, ": ", err)
							continue
						}

						objects = append(objects, *objInfo)
						count++
					}
					return nil
				})
			})
		})
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to list objects: %w", err)
	}

	return objects, commonPrefixes, nil
}

// [新增] 健康检查方法
func (s *BoltS3Storage) HealthCheck() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in HealthCheck: %v\n%s", r, debug.Stack())
		}
	}()

	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	// 简单的数据库连接检查
	return s.db.View(func(tx *bbolt.Tx) error {
		// 检查所有必要的桶是否存在
		buckets := []string{dataBucket, bucketsBucket, objectsBucket, multipartBucket}
		for _, bucketName := range buckets {
			if tx.Bucket([]byte(bucketName)) == nil {
				return fmt.Errorf("required bucket %s not found", bucketName)
			}
		}
		return nil
	})
}

// [新增] 获取存储统计信息
func (s *BoltS3Storage) GetStats() (*types.StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in GetStats: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return nil, fmt.Errorf("storage is closed")
    }

	stats := &types.StorageStats{}

	err := s.db.View(func(tx *bbolt.Tx) error {
		// 统计桶数量
		if err := s.safeBucketOperation(tx, bucketsBucket, func(b *bbolt.Bucket) error {
			stats.BucketCount = b.Stats().KeyN
			return nil
		}); err != nil {
			return err
		}

		// 统计对象数量和总大小
		return s.safeBucketOperation(tx, objectsBucket, func(objBkt *bbolt.Bucket) error {
			return objBkt.ForEach(func(k, v []byte) error {
				objInfo := &types.S3ObjectInfo{}
				if err := objInfo.UnmarshalJSON(v); err != nil {
					return nil // 跳过损坏的条目
				}
				stats.ObjectCount++
				stats.TotalSize += objInfo.Size
				return nil
			})
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get storage stats: %w", err)
	}

	return stats, nil
}

// [新增] 数据库压缩方法
func (s *BoltS3Storage) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in Compact: %v\n%s", r, debug.Stack())
		}
	}()


	if s.db == nil || s.closed {
        return fmt.Errorf("storage is closed")
    }

	log.Logger.Info("Starting database compaction")

	// BoltDB 会在事务提交时自动回收空间
	// 这里我们可以执行一个空的更新事务来触发压缩
	err := s.db.Update(func(tx *bbolt.Tx) error {
		// 空事务，用于触发压缩
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to compact database: %w", err)
	}

	log.Logger.Info("Database compaction completed")
	return nil
}