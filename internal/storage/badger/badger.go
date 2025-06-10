package badger

import (
	"bytes"
	"fmt"
	"runtime/debug" // [新增] 用于panic恢复
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
)

func init() {
	storage.BackendRegister("badger", NewBadgerS3Storage)
}

// 常量定义
const (
	// 键前缀定义 - Badger是扁平键值存储，使用前缀区分不同类型的数据
	dataBucketPrefix      = "data/"
	bucketsBucketPrefix   = "buckets/"
	objectsBucketPrefix   = "objects/"
	multipartBucketPrefix = "multipart/"
	// [新增] 重试相关常量
	maxRetries = 3
	retryDelay = 100 * time.Millisecond
)

// BadgerS3Storage 实现了S3types接口，使用Badger作为后端
type BadgerS3Storage struct {
	db            *badger.DB
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	mu            sync.RWMutex // 保护并发访问
}

// NewBadgerS3Storage 创建一个新的Badger存储实例
func NewBadgerS3Storage(path string) (storage.Storage, error) {
	// 配置Badger选项
	opts := badger.DefaultOptions(path)
	opts.Logger = nil       // 禁用Badger内部日志
	opts.SyncWrites = false // 提高性能，但在崩溃时可能丢失最近的写入
	// [修改] 增加 Badger 的值大小限制和其他优化设置
	opts.ValueLogFileSize = 1 * types.GB // 1GB
	opts.ValueThreshold = 1 * types.MB   // 1MB
	opts.NumMemtables = 5
	opts.NumLevelZeroTables = 5
	opts.NumLevelZeroTablesStall = 15
	opts.NumCompactors = 4
	opts.BlockCacheSize = 256 * types.MB // 256MB

	// 打开Badger数据库
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	// [修改] 添加一致性检查，带错误处理
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Errorf("Recovered from panic in GC routine: %v\n%s", r, debug.Stack())
			}
		}()
		if err := db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
			log.Logger.Errorf("Error running value log GC: %v", err)
		}
	}()

	s := &BadgerS3Storage{
		db:          db,
		cleanupDone: make(chan struct{}),
	}

	// 启动后台清理过期的分段上传
	s.startCleanupRoutine()

	return s, nil
}

// [新增] 安全的迭代器操作包装函数
func (s *BadgerS3Storage) safeIteratorOperation(txn *badger.Txn, prefix []byte, operation func(*badger.Iterator) error) error {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in iterator operation: %v\n%s", r, debug.Stack())
		}
	}()

	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false // [新增] 禁用预取以避免内存问题

	it := txn.NewIterator(opts)
	defer func() {
		if it != nil {
			it.Close()
		}
	}()

	return operation(it)
}

// [新增] 安全的Item值访问函数
func (s *BadgerS3Storage) safeItemValue(item *badger.Item, operation func([]byte) error) error {
	if item == nil {
		return fmt.Errorf("item is nil")
	}

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in item value access: %v\n%s", r, debug.Stack())
		}
	}()

	return item.Value(func(val []byte) error {
		if len(val) == 0 {
			return fmt.Errorf("empty value for key %s", item.Key())
		}
		return operation(val)
	})
}

// [新增] 重试机制包装函数
func (s *BadgerS3Storage) withRetry(operation func() error) error {
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
func (s *BadgerS3Storage) Close() error {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in Close: %v\n%s", r, debug.Stack())
		}
	}()

	// 停止清理例程
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
		close(s.cleanupDone)
	}

	// 关闭数据库
	return s.db.Close()
}

// startCleanupRoutine 启动后台例程，定期清理过期的分段上传
func (s *BadgerS3Storage) startCleanupRoutine() {
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

				// [修改] 安全的垃圾回收
				go func() {
					defer func() {
						if r := recover(); r != nil {
							log.Logger.Errorf("Recovered from panic in GC: %v\n%s", r, debug.Stack())
						}
					}()
					// 运行Badger垃圾回收
					err := s.db.RunValueLogGC(0.5) // 尝试回收至少50%的空间
					if err != nil && err != badger.ErrNoRewrite {
						log.Logger.Warn("Badger GC error: ", err)
					}
				}()
			case <-s.cleanupDone:
				return
			}
		}
	}()
}

// cleanupExpiredUploads 清理过期的分段上传
func (s *BadgerS3Storage) cleanupExpiredUploads() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in cleanupExpiredUploads: %v\n%s", r, debug.Stack())
		}
	}()

	now := time.Now()
	var expiredIDs []string

	// [修改] 使用安全的迭代器操作和重试机制
	// 查找过期的上传
	err := s.withRetry(func() error {
		return s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(multipartBucketPrefix), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					if item == nil {
						continue
					}
					key := item.Key()

					// 跳过分段数据，只处理上传信息
					if bytes.Contains(key, []byte("/parts/")) {
						continue
					}

					err := s.safeItemValue(item, func(val []byte) error {
						info := &types.MultipartUploadInfo{}
						if err := info.UnmarshalJSON(val); err != nil {
							return err
						}

						if now.Sub(info.CreatedAt) > storage.MaxMultipartLifetime {
							expiredIDs = append(expiredIDs, info.UploadID)
						}
						return nil
					})

					if err != nil {
						log.Logger.Warn("Failed to read multipart info: ", err)
					}
				}
				return nil
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
			err := s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
				if err != nil {
					return err
				}

				return s.safeItemValue(item, func(val []byte) error {
					return info.UnmarshalJSON(val)
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
func (s *BadgerS3Storage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		return "", fmt.Errorf("failed to marshal upload info: %w", err)
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(multipartBucketPrefix+uploadID), data)
	})

	if err != nil {
		return "", fmt.Errorf("failed to store upload info: %w", err)
	}

	return uploadID, nil
}

// UploadPart 上传一个分段
func (s *BadgerS3Storage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	log.Logger.Info("types: Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	// 计算MD5哈希作为ETag
	etag := storage.CalculateETag(data)

	// 获取上传信息
	uploadInfo := &types.MultipartUploadInfo{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return "", err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 创建分段信息
	pi := &types.PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       len(data),
	}

	partInfoData, err := pi.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal part info: %w", err)
	}

	// 存储分段数据和元数据
	partKey := fmt.Sprintf("%s%s/parts/%05d", multipartBucketPrefix, uploadID, partNumber)
	partInfoKey := fmt.Sprintf("%s%s/parts/%05d/info", multipartBucketPrefix, uploadID, partNumber)

	err = s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(partKey), data); err != nil {
			return fmt.Errorf("failed to store part data: %w", err)
		}

		if err := txn.Set([]byte(partInfoKey), partInfoData); err != nil {
			return fmt.Errorf("failed to store part info: %w", err)
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return etag, nil
}

// CompleteMultipartUpload 完成分段上传
func (s *BadgerS3Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in CompleteMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	log.Logger.Info("types: Completing multipart upload for ", key, " in bucket ", bucket, " with ", len(parts), " parts")

	if len(parts) == 0 {
		return "", fmt.Errorf("no parts specified")
	}

	var finalETag string
	var allData []byte
	var allETags []string
	var uploadInfo types.MultipartUploadInfo

	// 首先获取上传信息
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return "", err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 按照部分号排序
	sortedParts := make([]types.MultipartPart, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// 验证所有部分并收集数据
	for _, part := range sortedParts {
		partKey := fmt.Sprintf("%s%s/parts/%05d", multipartBucketPrefix, uploadID, part.PartNumber)
		partInfoKey := fmt.Sprintf("%s%s/parts/%05d/info", multipartBucketPrefix, uploadID, part.PartNumber)

		partInfo := &types.PartInfo{}
		var partData []byte

		err := s.db.View(func(txn *badger.Txn) error {
			// 获取分段信息
			infoItem, err := txn.Get([]byte(partInfoKey))
			if err != nil {
				return fmt.Errorf("part %d info not found: %w", part.PartNumber, err)
			}

			err = infoItem.Value(func(val []byte) error {
				return partInfo.UnmarshalJSON(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal part info: %w", err)
			}

			// 验证ETag
			if partInfo.ETag != part.ETag {
				return fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
					part.PartNumber, partInfo.ETag, part.ETag)
			}

			// 获取分段数据
			dataItem, err := txn.Get([]byte(partKey))
			if err != nil {
				return fmt.Errorf("part %d data not found: %w", part.PartNumber, err)
			}

			return dataItem.Value(func(val []byte) error {
				partData = make([]byte, len(val))
				copy(partData, val)
				return nil
			})
		})

		if err != nil {
			return "", err
		}

		allData = append(allData, partData...)
		allETags = append(allETags, part.ETag)
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
	err = s.db.Update(func(txn *badger.Txn) error {
		// 删除上传信息
		if err := txn.Delete([]byte(multipartBucketPrefix + uploadID)); err != nil {
			log.Logger.Warn("Failed to delete upload info: ", err)
		}

		// 删除所有分段数据
		return s.safeIteratorOperation(txn, []byte(multipartBucketPrefix+uploadID+"/parts/"), func(it *badger.Iterator) error {
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				if err := txn.Delete(item.Key()); err != nil {
					log.Logger.Warn("Failed to delete part data: ", err)
				}
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
func (s *BadgerS3Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in AbortMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	log.Logger.Info("types: Aborting multipart upload for ", key, " in bucket ", bucket)

	// 获取上传信息
	uploadInfo := &types.MultipartUploadInfo{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return fmt.Errorf("bucket or key mismatch")
	}

	// [修改] 安全的删除上传信息和所有分段数据
	// 删除上传信息和所有分段数据
	return s.db.Update(func(txn *badger.Txn) error {
		// 删除上传信息
		if err := txn.Delete([]byte(multipartBucketPrefix + uploadID)); err != nil {
			return fmt.Errorf("failed to delete upload info: %w", err)
		}

		// 删除所有分段数据
		return s.safeIteratorOperation(txn, []byte(multipartBucketPrefix+uploadID+"/parts/"), func(it *badger.Iterator) error {
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				if err := txn.Delete(item.Key()); err != nil {
					log.Logger.Warn("Failed to delete part data: ", err)
				}
			}
			return nil
		})
	})
}

// ListMultipartUploads 列出所有进行中的分段上传
func (s *BadgerS3Storage) ListMultipartUploads(bucket string) ([]types.MultipartUploadInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListMultipartUploads: %v\n%s", r, debug.Stack())
		}
	}()

	log.Logger.Info("types: Listing multipart uploads for bucket ", bucket)

	var uploads []types.MultipartUploadInfo

	// [修改] 使用安全的迭代器操作和重试机制
	err := s.withRetry(func() error {
		uploads = uploads[:0] // 重置切片
		return s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(multipartBucketPrefix), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					if item == nil {
						continue
					}
					key := item.Key()

					// 跳过分段数据
					if bytes.Contains(key, []byte("/parts/")) {
						continue
					}

					err := s.safeItemValue(item, func(val []byte) error {
						info := &types.MultipartUploadInfo{}
						if err := info.UnmarshalJSON(val); err != nil {
							return err
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

					if err != nil {
						log.Logger.Warn("Failed to read multipart info: ", err)
					}
				}
				return nil
			})
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	return uploads, nil
}

// ListParts 列出分段上传的所有部分
func (s *BadgerS3Storage) ListParts(bucket, key, uploadID string) ([]types.PartInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListParts: %v\n%s", r, debug.Stack())
		}
	}()

	log.Logger.Info("types: Listing parts for upload ", uploadID, " in bucket ", bucket)

	var parts []types.PartInfo
	uploadInfo := &types.MultipartUploadInfo{}

	// 获取上传信息
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return s.safeItemValue(item, func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return nil, err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return nil, fmt.Errorf("bucket or key mismatch")
	}

	// [修改] 使用安全的迭代器操作
	// 列出所有分段
	err = s.withRetry(func() error {
		parts = parts[:0] // 重置切片
		return s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(multipartBucketPrefix+uploadID+"/parts/"), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					if item == nil {
						continue
					}
					key := item.Key()

					// 只处理分段信息
					if !bytes.HasSuffix(key, []byte("/info")) {
						continue
					}

					err := s.safeItemValue(item, func(val []byte) error {
						pi := &types.PartInfo{}
						if err := pi.UnmarshalJSON(val); err != nil {
							log.Logger.Warn("Failed to unmarshal part info: ", err)
							return nil
						}

						parts = append(parts, *pi)
						return nil
					})

					if err != nil {
						log.Logger.Warn("Failed to read part info: ", err)
					}
				}
				return nil
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
func (s *BadgerS3Storage) bucketExistsInternal(bucket string) (bool, error) {
	var exists bool

	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(bucketsBucketPrefix + bucket))
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})

	return exists, err
}

func (s *BadgerS3Storage) ListBuckets() ([]types.BucketInfo, error) {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListBuckets: %v\n%s", r, debug.Stack())
		}
	}()

	var buckets []types.BucketInfo

	// [修改] 使用安全的迭代器操作和重试机制
	err := s.withRetry(func() error {
		buckets = buckets[:0] // 重置切片
		return s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(bucketsBucketPrefix), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					if item == nil {
						continue
					}

					err := s.safeItemValue(item, func(val []byte) error {
						info := &types.BucketInfo{}
						if err := info.UnmarshalJSON(val); err != nil {
							return err
						}
						buckets = append(buckets, *info)
						return nil
					})

					if err != nil {
						log.Logger.Warn("Failed to read bucket info: ", err)
					}
				}
				return nil
			})
		})
	})

	return buckets, err
}

// CreateBucket 创建一个新的存储桶
func (s *BadgerS3Storage) CreateBucket(bucket string) error {
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

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(bucketsBucketPrefix+bucket), data)
	})
}

// DeleteBucket 删除一个存储桶
func (s *BadgerS3Storage) DeleteBucket(bucket string) error {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in DeleteBucket: %v\n%s", r, debug.Stack())
		}
	}()

	// 检查存储桶是否存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket not found")
	}

	// [修改] 使用安全的迭代器操作检查存储桶是否为空
	// 检查存储桶是否为空
	var isEmpty = true
	err = s.db.View(func(txn *badger.Txn) error {
		return s.safeIteratorOperation(txn, []byte(objectsBucketPrefix+bucket+":"), func(it *badger.Iterator) error {
			it.Rewind()
			if it.Valid() {
				isEmpty = false
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
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(bucketsBucketPrefix + bucket))
	})
}

// BucketExists 检查存储桶是否存在
func (s *BadgerS3Storage) BucketExists(bucket string) (bool, error) {
	return s.bucketExistsInternal(bucket)
}

// ListObjects 列出存储桶中的对象
func (s *BadgerS3Storage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListObjects: %v\n%s", r, debug.Stack())
		}
	}()

	var objects []types.S3ObjectInfo
	var commonPrefixes []string
	prefixMap := make(map[string]bool)

	// [修改] 使用安全的迭代器操作和重试机制
	err := s.withRetry(func() error {
		objects = objects[:0] // 重置切片
		commonPrefixes = commonPrefixes[:0]
		prefixMap = make(map[string]bool)

		return s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(objectsBucketPrefix+bucket+":"), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					if item == nil {
						continue
					}
					key := item.Key()

					// 提取对象键（去掉存储桶前缀）
					fullKey := string(key)
					objKey := fullKey[len(objectsBucketPrefix+bucket+":"):]

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

					// 解析对象数据
					objectData := &types.S3ObjectData{}
					err := s.safeItemValue(item, func(val []byte) error {
						return objectData.UnmarshalJSON(val)
					})

					if err != nil {
						log.Logger.Warn("Warning: Failed to unmarshal object ", objKey, ": ", err)
						continue
					}

					objects = append(objects, types.S3ObjectInfo{
						Key:          objKey,
						Size:         int64(len(objectData.Data)),
						LastModified: objectData.LastModified,
						ETag:         objectData.ETag,
					})

					if len(objects) >= maxKeys {
						return nil
					}
				}
				return nil
			})
		})
	})

	return objects, commonPrefixes, err
}

// GetObject 获取一个对象
func (s *BadgerS3Storage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in GetObject: %v\n%s", r, debug.Stack())
		}
	}()

	objectData := &types.S3ObjectData{}

	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + key

	log.Logger.Info("Getting object, fullKey: ", fullKey)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fullKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("object not found")
			}
			return err
		}

		return s.safeItemValue(item, func(val []byte) error {
			return objectData.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return nil, err
	}

	return objectData, nil
}

// PutObject 存储一个对象
func (s *BadgerS3Storage) PutObject(bucket string, object *types.S3ObjectData) error {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in PutObject: %v\n%s", r, debug.Stack())
		}
	}()

	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + object.Key

	// 序列化对象数据
	data, err := object.MarshalJSON()
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fullKey), data)
	})
}

// DeleteObject 删除一个对象
func (s *BadgerS3Storage) DeleteObject(bucket, key string) error {
	// [新增] 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in DeleteObject: %v\n%s", r, debug.Stack())
		}
	}()

	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + key

	log.Logger.Info("Deleting object, fullKey: ", fullKey)

	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(fullKey))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("object not found")
		}
		if err != nil {
			return err
		}

		return txn.Delete([]byte(fullKey))
	})
}

/*
import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
)

func init() {
	storage.BackendRegister("badger", NewBadgerS3Storage)
}

// 常量定义
const (
	// 键前缀定义 - Badger是扁平键值存储，使用前缀区分不同类型的数据
	dataBucketPrefix      = "data/"
	bucketsBucketPrefix   = "buckets/"
	objectsBucketPrefix   = "objects/"
	multipartBucketPrefix = "multipart/"
)

// BadgerS3Storage 实现了S3types接口，使用Badger作为后端
type BadgerS3Storage struct {
	db            *badger.DB
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	mu            sync.RWMutex // 保护并发访问
}

// NewBadgerS3Storage 创建一个新的Badger存储实例
func NewBadgerS3Storage(path string) (storage.Storage, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = false
	opts.ValueLogFileSize = 1*types.GB  // 1GB
	opts.ValueThreshold = 1*types.MB    // 1MB

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	// 运行一致性检查
	if err := db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		log.Logger.Errorf("Error running value log GC: %v", err)
	}

	s := &BadgerS3Storage{
		db:          db,
		cleanupDone: make(chan struct{}),
	}

	s.startCleanupRoutine()

	return s, nil
}

// Close 关闭数据库连接和清理例程
func (s *BadgerS3Storage) Close() error {
	// 停止清理例程
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
		close(s.cleanupDone)
	}

	// 关闭数据库
	return s.db.Close()
}

// startCleanupRoutine 启动后台例程，定期清理过期的分段上传
func (s *BadgerS3Storage) startCleanupRoutine() {
	s.cleanupTicker = time.NewTicker(storage.CleanupInterval)

	go func() {
		for {
			select {
			case <-s.cleanupTicker.C:
				if err := s.cleanupExpiredUploads(); err != nil {
					log.Logger.Error("Failed to cleanup expired multipart uploads: ", err)
				}

				// 运行Badger垃圾回收
				err := s.db.RunValueLogGC(0.5) // 尝试回收至少50%的空间
				if err != nil && err != badger.ErrNoRewrite {
					log.Logger.Warn("Badger GC error: ", err)
				}
			case <-s.cleanupDone:
				return
			}
		}
	}()
}

// cleanupExpiredUploads 清理过期的分段上传
func (s *BadgerS3Storage) cleanupExpiredUploads() error {
	s.mu.Lock()
	defer s.mu.Unlock()

		defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in cleanupExpiredUploads: %v\n%s", r, debug.Stack())
		}
	}()

	now := time.Now()
	var expiredIDs []string

	// 查找过期的上传
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(multipartBucketPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 跳过分段数据，只处理上传信息
			if bytes.Contains(key, []byte("/parts/")) {
				continue
			}

			err := item.Value(func(val []byte) error {
				if len(val) == 0 {
					return fmt.Errorf("empty value for key %s", item.Key())
				}
				info := &types.MultipartUploadInfo{}
				if err := info.UnmarshalJSON(val); err != nil {
					return err
				}

				if now.Sub(info.CreatedAt) > storage.MaxMultipartLifetime {
					expiredIDs = append(expiredIDs, info.UploadID)
				}
				return nil
			})

			if err != nil {
				log.Logger.Warn("Failed to read multipart info: ", err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scan for expired uploads: %w", err)
	}

	// 清理过期的上传
	for _, uploadID := range expiredIDs {
		info := &types.MultipartUploadInfo{}

		// 首先获取上传信息
		err := s.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
			if err != nil {
				return err
			}

			return item.Value(func(val []byte) error {
				return info.UnmarshalJSON(val)
			})
		})

		if err != nil {
			log.Logger.Warn("Failed to get info for expired upload ", uploadID, ": ", err)
			continue
		}

		// 然后中止上传
		if err := s.AbortMultipartUpload(info.Bucket, info.Key, uploadID); err != nil {
			log.Logger.Warn("Failed to abort expired upload ", uploadID, ": ", err)
		} else {
			log.Logger.Info("Cleaned up expired multipart upload: ", uploadID)
		}
	}

	return nil
}

// CreateMultipartUpload 初始化分段上传
func (s *BadgerS3Storage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		return "", fmt.Errorf("failed to marshal upload info: %w", err)
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(multipartBucketPrefix+uploadID), data)
	})

	if err != nil {
		return "", fmt.Errorf("failed to store upload info: %w", err)
	}

	return uploadID, nil
}

// UploadPart 上传一个分段
func (s *BadgerS3Storage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	log.Logger.Info("types: Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	// 计算MD5哈希作为ETag
	etag := storage.CalculateETag(data)

	// 获取上传信息
	uploadInfo := &types.MultipartUploadInfo{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return "", err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 创建分段信息
	pi := &types.PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       len(data),
	}

	partInfoData, err := pi.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal part info: %w", err)
	}

	// 存储分段数据和元数据
	partKey := fmt.Sprintf("%s%s/parts/%05d", multipartBucketPrefix, uploadID, partNumber)
	partInfoKey := fmt.Sprintf("%s%s/parts/%05d/info", multipartBucketPrefix, uploadID, partNumber)

	err = s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(partKey), data); err != nil {
			return fmt.Errorf("failed to store part data: %w", err)
		}

		if err := txn.Set([]byte(partInfoKey), partInfoData); err != nil {
			return fmt.Errorf("failed to store part info: %w", err)
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return etag, nil
}

// CompleteMultipartUpload 完成分段上传
func (s *BadgerS3Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Logger.Info("types: Completing multipart upload for ", key, " in bucket ", bucket, " with ", len(parts), " parts")

	if len(parts) == 0 {
		return "", fmt.Errorf("no parts specified")
	}

	var finalETag string
	var allData []byte
	var allETags []string
	var uploadInfo types.MultipartUploadInfo

	// 首先获取上传信息
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return "", err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 按照部分号排序
	sortedParts := make([]types.MultipartPart, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// 验证所有部分并收集数据
	for _, part := range sortedParts {
		partKey := fmt.Sprintf("%s%s/parts/%05d", multipartBucketPrefix, uploadID, part.PartNumber)
		partInfoKey := fmt.Sprintf("%s%s/parts/%05d/info", multipartBucketPrefix, uploadID, part.PartNumber)

		partInfo := &types.PartInfo{}
		var partData []byte

		err := s.db.View(func(txn *badger.Txn) error {
			// 获取分段信息
			infoItem, err := txn.Get([]byte(partInfoKey))
			if err != nil {
				return fmt.Errorf("part %d info not found: %w", part.PartNumber, err)
			}

			err = infoItem.Value(func(val []byte) error {
				return partInfo.UnmarshalJSON(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal part info: %w", err)
			}

			// 验证ETag
			if partInfo.ETag != part.ETag {
				return fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
					part.PartNumber, partInfo.ETag, part.ETag)
			}

			// 获取分段数据
			dataItem, err := txn.Get([]byte(partKey))
			if err != nil {
				return fmt.Errorf("part %d data not found: %w", part.PartNumber, err)
			}

			return dataItem.Value(func(val []byte) error {
				partData = make([]byte, len(val))
				copy(partData, val)
				return nil
			})
		})

		if err != nil {
			return "", err
		}

		allData = append(allData, partData...)
		allETags = append(allETags, part.ETag)
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

	// 清理分段上传的数据
	err = s.db.Update(func(txn *badger.Txn) error {
		// 删除上传信息
		if err := txn.Delete([]byte(multipartBucketPrefix + uploadID)); err != nil {
			log.Logger.Warn("Failed to delete upload info: ", err)
		}

		// 删除所有分段数据
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(multipartBucketPrefix + uploadID + "/parts/")

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				log.Logger.Warn("Failed to delete part data: ", err)
			}
		}

		return nil
	})

	if err != nil {
		log.Logger.Warn("Failed to clean up multipart upload data: ", err)
		// 不返回错误，因为对象已经成功存储
	}

	log.Logger.Info("types: Completed multipart upload for ", key, " in bucket ", bucket, " with final ETag: ", finalETag)

	return finalETag, nil
}

// AbortMultipartUpload 中止分段上传
func (s *BadgerS3Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Logger.Info("types: Aborting multipart upload for ", key, " in bucket ", bucket)

	// 获取上传信息
	uploadInfo := &types.MultipartUploadInfo{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return fmt.Errorf("bucket or key mismatch")
	}

	// 删除上传信息和所有分段数据
	return s.db.Update(func(txn *badger.Txn) error {
		// 删除上传信息
		if err := txn.Delete([]byte(multipartBucketPrefix + uploadID)); err != nil {
			return fmt.Errorf("failed to delete upload info: %w", err)
		}

		// 删除所有分段数据
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(multipartBucketPrefix + uploadID + "/parts/")

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				log.Logger.Warn("Failed to delete part data: ", err)
			}
		}

		return nil
	})
}

// ListMultipartUploads 列出所有进行中的分段上传
func (s *BadgerS3Storage) ListMultipartUploads(bucket string) ([]types.MultipartUploadInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

		defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListMultipartUploads: %v\n%s", r, debug.Stack())
		}
	}()

	log.Logger.Info("types: Listing multipart uploads for bucket ", bucket)

	var uploads []types.MultipartUploadInfo
		err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(multipartBucketPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			if bytes.Contains(key, []byte("/parts/")) {
				continue
			}

			err := item.Value(func(val []byte) error {
				if len(val) == 0 {
					return fmt.Errorf("empty value for key %s", item.Key())
				}
				info := &types.MultipartUploadInfo{}
				if err := info.UnmarshalJSON(val); err != nil {
					return err
				}

				if info.Bucket == bucket {
					uploads = append(uploads, *info)
				}
				return nil
			})

			if err != nil {
				log.Logger.Warn("Failed to read multipart info: ", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	return uploads, nil
}

// ListParts 列出分段上传的所有部分
func (s *BadgerS3Storage) ListParts(bucket, key, uploadID string) ([]types.PartInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Logger.Info("types: Listing parts for upload ", uploadID, " in bucket ", bucket)

	var parts []types.PartInfo
	uploadInfo := &types.MultipartUploadInfo{}

	// 获取上传信息
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return fmt.Errorf("upload ID not found: %s", uploadID)
		}

		return item.Value(func(val []byte) error {
			return uploadInfo.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return nil, err
	}

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		return nil, fmt.Errorf("bucket or key mismatch")
	}

	// 列出所有分段
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(multipartBucketPrefix + uploadID + "/parts/")

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 只处理分段信息
			if !bytes.HasSuffix(key, []byte("/info")) {
				continue
			}

			err := item.Value(func(val []byte) error {
				pi := &types.PartInfo{}
				if err := pi.UnmarshalJSON(val); err != nil {
					log.Logger.Warn("Failed to unmarshal part info: ", err)
					return nil
				}

				parts = append(parts, *pi)
				return nil
			})

			if err != nil {
				log.Logger.Warn("Failed to read part info: ", err)
			}
		}

		return nil
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
func (s *BadgerS3Storage) bucketExistsInternal(bucket string) (bool, error) {
	var exists bool

	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(bucketsBucketPrefix + bucket))
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})

	return exists, err
}

func (s *BadgerS3Storage) ListBuckets() ([]types.BucketInfo, error) {
	var buckets []types.BucketInfo

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(bucketsBucketPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				info := &types.BucketInfo{}
				if err := info.UnmarshalJSON(val); err != nil {
					return err
				}
				buckets = append(buckets, *info)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return buckets, err
}

// CreateBucket 创建一个新的存储桶
func (s *BadgerS3Storage) CreateBucket(bucket string) error {
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

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(bucketsBucketPrefix+bucket), data)
	})
}

// DeleteBucket 删除一个存储桶
func (s *BadgerS3Storage) DeleteBucket(bucket string) error {
	// 检查存储桶是否存在
	exists, err := s.bucketExistsInternal(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket not found")
	}

	// 检查存储桶是否为空
	var isEmpty = true
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(objectsBucketPrefix + bucket + ":")

		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			isEmpty = false
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !isEmpty {
		return fmt.Errorf("bucket not empty")
	}

	// 删除存储桶
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(bucketsBucketPrefix + bucket))
	})
}

// BucketExists 检查存储桶是否存在
func (s *BadgerS3Storage) BucketExists(bucket string) (bool, error) {
	return s.bucketExistsInternal(bucket)
}

// ListObjects 列出存储桶中的对象
func (s *BadgerS3Storage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
		s.mu.RLock()
	defer s.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in ListObjects: %v\n%s", r, debug.Stack())
		}
	}()

	var objects []types.S3ObjectInfo
	var commonPrefixes []string
	prefixMap := make(map[string]bool)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(objectsBucketPrefix + bucket + ":")

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 提取对象键（去掉存储桶前缀）
			fullKey := string(key)
			objKey := fullKey[len(objectsBucketPrefix+bucket+":"):]

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

			// 解析对象数据
			objectData := &types.S3ObjectData{}
			err := item.Value(func(val []byte) error {
				if len(val) == 0 {
					return fmt.Errorf("empty value for key %s", item.Key())
				}
				return objectData.UnmarshalJSON(val)
			})

			if err != nil {
				log.Logger.Warn("Warning: Failed to unmarshal object ", objKey, ": ", err)
				continue
			}

			objects = append(objects, types.S3ObjectInfo{
				Key:          objKey,
				Size:         int64(len(objectData.Data)),
				LastModified: objectData.LastModified,
				ETag:         objectData.ETag,
			})

			if len(objects) >= maxKeys {
				return nil
			}
		}

		return nil
	})

	return objects, commonPrefixes, err
}

// GetObject 获取一个对象
func (s *BadgerS3Storage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	objectData := &types.S3ObjectData{}

	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + key

	log.Logger.Info("Getting object, fullKey: ", fullKey)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fullKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("object not found")
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return objectData.UnmarshalJSON(val)
		})
	})

	if err != nil {
		return nil, err
	}

	return objectData, nil
}

// PutObject 存储一个对象
func (s *BadgerS3Storage) PutObject(bucket string, object *types.S3ObjectData) error {
	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + object.Key

	// 序列化对象数据
	data, err := object.MarshalJSON()
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fullKey), data)
	})
}

// DeleteObject 删除一个对象
func (s *BadgerS3Storage) DeleteObject(bucket, key string) error {
	// 构建完整键
	fullKey := objectsBucketPrefix + bucket + ":" + key

	log.Logger.Info("Deleting object, fullKey: ", fullKey)

	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(fullKey))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("object not found")
		}
		if err != nil {
			return err
		}

		return txn.Delete([]byte(fullKey))
	})
}
*/
