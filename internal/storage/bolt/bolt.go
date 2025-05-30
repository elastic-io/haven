package bolt

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"

	"fmt"
	"sort"
	"sync"

	"strings"
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

	// 分段上传相关常量
	maxMultipartLifetime = 7 * 24 * time.Hour // 7天，与S3一致
	cleanupInterval      = 1 * time.Hour      // 清理过期上传的间隔
)

// BoltS3Storage 实现了S3types接口，使用BoltDB作为后端
type BoltS3Storage struct {
	db            *bbolt.DB
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	mu            sync.RWMutex // 保护并发访问
}

// NewBoltS3Storage 创建一个新的BoltDB存储实例
func NewBoltS3Storage(path string) (storage.Storage, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout: 1 * time.Second,
		// 增加这些选项以提高性能
		NoSync:       false, // 在生产环境中保持为false以确保数据安全
		FreelistType: bbolt.FreelistMapType,
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
		cleanupDone: make(chan struct{}),
	}

	// 启动后台清理过期的分段上传
	s.startCleanupRoutine()

	return s, nil
}

// Close 关闭数据库连接和清理例程
func (s *BoltS3Storage) Close() error {
	// 停止清理例程
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
		close(s.cleanupDone)
	}

	// 关闭数据库
	return s.db.Close()
}

// startCleanupRoutine 启动后台例程，定期清理过期的分段上传
func (s *BoltS3Storage) startCleanupRoutine() {
	s.cleanupTicker = time.NewTicker(cleanupInterval)

	go func() {
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

	now := time.Now()
	var expiredIDs []string

	// 查找过期的上传
	err := s.db.View(func(tx *bbolt.Tx) error {
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return nil // 没有multipart桶，不需要清理
		}

		return mpBkt.ForEach(func(k, v []byte) error {
			// 跳过子桶
			if v == nil {
				return nil
			}

			info := &types.MultipartUploadInfo{}
			err := info.UnmarshalJSON(v)
			if err != nil {
				return err
			}
			if now.Sub(info.CreatedAt) > maxMultipartLifetime {
				expiredIDs = append(expiredIDs, info.UploadID)
			}

			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("failed to scan for expired uploads: %w", err)
	}

	// 清理过期的上传
	for _, uploadID := range expiredIDs {
		info := &types.MultipartUploadInfo{}

		// 首先获取上传信息
		err := s.db.View(func(tx *bbolt.Tx) error {
			mpBkt := tx.Bucket([]byte(multipartBucket))
			if mpBkt == nil {
				return fmt.Errorf("multipart bucket not found")
			}

			data := mpBkt.Get([]byte(uploadID))
			if data == nil {
				return fmt.Errorf("upload info not found")
			}

			return info.UnmarshalJSON(data)
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
func (s *BoltS3Storage) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
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
	uploadID := generateUploadID(bucket, key)

	err = s.db.Update(func(tx *bbolt.Tx) error {
		// 获取multipart桶
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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

	if err != nil {
		return "", err
	}

	return uploadID, nil
}

// UploadPart 上传一个分段
func (s *BoltS3Storage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	log.Logger.Info("types: Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	// 计算MD5哈希作为ETag
	etag := calculateETag(data)

	err := s.db.Update(func(tx *bbolt.Tx) error {
		// 获取multipart桶
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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

	if err != nil {
		return "", err
	}

	return etag, nil
}

// CompleteMultipartUpload 完成分段上传
func (s *BoltS3Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		// 获取multipart桶
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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

			var partInfo struct {
				PartNumber int    `json:"partNumber"`
				ETag       string `json:"eTag"`
				Size       int    `json:"size"`
			}

			pi := &types.PartInfo{}
			if err := pi.UnmarshalJSON(partInfoData); err != nil {
				return fmt.Errorf("failed to unmarshal part info: %w", err)
			}

			// 验证ETag
			if partInfo.ETag != part.ETag {
				return fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
					part.PartNumber, partInfo.ETag, part.ETag)
			}

			// 获取分段数据
			partData := partBkt.Get([]byte("data"))
			if partData == nil {
				return fmt.Errorf("part %d data not found", part.PartNumber)
			}

			allData = append(allData, partData...)
			allETags = append(allETags, part.ETag)
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	// 生成最终的ETag (与S3兼容)
	finalETag = calculateMultipartETag(allETags)

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
	err = s.db.Update(func(tx *bbolt.Tx) error {
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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

	log.Logger.Info("types: Aborting multipart upload for ", key, " in bucket ", bucket)

	return s.db.Update(func(tx *bbolt.Tx) error {
		// 获取multipart桶
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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
}

// ListMultipartUploads 列出所有进行中的分段上传
func (s *BoltS3Storage) ListMultipartUploads(bucket string) ([]types.MultipartUploadInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Logger.Info("types: Listing multipart uploads for bucket ", bucket)

	var uploads []types.MultipartUploadInfo

	err := s.db.View(func(tx *bbolt.Tx) error {
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return nil // 没有multipart桶，返回空列表
		}

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

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	return uploads, nil
}

// ListParts 列出分段上传的所有部分
func (s *BoltS3Storage) ListParts(bucket, key, uploadID string) ([]types.PartInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Logger.Info("types: Listing parts for upload ", uploadID, " in bucket ", bucket)

	var parts []types.PartInfo

	err := s.db.View(func(tx *bbolt.Tx) error {
		// 获取multipart桶
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

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
			err := pi.UnmarshalJSON(partInfoData)
			if err != nil {
				return err
			}
			parts = append(parts, *pi)
			return nil
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
		dataBkt := tx.Bucket([]byte(dataBucket))
		if dataBkt == nil {
			return nil
		}

		b := dataBkt.Bucket([]byte(bucket))
		exists = (b != nil)
		return nil
	})

	return exists, err
}

func (s *BoltS3Storage) ListBuckets() ([]types.BucketInfo, error) {
	var buckets []types.BucketInfo

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketsBucket))

		return b.ForEach(func(k, v []byte) error {
			info := &types.BucketInfo{}

			if err := info.UnmarshalJSON(v); err != nil {
				return err
			}
			buckets = append(buckets, *info)
			return nil
		})
	})

	return buckets, err
}

// CreateBucket 创建一个新的存储桶
func (s *BoltS3Storage) CreateBucket(bucket string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketsBucket))

		// 检查存储桶是否已存在
		if b.Get([]byte(bucket)) != nil {
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

		return b.Put([]byte(bucket), data)
	})
}

// DeleteBucket 删除一个存储桶
func (s *BoltS3Storage) DeleteBucket(bucket string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))

		// 检查存储桶是否存在
		if bucketsBucket.Get([]byte(bucket)) == nil {
			return fmt.Errorf("bucket not found")
		}

		// 检查存储桶是否为空
		objectsBucket := tx.Bucket([]byte(objectsBucket))
		c := objectsBucket.Cursor()

		prefix := bucket + ":"
		for k, _ := c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			return fmt.Errorf("bucket not empty")
		}

		// 删除存储桶
		return bucketsBucket.Delete([]byte(bucket))
	})
}

// BucketExists 检查存储桶是否存在
func (s *BoltS3Storage) BucketExists(bucket string) (bool, error) {
	var exists bool

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketsBucket))
		exists = b.Get([]byte(bucket)) != nil
		return nil
	})

	return exists, err
}

// ListObjects 列出存储桶中的对象
func (s *BoltS3Storage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
	var objects []types.S3ObjectInfo
	var commonPrefixes []string

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(objectsBucket))
		c := b.Cursor()

		// 使用前缀搜索
		bucketPrefix := bucket + ":"
		searchPrefix := bucketPrefix
		if prefix != "" {
			searchPrefix = bucketPrefix + prefix
		}

		// 如果有分隔符，收集公共前缀
		prefixMap := make(map[string]bool)
		for k, v := c.Seek([]byte(searchPrefix)); k != nil && bytes.HasPrefix(k, []byte(bucketPrefix)); k, v = c.Next() {
			// 提取对象键（去掉存储桶前缀）
			key := string(k)[len(bucketPrefix):]

			// 应用前缀过滤
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			// 应用标记过滤（分页）
			if marker != "" && key <= marker {
				continue
			}

			// 如果有分隔符，检查是
			// 如果有分隔符，检查是否应该作为公共前缀
			if delimiter != "" {
				// 查找分隔符在键中的位置
				delimiterIndex := strings.Index(key[len(prefix):], delimiter)
				if delimiterIndex >= 0 {
					// 计算公共前缀
					commonPrefix := key[:len(prefix)+delimiterIndex+1] + delimiter
					if !prefixMap[commonPrefix] {
						prefixMap[commonPrefix] = true
						commonPrefixes = append(commonPrefixes, commonPrefix)
					}
					continue
				}
			}

			// 解析对象数据
			objectData := &types.S3ObjectData{}
			if err := objectData.UnmarshalJSON(v); err != nil {
				log.Logger.Warn("Warning: Failed to unmarshal object ", key, ": ", err)
				continue
			}

			objects = append(objects, types.S3ObjectInfo{
				Key:          key,
				Size:         int64(len(objectData.Data)),
				LastModified: objectData.LastModified,
				ETag:         objectData.ETag,
			})

			if len(objects) >= maxKeys {
				return fmt.Errorf("max keys reached: %d", maxKeys)
			}
		}

		return nil
	})

	return objects, commonPrefixes, err
}

// GetObject 获取一个对象
func (s *BoltS3Storage) GetObject(bucket, key string) (*types.S3ObjectData, error) {
	objectData := &types.S3ObjectData{}

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(objectsBucket))

		// 构建完整键
		fullKey := bucket + ":" + key

		// 获取对象数据
		log.Logger.Info("geting object, fullKey: ", fullKey)
		data := b.Get([]byte(fullKey))
		if data == nil {
			return fmt.Errorf("object not found")
		}

		// 解析对象数据
		if err := objectData.UnmarshalJSON(data); err != nil {
			return err
		}
		return nil
	})

	return objectData, err
}

// PutObject 存储一个对象
func (s *BoltS3Storage) PutObject(bucket string, object *types.S3ObjectData) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(objectsBucket))

		// 构建完整键
		fullKey := bucket + ":" + object.Key

		// 序列化对象数据
		data, err := object.MarshalJSON()
		if err != nil {
			return err
		}

		return b.Put([]byte(fullKey), data)
	})
}

// DeleteObject 删除一个对象
func (s *BoltS3Storage) DeleteObject(bucket, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(objectsBucket))

		// 构建完整键
		fullKey := bucket + ":" + key

		// 检查对象是否存在
		log.Logger.Info("deleting object, fullKey: ", fullKey)
		if b.Get([]byte(fullKey)) == nil {
			return fmt.Errorf("object not found")
		}

		return b.Delete([]byte(fullKey))
	})
}

// generateUploadID 生成唯一的上传ID
func generateUploadID(bucket, key string) string {
	now := time.Now().UnixNano()
	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%d", bucket, key, now)))
	return hex.EncodeToString(hash[:])
}

// calculateETag 计算数据的MD5哈希作为ETag
func calculateETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash[:]))
}

// calculateMultipartETag 计算分段上传的最终ETag
// S3兼容的格式: "{md5-of-all-etags}-{number-of-parts}"
func calculateMultipartETag(etags []string) string {
	// 移除每个ETag的引号
	cleanETags := make([]string, len(etags))
	for i, etag := range etags {
		cleanETags[i] = strings.Trim(etag, "\"")
	}

	// 连接所有ETag并计算MD5
	combined := strings.Join(cleanETags, "")
	hash := md5.Sum([]byte(combined))

	return fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(hash[:]), len(etags))
}
