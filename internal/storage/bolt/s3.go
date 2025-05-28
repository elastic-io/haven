package bolt





/*
import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
	"go.etcd.io/bbolt"
)

const (
    bucketsBucket   = "buckets"
    objectsBucket   = "objects"
    multipartBucket = "multipart"
)

// 初始化存储桶
func (s *BoltBackend) initBuckets() error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        buckets := []string{manifestsBucket, blobsBucket, bucketsBucket, objectsBucket, multipartBucket}
        for _, b := range buckets {
            _, err := tx.CreateBucketIfNotExists([]byte(b))
            if err != nil {
                return fmt.Errorf("create bucket %s: %w", b, err)
            }
        }
        return nil
    })
}

// ListBuckets 列出所有存储桶
func (s *BoltBackend) ListBuckets() ([]types.BucketInfo, error) {
    var buckets []types.BucketInfo
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(bucketsBucket))
        
        return b.ForEach(func(k, v []byte) error {
            var info types.BucketInfo
            if err := json.Unmarshal(v, &info); err != nil {
                return err
            }
            buckets = append(buckets, info)
            return nil
        })
    })
    
    return buckets, err
}

// CreateBucket 创建一个新的存储桶
func (s *BoltBackend) CreateBucket(bucket string) error {
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
        
        data, err := json.Marshal(info)
        if err != nil {
            return err
        }
        
        return b.Put([]byte(bucket), data)
    })
}

// DeleteBucket 删除一个存储桶
func (s *BoltBackend) DeleteBucket(bucket string) error {
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
func (s *BoltBackend) BucketExists(bucket string) (bool, error) {
    var exists bool
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(bucketsBucket))
        exists = b.Get([]byte(bucket)) != nil
        return nil
    })
    
    return exists, err
}

// ListObjects 列出存储桶中的对象
func (s *BoltBackend) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]types.S3ObjectInfo, []string, error) {
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
            var objectData types.S3ObjectData
            if err := json.Unmarshal(v, &objectData); err != nil {
                log.Printf("Warning: Failed to unmarshal object %s: %v", key, err)
                continue
            }
            
            objects = append(objects, types.S3ObjectInfo{
                Key:          key,
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
func (s *BoltBackend) GetObject(bucket, key string) (*types.S3ObjectData, error) {
    var objectData *types.S3ObjectData
    
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(objectsBucket))
        
        // 构建完整键
        fullKey := bucket + ":" + key
        
        // 获取对象数据
        data := b.Get([]byte(fullKey))
        if data == nil {
            return fmt.Errorf("object not found")
        }
        
        // 解析对象数据
        var obj types.S3ObjectData
        if err := json.Unmarshal(data, &obj); err != nil {
            return err
        }
        
        objectData = &obj
        return nil
    })
    
    return objectData, err
}

// PutObject 存储一个对象
func (s *BoltBackend) PutObject(bucket string, object types.S3ObjectData) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(objectsBucket))
        
        // 构建完整键
        fullKey := bucket + ":" + object.Key
        
        // 序列化对象数据
        data, err := json.Marshal(object)
        if err != nil {
            return err
        }
        
        return b.Put([]byte(fullKey), data)
    })
}

// DeleteObject 删除一个对象
func (s *BoltBackend) DeleteObject(bucket, key string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(objectsBucket))
        
        // 构建完整键
        fullKey := bucket + ":" + key
        
        // 检查对象是否存在
        if b.Get([]byte(fullKey)) == nil {
            return fmt.Errorf("object not found")
        }
        
        return b.Delete([]byte(fullKey))
    })
}

// CreateMultipartUpload 创建一个分段上传
func (s *BoltBackend) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
    uploadID := utils.GenerateUploadID()
    
    err := s.db.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(multipartBucket))
        
        // 创建分段上传信息
        uploadInfo := struct {
            Bucket      string            `json:"bucket"`
            Key         string            `json:"key"`
            UploadID    string            `json:"upload_id"`
            ContentType string            `json:"content_type"`
            Metadata    map[string]string `json:"metadata"`
            Parts       map[int][]byte    `json:"parts"`
            StartTime   time.Time         `json:"start_time"`
        }{
            Bucket:      bucket,
            Key:         key,
            UploadID:    uploadID,
            ContentType: contentType,
            Metadata:    metadata,
            Parts:       make(map[int][]byte),
            StartTime:   time.Now(),
        }
        
        // 序列化上传信息
        data, err := json.Marshal(uploadInfo)
        if err != nil {
            return err
        }
        
        // 存储上传信息
        uploadKey := fmt.Sprintf("%s:%s:%s", bucket, key, uploadID)
        return b.Put([]byte(uploadKey), data)
    })
    
    return uploadID, err
}

// UploadPart 上传一个分段
func (s *BoltBackend) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
    etag := fmt.Sprintf("\"%s\"", utils.ComputeMD5(data))
    
    err := s.db.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte(multipartBucket))
        
        // 获取上传信息
        uploadKey := fmt.Sprintf("%s:%s:%s", bucket, key, uploadID)
        uploadData := b.Get([]byte(uploadKey))
        if uploadData == nil {
            return fmt.Errorf("upload not found")
        }
        
        var uploadInfo struct {
            Bucket      string            `json:"bucket"`
            Key         string            `json:"key"`
            UploadID    string            `json:"upload_id"`
            ContentType string            `json:"content_type"`
            Metadata    map[string]string `json:"metadata"`
            Parts       map[string]string `json:"parts"`
            ETags       map[string]string `json:"etags"`
            StartTime   time.Time         `json:"start_time"`
        }
        
        if err := json.Unmarshal(uploadData, &uploadInfo); err != nil {
            return err
        }
        
        // 初始化 Parts 和 ETags 映射（如果需要）
        if uploadInfo.Parts == nil {
            uploadInfo.Parts = make(map[string]string)
        }
        if uploadInfo.ETags == nil {
            uploadInfo.ETags = make(map[string]string)
        }
        
        // 存储分段数据
        partKey := fmt.Sprintf("%s:%s:%s:part:%d", bucket, key, uploadID, partNumber)
        partBucket := tx.Bucket([]byte(objectsBucket))
        if err := partBucket.Put([]byte(partKey), data); err != nil {
            return err
        }
        
        // 更新上传信息
        uploadInfo.Parts[fmt.Sprintf("%d", partNumber)] = partKey
        uploadInfo.ETags[fmt.Sprintf("%d", partNumber)] = etag
        
        // 序列化更新后的上传信息
        updatedData, err := json.Marshal(uploadInfo)
        if err != nil {
            return err
        }
        
        return b.Put([]byte(uploadKey), updatedData)
    })
    
    return etag, err
}

// CompleteMultipartUpload 完成一个分段上传
func (s *BoltBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []types.MultipartPart) (string, error) {
    var finalETag string
    
    err := s.db.Update(func(tx *bbolt.Tx) error {
        multipartBucket := tx.Bucket([]byte(multipartBucket))
        objectsBucket := tx.Bucket([]byte(objectsBucket))
        
        // 获取上传信息
        uploadKey := fmt.Sprintf("%s:%s:%s", bucket, key, uploadID)
        uploadData := multipartBucket.Get([]byte(uploadKey))
        if uploadData == nil {
            return fmt.Errorf("upload not found")
        }
        
        var uploadInfo struct {
            Bucket      string            `json:"bucket"`
            Key         string            `json:"key"`
            UploadID    string            `json:"upload_id"`
            ContentType string            `json:"content_type"`
            Metadata    map[string]string `json:"metadata"`
            Parts       map[string]string `json:"parts"`
            ETags       map[string]string `json:"etags"`
            StartTime   time.Time         `json:"start_time"`
        }
        
        if err := json.Unmarshal(uploadData, &uploadInfo); err != nil {
            return err
        }
        
        // 验证所有分段
        for _, part := range parts {
            partKey := uploadInfo.Parts[fmt.Sprintf("%d", part.PartNumber)]
            if partKey == "" {
                return fmt.Errorf("part %d not found", part.PartNumber)
            }
            
            expectedETag := uploadInfo.ETags[fmt.Sprintf("%d", part.PartNumber)]
            if expectedETag != part.ETag {
                return fmt.Errorf("ETag mismatch for part %d", part.PartNumber)
            }
        }
        
        // 合并所有分段
        var allData []byte
        var allMD5s []byte
        
        for _, part := range parts {
            partKey := uploadInfo.Parts[fmt.Sprintf("%d", part.PartNumber)]
            partData := objectsBucket.Get([]byte(partKey))
            if partData == nil {
                return fmt.Errorf("part data not found")
            }
            
            allData = append(allData, partData...)
            
            // 提取 MD5 哈希（去掉引号）
            etag := part.ETag
            if len(etag) > 2 {
                etag = etag[1 : len(etag)-1]
            }
            md5Bytes, err := hex.DecodeString(etag)
            if err == nil {
                allMD5s = append(allMD5s, md5Bytes...)
            }
        }
        
        // 计算最终的 ETag（所有分段 MD5 的 MD5，后跟分段数）
        finalMD5 := md5.Sum(allMD5s)
        finalETag = fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(finalMD5[:]), len(parts))
        
        // 创建最终对象
        objectData := types.S3ObjectData{
            Key:          key,
            Data:         allData,
            ContentType:  uploadInfo.ContentType,
            LastModified: time.Now(),
            ETag:         finalETag,
            Metadata:     uploadInfo.Metadata,
        }
        
        // 序列化对象数据
        objectBytes, err := json.Marshal(objectData)
        if err != nil {
            return err
        }
        
        // 存储最终对象
        fullKey := bucket + ":" + key
        if err := objectsBucket.Put([]byte(fullKey), objectBytes); err != nil {
            return err
        }
        
        // 删除分段上传信息和所有分段
        if err := multipartBucket.Delete([]byte(uploadKey)); err != nil {
            return err
        }
        
        for _, part := range parts {
            partKey := uploadInfo.Parts[fmt.Sprintf("%d", part.PartNumber)]
            if err := objectsBucket.Delete([]byte(partKey)); err != nil {
                log.Printf("Warning: Failed to delete part %s: %v", partKey, err)
            }
        }
        
        return nil
    })
    
    return finalETag, err
}

// AbortMultipartUpload 中止一个分段上传
func (s *BoltBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        multipartBucket := tx.Bucket([]byte(multipartBucket))
        objectsBucket := tx.Bucket([]byte(objectsBucket))
        
        // 获取上传信息
        uploadKey := fmt.Sprintf("%s:%s:%s", bucket, key, uploadID)
        uploadData := multipartBucket.Get([]byte(uploadKey))
        if uploadData == nil {
            return fmt.Errorf("upload not found")
        }
        
        var uploadInfo struct {
            Parts map[string]string `json:"parts"`
        }
        
        if err := json.Unmarshal(uploadData, &uploadInfo); err != nil {
            return err
        }
        
        // 删除所有分段
        for _, partKey := range uploadInfo.Parts {
            if err := objectsBucket.Delete([]byte(partKey)); err != nil {
                log.Printf("Warning: Failed to delete part %s: %v", partKey, err)
            }
        }
        
        // 删除上传信息
        return multipartBucket.Delete([]byte(uploadKey))
    })
}

*/