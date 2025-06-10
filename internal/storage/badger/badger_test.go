package badger

import (
	"bytes"
	"fmt"
	"os"
	"strings" // 添加缺失的导入
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3" // 添加缺失的导入
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试辅助函数
func setupTestStorage(t *testing.T) (*BadgerS3Storage, func()) {
	log.Init("", "debug")
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "badger_test_")
	require.NoError(t, err)

	// 创建存储实例
	storage, err := NewBadgerS3Storage(tempDir)
	require.NoError(t, err)

	badgerStorage := storage.(*BadgerS3Storage)

	// 返回清理函数
	cleanup := func() {
		badgerStorage.Close()
		os.RemoveAll(tempDir)
	}

	return badgerStorage, cleanup
}

func createTestBucket(t *testing.T, s *BadgerS3Storage, bucketName string) {
	err := s.CreateBucket(bucketName)
	require.NoError(t, err)
}

func createTestObject(t *testing.T, s *BadgerS3Storage, bucket, key string, data []byte) {
	obj := &types.S3ObjectData{
		Key:          key,
		Data:         data,
		ContentType:  "application/octet-stream",
		LastModified: time.Now(),
		ETag:         storage.CalculateETag(data),
		Metadata:     make(map[string]string),
	}
	err := s.PutObject(bucket, obj)
	require.NoError(t, err)
}

// 测试NewBadgerS3Storage
func TestNewBadgerS3Storage(t *testing.T) {
	t.Run("成功创建存储实例", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "badger_test_")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		storage, err := NewBadgerS3Storage(tempDir)
		assert.NoError(t, err)
		assert.NotNil(t, storage)

		badgerStorage := storage.(*BadgerS3Storage)
		assert.NotNil(t, badgerStorage.db)
		assert.NotNil(t, badgerStorage.cleanupDone)
		assert.NotNil(t, badgerStorage.cleanupTicker)

		badgerStorage.Close()
	})

	t.Run("无效路径应该返回错误", func(t *testing.T) {
		// 使用一个不存在的父目录
		invalidPath := "/nonexistent/path/to/badger"
		storage, err := NewBadgerS3Storage(invalidPath)
		assert.Error(t, err)
		assert.Nil(t, storage)
	})

	t.Run("权限不足的路径应该返回错误", func(t *testing.T) {
		// 在Unix系统上，尝试在根目录创建数据库
		if os.Getuid() != 0 { // 非root用户
			storage, err := NewBadgerS3Storage("/root/badger_test")
			assert.Error(t, err)
			assert.Nil(t, storage)
		}
	})
}

// 测试桶操作
func TestBucketOperations(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("创建桶", func(t *testing.T) {
		err := s.CreateBucket("test-bucket")
		assert.NoError(t, err)

		// 验证桶存在
		exists, err := s.BucketExists("test-bucket")
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("创建重复桶应该返回错误", func(t *testing.T) {
		err := s.CreateBucket("duplicate-bucket")
		assert.NoError(t, err)

		err = s.CreateBucket("duplicate-bucket")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("检查不存在的桶", func(t *testing.T) {
		exists, err := s.BucketExists("nonexistent-bucket")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("列出桶", func(t *testing.T) {
		// 创建多个桶
		buckets := []string{"bucket1", "bucket2", "bucket3"}
		for _, bucket := range buckets {
			err := s.CreateBucket(bucket)
			assert.NoError(t, err)
		}

		// 列出桶
		result, err := s.ListBuckets()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), len(buckets))

		// 验证桶名称
		bucketNames := make([]string, len(result))
		for i, bucket := range result {
			bucketNames[i] = bucket.Name
		}

		for _, expectedBucket := range buckets {
			assert.Contains(t, bucketNames, expectedBucket)
		}
	})

	t.Run("删除空桶", func(t *testing.T) {
		bucketName := "empty-bucket"
		err := s.CreateBucket(bucketName)
		assert.NoError(t, err)

		err = s.DeleteBucket(bucketName)
		assert.NoError(t, err)

		// 验证桶不存在
		exists, err := s.BucketExists(bucketName)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("删除非空桶应该返回错误", func(t *testing.T) {
		bucketName := "non-empty-bucket"
		err := s.CreateBucket(bucketName)
		assert.NoError(t, err)

		// 添加一个对象
		createTestObject(t, s, bucketName, "test-key", []byte("test-data"))

		err = s.DeleteBucket(bucketName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not empty")
	})

	t.Run("删除不存在的桶应该返回错误", func(t *testing.T) {
		err := s.DeleteBucket("nonexistent-bucket")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("桶名称边界测试", func(t *testing.T) {
		// 测试空桶名
		err := s.CreateBucket("")
		assert.NoError(t, err) // Badger允许空键

		// 测试长桶名
		longName := strings.Repeat("a", 1000)
		err = s.CreateBucket(longName)
		assert.NoError(t, err)

		// 测试特殊字符
		specialName := "bucket-with-special-chars!@#$%^&*()"
		err = s.CreateBucket(specialName)
		assert.NoError(t, err)
	})
}

// 测试对象操作
func TestObjectOperations(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("存储和获取对象", func(t *testing.T) {
		key := "test-object"
		data := []byte("Hello, World!")

		obj := &types.S3ObjectData{
			Key:          key,
			Data:         data,
			ContentType:  "text/plain",
			LastModified: time.Now(),
			ETag:         storage.CalculateETag(data),
			Metadata:     map[string]string{"author": "test"},
		}

		// 存储对象
		err := s.PutObject(bucketName, obj)
		assert.NoError(t, err)

		// 获取对象
		retrieved, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)
		assert.Equal(t, obj.Key, retrieved.Key)
		assert.Equal(t, obj.Data, retrieved.Data)
		assert.Equal(t, obj.ContentType, retrieved.ContentType)
		assert.Equal(t, obj.ETag, retrieved.ETag)
		assert.Equal(t, obj.Metadata, retrieved.Metadata)
	})

	t.Run("获取不存在的对象应该返回错误", func(t *testing.T) {
		_, err := s.GetObject(bucketName, "nonexistent-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("从不存在的桶获取对象", func(t *testing.T) {
		_, err := s.GetObject("nonexistent-bucket", "test-key")
		assert.Error(t, err)
	})

	t.Run("删除对象", func(t *testing.T) {
		key := "object-to-delete"
		createTestObject(t, s, bucketName, key, []byte("data"))

		// 验证对象存在
		_, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)

		// 删除对象
		err = s.DeleteObject(bucketName, key)
		assert.NoError(t, err)

		// 验证对象不存在
		_, err = s.GetObject(bucketName, key)
		assert.Error(t, err)
	})

	t.Run("删除不存在的对象应该返回错误", func(t *testing.T) {
		err := s.DeleteObject(bucketName, "nonexistent-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("列出对象", func(t *testing.T) {
		// 创建多个对象
		objects := []string{"obj1", "obj2", "obj3", "folder/obj4", "folder/obj5"}
		for _, key := range objects {
			createTestObject(t, s, bucketName, key, []byte("data-"+key))
		}

		// 列出所有对象
		result, prefixes, err := s.ListObjects(bucketName, "", "", "", 100)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), len(objects))

		// 验证对象键
		resultKeys := make([]string, len(result))
		for i, obj := range result {
			resultKeys[i] = obj.Key
		}

		for _, expectedKey := range objects {
			assert.Contains(t, resultKeys, expectedKey)
		}
		assert.Empty(t, prefixes) // 没有分隔符时不应该有公共前缀
	})

	t.Run("带前缀列出对象", func(t *testing.T) {
		// 列出以"folder/"开头的对象
		result, _, err := s.ListObjects(bucketName, "folder/", "", "", 100)
		assert.NoError(t, err)

		for _, obj := range result {
			assert.True(t, strings.HasPrefix(obj.Key, "folder/"))
		}
	})

	t.Run("带分隔符列出对象", func(t *testing.T) {
		// 使用分隔符"/"
		_, prefixes, err := s.ListObjects(bucketName, "", "", "/", 100)
		assert.NoError(t, err)

		// 应该有公共前缀"folder/"
		assert.Contains(t, prefixes, "folder/")
	})

	t.Run("分页列出对象", func(t *testing.T) {
		// 限制返回数量
		result, _, err := s.ListObjects(bucketName, "", "", "", 2)
		assert.NoError(t, err)
		assert.LessOrEqual(t, len(result), 2)
	})

	t.Run("对象大小边界测试", func(t *testing.T) {
		// 测试空对象
		key := "empty-object"
		createTestObject(t, s, bucketName, key, []byte{})
		retrieved, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)
		assert.Empty(t, retrieved.Data)

		// 测试大对象
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		key = "large-object"
		createTestObject(t, s, bucketName, key, largeData)
		retrieved, err = s.GetObject(bucketName, key)
		assert.NoError(t, err)
		assert.Equal(t, largeData, retrieved.Data)
	})

	t.Run("对象键边界测试", func(t *testing.T) {
		// 测试空键
		createTestObject(t, s, bucketName, "", []byte("empty-key-data"))
		retrieved, err := s.GetObject(bucketName, "")
		assert.NoError(t, err)
		assert.Equal(t, []byte("empty-key-data"), retrieved.Data)

		// 测试长键
		longKey := strings.Repeat("k", 1000)
		createTestObject(t, s, bucketName, longKey, []byte("long-key-data"))
		retrieved, err = s.GetObject(bucketName, longKey)
		assert.NoError(t, err)
		assert.Equal(t, []byte("long-key-data"), retrieved.Data)

		// 测试特殊字符键
		specialKey := "key-with-special-chars!@#$%^&*()_+-=[]{}|;':\",./<>?"
		createTestObject(t, s, bucketName, specialKey, []byte("special-key-data"))
		retrieved, err = s.GetObject(bucketName, specialKey)
		assert.NoError(t, err)
		assert.Equal(t, []byte("special-key-data"), retrieved.Data)
	})
}

// 测试分段上传操作
func TestMultipartUploadOperations(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("完整的分段上传流程", func(t *testing.T) {
		key := "multipart-object"
		contentType := "application/octet-stream"
		metadata := map[string]string{"test": "value"}

		// 1. 初始化分段上传
		uploadID, err := s.CreateMultipartUpload(bucketName, key, contentType, metadata)
		assert.NoError(t, err)
		assert.NotEmpty(t, uploadID)

		// 2. 上传分段
		parts := []types.MultipartPart{}
		partData := [][]byte{
			[]byte("part1-data"),
			[]byte("part2-data"),
			[]byte("part3-data"),
		}

		for i, data := range partData {
			partNumber := i + 1
			etag, err := s.UploadPart(bucketName, key, uploadID, partNumber, data)
			assert.NoError(t, err)
			assert.NotEmpty(t, etag)

			parts = append(parts, types.MultipartPart{
				PartNumber: partNumber,
				ETag:       etag,
			})
		}

		// 3. 列出分段
		listedParts, err := s.ListParts(bucketName, key, uploadID)
		assert.NoError(t, err)
		assert.Len(t, listedParts, len(parts))

		// 验证分段信息
		for i, part := range listedParts {
			assert.Equal(t, i+1, part.PartNumber)
			assert.Equal(t, len(partData[i]), part.Size)
			assert.NotEmpty(t, part.ETag)
		}

		// 4. 完成分段上传
		finalETag, err := s.CompleteMultipartUpload(bucketName, key, uploadID, parts)
		assert.NoError(t, err)
		assert.NotEmpty(t, finalETag)

		// 5. 验证最终对象
		obj, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)

		expectedData := bytes.Join(partData, nil)
		assert.Equal(t, expectedData, obj.Data)
		assert.Equal(t, contentType, obj.ContentType)
		assert.Equal(t, metadata, obj.Metadata)
	})

	t.Run("中止分段上传", func(t *testing.T) {
		key := "aborted-multipart"

		// 初始化分段上传
		uploadID, err := s.CreateMultipartUpload(bucketName, key, "text/plain", nil)
		assert.NoError(t, err)

		// 上传一个分段
		_, err = s.UploadPart(bucketName, key, uploadID, 1, []byte("test-data"))
		assert.NoError(t, err)

		// 中止上传
		err = s.AbortMultipartUpload(bucketName, key, uploadID)
		assert.NoError(t, err)

		// 验证分段不存在
		_, err = s.ListParts(bucketName, key, uploadID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("列出分段上传", func(t *testing.T) {
		// 创建多个分段上传
		uploads := []string{}
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("multipart-key-%d", i)
			uploadID, err := s.CreateMultipartUpload(bucketName, key, "text/plain", nil)
			assert.NoError(t, err)
			uploads = append(uploads, uploadID)
		}

		// 列出分段上传
		result, err := s.ListMultipartUploads(bucketName)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), len(uploads))

		// 验证上传ID
		resultIDs := make([]string, len(result))
		for i, upload := range result {
			resultIDs[i] = upload.UploadID
		}

		for _, expectedID := range uploads {
			assert.Contains(t, resultIDs, expectedID)
		}
	})

	t.Run("分段上传错误情况", func(t *testing.T) {
		// 在不存在的桶中创建分段上传
		_, err := s.CreateMultipartUpload("nonexistent-bucket", "key", "text/plain", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")

		// 使用无效的分段号
		uploadID, err := s.CreateMultipartUpload(bucketName, "test-key", "text/plain", nil)
		assert.NoError(t, err)

		_, err = s.UploadPart(bucketName, "test-key", uploadID, 0, []byte("data")) // 分段号必须>=1
		assert.Error(t, err)

		_, err = s.UploadPart(bucketName, "test-key", uploadID, 10001, []byte("data")) // 分段号必须<=10000
		assert.Error(t, err)

		// 使用不存在的上传ID
		_, err = s.UploadPart(bucketName, "test-key", "nonexistent-upload-id", 1, []byte("data"))
		assert.Error(t, err)

		// 桶或键不匹配
		_, err = s.UploadPart("wrong-bucket", "test-key", uploadID, 1, []byte("data"))
		assert.Error(t, err)

		_, err = s.UploadPart(bucketName, "wrong-key", uploadID, 1, []byte("data"))
		assert.Error(t, err)

		// 完成没有分段的上传
		_, err = s.CompleteMultipartUpload(bucketName, "test-key", uploadID, []types.MultipartPart{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no parts")
	})

	t.Run("分段上传边界测试", func(t *testing.T) {
		key := "boundary-test"
		uploadID, err := s.CreateMultipartUpload(bucketName, key, "application/octet-stream", nil)
		assert.NoError(t, err)

		// 测试空分段
		etag, err := s.UploadPart(bucketName, key, uploadID, 1, []byte{})
		assert.NoError(t, err)
		assert.NotEmpty(t, etag)

		// 测试大分段
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		etag, err = s.UploadPart(bucketName, key, uploadID, 2, largeData)
		assert.NoError(t, err)
		assert.NotEmpty(t, etag)

		// 完成上传
		parts := []types.MultipartPart{
			{PartNumber: 1, ETag: storage.CalculateETag([]byte{})},
			{PartNumber: 2, ETag: storage.CalculateETag(largeData)},
		}

		// 需要重新获取正确的ETag
		listedParts, err := s.ListParts(bucketName, key, uploadID)
		assert.NoError(t, err)

		for i, part := range listedParts {
			parts[i].ETag = part.ETag
		}

		finalETag, err := s.CompleteMultipartUpload(bucketName, key, uploadID, parts)
		assert.NoError(t, err)
		assert.NotEmpty(t, finalETag)
	})
}

// 测试并发安全性
func TestConcurrencySafety(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "concurrent-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("并发对象操作", func(t *testing.T) {
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numOperations)

		// 并发写入对象
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("concurrent-obj-%d-%d", goroutineID, j)
					data := []byte(fmt.Sprintf("data-%d-%d", goroutineID, j))

					obj := &types.S3ObjectData{
						Key:          key,
						Data:         data,
						ContentType:  "text/plain",
						LastModified: time.Now(),
						ETag:         storage.CalculateETag(data),
					}

					if err := s.PutObject(bucketName, obj); err != nil {
						errors <- err
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// 检查是否有错误
		for err := range errors {
			t.Errorf("并发写入错误: %v", err)
		}

		// 验证所有对象都被正确写入
		objects, _, err := s.ListObjects(bucketName, "concurrent-obj-", "", "", 10000)
		assert.NoError(t, err)
		assert.Len(t, objects, numGoroutines*numOperations)
	})

	t.Run("并发分段上传", func(t *testing.T) {
		const numUploads = 5
		var wg sync.WaitGroup
		errors := make(chan error, numUploads*10)

		for i := 0; i < numUploads; i++ {
			wg.Add(1)
			go func(uploadID int) {
				defer wg.Done()

				key := fmt.Sprintf("concurrent-multipart-%d", uploadID)

				// 创建分段上传
				id, err := s.CreateMultipartUpload(bucketName, key, "text/plain", nil)
				if err != nil {
					errors <- err
					return
				}

				// 上传多个分段
				parts := []types.MultipartPart{}
				for partNum := 1; partNum <= 3; partNum++ {
					data := []byte(fmt.Sprintf("part-%d-%d", uploadID, partNum))
					etag, err := s.UploadPart(bucketName, key, id, partNum, data)
					if err != nil {
						errors <- err
						return
					}
					parts = append(parts, types.MultipartPart{
						PartNumber: partNum,
						ETag:       etag,
					})
				}

				// 完成上传
				_, err = s.CompleteMultipartUpload(bucketName, key, id, parts)
				if err != nil {
					errors <- err
					return
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// 检查是否有错误
		for err := range errors {
			t.Errorf("并发分段上传错误: %v", err)
		}

		// 验证所有对象都被正确创建
		objects, _, err := s.ListObjects(bucketName, "concurrent-multipart-", "", "", 100)
		assert.NoError(t, err)
		assert.Len(t, objects, numUploads)
	})
}

// 测试错误处理和恢复机制
func TestErrorHandlingAndRecovery(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("数据库关闭后的操作", func(t *testing.T) {
		// 创建一个新的存储实例用于测试
		tempDir, err := os.MkdirTemp("", "badger_test_close_*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		testStorage, err := NewBadgerS3Storage(tempDir)
		require.NoError(t, err)

		badgerStorage := testStorage.(*BadgerS3Storage)

		// 关闭数据库
		err = badgerStorage.Close()
		assert.NoError(t, err)

		// 尝试在关闭后进行操作应该返回错误
		err = badgerStorage.CreateBucket("test-bucket")
		assert.Error(t, err)
	})

	t.Run("无效数据处理", func(t *testing.T) {
		bucketName := "error-test-bucket"
		createTestBucket(t, s, bucketName)

		// 这些测试依赖于内部实现，主要测试边界情况
		// 在实际实现中，safeItemValue函数会处理空值等情况

		// 测试空对象数据
		obj := &types.S3ObjectData{
			Key:          "empty-metadata-test",
			Data:         []byte("test"),
			ContentType:  "",
			LastModified: time.Now(),
			ETag:         "",
			Metadata:     nil,
		}

		err := s.PutObject(bucketName, obj)
		assert.NoError(t, err)

		retrieved, err := s.GetObject(bucketName, "empty-metadata-test")
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
	})

	t.Run("过期分段上传清理", func(t *testing.T) {
		bucketName := "cleanup-test-bucket"
		createTestBucket(t, s, bucketName)

		// 这个测试需要模拟时间流逝，在实际环境中可能需要调整
		// 创建一个分段上传但不完成它
		key := "expired-upload"
		uploadID, err := s.CreateMultipartUpload(bucketName, key, "text/plain", nil)
		assert.NoError(t, err)

		// 上传一个分段
		_, err = s.UploadPart(bucketName, key, uploadID, 1, []byte("test-data"))
		assert.NoError(t, err)

		// 验证分段上传存在
		uploads, err := s.ListMultipartUploads(bucketName)
		assert.NoError(t, err)

		found := false
		for _, upload := range uploads {
			if upload.UploadID == uploadID {
				found = true
				break
			}
		}
		assert.True(t, found, "分段上传应该存在")

		// 手动触发清理（在实际实现中，这会在后台定期执行）
		// 注意：这个测试可能需要根据实际的清理逻辑进行调整
		err = s.cleanupExpiredUploads()
		assert.NoError(t, err)
	})
}

// 测试重试机制
func TestRetryMechanism(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("重试机制正常工作", func(t *testing.T) {
		// 测试withRetry函数
		callCount := 0
		err := s.withRetry(func() error {
			callCount++
			if callCount < 2 {
				return fmt.Errorf("temporary error")
			}
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, callCount, "应该重试一次后成功")
	})

	t.Run("重试机制达到最大次数", func(t *testing.T) {
		callCount := 0
		err := s.withRetry(func() error {
			callCount++
			return fmt.Errorf("persistent error")
		})

		assert.Error(t, err)
		assert.Equal(t, maxRetries, callCount, "应该重试最大次数")
		assert.Contains(t, err.Error(), "operation failed after")
	})
}

// 测试安全函数
func TestSafetyFunctions(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "safety-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("safeItemValue处理nil item", func(t *testing.T) {
		err := s.safeItemValue(nil, func([]byte) error {
			return nil
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "item is nil")
	})

	t.Run("safeIteratorOperation正常工作", func(t *testing.T) {
		// 创建一些测试数据
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			createTestObject(t, s, bucketName, key, []byte(fmt.Sprintf("data-%d", i)))
		}

		// 使用safeIteratorOperation遍历
		count := 0
		err := s.db.View(func(txn *badger.Txn) error {
			return s.safeIteratorOperation(txn, []byte(objectsBucketPrefix+bucketName+":"), func(it *badger.Iterator) error {
				for it.Rewind(); it.Valid(); it.Next() {
					count++
				}
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 5, count, "应该遍历到5个对象")
	})
}

// 性能测试
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试")
	}

	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "perf-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("大量对象写入性能", func(t *testing.T) {
		const numObjects = 1000
		start := time.Now()

		for i := 0; i < numObjects; i++ {
			key := fmt.Sprintf("perf-obj-%d", i)
			data := []byte(fmt.Sprintf("performance-test-data-%d", i))
			createTestObject(t, s, bucketName, key, data)
		}

		duration := time.Since(start)
		t.Logf("写入%d个对象耗时: %v", numObjects, duration)
		t.Logf("平均每个对象: %v", duration/numObjects)

		// 验证写入的对象数量
		objects, _, err := s.ListObjects(bucketName, "perf-obj-", "", "", numObjects+100)
		assert.NoError(t, err)
		assert.Len(t, objects, numObjects)
	})

	t.Run("大量对象读取性能", func(t *testing.T) {
		const numReads = 500
		start := time.Now()

		for i := 0; i < numReads; i++ {
			key := fmt.Sprintf("perf-obj-%d", i)
			_, err := s.GetObject(bucketName, key)
			assert.NoError(t, err)
		}

		duration := time.Since(start)
		t.Logf("读取%d个对象耗时: %v", numReads, duration)
		t.Logf("平均每个对象: %v", duration/numReads)
	})

	t.Run("大对象处理性能", func(t *testing.T) {
		// 测试10MB对象
		largeData := make([]byte, 10*1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		key := "large-perf-object"
		start := time.Now()
		createTestObject(t, s, bucketName, key, largeData)
		writeTime := time.Since(start)

		start = time.Now()
		retrieved, err := s.GetObject(bucketName, key)
		readTime := time.Since(start)

		assert.NoError(t, err)
		assert.Equal(t, largeData, retrieved.Data)

		t.Logf("写入10MB对象耗时: %v", writeTime)
		t.Logf("读取10MB对象耗时: %v", readTime)
	})
}

// 压力测试
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "stress-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("高并发读写压力测试", func(t *testing.T) {
		const numGoroutines = 20
		const numOperationsPerGoroutine = 50

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numOperationsPerGoroutine*2)

		// 启动多个goroutine进行并发读写
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for j := 0; j < numOperationsPerGoroutine; j++ {
					key := fmt.Sprintf("stress-obj-%d-%d", goroutineID, j)
					data := make([]byte, 1024) // 1KB数据
					for k := range data {
						data[k] = byte((goroutineID + j + k) % 256)
					}

					// 写入对象
					obj := &types.S3ObjectData{
						Key:          key,
						Data:         data,
						ContentType:  "application/octet-stream",
						LastModified: time.Now(),
						ETag:         storage.CalculateETag(data),
					}

					if err := s.PutObject(bucketName, obj); err != nil {
						errors <- fmt.Errorf("写入错误 %s: %v", key, err)
						continue
					}

					// 立即读取验证
					retrieved, err := s.GetObject(bucketName, key)
					if err != nil {
						errors <- fmt.Errorf("读取错误 %s: %v", key, err)
						continue
					}

					if !bytes.Equal(data, retrieved.Data) {
						errors <- fmt.Errorf("数据不匹配 %s", key)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// 收集所有错误
		var allErrors []error
		for err := range errors {
			allErrors = append(allErrors, err)
		}

		if len(allErrors) > 0 {
			t.Errorf("压力测试中发现 %d 个错误:", len(allErrors))
			for i, err := range allErrors {
				if i < 10 { // 只显示前10个错误
					t.Errorf("  %v", err)
				}
			}
			if len(allErrors) > 10 {
				t.Errorf("  ... 还有 %d 个错误", len(allErrors)-10)
			}
		}

		// 验证最终对象数量
		objects, _, err := s.ListObjects(bucketName, "stress-obj-", "", "", 10000)
		assert.NoError(t, err)
		expectedCount := numGoroutines * numOperationsPerGoroutine
		assert.Len(t, objects, expectedCount, "对象数量应该匹配")
	})

	t.Run("分段上传压力测试", func(t *testing.T) {
		const numUploads = 10
		const partsPerUpload = 5

		var wg sync.WaitGroup
		errors := make(chan error, numUploads*10)

		for i := 0; i < numUploads; i++ {
			wg.Add(1)
			go func(uploadIndex int) {
				defer wg.Done()

				key := fmt.Sprintf("stress-multipart-%d", uploadIndex)

				// 创建分段上传
				uploadID, err := s.CreateMultipartUpload(bucketName, key, "application/octet-stream", nil)
				if err != nil {
					errors <- fmt.Errorf("创建分段上传失败 %s: %v", key, err)
					return
				}

				// 上传多个分段
				parts := make([]types.MultipartPart, partsPerUpload)
				for partNum := 1; partNum <= partsPerUpload; partNum++ {
					data := make([]byte, 1024*10) // 10KB per part
					for j := range data {
						data[j] = byte((uploadIndex + partNum + j) % 256)
					}

					etag, err := s.UploadPart(bucketName, key, uploadID, partNum, data)
					if err != nil {
						errors <- fmt.Errorf("上传分段失败 %s part %d: %v", key, partNum, err)
						return
					}

					parts[partNum-1] = types.MultipartPart{
						PartNumber: partNum,
						ETag:       etag,
					}
				}

				// 完成上传
				_, err = s.CompleteMultipartUpload(bucketName, key, uploadID, parts)
				if err != nil {
					errors <- fmt.Errorf("完成分段上传失败 %s: %v", key, err)
					return
				}

				// 验证最终对象
				obj, err := s.GetObject(bucketName, key)
				if err != nil {
					errors <- fmt.Errorf("获取最终对象失败 %s: %v", key, err)
					return
				}

				expectedSize := partsPerUpload * 1024 * 10
				if len(obj.Data) != expectedSize {
					errors <- fmt.Errorf("对象大小不匹配 %s: expected %d, got %d", key, expectedSize, len(obj.Data))
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// 收集错误
		var allErrors []error
		for err := range errors {
			allErrors = append(allErrors, err)
		}

		if len(allErrors) > 0 {
			t.Errorf("分段上传压力测试中发现 %d 个错误:", len(allErrors))
			for _, err := range allErrors {
				t.Errorf("  %v", err)
			}
		}
	})
}

// 边界条件测试
func TestBoundaryConditions(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "boundary-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("极限大小测试", func(t *testing.T) {
		// 测试接近内存限制的对象（根据系统调整）
		if testing.Short() {
			t.Skip("跳过大对象测试")
		}

		// 50MB对象
		largeSize := 50 * 1024 * 1024
		largeData := make([]byte, largeSize)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		key := "extreme-large-object"
		start := time.Now()
		createTestObject(t, s, bucketName, key, largeData)
		t.Logf("写入50MB对象耗时: %v", time.Since(start))

		start = time.Now()
		retrieved, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)
		t.Logf("读取50MB对象耗时: %v", time.Since(start))

		assert.Equal(t, largeData, retrieved.Data)
	})

	t.Run("极限数量测试", func(t *testing.T) {
		if testing.Short() {
			t.Skip("跳过大量对象测试")
		}

		// 创建大量小对象
		const numObjects = 10000
		start := time.Now()

		for i := 0; i < numObjects; i++ {
			key := fmt.Sprintf("tiny-obj-%06d", i)
			data := []byte(fmt.Sprintf("%d", i))
			createTestObject(t, s, bucketName, key, data)

			if i%1000 == 0 {
				t.Logf("已创建 %d 个对象", i)
			}
		}

		t.Logf("创建%d个对象总耗时: %v", numObjects, time.Since(start))

		// 验证列出对象的性能
		start = time.Now()
		objects, _, err := s.ListObjects(bucketName, "tiny-obj-", "", "", numObjects+1000)
		assert.NoError(t, err)
		t.Logf("列出%d个对象耗时: %v", len(objects), time.Since(start))

		assert.GreaterOrEqual(t, len(objects), numObjects)
	})

	t.Run("Unicode和特殊字符测试", func(t *testing.T) {
		testCases := []struct {
			name string
			key  string
		}{
			{"中文键", "测试对象键"},
			{"日文键", "テストオブジェクトキー"},
			{"韩文键", "테스트객체키"},
			{"表情符号", "test-🚀-object-🎉"},
			{"特殊符号", "test!@#$%^&*()_+-=[]{}|;':\",./<>?"},
			{"空格和制表符", "test object\twith\nspecial\rchars"},
			{"长Unicode", "这是一个非常长的中文对象键名称用来测试Unicode字符的处理能力"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				data := []byte("Unicode test data: " + tc.key)
				createTestObject(t, s, bucketName, tc.key, data)

				retrieved, err := s.GetObject(bucketName, tc.key)
				assert.NoError(t, err)
				assert.Equal(t, data, retrieved.Data)
				assert.Equal(t, tc.key, retrieved.Key)
			})
		}
	})
}

// 数据一致性测试
func TestDataConsistency(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	bucketName := "consistency-test-bucket"
	createTestBucket(t, s, bucketName)

	t.Run("写入后立即读取一致性", func(t *testing.T) {
		const numTests = 100

		for i := 0; i < numTests; i++ {
			key := fmt.Sprintf("consistency-test-%d", i)
			originalData := make([]byte, 1024)
			for j := range originalData {
				originalData[j] = byte((i + j) % 256)
			}

			// 写入
			createTestObject(t, s, bucketName, key, originalData)

			// 立即读取
			retrieved, err := s.GetObject(bucketName, key)
			assert.NoError(t, err)
			assert.Equal(t, originalData, retrieved.Data, "第%d次测试数据不一致", i)
		}
	})

	t.Run("覆盖写入一致性", func(t *testing.T) {
		key := "overwrite-test"

		// 第一次写入
		data1 := []byte("first version of data")
		createTestObject(t, s, bucketName, key, data1)

		retrieved, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)
		assert.Equal(t, data1, retrieved.Data)

		// 覆盖写入
		data2 := []byte("second version of data - much longer than the first version")
		createTestObject(t, s, bucketName, key, data2)

		retrieved, err = s.GetObject(bucketName, key)
		assert.NoError(t, err)
		assert.Equal(t, data2, retrieved.Data)
		assert.NotEqual(t, data1, retrieved.Data)
	})

	t.Run("分段上传数据一致性", func(t *testing.T) {
		key := "multipart-consistency-test"

		// 准备测试数据
		partData := [][]byte{
			make([]byte, 1024),
			make([]byte, 2048),
			make([]byte, 512),
		}

		for i, data := range partData {
			for j := range data {
				data[j] = byte((i*1000 + j) % 256)
			}
		}

		// 创建分段上传
		uploadID, err := s.CreateMultipartUpload(bucketName, key, "application/octet-stream", nil)
		assert.NoError(t, err)

		// 上传分段
		parts := make([]types.MultipartPart, len(partData))
		for i, data := range partData {
			etag, err := s.UploadPart(bucketName, key, uploadID, i+1, data)
			assert.NoError(t, err)
			parts[i] = types.MultipartPart{
				PartNumber: i + 1,
				ETag:       etag,
			}
		}

		// 完成上传
		_, err = s.CompleteMultipartUpload(bucketName, key, uploadID, parts)
		assert.NoError(t, err)

		// 验证最终数据
		retrieved, err := s.GetObject(bucketName, key)
		assert.NoError(t, err)

		expectedData := bytes.Join(partData, nil)
		assert.Equal(t, expectedData, retrieved.Data)
	})
}

// Benchmark测试
func BenchmarkBadgerStorage(b *testing.B) {
	// 创建测试存储，但不使用testing.T
	tempDir, err := os.MkdirTemp("", "badger_bench_")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	s3Storage, err := NewBadgerS3Storage(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer s3Storage.Close()

	s := s3Storage.(*BadgerS3Storage)
	bucketName := "benchmark-bucket"
	s.CreateBucket(bucketName)

	b.Run("PutObject", func(b *testing.B) {
		data := make([]byte, 1024) // 1KB
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-obj-%d", i)
			obj := &types.S3ObjectData{
				Key:          key,
				Data:         data,
				ContentType:  "application/octet-stream",
				LastModified: time.Now(),
				ETag:         storage.CalculateETag(data),
			}
			s.PutObject(bucketName, obj)
		}
	})

	b.Run("GetObject", func(b *testing.B) {
		// 预先创建对象
		data := make([]byte, 1024)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("get-bench-obj-%d", i)
			obj := &types.S3ObjectData{
				Key:          key,
				Data:         data,
				ContentType:  "application/octet-stream",
				LastModified: time.Now(),
				ETag:         storage.CalculateETag(data),
			}
			s.PutObject(bucketName, obj)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("get-bench-obj-%d", i%1000)
			s.GetObject(bucketName, key)
		}
	})

	b.Run("ListObjects", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.ListObjects(bucketName, "", "", "", 100)
		}
	})
}

/*
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 创建临时测试目录
func setupTestDB(t *testing.T) (*BadgerS3Storage, string) {
	tempDir, err := ioutil.TempDir("", "badger-test-*")
	require.NoError(t, err)

	log.Init("", "debug")

	storage, err := NewBadgerS3Storage(tempDir)
	require.NoError(t, err)

	s, ok := storage.(*BadgerS3Storage)
	require.True(t, ok)

	return s, tempDir
}

// 清理测试资源
func teardownTestDB(s *BadgerS3Storage, tempDir string) {
	s.Close()
	os.RemoveAll(tempDir)
}

// 测试存储桶操作
func TestBucketOperations(t *testing.T) {
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 测试创建存储桶
	err := s.CreateBucket("testbucket")
	assert.NoError(t, err)

	// 测试存储桶存在
	exists, err := s.BucketExists("testbucket")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 测试不存在的存储桶
	exists, err = s.BucketExists("nonexistentbucket")
	assert.NoError(t, err)
	assert.False(t, exists)

	// 测试列出存储桶
	buckets, err := s.ListBuckets()
	assert.NoError(t, err)
	assert.Len(t, buckets, 1)
	assert.Equal(t, "testbucket", buckets[0].Name)

	// 测试删除存储桶
	err = s.DeleteBucket("testbucket")
	assert.NoError(t, err)

	// 验证存储桶已删除
	exists, err = s.BucketExists("testbucket")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// 测试对象操作
func TestObjectOperations(t *testing.T) {
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 创建测试存储桶
	err := s.CreateBucket("testbucket")
	require.NoError(t, err)

	// 测试上传对象
	testData := []byte("Hello, World!")
	testObject := &types.S3ObjectData{
		Key:          "testobject.txt",
		Data:         testData,
		ContentType:  "text/plain",
		LastModified: time.Now(),
		ETag:         storage.CalculateETag(testData),
		Metadata:     map[string]string{"test": "value"},
	}

	err = s.PutObject("testbucket", testObject)
	assert.NoError(t, err)

	// 测试获取对象
	retrievedObject, err := s.GetObject("testbucket", "testobject.txt")
	assert.NoError(t, err)
	assert.Equal(t, testObject.Key, retrievedObject.Key)
	assert.Equal(t, testObject.ContentType, retrievedObject.ContentType)
	assert.Equal(t, testObject.ETag, retrievedObject.ETag)
	assert.Equal(t, testObject.Metadata["test"], retrievedObject.Metadata["test"])
	assert.True(t, bytes.Equal(testObject.Data, retrievedObject.Data))

	// 测试列出对象
	objects, prefixes, err := s.ListObjects("testbucket", "", "", "", 1000)
	assert.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Len(t, prefixes, 0)
	assert.Equal(t, "testobject.txt", objects[0].Key)

	// 测试带前缀的列出对象
	objects, prefixes, err = s.ListObjects("testbucket", "test", "", "", 1000)
	assert.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Len(t, prefixes, 0)

	// 测试带分隔符的列出对象
	// 先添加一个带路径的对象
	pathObject := &types.S3ObjectData{
		Key:          "folder/nested.txt",
		Data:         []byte("Nested content"),
		ContentType:  "text/plain",
		LastModified: time.Now(),
		ETag:         storage.CalculateETag([]byte("Nested content")),
	}
	err = s.PutObject("testbucket", pathObject)
	assert.NoError(t, err)

	objects, prefixes, err = s.ListObjects("testbucket", "", "", "/", 1000)
	assert.NoError(t, err)
	assert.Len(t, objects, 1)  // 只有根目录的对象
	assert.Len(t, prefixes, 1) // 一个前缀 "folder/"
	assert.Equal(t, "folder/", prefixes[0])

	// 测试删除对象
	err = s.DeleteObject("testbucket", "testobject.txt")
	assert.NoError(t, err)

	// 验证对象已删除
	_, err = s.GetObject("testbucket", "testobject.txt")
	assert.Error(t, err)

	// 测试删除不存在的对象
	err = s.DeleteObject("testbucket", "nonexistent.txt")
	assert.Error(t, err)

	// 测试无法删除非空存储桶
	err = s.DeleteBucket("testbucket")
	assert.Error(t, err)

	// 删除剩余对象后应该可以删除存储桶
	err = s.DeleteObject("testbucket", "folder/nested.txt")
	assert.NoError(t, err)
	err = s.DeleteBucket("testbucket")
	assert.NoError(t, err)
}

// 测试分段上传
func TestMultipartUpload(t *testing.T) {
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 创建测试存储桶
	err := s.CreateBucket("testbucket")
	require.NoError(t, err)

	// 初始化分段上传
	uploadID, err := s.CreateMultipartUpload("testbucket", "multipart.txt", "text/plain", map[string]string{"test": "value"})
	assert.NoError(t, err)
	assert.NotEmpty(t, uploadID)

	// 上传分段
	part1Data := []byte("This is part 1 ")
	part2Data := []byte("and this is part 2")

	etag1, err := s.UploadPart("testbucket", "multipart.txt", uploadID, 1, part1Data)
	assert.NoError(t, err)
	assert.NotEmpty(t, etag1)

	etag2, err := s.UploadPart("testbucket", "multipart.txt", uploadID, 2, part2Data)
	assert.NoError(t, err)
	assert.NotEmpty(t, etag2)

	// 列出分段
	parts, err := s.ListParts("testbucket", "multipart.txt", uploadID)
	assert.NoError(t, err)
	assert.Len(t, parts, 2)
	assert.Equal(t, 1, parts[0].PartNumber)
	assert.Equal(t, 2, parts[1].PartNumber)

	// 列出进行中的上传
	uploads, err := s.ListMultipartUploads("testbucket")
	assert.NoError(t, err)
	assert.Len(t, uploads, 1)
	assert.Equal(t, "multipart.txt", uploads[0].Key)

	// 完成分段上传
	completeParts := []types.MultipartPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	}

	finalETag, err := s.CompleteMultipartUpload("testbucket", "multipart.txt", uploadID, completeParts)
	assert.NoError(t, err)
	assert.NotEmpty(t, finalETag)

	// 验证合并后的对象
	obj, err := s.GetObject("testbucket", "multipart.txt")
	assert.NoError(t, err)
	assert.Equal(t, "multipart.txt", obj.Key)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.Equal(t, "value", obj.Metadata["test"])

	expectedData := append(part1Data, part2Data...)
	assert.True(t, bytes.Equal(expectedData, obj.Data))

	// 验证上传已完成（不再在进行中列表中）
	uploads, err = s.ListMultipartUploads("testbucket")
	assert.NoError(t, err)
	assert.Len(t, uploads, 0)

	// 测试中止上传
	uploadID2, err := s.CreateMultipartUpload("testbucket", "aborted.txt", "text/plain", nil)
	assert.NoError(t, err)

	_, err = s.UploadPart("testbucket", "aborted.txt", uploadID2, 1, []byte("This will be aborted"))
	assert.NoError(t, err)

	err = s.AbortMultipartUpload("testbucket", "aborted.txt", uploadID2)
	assert.NoError(t, err)

	// 验证上传已中止
	uploads, err = s.ListMultipartUploads("testbucket")
	assert.NoError(t, err)
	assert.Len(t, uploads, 0)

	// 验证没有创建对象
	_, err = s.GetObject("testbucket", "aborted.txt")
	assert.Error(t, err)
}

// TestExpiredUploadCleanup 测试过期上传清理
func TestExpiredUploadCleanup(t *testing.T) {
	t.Skip("skip TestExpiredUploadCleanup")
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 创建测试存储桶
	err := s.CreateBucket("testbucket")
	require.NoError(t, err)

	// 初始化分段上传
	uploadID, err := s.CreateMultipartUpload("testbucket", "expired.txt", "text/plain", nil)
	assert.NoError(t, err)

	// 上传一个分段
	_, err = s.UploadPart("testbucket", "expired.txt", uploadID, 1, []byte("Test data"))
	assert.NoError(t, err)

	// 获取当前上传信息
	var uploadInfo types.MultipartUploadInfo
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(multipartBucketPrefix + uploadID))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &uploadInfo)
		})
	})
	assert.NoError(t, err)

	// 创建一个新的过期上传信息
	uploadInfo.CreatedAt = time.Now().Add(-8 * 24 * time.Hour)
	expiredData, err := json.Marshal(uploadInfo)
	assert.NoError(t, err)

	// 更新上传信息使其过期
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(multipartBucketPrefix+uploadID), expiredData)
	})
	assert.NoError(t, err)

	// 手动触发清理
	err = s.cleanupExpiredUploads()
	assert.NoError(t, err)

	// 验证上传已被清理
	uploads, err := s.ListMultipartUploads("testbucket")
	assert.NoError(t, err)
	assert.Len(t, uploads, 0)
}

// 测试边缘情况
func TestEdgeCases(t *testing.T) {
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 创建测试存储桶
	err := s.CreateBucket("testbucket")
	require.NoError(t, err)

	// 测试创建已存在的存储桶
	err = s.CreateBucket("testbucket")
	assert.Error(t, err)

	// 测试删除不存在的存储桶
	err = s.DeleteBucket("nonexistent")
	assert.Error(t, err)

	// 测试在不存在的存储桶中操作
	err = s.PutObject("nonexistent", &types.S3ObjectData{Key: "test.txt", Data: []byte("test")})
	assert.Error(t, err)

	_, err = s.GetObject("nonexistent", "test.txt")
	assert.Error(t, err)

	// 测试空对象
	emptyObject := &types.S3ObjectData{
		Key:          "empty.txt",
		Data:         []byte{},
		ContentType:  "text/plain",
		LastModified: time.Now(),
		ETag:         storage.CalculateETag([]byte{}),
	}
	err = s.PutObject("testbucket", emptyObject)
	assert.NoError(t, err)

	retrievedEmpty, err := s.GetObject("testbucket", "empty.txt")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(retrievedEmpty.Data))

	// 测试大对象
	largeData := make([]byte, 5*1024*1024) // 5MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	largeObject := &types.S3ObjectData{
		Key:          "large.bin",
		Data:         largeData,
		ContentType:  "application/octet-stream",
		LastModified: time.Now(),
		ETag:         storage.CalculateETag(largeData),
	}
	err = s.PutObject("testbucket", largeObject)
	assert.NoError(t, err)

	retrievedLarge, err := s.GetObject("testbucket", "large.bin")
	assert.NoError(t, err)
	assert.Equal(t, len(largeData), len(retrievedLarge.Data))
	assert.True(t, bytes.Equal(largeData, retrievedLarge.Data))

	// 测试分段上传的边缘情况
	// 无效的分段号
	_, err = s.UploadPart("testbucket", "test.txt", "invalid-id", 0, []byte("test"))
	assert.Error(t, err)

	_, err = s.UploadPart("testbucket", "test.txt", "invalid-id", 10001, []byte("test"))
	assert.Error(t, err)

	// 完成上传时没有指定分段
	uploadID, err := s.CreateMultipartUpload("testbucket", "noparts.txt", "text/plain", nil)
	assert.NoError(t, err)

	_, err = s.CompleteMultipartUpload("testbucket", "noparts.txt", uploadID, []types.MultipartPart{})
	assert.Error(t, err)
}

// 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	s, tempDir := setupTestDB(t)
	defer teardownTestDB(s, tempDir)

	// 创建测试存储桶
	err := s.CreateBucket("testbucket")
	require.NoError(t, err)

	// 并发上传多个对象
	const numObjects = 100
	done := make(chan bool, numObjects)

	for i := 0; i < numObjects; i++ {
		go func(idx int) {
			key := fmt.Sprintf("concurrent-%d.txt", idx)
			data := []byte(fmt.Sprintf("Data for object %d", idx))

			obj := &types.S3ObjectData{
				Key:          key,
				Data:         data,
				ContentType:  "text/plain",
				LastModified: time.Now(),
				ETag:         storage.CalculateETag(data),
			}

			err := s.PutObject("testbucket", obj)
			assert.NoError(t, err)

			done <- true
		}(i)
	}

	// 等待所有上传完成
	for i := 0; i < numObjects; i++ {
		<-done
	}

	// 验证所有对象都已上传
	objects, _, err := s.ListObjects("testbucket", "concurrent-", "", "", 1000)
	assert.NoError(t, err)
	assert.Len(t, objects, numObjects)

	// 并发获取所有对象
	for i := 0; i < numObjects; i++ {
		go func(idx int) {
			key := fmt.Sprintf("concurrent-%d.txt", idx)
			obj, err := s.GetObject("testbucket", key)
			assert.NoError(t, err)
			assert.Equal(t, key, obj.Key)
			assert.Equal(t, fmt.Sprintf("Data for object %d", idx), string(obj.Data))

			done <- true
		}(i)
	}

	// 等待所有获取完成
	for i := 0; i < numObjects; i++ {
		<-done
	}
}
*/
