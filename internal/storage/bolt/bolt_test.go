package bolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试辅助函数
func setupTestStorage(t *testing.T) (*BoltS3Storage, func()) {
	log.Init("", "debug")
	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "bolt_test_*")
	require.NoError(t, err)

	dbPath := filepath.Join(tempDir, "test.db")
	
	// 创建存储实例
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)
	require.NotNil(t, storage)

	boltStorage := storage.(*BoltS3Storage)

	// 返回清理函数
	cleanup := func() {
		boltStorage.Close()
		os.RemoveAll(tempDir)
	}

	return boltStorage, cleanup
}

func TestNewBoltS3Storage(t *testing.T) {
	t.Run("成功创建存储", func(t *testing.T) {
		storage, cleanup := setupTestStorage(t)
		defer cleanup()

		assert.NotNil(t, storage)
		assert.NotNil(t, storage.db)
		assert.NotNil(t, storage.cleanupDone)
	})

	t.Run("无效路径", func(t *testing.T) {
		_, err := NewBoltS3Storage("/invalid/path/test.db")
		assert.Error(t, err)
	})
}

func TestBucketOperations(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	testBucket := "test-bucket"

	t.Run("创建桶", func(t *testing.T) {
		err := storage.CreateBucket(testBucket)
		assert.NoError(t, err)
	})

	t.Run("桶已存在", func(t *testing.T) {
		err := storage.CreateBucket(testBucket)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("检查桶存在", func(t *testing.T) {
		exists, err := storage.BucketExists(testBucket)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("检查不存在的桶", func(t *testing.T) {
		exists, err := storage.BucketExists("non-existent")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("列出桶", func(t *testing.T) {
		buckets, err := storage.ListBuckets()
		assert.NoError(t, err)
		assert.Len(t, buckets, 1)
		assert.Equal(t, testBucket, buckets[0].Name)
		assert.False(t, buckets[0].CreationDate.IsZero())
	})

	t.Run("删除非空桶应该失败", func(t *testing.T) {
		// 先添加一个对象
		objData := &types.S3ObjectData{
			Key:          "test-key",
			Data:         []byte("test data"),
			ContentType:  "text/plain",
			LastModified: time.Now(),
			ETag:         "test-etag",
			Metadata:     map[string]string{"key": "value"},
		}
		err := storage.PutObject(testBucket, objData)
		require.NoError(t, err)

		// 尝试删除非空桶
		err = storage.DeleteBucket(testBucket)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not empty")
	})

	t.Run("删除空桶", func(t *testing.T) {
		// 先删除对象
		err := storage.DeleteObject(testBucket, "test-key")
		require.NoError(t, err)

		// 删除桶
		err = storage.DeleteBucket(testBucket)
		assert.NoError(t, err)

		// 验证桶不存在
		exists, err := storage.BucketExists(testBucket)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("删除不存在的桶", func(t *testing.T) {
		err := storage.DeleteBucket("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestObjectOperations(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	testBucket := "test-bucket"
	testKey := "test-key"

	// 创建测试桶
	err := storage.CreateBucket(testBucket)
	require.NoError(t, err)

	testData := &types.S3ObjectData{
		Key:          testKey,
		Data:         []byte("Hello, World!"),
		ContentType:  "text/plain",
		LastModified: time.Now().Truncate(time.Second), // 截断到秒，避免精度问题
		ETag:         "test-etag-123",
		Metadata:     map[string]string{"author": "test", "version": "1.0"},
		Size:         13,
	}

	t.Run("存储对象", func(t *testing.T) {
		err := storage.PutObject(testBucket, testData)
		assert.NoError(t, err)
	})

	t.Run("存储到不存在的桶", func(t *testing.T) {
		err := storage.PutObject("non-existent", testData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("检查对象存在", func(t *testing.T) {
		exists, err := storage.ObjectExists(testBucket, testKey)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("检查不存在的对象", func(t *testing.T) {
		exists, err := storage.ObjectExists(testBucket, "non-existent")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("获取对象", func(t *testing.T) {
		obj, err := storage.GetObject(testBucket, testKey)
		assert.NoError(t, err)
		assert.NotNil(t, obj)
		assert.Equal(t, testData.Key, obj.Key)
		assert.Equal(t, testData.Data, obj.Data)
		assert.Equal(t, testData.ContentType, obj.ContentType)
		assert.Equal(t, testData.ETag, obj.ETag)
		assert.Equal(t, testData.Metadata, obj.Metadata)
		assert.Equal(t, int64(len(testData.Data)), obj.Size)
	})

	t.Run("获取不存在的对象", func(t *testing.T) {
		_, err := storage.GetObject(testBucket, "non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("列出对象", func(t *testing.T) {
		// 添加更多测试对象
		objects := []*types.S3ObjectData{
			{Key: "dir1/file1.txt", Data: []byte("content1"), ContentType: "text/plain", ETag: "etag1", LastModified: time.Now()},
			{Key: "dir1/file2.txt", Data: []byte("content2"), ContentType: "text/plain", ETag: "etag2", LastModified: time.Now()},
			{Key: "dir2/file3.txt", Data: []byte("content3"), ContentType: "text/plain", ETag: "etag3", LastModified: time.Now()},
		}

		for _, obj := range objects {
			err := storage.PutObject(testBucket, obj)
			require.NoError(t, err)
		}

		// 测试基本列表
		objList, prefixes, err := storage.ListObjects(testBucket, "", "", "", 0)
		assert.NoError(t, err)
		assert.Len(t, objList, 4) // 包括原来的 test-key
		assert.Empty(t, prefixes)

		// 测试前缀过滤
		objList, prefixes, err = storage.ListObjects(testBucket, "dir1/", "", "", 0)
		assert.NoError(t, err)
		assert.Len(t, objList, 2)
		assert.Empty(t, prefixes)

		// 测试分隔符
		objList, prefixes, err = storage.ListObjects(testBucket, "", "", "/", 0)
		assert.NoError(t, err)
		assert.Contains(t, prefixes, "dir1/")
		assert.Contains(t, prefixes, "dir2/")

		// 测试最大键数限制
		objList, prefixes, err = storage.ListObjects(testBucket, "", "", "", 2)
		assert.NoError(t, err)
		assert.Len(t, objList, 2)
	})

	t.Run("删除对象", func(t *testing.T) {
		err := storage.DeleteObject(testBucket, testKey)
		assert.NoError(t, err)

		// 验证对象不存在
		exists, err := storage.ObjectExists(testBucket, testKey)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("删除不存在的对象", func(t *testing.T) {
		err := storage.DeleteObject(testBucket, "non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestMultipartUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	testBucket := "test-bucket"
	testKey := "multipart-test"
	contentType := "application/octet-stream"
	metadata := map[string]string{"test": "multipart"}

	// 创建测试桶
	err := storage.CreateBucket(testBucket)
	require.NoError(t, err)

	var uploadID string

	t.Run("创建分段上传", func(t *testing.T) {
		uploadID, err = storage.CreateMultipartUpload(testBucket, testKey, contentType, metadata)
		assert.NoError(t, err)
		assert.NotEmpty(t, uploadID)
	})

	t.Run("创建分段上传到不存在的桶", func(t *testing.T) {
		_, err := storage.CreateMultipartUpload("non-existent", testKey, contentType, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("列出分段上传", func(t *testing.T) {
		uploads, err := storage.ListMultipartUploads(testBucket)
		assert.NoError(t, err)
		assert.Len(t, uploads, 1)
		assert.Equal(t, uploadID, uploads[0].UploadID)
		assert.Equal(t, testBucket, uploads[0].Bucket)
		assert.Equal(t, testKey, uploads[0].Key)
	})

	var etags []string

	t.Run("上传分段", func(t *testing.T) {
		parts := [][]byte{
			[]byte("part1data"),
			[]byte("part2data"),
			[]byte("part3data"),
		}

		for i, partData := range parts {
			partNumber := i + 1
			etag, err := storage.UploadPart(testBucket, testKey, uploadID, partNumber, partData)
			assert.NoError(t, err)
			assert.NotEmpty(t, etag)
			etags = append(etags, etag)
		}
	})

	t.Run("上传无效分段号", func(t *testing.T) {
		_, err := storage.UploadPart(testBucket, testKey, uploadID, 0, []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid part number")

		_, err = storage.UploadPart(testBucket, testKey, uploadID, 10001, []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid part number")
	})

	t.Run("列出分段", func(t *testing.T) {
		parts, err := storage.ListParts(testBucket, testKey, uploadID)
		assert.NoError(t, err)
		assert.Len(t, parts, 3)

		// 验证分段按顺序排列
		for i, part := range parts {
			assert.Equal(t, i+1, part.PartNumber)
			assert.Equal(t, etags[i], part.ETag)
		}
	})

	t.Run("完成分段上传", func(t *testing.T) {
		t.Skip("")
		multipartParts := make([]types.MultipartPart, len(etags))
		for i, etag := range etags {
			multipartParts[i] = types.MultipartPart{
				PartNumber: i + 1,
				ETag:       etag,
			}
		}

		finalETag, err := storage.CompleteMultipartUpload(testBucket, testKey, uploadID, multipartParts)
		assert.NoError(t, err)
		assert.NotEmpty(t, finalETag)

		// 验证对象已创建
		obj, err := storage.GetObject(testBucket, testKey)
		assert.NoError(t, err)
		assert.Equal(t, "part1datapart2datapart3data", string(obj.Data))
		assert.Equal(t, contentType, obj.ContentType)
		assert.Equal(t, metadata, obj.Metadata)
	})

	t.Run("完成不存在的分段上传", func(t *testing.T) {
		_, err := storage.CompleteMultipartUpload(testBucket, testKey, "invalid-upload-id", []types.MultipartPart{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("中止分段上传", func(t *testing.T) {
		// 创建新的分段上传用于测试中止
		newUploadID, err := storage.CreateMultipartUpload(testBucket, "abort-test", contentType, metadata)
		require.NoError(t, err)

		// 上传一个分段
		_, err = storage.UploadPart(testBucket, "abort-test", newUploadID, 1, []byte("test"))
		require.NoError(t, err)

		// 中止上传
		err = storage.AbortMultipartUpload(testBucket, "abort-test", newUploadID)
		assert.NoError(t, err)

		// 验证上传已被删除
		uploads, err := storage.ListMultipartUploads(testBucket)
		assert.NoError(t, err)
		for _, upload := range uploads {
			assert.NotEqual(t, newUploadID, upload.UploadID)
		}
	})
}

func TestHealthCheckAndStats(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("健康检查", func(t *testing.T) {
		err := storage.HealthCheck()
		assert.NoError(t, err)
	})

	t.Run("获取统计信息", func(t *testing.T) {
		// 创建一些测试数据
		err := storage.CreateBucket("stats-test")
		require.NoError(t, err)

		objData := &types.S3ObjectData{
			Key:         "test-obj",
			Data:        []byte("test data for stats"),
			ContentType: "text/plain",
			ETag:        "stats-etag",
			LastModified: time.Now(),
		}
		err = storage.PutObject("stats-test", objData)
		require.NoError(t, err)

		stats, err := storage.GetStats()
		assert.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, 1, stats.BucketCount)
		assert.Equal(t, 1, stats.ObjectCount)
		assert.Equal(t, int64(len(objData.Data)), stats.TotalSize)
	})

	t.Run("数据库压缩", func(t *testing.T) {
		err := storage.Compact()
		assert.NoError(t, err)
	})
}

func TestConcurrency(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	testBucket := "concurrent-test"
	err := storage.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("并发写入", func(t *testing.T) {
		const numGoroutines = 10
		const objectsPerGoroutine = 5

		done := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				for j := 0; j < objectsPerGoroutine; j++ {
					key := fmt.Sprintf("obj-%d-%d", goroutineID, j)
					data := fmt.Sprintf("data from goroutine %d, object %d", goroutineID, j)
					
					objData := &types.S3ObjectData{
						Key:         key,
						Data:        []byte(data),
						ContentType: "text/plain",
						ETag:        fmt.Sprintf("etag-%d-%d", goroutineID, j),
						LastModified: time.Now(),
					}
					
					if err := storage.PutObject(testBucket, objData); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			err := <-done
			assert.NoError(t, err)
		}

		// 验证所有对象都已创建
		objects, _, err := storage.ListObjects(testBucket, "", "", "", 0)
		assert.NoError(t, err)
		assert.Len(t, objects, numGoroutines*objectsPerGoroutine)
	})
}

func TestErrorHandling(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("关闭后的操作", func(t *testing.T) {
		// 关闭存储
		err := storage.Close()
		assert.NoError(t, err)

		// 尝试操作应该失败
		err = storage.CreateBucket("test")
		assert.Error(t, err)
	})
}

func TestCleanupExpiredUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	testBucket := "cleanup-test"
	err := storage.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("清理过期上传", func(t *testing.T) {
		// 这个测试比较难实现，因为需要模拟时间过期
		// 可以通过直接调用清理方法来测试基本功能
		err := storage.cleanupExpiredUploads()
		assert.NoError(t, err)
	})
}

// 基准测试
func BenchmarkPutObject(b *testing.B) {
	storage, cleanup := setupTestStorage(&testing.T{})
	defer cleanup()

	testBucket := "bench-test"
	storage.CreateBucket(testBucket)

	objData := &types.S3ObjectData{
		Key:         "bench-key",
		Data:        make([]byte, 1024), // 1KB data
		ContentType: "application/octet-stream",
		ETag:        "bench-etag",
		LastModified: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objData.Key = fmt.Sprintf("bench-key-%d", i)
		storage.PutObject(testBucket, objData)
	}
}

func BenchmarkGetObject(b *testing.B) {
	storage, cleanup := setupTestStorage(&testing.T{})
	defer cleanup()

	testBucket := "bench-test"
	storage.CreateBucket(testBucket)

	objData := &types.S3ObjectData{
		Key:         "bench-key",
		Data:        make([]byte, 1024), // 1KB data
		ContentType: "application/octet-stream",
		ETag:        "bench-etag",
		LastModified: time.Now(),
	}
	storage.PutObject(testBucket, objData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.GetObject(testBucket, "bench-key")
	}
}