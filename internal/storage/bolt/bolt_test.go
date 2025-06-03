package bolt

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/elastic-io/haven/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBoltS3Storage_BasicOperations(t *testing.T) {
	// 创建临时数据库文件
	dbPath := "test_bolt.db"
	defer os.Remove(dbPath)

	// 初始化存储
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)
	defer storage.Close()

	boltStorage, ok := storage.(*BoltS3Storage)
	require.True(t, ok)

	// 测试桶操作
	t.Run("BucketOperations", func(t *testing.T) {
		// 创建桶
		err := boltStorage.CreateBucket("testbucket")
		require.NoError(t, err)

		// 检查桶是否存在
		exists, err := boltStorage.BucketExists("testbucket")
		require.NoError(t, err)
		assert.True(t, exists)

		// 列出所有桶
		buckets, err := boltStorage.ListBuckets()
		require.NoError(t, err)
		assert.Equal(t, 1, len(buckets))
		assert.Equal(t, "testbucket", buckets[0].Name)

		// 尝试创建已存在的桶
		err = boltStorage.CreateBucket("testbucket")
		assert.Error(t, err)
	})

	// 测试对象操作
	t.Run("ObjectOperations", func(t *testing.T) {
		// 创建测试对象
		testData := []byte("Hello, World!")
		obj := &types.S3ObjectData{
			Key:          "testkey",
			Data:         testData,
			ContentType:  "text/plain",
			LastModified: time.Now(),
			ETag:         "\"test-etag\"",
			Metadata:     map[string]string{"test": "value"},
		}

		// 存储对象
		err := boltStorage.PutObject("testbucket", obj)
		require.NoError(t, err)

		// 获取对象
		retrievedObj, err := boltStorage.GetObject("testbucket", "testkey")
		require.NoError(t, err)
		assert.Equal(t, obj.Key, retrievedObj.Key)
		assert.Equal(t, obj.ContentType, retrievedObj.ContentType)
		assert.Equal(t, obj.ETag, retrievedObj.ETag)
		assert.Equal(t, obj.Metadata["test"], retrievedObj.Metadata["test"])
		assert.Equal(t, testData, retrievedObj.Data)

		// 列出对象
		objects, prefixes, err := boltStorage.ListObjects("testbucket", "", "", "", 1000)
		require.NoError(t, err)
		assert.Equal(t, 1, len(objects))
		assert.Equal(t, 0, len(prefixes))
		assert.Equal(t, "testkey", objects[0].Key)
		assert.Equal(t, int64(len(testData)), objects[0].Size)

		// 删除对象
		err = boltStorage.DeleteObject("testbucket", "testkey")
		require.NoError(t, err)

		// 确认对象已删除
		_, err = boltStorage.GetObject("testbucket", "testkey")
		assert.Error(t, err)
	})

	// 测试删除桶
	t.Run("DeleteBucket", func(t *testing.T) {
		// 创建一个新桶
		err := boltStorage.CreateBucket("emptybucket")
		require.NoError(t, err)

		// 删除空桶
		err = boltStorage.DeleteBucket("emptybucket")
		require.NoError(t, err)

		// 确认桶已删除
		exists, err := boltStorage.BucketExists("emptybucket")
		require.NoError(t, err)
		assert.False(t, exists)

		// 尝试删除不存在的桶
		err = boltStorage.DeleteBucket("nonexistentbucket")
		assert.Error(t, err)

		// 创建一个有对象的桶
		err = boltStorage.CreateBucket("nonemptybucket")
		require.NoError(t, err)

		// 添加对象到桶
		obj := &types.S3ObjectData{
			Key:          "testkey",
			Data:         []byte("test"),
			ContentType:  "text/plain",
			LastModified: time.Now(),
		}
		err = boltStorage.PutObject("nonemptybucket", obj)
		require.NoError(t, err)

		// 尝试删除非空桶
		err = boltStorage.DeleteBucket("nonemptybucket")
		assert.Error(t, err)
	})
}

func TestBoltS3Storage_MultipartUpload(t *testing.T) {
	// 创建临时数据库文件
	dbPath := "test_multipart.db"
	defer os.Remove(dbPath)

	// 初始化存储
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)
	defer storage.Close()

	boltStorage, ok := storage.(*BoltS3Storage)
	require.True(t, ok)

	// 创建测试桶
	err = boltStorage.CreateBucket("testbucket")
	require.NoError(t, err)

	// 测试分段上传流程
	t.Run("MultipartUploadFlow", func(t *testing.T) {
		// 初始化分段上传
		uploadID, err := boltStorage.CreateMultipartUpload("testbucket", "multipartkey", "application/octet-stream", map[string]string{"test": "value"})
		require.NoError(t, err)
		assert.NotEmpty(t, uploadID)

		// 上传部分
		part1Data := []byte("This is part 1")
		etag1, err := boltStorage.UploadPart("testbucket", "multipartkey", uploadID, 1, part1Data)
		require.NoError(t, err)
		assert.NotEmpty(t, etag1)

		part2Data := []byte("This is part 2")
		etag2, err := boltStorage.UploadPart("testbucket", "multipartkey", uploadID, 2, part2Data)
		require.NoError(t, err)
		assert.NotEmpty(t, etag2)

		// 列出部分
		parts, err := boltStorage.ListParts("testbucket", "multipartkey", uploadID)
		require.NoError(t, err)
		assert.Equal(t, 2, len(parts))
		assert.Equal(t, 1, parts[0].PartNumber)
		assert.Equal(t, 2, parts[1].PartNumber)

		// 完成分段上传
		completeParts := []types.MultipartPart{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		}
		finalETag, err := boltStorage.CompleteMultipartUpload("testbucket", "multipartkey", uploadID, completeParts)
		require.NoError(t, err)
		assert.NotEmpty(t, finalETag)

		// 验证合并后的对象
		obj, err := boltStorage.GetObject("testbucket", "multipartkey")
		require.NoError(t, err)
		assert.Equal(t, "multipartkey", obj.Key)
		assert.Equal(t, "application/octet-stream", obj.ContentType)
		assert.Equal(t, finalETag, obj.ETag)
		assert.Equal(t, "value", obj.Metadata["test"])

		// 验证对象内容
		expectedData := append(part1Data, part2Data...)
		assert.Equal(t, expectedData, obj.Data)
	})

	// 测试中止分段上传
	t.Run("AbortMultipartUpload", func(t *testing.T) {
		// 初始化分段上传
		uploadID, err := boltStorage.CreateMultipartUpload("testbucket", "abortkey", "text/plain", nil)
		require.NoError(t, err)

		// 上传一个部分
		_, err = boltStorage.UploadPart("testbucket", "abortkey", uploadID, 1, []byte("test data"))
		require.NoError(t, err)

		// 中止上传
		err = boltStorage.AbortMultipartUpload("testbucket", "abortkey", uploadID)
		require.NoError(t, err)

		// 验证上传已被中止
		_, err = boltStorage.ListParts("testbucket", "abortkey", uploadID)
		assert.Error(t, err)
	})

	// 测试列出分段上传
	t.Run("ListMultipartUploads", func(t *testing.T) {
		// 初始化两个分段上传
		uploadID1, err := boltStorage.CreateMultipartUpload("testbucket", "listkey1", "text/plain", nil)
		require.NoError(t, err)

		uploadID2, err := boltStorage.CreateMultipartUpload("testbucket", "listkey2", "text/plain", nil)
		require.NoError(t, err)

		// 列出所有分段上传
		uploads, err := boltStorage.ListMultipartUploads("testbucket")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(uploads), 2)

		// 验证上传信息
		foundUpload1 := false
		foundUpload2 := false
		for _, upload := range uploads {
			if upload.UploadID == uploadID1 {
				assert.Equal(t, "testbucket", upload.Bucket)
				assert.Equal(t, "listkey1", upload.Key)
				foundUpload1 = true
			}
			if upload.UploadID == uploadID2 {
				assert.Equal(t, "testbucket", upload.Bucket)
				assert.Equal(t, "listkey2", upload.Key)
				foundUpload2 = true
			}
		}
		assert.True(t, foundUpload1)
		assert.True(t, foundUpload2)

		// 清理
		err = boltStorage.AbortMultipartUpload("testbucket", "listkey1", uploadID1)
		require.NoError(t, err)
		err = boltStorage.AbortMultipartUpload("testbucket", "listkey2", uploadID2)
		require.NoError(t, err)
	})
}

func TestBoltS3Storage_ListObjectsWithPrefixAndDelimiter(t *testing.T) {
	// 创建临时数据库文件
	dbPath := "test_list_objects.db"
	defer os.Remove(dbPath)

	// 初始化存储
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)
	defer storage.Close()

	boltStorage, ok := storage.(*BoltS3Storage)
	require.True(t, ok)

	// 创建测试桶
	err = boltStorage.CreateBucket("testbucket")
	require.NoError(t, err)

	// 创建测试对象
	testObjects := []struct {
		key  string
		data []byte
	}{
		{"folder1/file1.txt", []byte("file1")},
		{"folder1/file2.txt", []byte("file2")},
		{"folder2/file3.txt", []byte("file3")},
		{"folder2/subfolder/file4.txt", []byte("file4")},
		{"rootfile.txt", []byte("root")},
	}

	for _, obj := range testObjects {
		err = boltStorage.PutObject("testbucket", &types.S3ObjectData{
			Key:          obj.key,
			Data:         obj.data,
			ContentType:  "text/plain",
			LastModified: time.Now(),
		})
		require.NoError(t, err)
	}

	// 测试不同的列表场景
	t.Run("ListAllObjects", func(t *testing.T) {
		objects, prefixes, err := boltStorage.ListObjects("testbucket", "", "", "", 1000)
		require.NoError(t, err)
		assert.Equal(t, 5, len(objects))
		assert.Equal(t, 0, len(prefixes))

		// 验证对象按键排序
		keys := make([]string, len(objects))
		for i, obj := range objects {
			keys[i] = obj.Key
		}
		sort.Strings(keys)
		for i, obj := range objects {
			assert.Equal(t, keys[i], obj.Key)
		}
	})

	t.Run("ListWithPrefix", func(t *testing.T) {
		objects, prefixes, err := boltStorage.ListObjects("testbucket", "folder1/", "", "", 1000)
		require.NoError(t, err)
		assert.Equal(t, 2, len(objects))
		assert.Equal(t, 0, len(prefixes))

		// 验证只返回了folder1下的对象
		for _, obj := range objects {
			assert.True(t, obj.Key == "folder1/file1.txt" || obj.Key == "folder1/file2.txt")
		}
	})

	t.Run("ListWithDelimiter", func(t *testing.T) {
		objects, prefixes, err := boltStorage.ListObjects("testbucket", "", "", "/", 1000)
		require.NoError(t, err)

		// 应该只返回根目录的文件和文件夹前缀
		assert.Equal(t, 1, len(objects))  // rootfile.txt
		assert.Equal(t, 2, len(prefixes)) // folder1/, folder2/

		assert.Equal(t, "rootfile.txt", objects[0].Key)
		assert.Contains(t, prefixes, "folder1/")
		assert.Contains(t, prefixes, "folder2/")
	})

	t.Run("ListWithPrefixAndDelimiter", func(t *testing.T) {
		objects, prefixes, err := boltStorage.ListObjects("testbucket", "folder2/", "/", "", 1000)
		require.NoError(t, err)

		// 应该返回folder2下的文件和子文件夹前缀
		assert.Equal(t, 1, len(objects))  // folder2/file3.txt
		assert.Equal(t, 1, len(prefixes)) // folder2/subfolder/

		assert.Equal(t, "folder2/file3.txt", objects[0].Key)
		assert.Contains(t, prefixes, "folder2/subfolder/")
	})

	t.Run("ListWithMaxKeys", func(t *testing.T) {
		objects, _, err := boltStorage.ListObjects("testbucket", "", "", "", 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(objects))
	})

	t.Run("ListWithMarker", func(t *testing.T) {
		objects, _, err := boltStorage.ListObjects("testbucket", "", "folder1/file2.txt", "", 1000)
		require.NoError(t, err)

		// 应该只返回键大于marker的对象
		for _, obj := range objects {
			assert.True(t, obj.Key > "folder1/file2.txt")
		}
	})
}

func TestBoltS3Storage_CleanupExpiredUploads(t *testing.T) {
	// 创建临时数据库文件
	dbPath := "test_cleanup.db"
	defer os.Remove(dbPath)

	// 初始化存储
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)

	boltStorage, ok := storage.(*BoltS3Storage)
	require.True(t, ok)

	// 创建测试桶
	err = boltStorage.CreateBucket("testbucket")
	require.NoError(t, err)

	// 创建一个分段上传
	uploadID, err := boltStorage.CreateMultipartUpload("testbucket", "expiredkey", "text/plain", nil)
	require.NoError(t, err)

	// 上传一个部分
	_, err = boltStorage.UploadPart("testbucket", "expiredkey", uploadID, 1, []byte("test data"))
	require.NoError(t, err)

	// 直接修改数据库中的创建时间，使其看起来已过期
	err = boltStorage.db.Update(func(tx *bbolt.Tx) error {
		mpBkt := tx.Bucket([]byte(multipartBucket))
		if mpBkt == nil {
			return fmt.Errorf("multipart bucket not found")
		}

		data := mpBkt.Get([]byte(uploadID))
		if data == nil {
			return fmt.Errorf("upload not found")
		}

		info := &types.MultipartUploadInfo{}
		if err := info.UnmarshalJSON(data); err != nil {
			return err
		}

		// 将创建时间设置为8天前（超过7天的最大生命周期）
		info.CreatedAt = time.Now().Add(-8 * 24 * time.Hour)

		updatedData, err := info.MarshalJSON()
		if err != nil {
			return err
		}

		return mpBkt.Put([]byte(uploadID), updatedData)
	})
	require.NoError(t, err)

	// 手动触发清理
	err = boltStorage.cleanupExpiredUploads()
	require.NoError(t, err)

	// 验证上传已被清理
	uploads, err := boltStorage.ListMultipartUploads("testbucket")
	require.NoError(t, err)

	// 检查特定的上传ID是否已被清理
	found := false
	for _, upload := range uploads {
		if upload.UploadID == uploadID {
			found = true
			break
		}
	}
	assert.False(t, found, "Expired upload should have been cleaned up")

	// 关闭存储
	boltStorage.Close()
}

func TestBoltS3Storage_ConcurrentAccess(t *testing.T) {
	// 创建临时数据库文件
	dbPath := "test_concurrent.db"
	defer os.Remove(dbPath)

	// 初始化存储
	storage, err := NewBoltS3Storage(dbPath)
	require.NoError(t, err)
	defer storage.Close()

	boltStorage, ok := storage.(*BoltS3Storage)
	require.True(t, ok)

	// 创建测试桶
	err = boltStorage.CreateBucket("testbucket")
	require.NoError(t, err)

	// 并发测试
	t.Run("ConcurrentObjectOperations", func(t *testing.T) {
		const numGoroutines = 10
		const numOperations = 5

		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				for j := 0; j < numOperations; j++ {
					// 创建对象
					key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
					data := []byte(fmt.Sprintf("data-%d-%d", id, j))

					err := boltStorage.PutObject("testbucket", &types.S3ObjectData{
						Key:          key,
						Data:         data,
						ContentType:  "text/plain",
						LastModified: time.Now(),
					})
					if err != nil {
						t.Errorf("Failed to put object: %v", err)
					}

					// 获取对象
					obj, err := boltStorage.GetObject("testbucket", key)
					if err != nil {
						t.Errorf("Failed to get object: %v", err)
					} else if !bytes.Equal(obj.Data, data) {
						t.Errorf("Data mismatch for key %s", key)
					}

					// 删除对象
					err = boltStorage.DeleteObject("testbucket", key)
					if err != nil {
						t.Errorf("Failed to delete object: %v", err)
					}
				}
				done <- true
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}
