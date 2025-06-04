package badger

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
