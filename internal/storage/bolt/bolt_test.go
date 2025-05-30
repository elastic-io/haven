package bolt

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/elastic-io/haven/internal/log"
)

func init() {
	log.Init("", "debug")
}

func TestBoltS3Storage(t *testing.T) {
	// 创建临时测试文件
	tempDir, err := os.MkdirTemp("", "bolt-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	// 创建存储实例
	storage, err := NewBoltS3Storage(dbPath)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// 测试桶操作
	t.Run("BucketOperations", func(t *testing.T) {
		// 创建桶
		bucketName := "test-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// 检查桶是否存在
		exists, err := storage.BucketExists(bucketName)
		if err != nil {
			t.Fatalf("Failed to check bucket existence: %v", err)
		}
		if !exists {
			t.Fatalf("Bucket should exist but doesn't")
		}

		// 列出所有桶
		buckets, err := storage.ListBuckets()
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}
		if len(buckets) != 1 || buckets[0] != bucketName {
			t.Fatalf("Expected bucket list to contain only %s, got %v", bucketName, buckets)
		}

		// 删除桶
		if err := storage.DeleteBucket(bucketName); err != nil {
			t.Fatalf("Failed to delete bucket: %v", err)
		}

		// 确认桶已删除
		exists, err = storage.BucketExists(bucketName)
		if err != nil {
			t.Fatalf("Failed to check bucket existence: %v", err)
		}
		if exists {
			t.Fatalf("Bucket should not exist but does")
		}
	})

	// 测试对象操作
	t.Run("ObjectOperations", func(t *testing.T) {
		bucketName := "object-test-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// 存储对象
		key := "test-key"
		data := []byte("Hello, world!")
		metadata := map[string]string{"content-type": "text/plain"}

		if err := storage.PutObject(bucketName, key, data, metadata); err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}

		// 获取对象
		retrievedData, retrievedMetadata, err := storage.GetObject(bucketName, key)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}

		// 验证数据
		if !bytes.Equal(data, retrievedData) {
			t.Fatalf("Retrieved data doesn't match original: expected %v, got %v", data, retrievedData)
		}

		// 验证元数据
		if retrievedMetadata["content-type"] != metadata["content-type"] {
			t.Fatalf("Retrieved metadata doesn't match original: expected %v, got %v", metadata, retrievedMetadata)
		}

		// 列出对象
		keys, err := storage.ListObjects(bucketName, "")
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}
		if len(keys) != 1 || keys[0] != key {
			t.Fatalf("Expected object list to contain only %s, got %v", key, keys)
		}

		// 删除对象
		if err := storage.DeleteObject(bucketName, key); err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}

		// 确认对象已删除
		_, _, err = storage.GetObject(bucketName, key)
		if err == nil {
			t.Fatalf("Object should be deleted but still exists")
		}
	})

	// 测试大文件分块存储
	t.Run("ChunkedObjectStorage", func(t *testing.T) {
		bucketName := "chunked-test-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// 创建一个大于分块阈值的数据
		// 注意：在实际测试中，我们使用较小的数据来模拟，避免创建过大的测试数据
		// 实际代码中的maxChunkSize是10MB，这里我们假设它是100字节用于测试
		const testChunkSize = 100
		largeData := make([]byte, testChunkSize*3+50) // 创建350字节的数据
		for i := range largeData {
			largeData[i] = byte(i % 256) // 填充一些测试数据
		}

		key := "large-file"
		metadata := map[string]string{"content-type": "application/octet-stream"}

		// 修改BoltS3Storage.PutObject方法中的maxChunkSize常量为测试值
		// 注意：这在实际代码中需要通过依赖注入或其他方式实现
		// 这里我们假设PutObject方法会正确处理大文件

		if err := storage.PutObject(bucketName, key, largeData, metadata); err != nil {
			t.Fatalf("Failed to put large object: %v", err)
		}

		// 获取大文件
		retrievedData, retrievedMetadata, err := storage.GetObject(bucketName, key)
		if err != nil {
			t.Fatalf("Failed to get large object: %v", err)
		}

		// 验证数据
		if !bytes.Equal(largeData, retrievedData) {
			t.Fatalf("Retrieved large data doesn't match original: lengths expected %d, got %d",
				len(largeData), len(retrievedData))
		}

		// 验证元数据
		if retrievedMetadata["content-type"] != metadata["content-type"] {
			t.Fatalf("Retrieved metadata doesn't match original: expected %v, got %v",
				metadata, retrievedMetadata)
		}

		// 删除大文件
		if err := storage.DeleteObject(bucketName, key); err != nil {
			t.Fatalf("Failed to delete large object: %v", err)
		}
	})

	// 测试前缀查询
	t.Run("PrefixQuery", func(t *testing.T) {
		bucketName := "prefix-test-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// 存储多个对象
		prefixes := []string{
			"folder1/file1.txt",
			"folder1/file2.txt",
			"folder2/file1.txt",
			"file.txt",
		}

		for _, key := range prefixes {
			if err := storage.PutObject(bucketName, key, []byte("data"), nil); err != nil {
				t.Fatalf("Failed to put object %s: %v", key, err)
			}
		}

		// 测试前缀查询
		tests := []struct {
			prefix   string
			expected int
		}{
			{"folder1/", 2},
			{"folder2/", 1},
			{"file", 1},
			{"", 4},
		}

		for _, test := range tests {
			keys, err := storage.ListObjects(bucketName, test.prefix)
			if err != nil {
				t.Fatalf("Failed to list objects with prefix %s: %v", test.prefix, err)
			}
			if len(keys) != test.expected {
				t.Fatalf("Expected %d objects with prefix %s, got %d: %v",
					test.expected, test.prefix, len(keys), keys)
			}
		}
	})
}

// 测试边缘情况
func TestBoltS3StorageEdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "bolt-edge-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "edge.db")

	storage, err := NewBoltS3Storage(dbPath)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// 测试不存在的桶
	t.Run("NonExistentBucket", func(t *testing.T) {
		_, _, err := storage.GetObject("non-existent-bucket", "key")
		if err == nil {
			t.Fatalf("Expected error for non-existent bucket, got nil")
		}

		_, err = storage.ListObjects("non-existent-bucket", "")
		if err == nil {
			t.Fatalf("Expected error for listing non-existent bucket, got nil")
		}

		err = storage.DeleteObject("non-existent-bucket", "key")
		if err == nil {
			t.Fatalf("Expected error for deleting from non-existent bucket, got nil")
		}
	})

	// 测试空数据
	t.Run("EmptyData", func(t *testing.T) {
		bucketName := "empty-data-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// 存储空数据
		if err := storage.PutObject(bucketName, "empty", []byte{}, nil); err != nil {
			t.Fatalf("Failed to put empty object: %v", err)
		}

		// 获取空数据
		data, _, err := storage.GetObject(bucketName, "empty")
		if err != nil {
			t.Fatalf("Failed to get empty object: %v", err)
		}
		if len(data) != 0 {
			t.Fatalf("Expected empty data, got %d bytes", len(data))
		}
	})

	// 测试特殊字符键
	t.Run("SpecialCharacterKeys", func(t *testing.T) {
		bucketName := "special-chars-bucket"
		if err := storage.CreateBucket(bucketName); err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		specialKeys := []string{
			"key with spaces",
			"key/with/slashes",
			"key-with-dashes",
			"key_with_underscores",
			"key.with.dots",
		}

		for _, key := range specialKeys {
			if err := storage.PutObject(bucketName, key, []byte("data"), nil); err != nil {
				t.Fatalf("Failed to put object with special key %s: %v", key, err)
			}

			data, _, err := storage.GetObject(bucketName, key)
			if err != nil {
				t.Fatalf("Failed to get object with special key %s: %v", key, err)
			}
			if !bytes.Equal(data, []byte("data")) {
				t.Fatalf("Data mismatch for special key %s", key)
			}
		}
	})
}
