package clients

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试配置
const (
	testRegion   = "us-east-1"
	testEndpoint = "https://127.0.0.1:8900/s3"
	testAK       = "havenadmin"
	testSK       = "havenadmin"
	testToken    = ""
	testBucket   = "test-bucket"
)

// 设置测试环境
func setupTestS3Client(t *testing.T) S3Client {
	client, err := News3Client(testAK, testSK, testToken, testRegion, testEndpoint, S3Options{
		S3ForcePathStyle:   true,
		DisableSSL:         true,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err, "Failed to create test bucket")
	require.NotNil(t, client, "S3 client should not be nil")

	// 创建测试桶
	err = client.CreateBucket(testBucket)
	require.NoError(t, err, "Failed to create test bucket")

	return client
}

// 清理测试环境
func cleanupTestS3Client(t *testing.T, client S3Client) {
	// 列出并删除所有对象
	objects, err := client.ListObjectsInBucket(testBucket)
	require.NoError(t, err, "Failed to list objects")

	for _, obj := range objects {
		err = client.DeleteObject(testBucket, obj)
		require.NoError(t, err, "Failed to delete object: %s", obj)
	}

	err = client.DeleteBucket(testBucket)
	require.NoError(t, err, "Failed to delete bucket: %s", testBucket)
}

func TestUploadAndDownloadObject(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 测试数据
	objectKey := "test-object.txt"
	content := []byte("Hello, S3 testing!")

	// 上传对象
	err := client.UploadObject(testBucket, objectKey, content)
	assert.NoError(t, err, "Failed to upload object")

	// 下载对象
	downloadedContent, err := client.DownloadObject(testBucket, objectKey)
	assert.NoError(t, err, "Failed to download object")
	assert.Equal(t, content, downloadedContent, "Downloaded content doesn't match uploaded content")
}

func TestUploadObjectStream(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 创建临时文件
	tempFile, err := ioutil.TempFile("", "s3-test-*.txt")
	require.NoError(t, err, "Failed to create temp file")
	defer os.Remove(tempFile.Name())

	// 写入测试数据
	testContent := []byte("This is a test file for streaming upload")
	_, err = tempFile.Write(testContent)
	require.NoError(t, err, "Failed to write to temp file")
	tempFile.Close()

	// 上传文件
	objectKey := "test-stream-object.txt"
	err = client.UploadObjectStream(testBucket, objectKey, tempFile.Name())
	assert.NoError(t, err, "Failed to upload object stream")

	// 验证上传成功
	downloadedContent, err := client.DownloadObject(testBucket, objectKey)
	assert.NoError(t, err, "Failed to download streamed object")
	assert.Equal(t, testContent, downloadedContent, "Downloaded content doesn't match uploaded content")
}

func TestCopyObject(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 上传源对象
	sourceKey := "source-object.txt"
	content := []byte("This is the source object")
	err := client.UploadObject(testBucket, sourceKey, content)
	require.NoError(t, err, "Failed to upload source object")

	// 复制对象
	destinationKey := "destination-object.txt"
	err = client.CopyObject(testBucket, sourceKey, testBucket, destinationKey)
	assert.NoError(t, err, "Failed to copy object")

	// 验证复制成功
	downloadedContent, err := client.DownloadObject(testBucket, destinationKey)
	assert.NoError(t, err, "Failed to download copied object")
	assert.Equal(t, content, downloadedContent, "Copied content doesn't match source content")
}

func TestDeleteObject(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 上传对象
	objectKey := "to-be-deleted.txt"
	content := []byte("This object will be deleted")
	err := client.UploadObject(testBucket, objectKey, content)
	require.NoError(t, err, "Failed to upload object")

	// 删除对象
	err = client.DeleteObject(testBucket, objectKey)
	assert.NoError(t, err, "Failed to delete object")

	// 验证对象已删除
	objects, err := client.ListObjectsInBucket(testBucket)
	assert.NoError(t, err, "Failed to list objects")
	assert.NotContains(t, objects, objectKey, "Object should have been deleted")
}

func TestListBuckets(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 列出所有桶
	buckets, err := client.ListBuckets()
	assert.NoError(t, err, "Failed to list buckets")
	assert.Contains(t, buckets, testBucket, "Test bucket should be in the list")
}

func TestListObjectsInBucket(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 上传多个对象
	objectKeys := []string{"object1.txt", "object2.txt", "object3.txt"}
	for i, key := range objectKeys {
		content := []byte("Content " + strconv.Itoa(i))
		err := client.UploadObject(testBucket, key, content)
		require.NoError(t, err, "Failed to upload object %s", key)
	}

	// 列出所有对象
	objects, err := client.ListObjectsInBucket(testBucket)
	assert.NoError(t, err, "Failed to list objects")

	// 验证所有对象都在列表中
	for _, key := range objectKeys {
		assert.Contains(t, objects, key, "Object %s should be in the list", key)
	}
}

func TestListObjectsInBucketDir(t *testing.T) {
	t.Skip("skip TestListObjectsInBucketDir")
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 上传不同目录的对象
	dirObjects := map[string][]string{
		"dir1/": {"dir1/file1.txt", "dir1/file2.txt"},
		"dir2/": {"dir2/file1.txt", "dir2/subdir/file.txt"},
	}

	for _, objects := range dirObjects {
		for _, key := range objects {
			err := client.UploadObject(testBucket, key, []byte("Content"))
			require.NoError(t, err, "Failed to upload object %s", key)
		}
	}

	// 测试每个目录
	for dir, expectedObjects := range dirObjects {
		objects, err := client.ListObjectsInBucketDir(testBucket, dir)
		assert.NoError(t, err, "Failed to list objects in directory %s", dir)

		// 验证目录中的所有对象
		for _, key := range expectedObjects {
			assert.Contains(t, objects, key, "Object %s should be in directory %s", key, dir)
		}

		// 验证其他目录的对象不在列表中
		for otherDir, otherObjects := range dirObjects {
			if otherDir != dir {
				for _, key := range otherObjects {
					assert.NotContains(t, objects, key, "Object %s should not be in directory %s", key, dir)
				}
			}
		}
	}
}

func TestGeneratePresignedURL(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 上传对象
	objectKey := "presigned-url-test.txt"
	content := []byte("Test content for presigned URL")
	err := client.UploadObject(testBucket, objectKey, content)
	require.NoError(t, err, "Failed to upload object")

	// 生成预签名URL
	url, err := client.GeneratePresignedURL(testBucket, objectKey, 3600)
	assert.NoError(t, err, "Failed to generate presigned URL")
	assert.NotEmpty(t, url, "Presigned URL should not be empty")

	// 注意：这里不测试URL的实际访问，因为这需要HTTP客户端
}

func TestIsBucketPubliclyReadable(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 测试桶是否可公开读取
	isReadable, err := client.IsBucketPubliclyReadable(testBucket)
	assert.NoError(t, err, "Failed to check if bucket is publicly readable")

	// 注意：这个测试结果取决于测试环境中桶的实际配置
	t.Logf("Bucket %s is publicly readable: %v", testBucket, isReadable)
}

func TestIsBucketPubliclyWritable(t *testing.T) {
	client := setupTestS3Client(t)
	defer cleanupTestS3Client(t, client)

	// 测试桶是否可公开写入
	isWritable, err := client.IsBucketPubliclyWritable(testBucket)
	assert.NoError(t, err, "Failed to check if bucket is publicly writable")

	// 注意：这个测试结果取决于测试环境中桶的实际配置
	t.Logf("Bucket %s is publicly writable: %v", testBucket, isWritable)
}
