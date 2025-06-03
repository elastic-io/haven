package registry

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// 测试创建新的上传数据
func TestNewUploadData(t *testing.T) {
	data, err := NewUploadData()
	if err != nil {
		t.Fatalf("Failed to create new upload data: %v", err)
	}
	if data == nil {
		t.Fatal("Expected non-nil UploadData")
	}
	if !data.useMemory {
		t.Error("New UploadData should use memory by default")
	}
	defer data.Close()
}

// 测试小数据的追加和获取
func TestUploadDataSmallData(t *testing.T) {
	data, err := NewUploadData()
	if err != nil {
		t.Fatalf("Failed to create new upload data: %v", err)
	}
	defer data.Close()

	testData := []byte("Hello, World!")
	if err := data.AppendData(testData); err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	if data.Size() != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), data.Size())
	}

	retrieved, err := data.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(retrieved, testData) {
		t.Errorf("Retrieved data doesn't match original data")
	}

	// 确认仍然使用内存存储
	if !data.useMemory {
		t.Error("Should still be using memory for small data")
	}
}

// 测试大数据的追加和获取（超过内存阈值，应切换到文件存储）
func TestUploadDataLargeData(t *testing.T) {
	data, err := NewUploadData()
	if err != nil {
		t.Fatalf("Failed to create new upload data: %v", err)
	}
	defer data.Close()

	// 创建一个比maxMemSize大的数据块
	largeSize := data.maxMemSize + 1024*1024 // 比最大内存大小多1MB
	largeData := make([]byte, largeSize)
	if _, err := rand.Read(largeData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// 分批追加数据，模拟实际使用场景
	chunkSize := 1024 * 1024 // 1MB chunks
	for i := 0; i < len(largeData); i += chunkSize {
		end := i + chunkSize
		if end > len(largeData) {
			end = len(largeData)
		}
		if err := data.AppendData(largeData[i:end]); err != nil {
			t.Fatalf("Failed to append data chunk: %v", err)
		}
	}

	// 验证大小
	if data.Size() != int64(len(largeData)) {
		t.Errorf("Expected size %d, got %d", len(largeData), data.Size())
	}

	// 验证已切换到文件存储
	if data.useMemory {
		t.Error("Should have switched to file storage for large data")
	}
	if data.tempFile == nil {
		t.Error("Temp file should be created for large data")
	}

	// 获取并验证数据
	retrieved, err := data.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(retrieved, largeData) {
		t.Errorf("Retrieved data doesn't match original large data")
	}
}

// 测试多次追加数据
func TestUploadDataMultipleAppends(t *testing.T) {
	data, err := NewUploadData()
	if err != nil {
		t.Fatalf("Failed to create new upload data: %v", err)
	}
	defer data.Close()

	// 追加多个数据块
	testData1 := []byte("First chunk of data")
	testData2 := []byte("Second chunk of data")
	testData3 := []byte("Third chunk of data")

	if err := data.AppendData(testData1); err != nil {
		t.Fatalf("Failed to append first data chunk: %v", err)
	}
	if err := data.AppendData(testData2); err != nil {
		t.Fatalf("Failed to append second data chunk: %v", err)
	}
	if err := data.AppendData(testData3); err != nil {
		t.Fatalf("Failed to append third data chunk: %v", err)
	}

	// 验证大小
	expectedSize := int64(len(testData1) + len(testData2) + len(testData3))
	if data.Size() != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, data.Size())
	}

	// 验证数据
	expectedData := append(append(testData1, testData2...), testData3...)
	retrieved, err := data.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(retrieved, expectedData) {
		t.Errorf("Retrieved data doesn't match expected combined data")
	}
}

// 测试全局上传数据存储功能
func TestGlobalUploadDataStorage(t *testing.T) {
	// 清理可能存在的测试数据
	repository := "test-repo"
	uuid := "test-uuid"
	removeUploadData(repository, uuid)

	// 获取或创建上传数据
	data := getOrCreateUploadData(repository, uuid)
	if data == nil {
		t.Fatal("Failed to create upload data")
	}

	// 添加一些数据
	testData := []byte("Test data for global storage")
	if err := data.AppendData(testData); err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	// 再次获取同一个上传数据
	sameData := getUploadData(repository, uuid)
	if sameData == nil {
		t.Fatal("Failed to get existing upload data")
	}

	// 验证是否是同一个实例
	if sameData != data {
		t.Error("getUploadData should return the same instance as getOrCreateUploadData")
	}

	// 验证数据
	retrieved, err := sameData.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(retrieved, testData) {
		t.Errorf("Retrieved data doesn't match original data")
	}

	// 测试移除上传数据
	removeUploadData(repository, uuid)

	// 验证数据已被移除
	removedData := getUploadData(repository, uuid)
	if removedData != nil {
		t.Error("Upload data should have been removed")
	}
}

// 测试并发安全性
func TestConcurrentAccess(t *testing.T) {
	data, err := NewUploadData()
	if err != nil {
		t.Fatalf("Failed to create new upload data: %v", err)
	}
	defer data.Close()

	// 创建多个goroutine同时访问
	const goroutines = 10
	const iterations = 100

	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < iterations; j++ {
				// 追加数据
				testData := []byte("Concurrent test data")
				if err := data.AppendData(testData); err != nil {
					t.Errorf("Goroutine %d failed to append data: %v", id, err)
				}

				// 获取大小
				_ = data.Size()

				// 获取数据
				_, err := data.GetData()
				if err != nil {
					t.Errorf("Goroutine %d failed to get data: %v", id, err)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < goroutines; i++ {
		<-done
	}
}
