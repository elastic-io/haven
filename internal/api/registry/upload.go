package registry

import (
	"io"
	"os"
	"sync"

	"github.com/elastic-io/haven/internal/log"
)

// 上传数据结构，使用临时文件存储大数据
type UploadData struct {
	tempFile   *os.File
	size       int64
	mu         sync.Mutex
	useMemory  bool
	memoryData []byte
	maxMemSize int
}

func NewUploadData() (*UploadData, error) {
	return &UploadData{
		memoryData: make([]byte, 0, 10*1024*1024), // 初始分配10MB内存
		useMemory:  true,
		maxMemSize: 50 * 1024 * 1024, // 50MB以下使用内存，超过则使用临时文件
	}, nil
}

func (u *UploadData) AppendData(data []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// 如果当前使用内存存储，检查是否需要切换到文件存储
	if u.useMemory {
		if len(u.memoryData)+len(data) > u.maxMemSize {
			// 需要切换到文件存储
			tempFile, err := os.CreateTemp("", "upload-*")
			if err != nil {
				return err
			}

			// 写入已有数据
			if _, err := tempFile.Write(u.memoryData); err != nil {
				tempFile.Close()
				os.Remove(tempFile.Name())
				return err
			}

			u.tempFile = tempFile
			u.size = int64(len(u.memoryData))
			u.useMemory = false
			u.memoryData = nil // 释放内存
		} else {
			// 继续使用内存存储
			u.memoryData = append(u.memoryData, data...)
			return nil
		}
	}

	// 写入文件
	n, err := u.tempFile.Write(data)
	if err != nil {
		return err
	}

	u.size += int64(n)
	return nil
}

func (u *UploadData) GetData() ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.useMemory {
		result := make([]byte, len(u.memoryData))
		copy(result, u.memoryData)
		return result, nil
	}

	// 重置文件指针
	if _, err := u.tempFile.Seek(0, 0); err != nil {
		return nil, err
	}

	// 读取整个文件
	data := make([]byte, u.size)
	if _, err := io.ReadFull(u.tempFile, data); err != nil {
		return nil, err
	}

	return data, nil
}

func (u *UploadData) Size() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.useMemory {
		return int64(len(u.memoryData))
	}
	return u.size
}

func (u *UploadData) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.useMemory && u.tempFile != nil {
		tempFileName := u.tempFile.Name()
		u.tempFile.Close()
		os.Remove(tempFileName)
		u.tempFile = nil
	}

	u.memoryData = nil
	return nil
}

// 全局上传数据存储
var (
	uploadDataMap = make(map[string]*UploadData)
	uploadDataMu  sync.Mutex
)

// 获取或创建上传数据
func getOrCreateUploadData(repository, uuid string) *UploadData {
	key := repository + ":" + uuid

	uploadDataMu.Lock()
	defer uploadDataMu.Unlock()

	if data, exists := uploadDataMap[key]; exists {
		return data
	}

	data, err := NewUploadData()
	if err != nil {
		log.Logger.Error("Failed to create upload data: ", err)
		return nil
	}

	uploadDataMap[key] = data
	return data
}

// 获取上传数据
func getUploadData(repository, uuid string) *UploadData {
	key := repository + ":" + uuid

	uploadDataMu.Lock()
	defer uploadDataMu.Unlock()

	return uploadDataMap[key]
}

// 移除上传数据
func removeUploadData(repository, uuid string) {
	key := repository + ":" + uuid

	uploadDataMu.Lock()
	defer uploadDataMu.Unlock()

	if data, exists := uploadDataMap[key]; exists {
		data.Close()
		delete(uploadDataMap, key)
	}
}
