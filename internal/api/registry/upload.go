package registry

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/elastic-io/haven/internal/log"
)

// UploadData 结构，支持流式处理
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

// AppendData 追加字节数据
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
			u.size = int64(len(u.memoryData))
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

// AppendStream 从流中追加数据 - 新增方法
func (u *UploadData) AppendStream(reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("nil reader provided")
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// 如果当前使用内存存储，直接切换到文件存储以处理流式数据
	if u.useMemory {
		// 创建临时文件
		tempFile, err := os.CreateTemp("", "upload-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}

		// 写入已有数据
		if len(u.memoryData) > 0 {
			if _, err := tempFile.Write(u.memoryData); err != nil {
				tempFile.Close()
				os.Remove(tempFile.Name())
				return fmt.Errorf("failed to write existing data: %w", err)
			}
		}

		// 设置文件和状态
		u.tempFile = tempFile
		u.size = int64(len(u.memoryData))
		u.useMemory = false
		u.memoryData = nil // 释放内存
	}

	// 使用固定大小的缓冲区安全地复制数据
	buf := make([]byte, 32*1024) // 32KB 缓冲区
	totalWritten := int64(0)

	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read error: %w", err)
		}

		if n > 0 {
			nw, err := u.tempFile.Write(buf[:n])
			if err != nil {
				return fmt.Errorf("write error: %w", err)
			}
			if nw != n {
				return fmt.Errorf("short write: wrote %d bytes out of %d", nw, n)
			}
			totalWritten += int64(nw)
		}

		if err == io.EOF {
			break
		}
	}

	u.size += totalWritten
	return nil
}

// GetData 获取所有数据 - 可能导致内存问题的方法
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

// WriteTo 流式写入到目标 - 新增方法
func (u *UploadData) WriteTo(writer io.Writer) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.useMemory {
		_, err := writer.Write(u.memoryData)
		return err
	}

	// 重置文件指针
	if _, err := u.tempFile.Seek(0, 0); err != nil {
		return err
	}

	// 流式复制
	_, err := io.Copy(writer, u.tempFile)
	return err
}

// CopyToFile 流式复制到文件 - 新增方法
func (u *UploadData) CopyToFile(filePath string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// 创建目标文件
	destFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	if u.useMemory {
		_, err := destFile.Write(u.memoryData)
		return err
	}

	// 重置文件指针
	if _, err := u.tempFile.Seek(0, 0); err != nil {
		return err
	}

	// 流式复制
	_, err = io.Copy(destFile, u.tempFile)
	return err
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

// GetReader 获取数据读取器 - 新增方法
func (u *UploadData) GetReader() (io.ReadCloser, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.useMemory {
		return io.NopCloser(bytes.NewReader(u.memoryData)), nil
	}

	// 复制文件路径，创建新的文件句柄
	filePath := u.tempFile.Name()
	reader, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return reader, nil
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