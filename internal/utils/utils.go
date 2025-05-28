package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"
)

// 生成唯一的上传 ID
func GenerateUploadID() string {
    return fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomString(16))
}

// 生成随机字符串
func randomString(length int) string {
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    result := make([]byte, length)
    for i := range result {
        result[i] = chars[time.Now().UnixNano()%int64(len(chars))]
        time.Sleep(1 * time.Nanosecond)
    }
    return string(result)
}

func GenerateUploadUUID() string {
    rand.Seed(time.Now().UnixNano())
    return fmt.Sprintf("upload-%d", rand.Int63())
}

// 计算 MD5 哈希
func ComputeMD5(data []byte) string {
    hash := md5.Sum(data)
    return hex.EncodeToString(hash[:])
}

// 辅助函数：计算SHA256哈希
func ComputeSHA256(data []byte) string {
    hash := sha256.Sum256(data)
    return fmt.Sprintf("%x", hash)
}