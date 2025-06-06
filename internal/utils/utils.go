package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic-io/haven/internal/log"
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

func ParseSize(s, unit string) (int, error) {
	sz := strings.TrimRight(s, "gGmMkK")
	if len(sz) == 0 {
		return -1, fmt.Errorf("%q:can't parse as num[gGmMkK]:%w", s, strconv.ErrSyntax)
	}
	amt, err := strconv.ParseUint(sz, 0, 0)
	if err != nil {
		return -1, err
	}
	if len(s) > len(sz) {
		unit = s[len(sz):]
	}
	switch unit {
	case "G", "g":
		return int(amt) << 30, nil
	case "M", "m":
		return int(amt) << 20, nil
	case "K", "k":
		return int(amt) << 10, nil
	case "":
		return int(amt), nil
	}
	return -1, fmt.Errorf("can not parse %q as num[gGmMkK]:%w", s, strconv.ErrSyntax)
}

func FileExist(file string) bool {
	_, err := os.Stat(file)
	if err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	}
	panic(err)
}

func SafeGo(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error(fmt.Errorf("goroutine panic: %v\n", r), "goroutine panic")
			}
		}()
		fn()
	}()
}

func SafeJobWrapper(job func(), funcName string) func() {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error(fmt.Errorf("panic reason: %v", r), "cron task recovered from panic", "funcName", funcName)
			}
		}()
		job()
	}
}
