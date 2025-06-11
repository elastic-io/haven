package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/google/uuid"
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

const (
	HEX          = "hex"
	BASE32       = "base32"
	ALPHANUMERIC = "alphanumeric"
)

func UID(style string, length int) string {
	uid := uuid.New()

	switch style {
	case HEX:
		// 十六进制风格
		noDash := strings.ReplaceAll(uid.String(), "-", "")
		if len(noDash) >= length {
			return noDash[:length]
		}
		return noDash

	case BASE32:
		// Base32 风格
		encoded := base32.StdEncoding.EncodeToString(uid[:])
		clean := strings.ToLower(strings.TrimRight(encoded, "="))
		if len(clean) >= length {
			return clean[:length]
		}
		return clean

	case ALPHANUMERIC:
		// 纯字母数字
		noDash := strings.ReplaceAll(uid.String(), "-", "")
		// 移除数字，只保留字母
		var result strings.Builder
		for _, char := range noDash {
			if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
				result.WriteRune(char)
				if result.Len() >= length {
					break
				}
			}
		}
		return result.String()

	default:
		// 默认截取
		return uid.String()[:length]
	}
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

func MustParseSize(s string) int {
	ls := len(s)
	res, err := ParseSize(s[0:ls-1], s[ls-1:])
	if err != nil {
		panic(err)
	}
	return res
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

func GetFileID(filename string) (uint64, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(filename, &stat)
	if err != nil {
		return 0, err
	}
	return stat.Ino, nil
}

func MustGetFileID(filename string) uint64 {
	id, _ := GetFileID(filename)
	return id
}

func CalculateLargeFileSHA256(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()

	// 使用缓冲区分块读取
	buffer := make([]byte, 8*types.KB) // 8KB 缓冲区

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return "", err
		}

		if n == 0 {
			break
		}

		// 写入到 hasher
		hasher.Write(buffer[:n])
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func FindKey(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
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
