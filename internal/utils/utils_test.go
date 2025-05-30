package utils

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// TestGenerateUploadID 测试上传ID生成是否正确
func TestGenerateUploadID(t *testing.T) {
	id1 := GenerateUploadID()
	time.Sleep(10 * time.Millisecond) // 确保时间戳不同
	id2 := GenerateUploadID()

	// 验证生成的ID不为空
	if id1 == "" || id2 == "" {
		t.Error("生成的上传ID不应为空")
	}

	// 验证两次生成的ID不同
	if id1 == id2 {
		t.Error("两次生成的上传ID不应相同")
	}

	// 验证ID格式是否符合预期（时间戳-随机字符串）
	parts := strings.Split(id1, "-")
	if len(parts) != 2 {
		t.Errorf("上传ID格式不正确，应为'timestamp-randomstring'，实际为: %s", id1)
	}

	// 验证随机字符串部分长度是否为16
	if len(parts) == 2 && len(parts[1]) != 16 {
		t.Errorf("随机字符串部分长度应为16，实际为: %d", len(parts[1]))
	}
}

// TestRandomString 测试随机字符串生成
func TestRandomString(t *testing.T) {
	// 测试不同长度的随机字符串
	lengths := []int{5, 10, 16, 20}

	for _, length := range lengths {
		str := randomString(length)

		// 验证长度是否符合预期
		if len(str) != length {
			t.Errorf("随机字符串长度应为 %d，实际为: %d", length, len(str))
		}

		// 验证字符是否都在允许的字符集中
		const validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		for _, char := range str {
			if !strings.ContainsRune(validChars, char) {
				t.Errorf("随机字符串包含无效字符: %c", char)
			}
		}
	}

	// 验证两次生成的随机字符串不同
	str1 := randomString(16)
	str2 := randomString(16)
	if str1 == str2 {
		t.Error("两次生成的随机字符串不应相同")
	}
}

// TestGenerateUploadUUID 测试上传UUID生成
func TestGenerateUploadUUID(t *testing.T) {
	uuid1 := GenerateUploadUUID()
	uuid2 := GenerateUploadUUID()

	// 验证生成的UUID不为空
	if uuid1 == "" || uuid2 == "" {
		t.Error("生成的上传UUID不应为空")
	}

	// 验证两次生成的UUID不同
	if uuid1 == uuid2 {
		t.Error("两次生成的上传UUID不应相同")
	}

	// 验证UUID格式是否符合预期（upload-数字）
	if !strings.HasPrefix(uuid1, "upload-") {
		t.Errorf("上传UUID应以'upload-'开头，实际为: %s", uuid1)
	}
}

// TestComputeMD5 测试MD5哈希计算
func TestComputeMD5(t *testing.T) {
	// 测试用例
	testCases := []struct {
		input    []byte
		expected string
	}{
		{[]byte("hello world"), "5eb63bbbe01eeed093cb22bb8f5acdc3"},
		{[]byte(""), "d41d8cd98f00b204e9800998ecf8427e"}, // 空字符串的MD5
		{[]byte("测试中文"), "20bb5331c52a81f440b2b0b9882f9c42"},
	}

	for _, tc := range testCases {
		result := ComputeMD5(tc.input)
		if result != tc.expected {
			t.Errorf("ComputeMD5(%s) = %s; 期望: %s", tc.input, result, tc.expected)
		}
	}
}

// TestComputeSHA256 测试SHA256哈希计算
func TestComputeSHA256(t *testing.T) {
	// 测试用例
	testCases := []struct {
		input    []byte
		expected string
	}{
		{[]byte("hello world"), "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"},
		{[]byte(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}, // 空字符串的SHA256
		{[]byte("测试中文"), "72726d8818f693066ceb69afa364218b692e62ea92b385782363780f47529c21"},
	}

	for _, tc := range testCases {
		result := ComputeSHA256(tc.input)

		// 由于ComputeSHA256使用fmt.Sprintf("%x", hash)，我们需要确保比较的格式一致
		// 将预期结果转换为相同的格式进行比较
		hash := result
		expectedHash := tc.expected

		if hash != expectedHash {
			t.Errorf("ComputeSHA256(%s) = %s; 期望: %s", tc.input, hash, expectedHash)
		}

		// 验证结果是否为有效的SHA256哈希（64个十六进制字符）
		if len(hash) != 64 {
			t.Errorf("SHA256哈希长度应为64个字符，实际为: %d", len(hash))
		}

		// 验证结果是否为有效的十六进制字符串
		_, err := hex.DecodeString(hash)
		if err != nil {
			t.Errorf("SHA256哈希不是有效的十六进制字符串: %v", err)
		}
	}
}

// TestComputeMD5_Binary 测试MD5对二进制数据的处理
func TestComputeMD5_Binary(t *testing.T) {
	// 创建一个包含一些二进制数据的切片
	binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}

	// 计算MD5
	result := ComputeMD5(binaryData)

	// 手动计算预期结果进行比较
	expected := "c6c70c9d9aac432b9f22d34d69d8e5c1"

	if result != expected {
		t.Errorf("二进制数据的MD5计算错误，得到: %s, 期望: %s", result, expected)
	}
}

// TestComputeSHA256_Binary 测试SHA256对二进制数据的处理
func TestComputeSHA256_Binary(t *testing.T) {
	// 创建一个包含一些二进制数据的切片
	binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}

	// 计算SHA256
	result := ComputeSHA256(binaryData)

	// 手动计算预期结果进行比较
	expected := "1d5e11a6d1a69cce8f9f34e61d215f06a7a6f4b6ff584aa0d2a31144a0845b68"

	if result != expected {
		t.Errorf("二进制数据的SHA256计算错误，得到: %s, 期望: %s", result, expected)
	}
}

// TestRandomString_Uniqueness 测试随机字符串的唯一性
func TestRandomString_Uniqueness(t *testing.T) {
	// 生成多个随机字符串并检查唯一性
	count := 100
	length := 16
	strings := make(map[string]bool)

	for i := 0; i < count; i++ {
		str := randomString(length)
		if strings[str] {
			t.Errorf("发现重复的随机字符串: %s", str)
		}
		strings[str] = true
	}
}

// TestGenerateUploadID_Format 测试上传ID的格式
func TestGenerateUploadID_Format(t *testing.T) {
	for i := 0; i < 10; i++ {
		id := GenerateUploadID()
		parts := strings.Split(id, "-")

		// 验证格式
		if len(parts) != 2 {
			t.Errorf("上传ID格式不正确: %s", id)
			continue
		}

		// 验证时间戳部分是否为数字
		timestamp := parts[0]
		for _, char := range timestamp {
			if char < '0' || char > '9' {
				t.Errorf("时间戳部分包含非数字字符: %s", timestamp)
				break
			}
		}

		// 验证随机字符串部分长度
		randomPart := parts[1]
		if len(randomPart) != 16 {
			t.Errorf("随机字符串部分长度应为16，实际为: %d", len(randomPart))
		}
	}
}
