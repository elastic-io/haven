package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "haven-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	testCases := []struct {
		name        string
		opts        *options.Options
		expectError bool
	}{
		{
			name: "valid options",
			opts: &options.Options{
				DataDir: tempDir,
				RepoId:  "test-repo",
				Backend: "memory", // 使用内存存储后端进行测试
				Config:  &config.Config{},
			},
			expectError: false,
		},
		{
			name: "invalid backend",
			opts: &options.Options{
				DataDir: tempDir,
				RepoId:  "test-repo",
				Backend: "invalid-backend",
				Config:  &config.Config{},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := New(tc.opts)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, app)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, app)
				assert.Equal(t, tc.opts, app.opts)
				assert.Equal(t, "repo.db", app.dbname)
				assert.NotNil(t, app.storage)

				// 验证数据目录是否已创建
				_, err := os.Stat(tc.opts.DataDir)
				assert.NoError(t, err)

				// 测试完成后关闭应用
				err = app.Stop()
				assert.NoError(t, err)
			}
		})
	}
}

func TestAppLifecycle(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "haven-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建一个带有模拟服务器的测试应用
	opts := &options.Options{
		DataDir: tempDir,
		RepoId:  "test-repo",
		Backend: "memory",
		Config: &config.Config{
			Endpoint: "localhost:0", // 使用随机端口
		},
	}

	app, err := New(opts)
	require.NoError(t, err)

	// 在一个 goroutine 中运行服务器
	errCh := make(chan error, 1)
	go func() {
		errCh <- app.Run()
	}()

	// 给服务器一点时间启动
	// 在实际测试中，可能需要等待服务器准备就绪的信号

	// 停止应用
	err = app.Stop()
	assert.NoError(t, err)

	// 检查服务器是否正常退出
	select {
	case err := <-errCh:
		// 服务器可能会返回错误，这取决于 api.Server.Done() 的实现
		// 这里我们不对错误进行断言，因为它可能是预期的关闭错误
		t.Logf("Server exited with: %v", err)
	default:
		t.Log("Server did not exit properly")
		t.Fail()
	}
}

func TestDataDirCreation(t *testing.T) {
	// 创建一个不存在的目录路径
	tempDir, err := os.MkdirTemp("", "haven-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	nonExistentDir := filepath.Join(tempDir, "non-existent")

	opts := &options.Options{
		DataDir: nonExistentDir,
		RepoId:  "test-repo",
		Backend: "memory",
		Config:  &config.Config{},
	}

	app, err := New(opts)
	require.NoError(t, err)

	// 验证目录是否已创建
	_, err = os.Stat(nonExistentDir)
	assert.NoError(t, err)

	err = app.Stop()
	assert.NoError(t, err)
}
