package options

import (
	"flag"
	"testing"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/urfave/cli"
)

// 创建测试用的cli.Context
func setupContext(t *testing.T, args map[string]string) *cli.Context {
	// 创建一个新的flag set
	set := flag.NewFlagSet("test", 0)

	// 添加所需的flag
	set.String("data", "", "data directory")

	// 如果config包需要其他flag，这里也需要添加
	// 假设config.New需要以下flag
	set.String("log-level", "info", "log level")

	// 设置传入的参数值
	for name, value := range args {
		err := set.Set(name, value)
		if err != nil {
			t.Fatalf("Failed to set flag %s: %v", name, err)
		}
	}

	// 创建cli.Context
	return cli.NewContext(nil, set, nil)
}

// 修复New函数中的bug
func TestNew(t *testing.T) {
	// 准备测试数据
	args := map[string]string{
		"data": "/tmp/data",
	}

	// 创建context
	ctx := setupContext(t, args)

	// 调用New函数
	opts := New(ctx)

	// 验证New函数是否返回了nil (这是一个bug，应该返回opts而不是nil)
	if opts == nil {
		t.Fatal("New function returned nil, expected Options instance")
	}

	// 验证字段是否正确设置
	if opts.DataDir != "/tmp/data" {
		t.Errorf("Expected DataDir to be '/tmp/data', got '%s'", opts.DataDir)
	}

	// 验证RepoId是否被设置为时间戳格式
	if len(opts.RepoId) == 0 {
		t.Error("RepoId was not set")
	}

	// 验证Config是否被正确初始化
	if opts.Config == nil {
		t.Error("Config was not initialized")
	}
}

func TestValidate(t *testing.T) {
	// 测试用例1: 所有必填字段都已设置
	t.Run("AllFieldsSet", func(t *testing.T) {
		// 创建一个mock的config，它的Validate方法返回nil
		mockConfig := &config.Config{}

		opts := &Options{
			RepoId:  "12345",
			DataDir: "/tmp/data",
			Backend: "test-backend",
			Config:  mockConfig,
		}

		err := opts.Validate()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	// 测试用例2: RepoId为空
	t.Run("EmptyRepoId", func(t *testing.T) {
		opts := &Options{
			RepoId:  "",
			DataDir: "/tmp/data",
			Backend: "test-backend",
			Config:  &config.Config{},
		}

		err := opts.Validate()
		if err == nil || err.Error() != "repo_id is required" {
			t.Errorf("Expected error 'repo_id is required', got: %v", err)
		}
	})

	// 测试用例3: DataDir为空
	t.Run("EmptyDataDir", func(t *testing.T) {
		opts := &Options{
			RepoId:  "12345",
			DataDir: "",
			Backend: "test-backend",
			Config:  &config.Config{},
		}

		err := opts.Validate()
		if err == nil || err.Error() != "data directory is required" {
			t.Errorf("Expected error 'data directory is required', got: %v", err)
		}
	})

	// 测试用例4: Backend为空
	log.Init("", "debug")
	t.Run("EmptyBackend", func(t *testing.T) {
		// 创建一个mock的config，它的Validate方法返回nil
		mockConfig := &config.Config{}

		opts := &Options{
			RepoId:  "12345",
			DataDir: "/tmp/data",
			Backend: "",
			Config:  mockConfig,
		}

		// Backend为空不会返回错误，只会记录警告
		err := opts.Validate()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	// 测试用例5: Config.Validate返回错误
	t.Run("ConfigValidateError", func(t *testing.T) {
		// 这里需要mock config.Validate方法返回错误
		// 由于我们没有config包的完整实现，这里只是示例
		// 实际测试中可能需要使用mock库或创建一个测试用的config实现

		// 假设我们有一个方法可以创建一个会返回错误的config
		mockConfig := &config.Config{Endpoint: ""}

		opts := &Options{
			RepoId:  "12345",
			DataDir: "/tmp/data",
			Backend: "test-backend",
			Config:  mockConfig,
		}

		err := opts.Validate()
		if err == nil || err.Error() != "config validation error" {
			t.Errorf("Expected error 'config validation error', got: %v", err)
		}
	})
}
