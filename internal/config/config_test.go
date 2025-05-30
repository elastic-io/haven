package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli"
)

func TestNew(t *testing.T) {
	// 创建一个新的 CLI 应用
	app := cli.NewApp()

	// 定义全局标志
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "endpoint", Value: ""},
		cli.BoolFlag{Name: "auth"},
		cli.StringFlag{Name: "username", Value: ""},
		cli.StringFlag{Name: "cert", Value: ""},
		cli.StringFlag{Name: "key", Value: ""},
		cli.StringSliceFlag{Name: "mod", Value: &cli.StringSlice{}},
	}

	// 模拟命令行参数
	args := []string{
		"app",
		"--endpoint=http://localhost:8080",
		"--auth",
		"--username=testuser",
		"--cert=/path/to/cert.pem",
		"--key=/path/to/key.pem",
		"--mod=module1",
		"--mod=module2",
	}

	// 运行应用以获取上下文
	var capturedContext *cli.Context
	app.Action = func(c *cli.Context) error {
		capturedContext = c
		return nil
	}

	err := app.Run(args)
	assert.NoError(t, err)

	// 使用捕获的上下文测试 New 函数
	config := New(capturedContext)

	// 验证配置值
	assert.Equal(t, "http://localhost:8080", config.Endpoint)
	assert.True(t, config.EnableAuth)
	assert.Equal(t, "testuser", config.Username)
	assert.Equal(t, "/path/to/cert.pem", config.CertFile)
	assert.Equal(t, "/path/to/key.pem", config.KeyFile)
	assert.Equal(t, []string{"module1", "module2"}, config.Modules)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid config",
			config: &Config{
				Endpoint: "http://localhost:8080",
				Modules:  []string{"module1"},
			},
			expectError: false,
		},
		{
			name: "Missing endpoint",
			config: &Config{
				Modules: []string{"module1"},
			},
			expectError: true,
			errorMsg:    "endpoint is required",
		},
		{
			name: "No modules",
			config: &Config{
				Endpoint: "http://localhost:8080",
				Modules:  []string{},
			},
			expectError: true,
			errorMsg:    "at least one module is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
