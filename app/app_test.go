package app

import (
	"errors"
	"flag"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/elastic-io/haven/internal/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/urfave/cli"
)

// MockApp 实现 App 接口用于测试
type MockApp struct {
	mock.Mock
}

func (m *MockApp) Run() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockApp) Stop() error {
	args := m.Called()
	return args.Error(0)
}

// 创建测试用的 cli.Context
func createTestContext() *cli.Context {
	app := cli.NewApp()
	app.Name = "test-app"

	// 创建一个空的 flag.FlagSet
	set := flag.NewFlagSet("test", flag.ContinueOnError)

	// 使用 cli.NewContext 创建上下文
	return cli.NewContext(app, set, nil)
}

// 或者更简单的方式，如果不需要特定的 flags
func createSimpleTestContext() *cli.Context {
	app := cli.NewApp()
	return cli.NewContext(app, nil, nil)
}

func TestMain_ProgramError(t *testing.T) {
	ctx := createTestContext()
	expectedErr := errors.New("program initialization error")

	program := func(opts *options.Options) (App, error) {
		return nil, expectedErr
	}

	err := Main(ctx, program, "test-app")
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestMain_AppRunError(t *testing.T) {
	ctx := createTestContext()
	mockApp := new(MockApp)
	runErr := errors.New("app run error")

	mockApp.On("Run").Return(runErr)
	mockApp.On("Stop").Return(nil)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	err := Main(ctx, program, "test-app")

	assert.Error(t, err)
	assert.Equal(t, runErr, err)
	mockApp.AssertExpectations(t)
}

func TestMain_AppStopError(t *testing.T) {
	ctx := createTestContext()
	mockApp := new(MockApp)
	stopErr := errors.New("app stop error")

	// 模拟 Run 立即完成（无错误）
	mockApp.On("Run").Return(nil)
	mockApp.On("Stop").Return(stopErr)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	err := Main(ctx, program, "test-app")

	assert.Error(t, err)
	assert.Equal(t, stopErr, err)
	mockApp.AssertExpectations(t)
}

func TestMain_SuccessfulRunAndStop(t *testing.T) {
	ctx := createTestContext()
	mockApp := new(MockApp)

	mockApp.On("Run").Return(nil)
	mockApp.On("Stop").Return(nil)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	err := Main(ctx, program, "test-app")

	assert.NoError(t, err)
	mockApp.AssertExpectations(t)
}

func TestMain_SignalHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过信号测试在短测试模式下")
	}

	ctx := createTestContext()
	mockApp := new(MockApp)

	// 模拟长时间运行的应用
	mockApp.On("Run").Run(func(args mock.Arguments) {
		// 模拟应用运行一段时间
		time.Sleep(200 * time.Millisecond)
	}).Return(nil)
	mockApp.On("Stop").Return(nil)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	// 在另一个 goroutine 中发送信号
	go func() {
		time.Sleep(100 * time.Millisecond)
		// 发送 SIGTERM 信号给当前进程
		if process, err := os.FindProcess(os.Getpid()); err == nil {
			process.Signal(syscall.SIGTERM)
		}
	}()

	start := time.Now()
	err := Main(ctx, program, "test-app")
	duration := time.Since(start)

	assert.NoError(t, err)
	// 应该在信号处理后快速完成
	assert.True(t, duration < 500*time.Millisecond)
	mockApp.AssertExpectations(t)
}

func TestMain_StopTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过超时测试在短测试模式下")
	}

	ctx := createTestContext()
	mockApp := new(MockApp)

	mockApp.On("Run").Return(nil)
	// 模拟 Stop 方法超时 - 使用较短的时间进行测试
	mockApp.On("Stop").Run(func(args mock.Arguments) {
		time.Sleep(12 * time.Second) // 超过 10 秒超时时间
	}).Return(nil)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	start := time.Now()
	err := Main(ctx, program, "test-app")
	duration := time.Since(start)

	// 应该在大约 10 秒后超时
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.True(t, duration >= 10*time.Second)
	assert.True(t, duration < 12*time.Second) // 给一些缓冲时间
}

func TestMain_LongRunningApp(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过长时间运行测试在短测试模式下")
	}

	ctx := createTestContext()
	mockApp := new(MockApp)

	// 模拟长时间运行的应用，但会被信号中断
	mockApp.On("Run").Run(func(args mock.Arguments) {
		time.Sleep(3 * time.Second)
	}).Return(nil)
	mockApp.On("Stop").Return(nil)

	program := func(opts *options.Options) (App, error) {
		return mockApp, nil
	}

	// 在 500ms 后发送中断信号
	go func() {
		time.Sleep(500 * time.Millisecond)
		if process, err := os.FindProcess(os.Getpid()); err == nil {
			process.Signal(syscall.SIGINT)
		}
	}()

	start := time.Now()
	err := Main(ctx, program, "test-app")
	duration := time.Since(start)

	assert.NoError(t, err)
	// 应该在信号发送后很快完成，而不是等待 3 秒
	assert.True(t, duration < 2*time.Second)
	mockApp.AssertExpectations(t)
}

// 基准测试
func BenchmarkMain_QuickStartStop(b *testing.B) {
	ctx := createTestContext()

	for i := 0; i < b.N; i++ {
		mockApp := new(MockApp)
		mockApp.On("Run").Return(nil)
		mockApp.On("Stop").Return(nil)

		program := func(opts *options.Options) (App, error) {
			return mockApp, nil
		}

		Main(ctx, program, "benchmark-app")
	}
}

// 辅助函数：创建一个会在指定时间后完成的 App
func createTimedApp(runDuration time.Duration, runErr error, stopDuration time.Duration, stopErr error) App {
	mockApp := new(MockApp)

	if runDuration > 0 {
		mockApp.On("Run").Run(func(args mock.Arguments) {
			time.Sleep(runDuration)
		}).Return(runErr)
	} else {
		mockApp.On("Run").Return(runErr)
	}

	if stopDuration > 0 {
		mockApp.On("Stop").Run(func(args mock.Arguments) {
			time.Sleep(stopDuration)
		}).Return(stopErr)
	} else {
		mockApp.On("Stop").Return(stopErr)
	}

	return mockApp
}

func TestMain_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		app         App
		expectError bool
	}{
		{
			name:        "immediate_completion",
			app:         createTimedApp(0, nil, 0, nil),
			expectError: false,
		},
		{
			name:        "run_error_with_stop_error",
			app:         createTimedApp(0, errors.New("run error"), 0, errors.New("stop error")),
			expectError: true,
		},
		{
			name:        "only_run_error",
			app:         createTimedApp(0, errors.New("run error"), 0, nil),
			expectError: true,
		},
		{
			name:        "only_stop_error",
			app:         createTimedApp(0, nil, 0, errors.New("stop error")),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := createTestContext()

			program := func(opts *options.Options) (App, error) {
				return tt.app, nil
			}

			err := Main(ctx, program, "test-app")

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// 测试 options 验证失败的情况
// 注意：这个测试需要根据实际的 options.New 实现来调整
func TestMain_OptionsValidation(t *testing.T) {
	// 如果你能够 mock options.New 函数，可以这样测试：
	/*
		ctx := createTestContext()

		// 假设有方法可以注入 mock options
		program := func(opts *options.Options) (App, error) {
			return nil, errors.New("should not reach here")
		}

		// 这里需要根据实际情况来模拟 options 验证失败
		err := Main(ctx, program, "test-app")
		assert.Error(t, err)
	*/

	t.Skip("需要根据实际的 options.New 实现来调整此测试")
}

// 测试运行器 - 可以用来运行特定的测试组
func TestMain_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试在短测试模式下")
	}

	// 这里可以放置需要更长时间运行的集成测试
	t.Run("signal_handling", TestMain_SignalHandling)
	t.Run("long_running_app", TestMain_LongRunningApp)
}

// 如果需要测试特定的 CLI 参数，可以这样创建 context
func createTestContextWithArgs(args []string) *cli.Context {
	app := cli.NewApp()
	app.Name = "test-app"

	// 如果你的应用有特定的 flags，在这里定义
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config",
			Usage: "config file path",
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug mode",
		},
	}

	set := flag.NewFlagSet("test", flag.ContinueOnError)

	// 解析参数到 flagset
	for _, f := range app.Flags {
		f.Apply(set)
	}

	if len(args) > 0 {
		set.Parse(args)
	}

	return cli.NewContext(app, set, nil)
}

// 使用特定参数的测试示例
func TestMain_WithSpecificArgs(t *testing.T) {
	ctx := createTestContextWithArgs([]string{"--debug", "--config", "/tmp/test.conf"})
	mockApp := new(MockApp)

	mockApp.On("Run").Return(nil)
	mockApp.On("Stop").Return(nil)

	program := func(opts *options.Options) (App, error) {
		// 这里可以验证 opts 是否正确解析了参数
		return mockApp, nil
	}

	err := Main(ctx, program, "test-app")
	assert.NoError(t, err)
	mockApp.AssertExpectations(t)
}
