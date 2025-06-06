package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger 全局日志对象
var Logger *zap.SugaredLogger

// LogConfig 日志配置
type LogConfig struct {
	Filename   string        // 日志文件路径，为空时使用/dev/stderr
	MaxSize    int           // 单个日志文件最大大小，单位MB
	MaxBackups int           // 最大保留的旧日志文件数量
	MaxAge     int           // 旧日志文件保留的最大天数
	Compress   bool          // 是否压缩旧日志文件
	Level      zapcore.Level // 日志级别
	Console    bool          // 是否同时输出到控制台
}

// DefaultLogConfig 返回默认日志配置
func DefaultLogConfig() LogConfig {
	return LogConfig{
		Filename:   "app.log",
		MaxSize:    10,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
		Level:      zapcore.InfoLevel,
		Console:    true,
	}
}

// InitLogger 初始化日志系统
func InitLogger(config LogConfig) {
	// 创建编码器
	encoder := getEncoder()

	// 定义日志级别过滤器
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})

	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel && lvl >= config.Level
	})

	// 创建核心组件集合
	var cores []zapcore.Core

	// 处理文件输出
	if config.Filename != "" {

		fileWriter := getLogWriter(config)
		// 文件同时接收所有级别日志
		fileCore := zapcore.NewCore(encoder, fileWriter, zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= config.Level
		}))

		cores = append(cores, fileCore)
		config.Console = false
	}

	// 处理控制台输出
	if config.Console {
		// 标准输出处理低优先级日志
		stdoutSyncer := zapcore.AddSync(os.Stdout)
		stdoutCore := zapcore.NewCore(encoder, stdoutSyncer, lowPriority)

		// 标准错误处理高优先级日志
		stderrSyncer := zapcore.AddSync(os.Stderr)
		stderrCore := zapcore.NewCore(encoder, stderrSyncer, highPriority)

		cores = append(cores, stdoutCore, stderrCore)
	}

	// 组合所有core
	core := zapcore.NewTee(cores...)

	// 创建logger
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	Logger = logger.Sugar()
}

// 简化初始化，使用默认配置
func Init(filename, level string) {
	config := DefaultLogConfig()
	config.Filename = filename
	l, err := zapcore.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	config.Level = l
	InitLogger(config)
}

// Close 关闭日志，确保所有日志都被写入
func Close() {
	if Logger != nil {
		_ = Logger.Sync()
	}
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(config LogConfig) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   config.Filename,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
		Compress:   config.Compress,
	}
	return zapcore.AddSync(lumberJackLogger)
}
