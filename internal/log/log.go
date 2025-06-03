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
	var writeSyncers []zapcore.WriteSyncer

	// 处理文件输出
	if config.Filename == "" {
		// 如果文件名为空，使用/dev/stderr
		stderrSyncer := zapcore.AddSync(os.Stderr)
		writeSyncers = append(writeSyncers, stderrSyncer)
	} else {
		// 否则使用指定的文件
		fileWriter := getLogWriter(config)
		writeSyncers = append(writeSyncers, fileWriter)

		// 只有当输出到实际文件时，才考虑额外的控制台输出
		if config.Console {
			consoleSyncer := zapcore.AddSync(os.Stdout)
			writeSyncers = append(writeSyncers, consoleSyncer)
		}
	}

	// 合并所有输出
	multiWriteSyncer := zapcore.NewMultiWriteSyncer(writeSyncers...)

	encoder := getEncoder()
	core := zapcore.NewCore(encoder, multiWriteSyncer, config.Level)
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
