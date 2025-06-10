package monitor

import (
	"runtime"
	"runtime/debug"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
)

func SmartMonitor(base int, interval time.Duration, gracefulRestart func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		lastGCTime   time.Time
		highMemCount int
		lastMemStats runtime.MemStats
	)

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// 计算内存增长率
		memGrowthRate := float64(m.Alloc-lastMemStats.Alloc) / float64(lastMemStats.Alloc)
		lastMemStats = m

		currentMemMB := m.Alloc / types.MB

		// 分级处理
		switch {
		case m.Alloc > uint64(float32(base)*0.9): // > 90% - 危险区域
			log.Logger.Infof("CRITICAL: Memory usage %d MB (growth: %.2f%%)",
				currentMemMB, memGrowthRate*100)

			// 立即GC并考虑重启
			runtime.GC()
			debug.FreeOSMemory()

			// 连续3次超过危险线就重启
			highMemCount++
			if highMemCount >= 3 {
				log.Logger.Info("Memory consistently high, initiating restart")
				if gr := gracefulRestart; gr != nil {
					gr()
				}
			}

		case m.Alloc > uint64(float32(base)*0.7): // 1.5GB - 警告区域
			log.Logger.Infof("WARNING: Memory usage %d MB (growth: %.2f%%)",
				currentMemMB, memGrowthRate*100)

			// 适度GC
			if time.Since(lastGCTime) > 30*time.Second {
				runtime.GC()
				lastGCTime = time.Now()
			}

		case m.Alloc > uint64(float32(base)*0.5): // 1.2GB - 注意区域
			log.Logger.Infof("INFO: Memory usage %d MB (growth: %.2f%%)",
				currentMemMB, memGrowthRate*100)

			// 重置高内存计数器
			highMemCount = 0

		default:
			// 内存正常，重置计数器
			highMemCount = 0
		}

		// 记录详细的内存统计
		if m.Alloc > types.GB { // 1GB以上记录详细信息
			logDetailedMemStats(m)
		}
	}
}

func logDetailedMemStats(m runtime.MemStats) {
	log.Logger.Infof("Memory Stats - Alloc: %d MB, Sys: %d MB, NumGC: %d, GCCPUFraction: %.4f",
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC,
		m.GCCPUFraction,
	)
}
