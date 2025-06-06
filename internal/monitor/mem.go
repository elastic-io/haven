package monitor

import (
	"fmt"
	"runtime"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
)

func MemoryUsage() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		log.Logger.Info(fmt.Sprintf("Memory usage: Alloc=%v MiB, TotalAlloc=%v MiB, Sys=%v MiB, NumGC=%v",
			m.Alloc/types.MB,
			m.TotalAlloc/types.MB,
			m.Sys/types.MB,
			m.NumGC))

		// 如果内存使用过高，触发 GC
		if m.Alloc > 2*types.GB { // 2GB
			log.Logger.Warn("High memory usage detected, triggering GC")
			runtime.GC()
		}
	}
}
