package init

import (
	"runtime/debug"
	"time"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
)

func init() {
	// 设置更积极的 GC
	debug.SetGCPercent(20)

	// 限制内存使用
	debug.SetMemoryLimit(4 * types.GB)
}
