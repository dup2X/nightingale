package funcs

import (
	"github.com/dup2X/nightingale/src/common/dataobj"
	"github.com/dup2X/nightingale/src/modules/agent/core"
)

func CollectorMetrics() []*dataobj.MetricValue {
	return []*dataobj.MetricValue{
		core.GaugeValue("proc.agent.alive", 1),
	}
}
