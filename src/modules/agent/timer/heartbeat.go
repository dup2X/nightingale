package timer

import (
	"math/rand"
	"time"

	"github.com/toolkits/pkg/logger"

	"github.com/dup2X/nightingale/src/common/dataobj"
	"github.com/dup2X/nightingale/src/modules/agent/client"
	"github.com/dup2X/nightingale/src/modules/agent/config"
)

func Heartbeat() {
	d := rand.Intn(2000)
	logger.Infof("sleep %dms then heartbeat", d)
	time.Sleep(time.Duration(d) * time.Millisecond)

	interval := time.Duration(config.Config.Job.Interval) * time.Second

	for {
		heartbeat()
		time.Sleep(interval)
	}
}

func heartbeat() {
	ident := config.Endpoint

	req := dataobj.ReportRequest{
		Ident:       ident,
		ReportTasks: Locals.ReportTasks(),
	}

	var resp dataobj.ReportResponse
	err := client.GetCli().Call("Scheduler.Report", req, &resp)
	if err != nil {
		logger.Error("rpc call Scheduler.Report fail:", err)
		client.CloseCli()
		return
	}

	if resp.Message != "" {
		logger.Errorf("error from server:", resp.Message)
		return
	}

	assigned := make(map[int64]struct{})

	if resp.AssignTasks != nil {
		count := len(resp.AssignTasks)
		for i := 0; i < count; i++ {
			at := resp.AssignTasks[i]
			assigned[at.Id] = struct{}{}
			Locals.AssignTask(at)
		}
	}

	logger.Debug("assigned tasks:", assigned)

	Locals.Clean(assigned)
}
