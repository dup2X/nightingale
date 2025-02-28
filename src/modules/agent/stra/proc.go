package stra

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/toolkits/pkg/file"
	"github.com/toolkits/pkg/logger"

	"github.com/dup2X/nightingale/src/models"
	"github.com/dup2X/nightingale/src/modules/agent/config"
	"github.com/dup2X/nightingale/src/toolkits/str"
)

func NewProcCollect(method, name, tags string, step int, modTime time.Time) *models.ProcCollect {
	return &models.ProcCollect{
		CollectType:   "proc",
		CollectMethod: method,
		Target:        name,
		Step:          step,
		Tags:          tags,
		LastUpdated:   modTime,
	}
}

func GetProcCollects() map[string]*models.ProcCollect {
	procs := make(map[string]*models.ProcCollect)

	if config.Config.Stra.Enable {
		procs = Collect.GetProcs()
		for _, p := range procs {
			tagsMap := str.DictedTagstring(p.Tags)
			tagsMap["target"] = p.Target
			p.Tags = str.SortedTags(tagsMap)
		}
	}

	procPath := config.Config.Stra.ProcPath
	files, err := file.FilesUnder(procPath)
	if err != nil {
		logger.Error(err)
		return procs
	}

	//扫描文件采集配置
	for _, f := range files {
		method, name, step, err := parseProcName(f)
		if err != nil {
			logger.Warning(err)
			continue
		}

		filePath := filepath.Join(procPath, f)
		service, err := file.ToTrimString(filePath)
		if err != nil {
			logger.Warning(err)
			continue
		}

		info, err := os.Stat(filePath)
		if err != nil {
			logger.Warning(err)
			continue
		}

		tags := fmt.Sprintf("target=%s,service=%s", name, service)
		p := NewProcCollect(method, name, tags, step, info.ModTime())
		procs[name] = p
	}

	return procs
}

func parseProcName(fname string) (method string, name string, step int, err error) {
	arr := strings.Split(fname, "_")
	if len(arr) < 3 {
		err = fmt.Errorf("name is illegal %s, split _ < 3", fname)
		return
	}

	step, err = strconv.Atoi(arr[0])
	if err != nil {
		err = fmt.Errorf("name is illegal %s %v", fname, err)
		return
	}

	method = arr[1]

	name = strings.Join(arr[2:], "_")
	return
}
