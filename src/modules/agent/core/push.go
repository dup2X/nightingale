package core

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/toolkits/pkg/logger"

	"github.com/dup2X/nightingale/src/common/dataobj"
	"github.com/dup2X/nightingale/src/modules/agent/cache"
	"github.com/dup2X/nightingale/src/modules/agent/config"
)

var (
	gau     *prometheus.GaugeVec
	gPusher *push.Pusher
)

func init() {
	var prometheusReg = prometheus.NewRegistry()
	gau = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "demo",
		Name:      "common",
		Help:      "common prometheus gauge",
	}, []string{"host", "service", "metric"})
	prometheusReg.Register(gau)
	gPusher = push.New("http://127.0.0.1:9091", "demo-xxxx")
	gPusher.Gatherer(prometheusReg)
}

func Push(metricItems []*dataobj.MetricValue) error {
	var err error
	now := time.Now().Unix()

	for _, item := range metricItems {
		logger.Debugf("->recv:%+v", item)
		if item.Endpoint == "" {
			item.Endpoint = config.Endpoint
		}
		err = item.CheckValidity(now)
		if err != nil {
			msg := fmt.Errorf("metric:%v err:%v", item, err)
			logger.Warning(msg)
			// 如果数据有问题，直接跳过吧，比如mymon采集的到的数据，其实只有一个有问题，剩下的都没问题
			continue
		}
		gau.WithLabelValues(item.Endpoint, item.Metric).Set(item.Value)
	}
	err = gPusher.Push()
	return err
}

func CounterToGauge(item *dataobj.MetricValue) *dataobj.MetricValue {
	key := item.PK()

	old, exists := cache.MetricHistory.Get(key)
	cache.MetricHistory.Set(key, *item)

	if !exists {
		logger.Debugf("not found old item:%v, maybe this is the first item", item)
		return nil
	}

	if old.Value > item.Value {
		logger.Warningf("item:%v old value:%v greater than new value:%v", item, old.Value, item.Value)
		return nil
	}

	if old.Timestamp >= item.Timestamp {
		logger.Warningf("item:%v old timestamp:%v greater than new timestamp:%v", item, old.Timestamp, item.Timestamp)
		return nil
	}

	item.ValueUntyped = (item.Value - old.Value) / float64(item.Timestamp-old.Timestamp)
	item.CounterType = dataobj.GAUGE
	return item
}

func SubtractToGauge(item *dataobj.MetricValue) *dataobj.MetricValue {
	key := item.PK()

	old, exists := cache.MetricHistory.Get(key)
	cache.MetricHistory.Set(key, *item)

	if !exists {
		logger.Debugf("not found old item:%v, maybe this is the first item", item)
		return nil
	}

	if old.Timestamp >= item.Timestamp {
		logger.Warningf("item:%v old timestamp:%v greater than new timestamp:%v", item, old.Timestamp, item.Timestamp)
		return nil
	}

	if old.Timestamp <= item.Timestamp-2*item.Step {
		logger.Warningf("item:%v old timestamp:%v too old <= %v = (new timestamp: %v - 2 * step: %v), maybe some point lost", item, old.Timestamp, item.Timestamp-2*item.Step, item.Timestamp, item.Step)
		return nil
	}

	item.ValueUntyped = item.Value - old.Value
	item.CounterType = dataobj.GAUGE
	return item
}
