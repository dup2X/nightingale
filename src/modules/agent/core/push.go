package core

import (
	"fmt"
	"time"

	"github.com/toolkits/pkg/logger"

	"github.com/dup2X/nightingale/src/common/dataobj"
	"github.com/dup2X/nightingale/src/modules/agent/cache"
	"github.com/dup2X/nightingale/src/modules/agent/config"
)

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
	}

	retry := 0
	for len(metricItems) > 0 {
		if retry >= 3 {
			break
		}
		_, err := rpcCall()
		if err != nil {
			logger.Error(err)
			retry += 1
			time.Sleep(time.Millisecond * 500)
			continue
		}
		return nil
	}

	return err
}

func rpcCall() (dataobj.TransferResp, error) {
	var reply dataobj.TransferResp
	var err error
	if true {
		return reply, err
	}

	timeout := time.Duration(8) * time.Second
	done := make(chan error, 1)

	go func() {
		done <- err
	}()

	select {
	case <-time.After(timeout):
		logger.Warningf("rpc call timeout, transfer addr\n")
		return reply, fmt.Errorf("rpc call timeout")
	case err := <-done:
		if err != nil {
			return reply, fmt.Errorf("rpc call done, but fail: %v", err)
		}
	}

	return reply, nil
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
